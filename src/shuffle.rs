use std::sync::atomic::{Ordering, AtomicUsize};
use anyhow::{Error, Result};
use dashmap::DashMap;
use std::{
	fs,
    fs::{create_dir_all, File, OpenOptions},
    io::{Write, BufRead},
    os::unix::fs::OpenOptionsExt,
    path::PathBuf,
    sync::{Arc, Mutex},
    time::Instant,
};
use rayon::prelude::*;
use mj_io::{expand_dirs, read_pathbuf_to_mem, build_pbar};
use zstd::stream::Encoder;
 
use fastrand;


pub fn shuffle(input_dir: &PathBuf, output_dir: &PathBuf, num_outputs: usize, max_len: usize,  delete_after_read: bool) -> Result<(), Error> {
	println!("Starting shuffle");
	let start_main = Instant::now();
	let subext = "shuffled";

	let gen_writer = GenWriter::new(output_dir, num_outputs, &subext, max_len);

	let input_paths = expand_dirs(vec![input_dir.clone()], None).unwrap();
	let total_docs_seen = AtomicUsize::new(0);
	let pbar = build_pbar(input_paths.len(), "Paths");
	input_paths.into_par_iter().for_each(|p| {
		let mut seen_docs = 0;
		let contents = read_pathbuf_to_mem(&p).unwrap();
		for line in contents.lines() {
			let line = line.unwrap();
			let mut line_bytes = line.into_bytes();
			line_bytes.push(b'\n');
			let chunk_num = fastrand::usize(0..usize::MAX) % num_outputs;
			gen_writer.write_line(chunk_num, &line_bytes).unwrap();
			seen_docs += 1;
		}
		total_docs_seen.fetch_add(seen_docs, Ordering::SeqCst);
		if delete_after_read {
			fs::remove_file(&p).unwrap();
		}
		
		pbar.inc(1);
	});

	gen_writer.finish().unwrap();


	let total_output_docs = expand_dirs(vec![output_dir.clone()], None).unwrap().len();

	println!("Shuffled {:?} docs into {:?} new files in {:?} seconds", total_docs_seen.into_inner(), total_output_docs, start_main.elapsed().as_secs());

	Ok(())
}


/*==========================================================
=                        GEN WRITER STUFF                  =
==========================================================*/
#[allow(dead_code)]
pub struct GenWriter<'a> {
	pub writer: DashMap<usize, Arc<Mutex<WriterInfo<'a>>>>,
	#[allow(dead_code)]
	storage_loc: PathBuf,	
	num_chunks: usize,
	max_len: usize
}

pub struct WriterInfo<'a> {
	encoder: Option<Encoder<'a, File>>,
	bytes_written: usize,
	file_idx: usize,
	subext: String,
}
	

impl<'a> GenWriter<'a> {
	pub fn new(storage_loc: &PathBuf, num_chunks: usize, subext: &str, max_len: usize) -> Self {
		let writer : DashMap<usize, Arc<Mutex<WriterInfo<'a>>>> = DashMap::new();
		// Create writers
		println!("Opening {:?} writer files", num_chunks);
		for chunk in 0..num_chunks {
			let filename = GenWriter::get_filename(storage_loc, chunk, 0, subext);
			if let Some(parent_dir) = filename.parent() {
		        if !parent_dir.exists() {
		            create_dir_all(parent_dir).unwrap()
		         }
		    }		    
            let writer_info = WriterInfo {
                encoder: Some(Encoder::new(
                    OpenOptions::new()
                    .append(true)
                    .create(true)
                    .mode(0o644)
                    .open(filename)
                    .unwrap(),
                3).unwrap()),
                bytes_written: 0,
                file_idx: 0,
                subext: subext.to_string(),
            };
			writer.insert(chunk, Arc::new(Mutex::new(writer_info)));
		}
		GenWriter { writer, storage_loc: storage_loc.clone(), num_chunks, max_len }
	}


	pub fn get_filename(storage_loc: &PathBuf, chunk: usize, file_idx: usize, subext: &str) -> PathBuf {
		storage_loc.clone()
			.join(format!("chunk_{:08}.{:08}.{}.jsonl.zst", chunk, file_idx, subext))
	}

    fn create_new_encoder(&self, key: usize, file_idx: usize, subext: &str) -> Encoder<'a, File> {
        let new_filename = GenWriter::get_filename(&self.storage_loc, key, file_idx, subext);
        if let Some(parent_dir) = new_filename.parent() {
            if !parent_dir.exists() {
                create_dir_all(parent_dir).unwrap()
            }
        }
        
        Encoder::new(
            OpenOptions::new()
            .append(true)
            .create(true)
            .mode(0o644)
            .open(new_filename)
            .unwrap(),
        3).unwrap()
    }	

    pub fn write_batch(&self, key: usize, contents: Vec<u8>) -> Result<(), Error> {
        let binding = self.writer.get(&key).unwrap();
        let mut writer_info = binding.lock().unwrap();
        
        writer_info.bytes_written += contents.len();
        
        if let Some(encoder) = &mut writer_info.encoder {
            encoder.write_all(&contents)?;
            
            // Handle file rotation if needed
            if writer_info.bytes_written >= self.max_len {
                let mut old_encoder = writer_info.encoder.take().unwrap();
                old_encoder.flush()?;
                old_encoder.finish()?;
                writer_info.file_idx += 1;
                let new_encoder = self.create_new_encoder(key, writer_info.file_idx, &writer_info.subext);
                writer_info.encoder = Some(new_encoder);
                writer_info.bytes_written = 0;
            }
        }
        
        Ok(())
    }


	pub fn write_line(&self, key: usize, contents: &Vec<u8>) -> Result<(), Error> {
		// hash the key and take mod num_chunks to get location

		let binding = self.writer.get(&key).unwrap();
		let mut writer_info = binding.lock().unwrap();
		writer_info.bytes_written += contents.len();		
		if let Some(encoder) = &mut writer_info.encoder {
			encoder.write_all(contents).unwrap();
			if writer_info.bytes_written >= self.max_len {
				let mut old_encoder = writer_info.encoder.take().unwrap();
				old_encoder.flush().unwrap();
				old_encoder.finish().unwrap();
				writer_info.file_idx += 1;
				let new_encoder = self.create_new_encoder(key, writer_info.file_idx, &writer_info.subext);
				writer_info.encoder = Some(new_encoder);
				writer_info.bytes_written = 0;
			}
		}
		
		Ok(())

	}

	pub fn finish(self) -> Result<(), Error> {
		// Flushes all the open writers
		self.writer.into_par_iter()
			.for_each(|(_, value)| {
				match Arc::try_unwrap(value) {
					Ok(mutex) => {
						let mut writer_info = mutex.into_inner().unwrap();
						if writer_info.bytes_written > 0 {
							let mut encoder = writer_info.encoder.take().unwrap();
							encoder.flush().unwrap();
							encoder.finish().unwrap();
						}
					},
					_ => panic!("WHAT?")
				}
		});
		Ok(())
	}
}
