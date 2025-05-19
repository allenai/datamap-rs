use anyhow::{Error, Result};
use dashmap::DashMap;
use std::{
    fs::{create_dir_all, File, OpenOptions},
    hash::{DefaultHasher, Hash, Hasher},
    io::{Write, BufRead},
    os::unix::fs::OpenOptionsExt,
    path::PathBuf,
    sync::{Arc, Mutex},
    time::Instant,
};
use serde_json;
use rayon::prelude::*;
use crate::utils::json_get;
use mj_io::{expand_dirs, read_pathbuf_to_mem, build_pbar};
use zstd::stream::Encoder;
use serde::{Deserialize, Serialize};



/*
Multinode grouping and sorting

Proceeds in phases:
- first reorganizes multiple files in directories based on hashes of their "group" values
- then loads all of these chunks and sorts according to [group, sort] keys

*/

#[derive(Debug, Serialize, Deserialize)]
struct GroupsortConfig {
	name: String,
	group_keys: Vec<String>,
	sort_keys: Vec<String>,
	num_buckets: usize,
	#[serde(default="default_max_file_size")]
	max_file_size: usize

}


fn default_max_file_size() -> usize {
	256_000_000
}



/*============================================================
=                            GROUP SORT STUFF                =
============================================================*/

pub fn distributed_group(input_dir: &PathBuf, group_dir: &PathBuf, config_path: &PathBuf) -> Result<(), Error> {

	let start_main = Instant::now();
	println!("Starting group operation");	
	let input_paths = expand_dirs(vec![input_dir.clone()], None).unwrap();
	let config_contents = read_pathbuf_to_mem(config_path).unwrap();
	let config: GroupsortConfig = serde_yaml::from_reader(config_contents).unwrap();
	let num_buckets = config.num_buckets;
	let writer = GenWriter::new(group_dir, num_buckets, "group", config.max_file_size);
	let pbar = build_pbar(input_paths.len(), "Paths");
	input_paths.par_iter().for_each(|p| {
		group_path(p, &config.group_keys, &writer).unwrap();
		pbar.inc(1);
	});

	println!("Finished group op in {:?} secs", start_main.elapsed().as_secs());

	Ok(())
}


fn group_path(path: &PathBuf, group_keys: &Vec<String>, writer: &GenWriter) -> Result<(), Error> {
	let num_chunks = writer.num_chunks;
	let contents = read_pathbuf_to_mem(path).unwrap();
	for line in contents.lines() {
		let line = line.unwrap();
		let value : serde_json::Value = serde_json::from_str(&line).unwrap();

		let mut hasher = DefaultHasher::new();
		for k in group_keys {
			let group_val = json_get(&value, &k).unwrap();
			let group_val_string = group_val.to_string();
			group_val_string.hash(&mut hasher)
		}
		let hash_val = hasher.finish() as usize % num_chunks;
		writer.write_line(hash_val, line.into()).unwrap();
	}


	Ok(())
}




/*==========================================================
=                        GEN WRITER STUFF                  =
==========================================================*/

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


	pub fn write_line(&self, key: usize, contents: Vec<u8>) -> Result<(), Error> {
		// hash the key and take mod num_chunks to get location

		let binding = self.writer.get(&key).unwrap();
		let mut writer_info = binding.lock().unwrap();
		writer_info.bytes_written += contents.len();		
		if let Some(encoder) = &mut writer_info.encoder {
			encoder.write_all(&contents).unwrap();
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


