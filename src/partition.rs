use std::sync::atomic::Ordering;
use std::sync::atomic::AtomicUsize;
use std::collections::{HashMap, HashSet};
use anyhow::{Error, Result};
use dashmap::DashMap;
use std::{
    fs::{create_dir_all, File, OpenOptions},
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


#[derive(Debug, Serialize, Deserialize)]
struct PartitionConfig {
	name: String,
	partition_key: String,
	choices: Vec<String>,
	#[serde(default="default_max_file_size")]
	max_file_size: usize,
}


fn default_max_file_size() -> usize {
	256_000_000
}




pub fn partition(input_dir: &PathBuf, output_dir: &PathBuf, config_path: &PathBuf) -> Result<(), Error> {
	let start_main = Instant::now();
	println!("Starting partition operation");
	let input_paths = expand_dirs(vec![input_dir.clone()], None).unwrap();
	let config_contents = read_pathbuf_to_mem(config_path).unwrap();
	let config: PartitionConfig = serde_yaml::from_reader(config_contents).unwrap();


	let writer = GenWriter::new(output_dir, &config.choices, config.max_file_size);
	let global_counts: DashMap<Option<String>, AtomicUsize> = DashMap::new();
	let pbar = build_pbar(input_paths.len(), "Paths");
	input_paths.par_iter().for_each(|p| {

		let local_counts = partition_single_path(p, &config, &writer).unwrap();
		local_counts.into_iter().for_each(|(k, v)| {
		    global_counts.entry(k).or_insert_with(|| AtomicUsize::new(0)).fetch_add(v, Ordering::Relaxed);
		});
		pbar.inc(1);
	});
	writer.finish().unwrap();
	println!("Finished partition in {:?} secs", start_main.elapsed().as_secs());
	let global_counts: HashMap<Option<String>, usize> = global_counts
		.into_par_iter()
		.map(|(k,v)| {
			(k, v.into_inner())
		}).collect();
	let total_values: usize = global_counts.iter().map(|(_k,v)| *v).sum();
	println!("Saw {:?} documents...", total_values);
	global_counts.into_iter().for_each(|(k,v)| {
		let printkey: String = if k.is_none() {
			String::from("None")
		} else {
			k.unwrap()
		};
		println!("Saw {:?} documents with type {:?}", v, printkey);
	});

	Ok(())
}


fn partition_single_path(path: &PathBuf, config: &PartitionConfig, writer: &GenWriter) -> Result<HashMap<Option<String>, usize>, Error> {

	let contents = read_pathbuf_to_mem(path).unwrap();
	let mut partitioned_bytes: HashMap<Option<String>, Vec<u8>> = HashMap::new();
	let mut counts: HashMap<Option<String>, usize> = HashMap::new();
	for line in contents.lines() {
		let line = line.unwrap();
		let json_value = serde_json::from_str(&line).unwrap();
		let partition_value = json_get(&json_value, &config.partition_key).unwrap().as_str().unwrap().to_string();
		let key: &Option<String> = if writer.full_choices.contains(&Some(partition_value.clone())) {
			&Some(partition_value)
		} else {
			&None
		};
		let append_vec = partitioned_bytes.entry(key.clone()).or_default();
		*counts.entry(key.clone()).or_insert(0) += 1;
		append_vec.extend(line.as_bytes());
		append_vec.push(b'\n');
	}
	partitioned_bytes.into_iter().for_each(|(key, val)| {
		writer.write_contents(&key, val).unwrap();
	});

	Ok(counts)	
}




/*==========================================================
=                        GEN WRITER STUFF                  =
==========================================================*/

pub struct GenWriter<'a> {
	pub writer: DashMap<Option<String>, Arc<Mutex<WriterInfo<'a>>>>,
	#[allow(dead_code)]
	storage_loc: PathBuf,	
	full_choices: HashSet<Option<String>>,
	max_len: usize
}

pub struct WriterInfo<'a> {
	encoder: Option<Encoder<'a, File>>,
	bytes_written: usize,
	file_idx: usize,
}
	

impl<'a> GenWriter<'a> {
	pub fn new(storage_loc: &PathBuf, choices: &Vec<String>, max_len: usize) -> Self {
		let writer : DashMap<Option<String>, Arc<Mutex<WriterInfo<'a>>>> = DashMap::new();
		// Create writers
		let mut full_choices: HashSet<Option<String>> = choices.iter().map(|c| Some(c.clone())).collect::<HashSet<Option<String>>>();
		full_choices.insert(None);
		println!("Opening {:?} writer files", full_choices.len());

		/*

		for choice in full_choices {
			let filename = GenWriter::get_filename(storage_loc, &choice, 0);
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
            };
			writer.insert(choice, Arc::new(Mutex::new(writer_info)));
		}
		*/
		GenWriter { writer, storage_loc: storage_loc.clone(), full_choices, max_len}
	}


	pub fn get_filename(storage_loc: &PathBuf, choice: &Option<String>, file_idx: usize) -> PathBuf {

		if choice.is_none() {
			storage_loc.clone().join(format!("no_category.{:08}.jsonl.zst", file_idx))
		} else {
			storage_loc.clone()
				.join(format!("chunk_{:}.{:08}.jsonl.zst", choice.clone().unwrap(), file_idx))
		}
	}

    fn create_new_encoder(&self, key: &Option<String>, file_idx: usize) -> Encoder<'a, File> {

    	let new_filename = if key.is_none() {
        	GenWriter::get_filename(&self.storage_loc, &key, file_idx)    		
    	} else {
    	    GenWriter::get_filename(&self.storage_loc, &key, file_idx)	
    	};

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



    pub fn write_contents(&self, key: &Option<String>, contents: Vec<u8>) -> Result<(), Error> {
        // Get or create the writer for this key
        let writer_arc = self.writer.entry(key.clone()).or_insert_with(|| {

            let filename = GenWriter::get_filename(&self.storage_loc, key, 0);
            if let Some(parent_dir) = filename.parent() {
                if !parent_dir.exists() {
                    create_dir_all(parent_dir).unwrap()
                }
            }
            let writer_info = WriterInfo {
                encoder: Some(self.create_new_encoder(key, 0)),
                bytes_written: 0,
                file_idx: 0,
            };
            Arc::new(Mutex::new(writer_info))
        });

        let mut writer_info = writer_arc.lock().unwrap();
        writer_info.bytes_written += contents.len();

        if writer_info.encoder.is_none() {
        	writer_info.encoder = Some(self.create_new_encoder(key, writer_info.file_idx));
        }



		if let Some(encoder) = &mut writer_info.encoder {
			encoder.write_all(&contents).unwrap();
			if writer_info.bytes_written >= self.max_len {
				let mut old_encoder = writer_info.encoder.take().unwrap();
				old_encoder.flush().unwrap();
				old_encoder.finish().unwrap();
				writer_info.file_idx += 1;
				writer_info.encoder = None;
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
