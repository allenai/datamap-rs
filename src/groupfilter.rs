use std::hash::BuildHasher;
use ahash::RandomState;
use std::sync::atomic;
use serde_json::Value;
use std::sync::atomic::AtomicUsize;
use anyhow::{Error, Result};
use dashmap::DashMap;
use std::{
    fs::{create_dir_all, File, OpenOptions, remove_file},
    hash::{Hash, Hasher},
    io::{Write, BufRead},
    os::unix::fs::OpenOptionsExt,
    path::PathBuf,
    sync::{Arc, Mutex},
    time::Instant,
};
use serde_json;
use rayon::prelude::*;
use crate::utils::json_get;
use mj_io::{expand_dirs, read_pathbuf_to_mem, build_pbar, write_mem_to_pathbuf, get_output_filename};
use zstd::stream::Encoder;
use serde::{Deserialize, Serialize};
use ahash::AHasher; 
use sonic_rs::{JsonValueTrait, Value as SonicValue};
use fastrand;


/*
Massively parallelizable grouping and filtering.
This assumes that either the dataset is too big to fit on memory or even disk.

There are two modes of operation:
  - Simple grouping:
  	You have a dataset that you want to group along
  	The output will be a mixed-up version of the dataset where all groups are 
  	Example use case: you have a dataset with a groupID and you want all documents with the same groupID to live in the same jsonl file
  - Groupfilter:
  	You have a dataset that has groupkeys and you want to keep exactly one document per group
  	e.g., You have a "document hash" key and you want to keep exactly one copy of each document. This assumes the data has previously been grouped


Canonical use case:
You just ran fuzzy deduplication and annotated your dataset with the "fuzzy duplicate ID". But now you want to do something fancy like
keeping all the duplicates within a group together, or just want to keep the duplicate that is most-recent according to a date field.

The recipe to do this would be to:
1) First group all of the data according to the groupID (do this in slices of data across many nodes if big)
2) Then filter the groups to keep only the most-recent 

*All of this will explode if certain sortkeys are "hot", in that there are too many elements with one sortkey.
 e.g., if you have 10^9 documents that have a shared sortkey, this will explode. Don't let that happen.
*/

#[derive(Debug, Serialize, Deserialize)]
struct GroupFilterConfig {
	name: String,
	group_keys: Vec<String>,
	sort_keys: Vec<Vec<String>>, // of the form [[sortkey1, backup_sortkey1, ...], [sortkey2, ...]]
	num_buckets: usize,
	#[serde(default="default_max_file_size")]
	max_file_size: usize,
	keep_idx: i32, // 0 means keep first, -1 means keep last
	size_key: Option<String>, // if present, add the size of this chunk to the doc we keep in the filter step 
	#[serde(default="default_delete_after_read")]
	delete_after_read: bool,
}


fn default_max_file_size() -> usize {
	256_000_000
}

fn default_delete_after_read() -> bool {
	false
}



/*============================================================
=                            GROUP STUFF                     =
============================================================*/

pub fn group(input_dir: &PathBuf, group_dir: &PathBuf, config_path: &PathBuf, subext: Option<String>) -> Result<(), Error> {

	let start_main = Instant::now();
	println!("Starting group operation");	
	let input_paths = expand_dirs(vec![input_dir.clone()], None).unwrap();
	let config_contents = read_pathbuf_to_mem(config_path).unwrap();
	let config: GroupFilterConfig = serde_yaml::from_reader(config_contents).unwrap();
	let num_buckets = config.num_buckets;
	let subext = if let Some(subext) = subext {
		subext
	} else {
		"group".to_string()
	};
	let writer = GenWriter::new(group_dir, num_buckets, &subext, config.max_file_size);
	let pbar = build_pbar(input_paths.len(), "Paths");
	input_paths.par_iter().for_each(|p| {
		group_path(p, &config.group_keys, &writer, &config.delete_after_read).unwrap();
		pbar.inc(1);
	});

	writer.finish().unwrap();
	println!("Finished group op in {:?} secs", start_main.elapsed().as_secs());

	Ok(())
}


fn group_path(path: &PathBuf, group_keys: &Vec<String>, writer: &GenWriter, delete_after_read: &bool) -> Result<(), Error> {
	let num_chunks = writer.num_chunks;
	let contents = read_pathbuf_to_mem(path).unwrap();
    let mut buckets: Vec<Vec<u8>> = vec![Vec::new(); num_chunks];

	for line in contents.lines() {
		let line = line.unwrap();
        let value: SonicValue = sonic_rs::from_str(&line).unwrap();

		let hash_val = if let Some(hash_val) = get_group_hash_sonic(&value, group_keys).unwrap() {
			hash_val
		} else {
			// missing group info, put in random shard 			
			fastrand::usize(0..usize::MAX)
		};

		let bucket_id = hash_val % num_chunks;
		buckets[bucket_id].extend_from_slice(line.as_bytes());
		buckets[bucket_id].push(b'\n');

	}
	for (bucket_id, contents) in buckets.into_iter().enumerate() {
		if !contents.is_empty() {
			writer.write_batch(bucket_id, contents).unwrap();
		}
	}
	if *delete_after_read {
        remove_file(path).unwrap();
	}
	Ok(())
}

fn get_group_hash_sonic(
    value: &sonic_rs::Value, 
    group_keys: &Vec<String>,
) -> Result<Option<usize>, Error> {
    let hash_builder = RandomState::with_seeds(1,2,3,4);
    let mut hasher = hash_builder.build_hasher();
    for k in group_keys {
        if let Some(group_val) = get_nested_value(value, k)? {
            // Use the JsonValueTrait methods instead of pattern matching
            if group_val.is_str() {
                group_val.as_str().unwrap().hash(&mut hasher);
            } else if group_val.is_number() {
                // Hash the string representation for consistency
                group_val.as_f64().unwrap().to_string().hash(&mut hasher);
            } else if group_val.is_boolean() {
                group_val.as_bool().unwrap().hash(&mut hasher);
            } else if group_val.is_null() {
                "null".hash(&mut hasher);
            } else if group_val.is_array() || group_val.is_object() {
                // For complex types, hash the JSON string representation
                group_val.to_string().hash(&mut hasher);
            }
        } else {
            return Ok(None);
        }
    }
    Ok(Some(hasher.finish() as usize))
}

fn get_nested_value<'a>(
    value: &'a sonic_rs::Value, 
    key_path: &str
) -> Result<Option<&'a sonic_rs::Value>, Error> {
    let keys: Vec<&str> = key_path.split('.').collect();
    let mut current = value;
    
    for key in keys {
        if current.is_object() {
            if let Some(next) = current.get(key) {
                current = next;
            } else {
                return Ok(None);
            }
        } else {
            return Ok(None);
        }
    }
    
    Ok(Some(current))
}
fn get_group_hash(value: &serde_json::Value, group_keys: &Vec<String>) -> Result<Option<usize>, Error> {
	let mut hasher = AHasher::default();
	for k in group_keys {
		if let Some(group_val) = json_get(value, &k) {
			let group_val_string = group_val.to_string();
			group_val_string.hash(&mut hasher)
		} else {
			return Ok(None);
		}
	}
	Ok(Some(hasher.finish() as usize))
}



pub fn group_filter(input_dir: &PathBuf, output_dir: &PathBuf, config_path: &PathBuf) -> Result<(), Error> {
	let start_main = Instant::now();
	println!("Starting filter operation");	
	let input_paths = expand_dirs(vec![input_dir.clone()], None).unwrap();
	let config_contents = read_pathbuf_to_mem(config_path).unwrap();
	let config: GroupFilterConfig = serde_yaml::from_reader(config_contents).unwrap();	
	let pbar = build_pbar(input_paths.len(), "Paths");

	let docs_seen = AtomicUsize::new(0);
	let docs_kept = AtomicUsize::new(0);

	input_paths.into_par_iter().for_each(|p| {
		let output_path = get_output_filename(&p, input_dir, output_dir).unwrap();	
		let (path_seen, path_kept) = group_filter_path(&p, &output_path, &config).unwrap();
		docs_seen.fetch_add(path_seen, atomic::Ordering::SeqCst);
		docs_kept.fetch_add(path_kept, atomic::Ordering::SeqCst);
		pbar.inc(1);
	});

	println!("Finished filtering in {:?} secs", start_main.elapsed().as_secs());
	println!("Saw {:?} docs", docs_seen.into_inner());
	println!("Kept {:?} docs", docs_kept.into_inner());
	Ok(())
}



fn group_filter_path(input_path: &PathBuf, output_path: &PathBuf, config: &GroupFilterConfig) -> Result<(usize, usize), Error> {
	let mut docs_seen = 0;
	let mut docs_kept = 0;
	let contents = read_pathbuf_to_mem(input_path).unwrap();
	let keep_idx = config.keep_idx;
	let mut prev_hash : Option<usize> = None;
	let mut prev_line : Option<String> = None;
	
	let mut output_bytes: Vec<u8> = Vec::new();
	for line in contents.lines() {
		docs_seen += 1;
		let line = line.unwrap();
		let line_value: Value = serde_json::from_str(&line).unwrap();
		let group_hash = get_group_hash(&line_value, &config.group_keys).unwrap();

		// always keep the things without groups
		if group_hash.is_none() {
			output_bytes.extend(line.as_bytes());
			output_bytes.push(b'\n');
			docs_kept += 1;
			prev_hash = group_hash;
			prev_line = Some(line);
			continue
		}

		if group_hash != prev_hash {
			if keep_idx == 0 {
				output_bytes.extend(line.as_bytes());
				output_bytes.push(b'\n');
				docs_kept += 1;
			} else {
				if !prev_line.is_none() {
					output_bytes.extend(prev_line.unwrap().as_bytes());
					output_bytes.push(b'\n');
					docs_kept += 1;
				}
			}		
			prev_hash = group_hash;
			prev_line = Some(line);
		}
	}

	if keep_idx == -1 && prev_hash.is_some() {		
		docs_kept += 1;
		if prev_line.is_some() {
			output_bytes.extend(prev_line.unwrap().as_bytes());
		}
		output_bytes.push(b'\n');
	}	
	if config.delete_after_read {
		remove_file(input_path).unwrap();
	}

	write_mem_to_pathbuf(&output_bytes, output_path).unwrap();
	Ok((docs_seen, docs_kept))

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

