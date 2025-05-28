use rayon::ThreadPoolBuilder;
use std::sync::atomic;
use serde_json::Value;
use std::sync::atomic::AtomicUsize;
use std::collections::HashMap;
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
use crate::utils::{json_get, json_set};
use mj_io::{expand_dirs, read_pathbuf_to_mem, build_pbar, write_mem_to_pathbuf, get_output_filename};
use zstd::stream::Encoder;
use serde::{Deserialize, Serialize};
use regex::Regex;
use std::cmp::Ordering;
use rand::Rng;


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
	sort_keys: Vec<Vec<String>>, // of the form [[sortkey1, backup_sortkey1, ...], [sortkey2, ...]]
	num_buckets: usize,
	#[serde(default="default_max_file_size")]
	max_file_size: usize,
	keep_idx: i32, // 0 means keep first, -1 means keep last
	size_key: Option<String> // if present, add the size of this chunk to the doc we keep in the filter step 
}


fn default_max_file_size() -> usize {
	256_000_000
}



/*============================================================
=                            GROUP SORT STUFF                =
============================================================*/

pub fn distributed_group(input_dir: &PathBuf, group_dir: &PathBuf, config_path: &PathBuf, subext: Option<String>) -> Result<(), Error> {

	let start_main = Instant::now();
	println!("Starting group operation");	
	let input_paths = expand_dirs(vec![input_dir.clone()], None).unwrap();
	let config_contents = read_pathbuf_to_mem(config_path).unwrap();
	let config: GroupsortConfig = serde_yaml::from_reader(config_contents).unwrap();
	let num_buckets = config.num_buckets;
	let subext = if let Some(subext) = subext {
		subext
	} else {
		"group".to_string()
	};
	let writer = GenWriter::new(group_dir, num_buckets, &subext, config.max_file_size);
	let pbar = build_pbar(input_paths.len(), "Paths");
	input_paths.par_iter().for_each(|p| {
		group_path(p, &config.group_keys, &writer).unwrap();
		pbar.inc(1);
	});

	writer.finish().unwrap();
	println!("Finished group op in {:?} secs", start_main.elapsed().as_secs());

	Ok(())
}


fn group_path(path: &PathBuf, group_keys: &Vec<String>, writer: &GenWriter) -> Result<(), Error> {
	let num_chunks = writer.num_chunks;
	let contents = read_pathbuf_to_mem(path).unwrap();
	for line in contents.lines() {
		let line = line.unwrap();
		let value : serde_json::Value = serde_json::from_str(&line).unwrap();
		let hash_val = if let Some(hash_val) = get_group_hash(&value, group_keys).unwrap() {
			hash_val
		} else {
			// missing group info, put in random shard 			
			let mut rng = rand::rng();
			let random_usize: usize = rng.random_range(0..=usize::MAX);
			random_usize 
		};
		let mut line_bytes: Vec<u8> = line.into();
		line_bytes.push(b'\n');

		writer.write_line(hash_val % num_chunks, line_bytes).unwrap();
	}
	Ok(())
}


fn get_group_hash(value: &serde_json::Value, group_keys: &Vec<String>) -> Result<Option<usize>, Error> {
	let mut hasher = DefaultHasher::new();
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


pub fn distributed_sort(group_dir: &PathBuf, sorted_dir: &PathBuf, config_path: &PathBuf) -> Result<(), Error> {
	let start_main = Instant::now();
	println!("Starting main sort operation");
	let group_paths = expand_dirs(vec![group_dir.clone()], None).unwrap();
	let config_contents = read_pathbuf_to_mem(config_path).unwrap();
	let config: GroupsortConfig = serde_yaml::from_reader(config_contents).unwrap();

	// Group the "grouped" files according to the hashes of their values
	let group_groups: DashMap<usize, Vec<PathBuf>> = DashMap::new();
	group_paths.into_par_iter().for_each(|p| {
		let chunk_id = extract_chunk_regex(&p).unwrap();
		group_groups.entry(chunk_id).or_default().push(p);
	});

	// And then group and sort all chunks here
	let shard_id = AtomicUsize::new(0);
	let pbar = build_pbar(group_groups.len(), "Groups");

	let group_groups : Vec<Vec<PathBuf>> = group_groups.into_iter().map(|(_k,v)| v).collect::<Vec<Vec<PathBuf>>>();
	let chunks: Vec<_> = group_groups.chunks(group_groups.len() / 8).map(|chunk| chunk.to_vec()).collect();
	chunks.into_par_iter().enumerate().for_each(|(i, chunk)| {
	    // Each chunk gets its own 8-thread pool
	    let pool = ThreadPoolBuilder::new()
	        .num_threads(8)
	        .thread_name(move |idx| format!("pool-{i}-thread-{idx}"))
	        .build()
	        .unwrap();

	    pool.install(|| {
	    	chunk.into_iter().for_each(|group| {
	    		sort_group(group, sorted_dir, &config, &shard_id).unwrap();
	    		pbar.inc(1);
	    	});
	    });
	});



	println!("Finished sort in {:?} secs | wrote {:} new shards", 
			 start_main.elapsed().as_secs(),
			 shard_id.fetch_add(0, atomic::Ordering::SeqCst));
	Ok(())
}


fn extract_chunk_regex(filename: &PathBuf) -> Result<usize, Error> {
    let re = Regex::new(r"chunk_(\d{8})\.").unwrap();
    let caps = re.captures(filename.to_str().unwrap()).unwrap();
    let chunk_id = caps.get(1).unwrap().as_str().parse::<usize>().unwrap();
    Ok(chunk_id)
}


fn sort_group(group: Vec<PathBuf>, sorted_dir: &PathBuf, config: &GroupsortConfig, shard_id: &AtomicUsize) -> Result<(), Error> {
	let value_group: DashMap<usize, Vec<serde_json::Value>> = DashMap::new();
	//let mut null_group: Vec<Value> = Vec::new();
	// First load all elements in the group into values
	// 

	group.par_iter().for_each(|path| {
		let path = path.clone();
		let contents = read_pathbuf_to_mem(&path).unwrap();
		for line in contents.lines() {
			let line = line.unwrap();
			let line_value : serde_json::Value = serde_json::from_str(&line).unwrap();
			if let Some(group_hash) = get_group_hash(&line_value, &config.group_keys).unwrap() {
				value_group.entry(group_hash).or_default().push(line_value);
			} else {
				value_group.entry(usize::MAX).or_default().push(line_value); // hope this never hashes to usize::MAX
			}
		}		
	});


	let value_bytes: DashMap<usize, Vec<u8>> = value_group.into_par_iter().map(|(k,v)| {
		let mut result: Vec<u8> = Vec::new();
		for value in v {
			let line = serde_json::to_vec(&value).unwrap(); // serialize to Vec<u8>
        	result.extend_from_slice(&line);
        	result.push(b'\n'); // add newline
		}
		(k, result)
	}).collect();




	// And then write into output files
	let mut cur_size = 0;
	let mut cur_contents: Vec<u8> = Vec::new();
	value_bytes.into_iter().for_each(|(_k, content)| {
		cur_size += content.len();		
		cur_contents.extend(content);
		if cur_size >= config.max_file_size {
			write_output_contents(&cur_contents, sorted_dir, shard_id).unwrap();
			cur_size = 0;
			cur_contents.clear();
		}
	});


	Ok(())
}

fn get_backup_sortval<'a>(val: &'a Value, sortkey: &Vec<String>) -> Option<&'a Value> {
	for k in sortkey {
		if let Some(sort_val) = json_get(val, k) {
			return Some(sort_val);
		}
	}
	return None
}


fn compare_json_values(a: &Value, b: &Value) -> Ordering {
    match (a, b) {
        (Value::Null, Value::Null) => Ordering::Equal,
        (Value::Null, _) => Ordering::Less,
        (_, Value::Null) => Ordering::Greater,
        
        (Value::Bool(a_bool), Value::Bool(b_bool)) => a_bool.cmp(b_bool),
        (Value::Bool(_), _) => Ordering::Less,
        (_, Value::Bool(_)) => Ordering::Greater,
        
        (Value::Number(a_num), Value::Number(b_num)) => {
            // For numbers, we need to handle the comparison more carefully
            if let (Some(a_f), Some(b_f)) = (a_num.as_f64(), b_num.as_f64()) {
                a_f.partial_cmp(&b_f).unwrap_or(Ordering::Equal)
            } else {
                Ordering::Equal
            }
        },
        (Value::Number(_), _) => Ordering::Less,
        (_, Value::Number(_)) => Ordering::Greater,
        
        (Value::String(a_str), Value::String(b_str)) => a_str.cmp(b_str),
        (Value::String(_), _) => Ordering::Less,
        (_, Value::String(_)) => Ordering::Greater,
        
        (Value::Array(a_arr), Value::Array(b_arr)) => {
            // Compare arrays element by element
            let min_len = a_arr.len().min(b_arr.len());
            for i in 0..min_len {
                let cmp = compare_json_values(&a_arr[i], &b_arr[i]);
                if cmp != Ordering::Equal {
                    return cmp;
                }
            }
            // If all elements are equal, shorter array comes first
            a_arr.len().cmp(&b_arr.len())
        },
        (Value::Array(_), _) => Ordering::Less,
        (_, Value::Array(_)) => Ordering::Greater,
        
        (Value::Object(_), Value::Object(_)) => Ordering::Equal, // Objects are considered equal for this purpose
    }
}

fn write_output_contents(contents: &Vec<u8>, sorted_dir: &PathBuf, shard_id: &AtomicUsize) -> Result<(), Error> {
	let proper_shard_id = shard_id.fetch_add(1, atomic::Ordering::SeqCst);
	let output_path = sorted_dir.clone().join(format!("sorted_chunk_{:08}.jsonl.zst", proper_shard_id));
	write_mem_to_pathbuf(contents, &output_path)
}



pub fn groupsort_filter(input_dir: &PathBuf, output_dir: &PathBuf, config_path: &PathBuf) -> Result<(), Error> {
	let start_main = Instant::now();
	println!("Starting filter operation");	
	let input_paths = expand_dirs(vec![input_dir.clone()], None).unwrap();
	let config_contents = read_pathbuf_to_mem(config_path).unwrap();
	let config: GroupsortConfig = serde_yaml::from_reader(config_contents).unwrap();	
	let pbar = build_pbar(input_paths.len(), "Paths");

	let docs_seen = AtomicUsize::new(0);
	let docs_kept = AtomicUsize::new(0);

	input_paths.into_par_iter().for_each(|p| {
		let output_path = get_output_filename(&p, input_dir, output_dir).unwrap();	
		let (path_seen, path_kept) = groupsort_filter_path(&p, &output_path, &config).unwrap();
		docs_seen.fetch_add(path_seen, atomic::Ordering::SeqCst);
		docs_kept.fetch_add(path_kept, atomic::Ordering::SeqCst);
		pbar.inc(1);
	});

	println!("Finished filtering in {:?} secs", start_main.elapsed().as_secs());
	println!("Saw {:?} docs", docs_seen.into_inner());
	println!("Kept {:?} docs", docs_kept.into_inner());
	Ok(())
}


fn groupsort_filter_path(input_path: &PathBuf, output_path: &PathBuf, config: &GroupsortConfig) -> Result<(usize, usize), Error> {
	let mut docs_seen = 0;
	let mut docs_kept = 0;

	let contents = read_pathbuf_to_mem(input_path).unwrap();
	let mut groups : HashMap<usize, Vec<Value>> = HashMap::new();
	let mut survivors : Vec<Value> = Vec::new();
	
	for line in contents.lines() {
		docs_seen += 1;
		let line = line.unwrap();
		let mut line_value: Value = serde_json::from_str(&line).unwrap();
		let group_hash = get_group_hash(&line_value, &config.group_keys).unwrap();
		if let Some(group_hash_full) = group_hash {
			groups.entry(group_hash_full).or_default().push(line_value);
		} else {
			if let Some(size_key) = &config.size_key {
				json_set(&mut line_value, &size_key, Value::from(1)).unwrap();
			}
			survivors.push(line_value);
		}

	}
	docs_kept += survivors.len() + groups.len();
	let mut output_bytes: Vec<u8> = Vec::new();
	for survivor in survivors {
		output_bytes.extend(serde_json::to_vec(&survivor).unwrap());
		output_bytes.push(b'\n');
	}
	groups.into_iter().for_each(|(_k, mut v)| {
		let mut survivor = if config.keep_idx == 0 {
			v.remove(0)
		} else {
			v.pop().unwrap()
		};
		if let Some(size_key) = &config.size_key {
			json_set(&mut survivor, &size_key, Value::from(v.len())).unwrap();
		}
		output_bytes.extend(serde_json::to_vec(&survivor).unwrap());
		output_bytes.push(b'\n')

	});

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


