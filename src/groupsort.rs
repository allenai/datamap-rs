use dashmap::DashSet;
use rand::SeedableRng;
use xxhash_rust::xxh3::Xxh3;
use std::collections::VecDeque;
use std::collections::HashSet;
use rand::prelude::SliceRandom;
use rand::rng;
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
use ndarray::Array1;
use tiktoken_rs::{CoreBPE, p50k_base};
use disjoint_sets::UnionFind;
use ahash::RandomState;
use rand_chacha::ChaCha20Rng;



const BIG_PRIME: u64 = 18446744073709551557;
const MAX_HASH: u64 = BIG_PRIME;

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
	let survivors: Arc<Mutex<Vec<Value>>> = Arc::new(Mutex::new(Vec::new()));
	group.par_iter().for_each(|path| {
		let path = path.clone();
		let contents = read_pathbuf_to_mem(&path).unwrap();
		for line in contents.lines() {
			let line = line.unwrap();
			let line_value : serde_json::Value = serde_json::from_str(&line).unwrap();
			if let Some(group_hash) = get_group_hash(&line_value, &config.group_keys).unwrap() {
				value_group.entry(group_hash).or_default().push(line_value);
			} else {
				let mut survivors_list = survivors.lock().unwrap();
				survivors_list.push(line_value);
				//value_group.entry(usize::MAX).or_default().push(line_value); // hope this never hashes to usize::MAX
			}
		}		
	});


	let value_bytes: DashMap<usize, Vec<u8>> = value_group.into_par_iter().map(|(k, mut v)| {
		let mut result: Vec<u8> = Vec::new();
		if k < usize::MAX {
			v.sort_by(|a, b| {
				for kgroup in &config.sort_keys {
					let a_val = get_backup_sortval(&a, kgroup);
					let b_val = get_backup_sortval(&b, kgroup);

					match (a_val, b_val) {
						(Some(a_v), Some(b_v)) => {
							let cmp = compare_json_values(a_v, b_v);
							if cmp != Ordering::Equal {
								return cmp;
							}
						}
						(Some(_), None) => return Ordering::Less,
						(None, Some(_)) => return Ordering::Greater,
						(None, None) => {}
					}
				}
				return Ordering::Equal
			});
		}
				

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

	for survivor in survivors.lock().unwrap().iter() {
		let survivor_bytes = serde_json::to_vec(&survivor).unwrap();
		cur_size += survivor_bytes.len();
		cur_contents.extend(survivor_bytes);
		cur_contents.push(b'\n');
		if cur_size >= config.max_file_size {
			write_output_contents(&cur_contents, sorted_dir, shard_id).unwrap();
			cur_size = 0;
			cur_contents.clear();
		}		
	}

	if cur_size > 0 {
		write_output_contents(&cur_contents, sorted_dir, shard_id).unwrap();
	}

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
		let (path_seen, path_kept) = groupsort_filter_path2(&p, &output_path, &config).unwrap();
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


fn groupsort_filter_path2(input_path: &PathBuf, output_path: &PathBuf, config: &GroupsortConfig) -> Result<(usize, usize), Error> {
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

	write_mem_to_pathbuf(&output_bytes, output_path).unwrap();
	Ok((docs_seen, docs_kept))

}



pub fn sorted_dupaware(input_dir: &PathBuf, output_dir: &PathBuf, dupkey: &String, subsample: f32, max_cc_size: usize) -> Result<(), Error> {
	let start_main = Instant::now();
	println!("Starting main dupaware subsample operation");
	let input_paths = expand_dirs(vec![input_dir.clone()], None).unwrap();	
	let pbar = build_pbar(input_paths.len(), "Paths");
	let docs_seen: AtomicUsize = AtomicUsize::new(0);
	let docs_kept: AtomicUsize = AtomicUsize::new(0);
	let ccs_seen: AtomicUsize = AtomicUsize::new(0);
	let ccs_kept: AtomicUsize = AtomicUsize::new(0);
	input_paths.into_par_iter().for_each(|p| {
		// keep track of total docs seen/kept, total ccs seen/kept

		let output_path = get_output_filename(&p, input_dir, output_dir).unwrap();
		dupaware_subsample_path(p, output_path, dupkey, subsample, max_cc_size, &docs_seen, &docs_kept, &ccs_seen, &ccs_kept).unwrap();
		pbar.inc(1);
	});

	println!("Finished dupaware sort in {:?} secs", start_main.elapsed().as_secs());
	println!("Saw {:?} docs | Kept {:?} docs", docs_seen.into_inner(), docs_kept.into_inner());
	println!("Saw {:?} ccs | Kept {:?} ccs", ccs_seen.into_inner(), ccs_kept.into_inner());
	Ok(())
}

fn dupaware_subsample_path(input_path: PathBuf, output_path: PathBuf, dupkey: &String, subsample: f32, max_cc_size: usize,
						   docs_seen: &AtomicUsize, docs_kept: &AtomicUsize, ccs_seen: &AtomicUsize, ccs_kept: &AtomicUsize) -> Result<(), Error> {
	let contents = read_pathbuf_to_mem(&input_path).unwrap();
	let mut groups: HashMap<Value, Vec<Value>> = HashMap::new();
	let mut output: Vec<u8> = Vec::new();
	let mut docs_seen_path = 0;
	let mut docs_kept_path = 0;
	let mut ccs_seen_path = 0;
	let mut ccs_kept_path = 0;
	for line in contents.lines() {
		docs_seen_path += 1;
		let line = line.unwrap();
		let json_line : Value = serde_json::from_str(&line).unwrap();
		let dupval = json_get(&json_line, dupkey);
		if let Some(val) = dupval {
			groups.entry(val.clone()).or_default().push(json_line);
		} else {
			ccs_seen_path += 1;
			if rng().random::<f32>() < subsample {
				docs_kept_path += 1;
				ccs_kept_path += 1;
				output.extend(line.as_bytes());
				output.push(b'\n');
			}
		}
	}
	ccs_seen_path += groups.len();
	groups.into_iter().for_each(|(_k, mut v)| {
		if rng().random::<f32>() < subsample {
			ccs_kept_path += 1;
			docs_kept_path += v.len();
			v.shuffle(&mut rng());
			v.truncate(max_cc_size);
			for val in v {
				output.extend(serde_json::to_vec(&val).unwrap());
				output.push(b'\n');
			}
		}
	});

	write_mem_to_pathbuf(&output, &output_path).unwrap();
	docs_seen.fetch_add(docs_seen_path, atomic::Ordering::SeqCst);
	docs_kept.fetch_add(docs_kept_path, atomic::Ordering::SeqCst);
	ccs_seen.fetch_add(ccs_seen_path, atomic::Ordering::SeqCst);
	ccs_kept.fetch_add(ccs_kept_path, atomic::Ordering::SeqCst);
	Ok(())
}


/*===========================================================
=                         JACCARD SIMILARITY                =
===========================================================*/
// Breaks things into connected components and then keeps the most/least recent amongst each component.
// Uses the group key to set up initial groups and then jaccards from there.

pub fn jaccard_filter(input_dir: &PathBuf, output_dir: &PathBuf, config_path: &PathBuf, jaccard: f32) -> Result<(), Error> {
	let start_main = Instant::now();
	let config_contents = read_pathbuf_to_mem(config_path).unwrap();
	let config: GroupsortConfig = serde_yaml::from_reader(config_contents).unwrap();	

	let input_paths = expand_dirs(vec![input_dir.clone()], None).unwrap();	
	let pbar = build_pbar(input_paths.len(), "Paths");
	let docs_seen: AtomicUsize = AtomicUsize::new(0);
	let docs_kept: AtomicUsize = AtomicUsize::new(0);
	let groups_seen: AtomicUsize = AtomicUsize::new(0);
	let singletons: AtomicUsize = AtomicUsize::new(0);
	let true_groups: AtomicUsize = AtomicUsize::new(0);


	input_paths.into_iter().for_each(|p| {
		let output_file = get_output_filename(&p, input_dir, output_dir).unwrap();
		let (docs_seen_path, docs_kept_path, singletons_path, groups_seen_path, true_groups_path) = jaccard_filter_path(&p, &output_file, &config, jaccard).unwrap();
		docs_seen.fetch_add(docs_seen_path, atomic::Ordering::Relaxed);
		docs_kept.fetch_add(docs_kept_path, atomic::Ordering::Relaxed);
		groups_seen.fetch_add(groups_seen_path, atomic::Ordering::Relaxed);
		singletons.fetch_add(singletons_path, atomic::Ordering::Relaxed);
		true_groups.fetch_add(true_groups_path, atomic::Ordering::Relaxed);
		pbar.inc(1);
	});


	println!("Finished jaccard filtering of data in {:?} secs", start_main.elapsed().as_secs());
	println!("Saw {:?} docs | kept {:?} docs", docs_seen.into_inner(), docs_kept.into_inner());
	println!("Saw {:?} singletons | Saw {:?} groups | saw {:?} true groups", singletons.into_inner(), groups_seen.into_inner(), true_groups.into_inner());
	Ok(())
}



fn jaccard_filter_path(input_path: &PathBuf, output_path: &PathBuf, config: &GroupsortConfig, jaccard: f32) -> Result<(usize, usize, usize, usize, usize), Error> {
	let contents = read_pathbuf_to_mem(input_path).unwrap();
	let mut output_bytes: Vec<u8> = Vec::new();
	let tokenizer = p50k_base().unwrap();
	let mut groups: HashMap<usize, Vec<Value>> = HashMap::new();
	let mut docs_seen = 0;
	let mut docs_kept = 0;
	let mut groups_seen = 0;
	let mut true_groups = 0;
	let mut singletons = 0;
	let all_lines : Vec<String> = contents.lines().map(|el| el.unwrap()).collect();


	for line in all_lines {
		docs_seen += 1;
		let line_value: Value = serde_json::from_str(&line).unwrap();
		let group_hash = get_group_hash(&line_value, &config.group_keys).unwrap();
		if let Some(h) = group_hash {
			groups.entry(h).or_default().push(line_value);
		} else {
			singletons += 1;
			docs_kept += 1;
			output_bytes.extend(line.as_bytes());
			output_bytes.push(b'\n')
		}	
	}
	groups_seen += groups.len();
	let group_pbar = build_pbar(groups.len(), "groups");
	groups.values().into_iter().for_each(|v| {
		let ccs = if v.len() > 500 {
			minhash(v, &tokenizer).unwrap()
		} else {
			get_jaccard_survivors(v, jaccard, &tokenizer).unwrap()
		};

		let mut jaccard_indices: Vec<usize> = Vec::new();
		for cc in ccs {
			if cc.len() == 0 {
				continue
			};
			if config.keep_idx == 0 {
				jaccard_indices.push(*cc.first().unwrap());
			} else {
				jaccard_indices.push(*cc.last().unwrap());
			}
		}

		docs_kept += jaccard_indices.len();
		true_groups += jaccard_indices.len();

		for i in jaccard_indices {
			output_bytes.extend(serde_json::to_vec(&v[i]).unwrap());
			output_bytes.push(b'\n');
		}
		group_pbar.inc(1);
	});


	write_mem_to_pathbuf(&output_bytes, output_path).unwrap();
	Ok((docs_seen, docs_kept, singletons, groups_seen, true_groups))
}

fn get_jaccard_survivors(values: &Vec<Value>, jaccard: f32, tokenizer: &CoreBPE) -> Result<Vec<Vec<usize>>, Error> {
	// outputs just the indices that we should keep
	let hash_sets: Vec<HashSet<u64>> = values.par_iter().map(|v| {
		let text = json_get(v, "text").unwrap().as_str().unwrap().to_string();
		get_jacc_hashset(text, tokenizer)
	}).collect();

	//let mut edges: Vec<(usize, usize)> = Vec::new();
	let edges : DashSet<(usize, usize)> = DashSet::new();
	(0..hash_sets.len()).into_par_iter().for_each(|i| {
		for j in i+1..hash_sets.len() {
			let int_size = hash_sets[i].intersection(&hash_sets[j]).count() as f32;
			let un_size = hash_sets[i].union(&hash_sets[j]).count() as f32;
			if un_size > 0.0 && int_size / un_size > jaccard {
				edges.insert((i,j));
			}
		}
	});

	let mut uf = UnionFind::new(hash_sets.len());
	for (i, j) in edges {
	    uf.union(i, j);
	}

    let mut components: HashMap<usize, Vec<usize>> = HashMap::new();
    for node in 0..hash_sets.len() {
        let root = uf.find(node);
        components.entry(root).or_default().push(node);
    }
    

    // Convert to vector of components
    Ok(components.into_values().collect())
}


fn get_jacc_hashset(text: String, tokenizer: &CoreBPE) -> HashSet<u64> {
	let mut output_set : HashSet<u64> = HashSet::new();
	let NGRAM_SIZE = 3;
	let mut ngram: VecDeque<usize> = VecDeque::with_capacity(NGRAM_SIZE);
	let mut ngram_count = 0;
	let tokens = preprocess_text(&text, tokenizer);
    for token in tokens {
        ngram.push_back(token);
        if ngram.len() >= NGRAM_SIZE {
            ngram_count += 1;
            output_set.insert(hash_vecdeque(&ngram));
            ngram.pop_front();
        }
    }
    if ngram_count == 0 {
        output_set.insert(hash_vecdeque(&ngram));
    }

	output_set
}

fn hash_vecdeque(deque: &VecDeque<usize>) -> u64 {
    let mut hasher = Xxh3::new();
    deque.hash(&mut hasher);
    hasher.finish()
}

fn preprocess_text(text: &str, tokenizer: &CoreBPE) -> Vec<usize> 
{
    let text = clean_text(text);
    tokenizer.encode_with_special_tokens(&text)
}


fn clean_text(text: &str) -> String {
    // SlimPajama text cleaning process

    // Convert the document to lowercase
    let mut text = text.to_lowercase();

    // Remove punctuation
    let punctuation: &[_] = &['!', '"', '#', '$', '%', '&', '\'', '(', ')', '*', '+', ',', '-', '.', '/', ':', ';', '<', '=', '>', '?', '@', '[', '\\', ']', '^', '_', '`', '{', '|', '}', '~'];
    text.retain(|c| !punctuation.contains(&c));

    // Replace multiple whitespace characters with a single space
    let re = Regex::new(r"\s+").unwrap();
    text = re.replace_all(&text, " ").to_string();

    // Trim leading and trailing whitespace
    text.trim().to_string()
}


fn minhash(values: &Vec<Value>, tokenizer: &CoreBPE) -> Result<Vec<Vec<usize>>, Error> {
	// do some hacky minhash stuff
	// Try 31, 200 here
	let BAND_SIZE = 31;
	let NUM_BANDS = 200;
	let NGRAM_SIZE = 3; // NGRAM OF THREE -- MIDWAY HERE 

	let perm_seeds = (0..BAND_SIZE * NUM_BANDS).map(|i| i).collect::<Vec<u64>>();

	/*
	let mut band_data: Vec<HashMap<u64, Vec<usize>>> = (0..NUM_BANDS).into_iter()
		.map(|_i| HashMap::new())
		.collect::<Vec<HashMap<u64, Vec<usize>>>>();
	*/
	let band_data: DashMap<usize, DashMap<u64, Vec<usize>>> = DashMap::new();


	// Get all hashes for all values
	values.par_iter().enumerate().for_each(|(i, v)| {
		let tokens = preprocess_text(v.get("text").unwrap().as_str().unwrap(), tokenizer);
		let full_hash = get_hash_vals_from_tokens(tokens, &perm_seeds, NGRAM_SIZE);
		let bands = full_hash.to_shape((NUM_BANDS as usize, BAND_SIZE as usize)).unwrap();
		for (row_num, row) in bands.rows().into_iter().enumerate() {
			let mut hasher = Xxh3::new();
		    row.as_slice().unwrap().hash(&mut hasher);
		    let row_hash = hasher.finish();
			band_data.entry(row_num).or_default().entry(row_hash).or_default().push(i);
		}
	});


	// And then gather edges
	let mut uf = UnionFind::new(values.len());
	band_data.into_iter().for_each(|(_k, band_datum)| {
		band_datum.into_iter().for_each(|(_k, v)| {
			for i in 0..(v.len() -1) {
				uf.union(v[i], v[i+1]);
			}
		})
	});

    let mut components: HashMap<usize, Vec<usize>> = HashMap::new();
    for node in 0..values.len() {
        let root = uf.find(node);
        components.entry(root).or_default().push(node);
    }
    



    // Convert to vector of components
    Ok(components.into_values().collect())
}	




fn get_hash_vals_from_tokens(tokens: Vec<usize>, perm_seeds: &Vec<u64>, ngram_size: usize) -> Array1<u64> {
    let a = _init_permutations(perm_seeds);
    let n = perm_seeds.len();

    let mut hash_vals = Array1::ones(n) * MAX_HASH;
    let mut ngram: VecDeque<usize> = VecDeque::with_capacity(ngram_size);
    let mut ngram_count = 0; 
    for token in tokens {
        ngram.push_back(token);
        if ngram.len() >= ngram_size {
            ngram_count += 1;
            hash_vals = _update_hash_vals(hash_vals, &a, &ngram);
            ngram.pop_front();
        }
    }
    hash_vals = if ngram_count == 0 {
        _update_hash_vals(hash_vals, &a, &ngram) // short document, still wanna hash it
    } else {
        hash_vals
    };

    hash_vals
}


fn _init_permutations(seeds: &Vec<u64>) -> Array1<u128> {
    // Initialize the permutations needed for each minhash
    let n = seeds.len();
    let mut a = Array1::zeros(n);
    for (i, &seed) in seeds.iter().enumerate() {
        let mut rng = ChaCha20Rng::seed_from_u64(seed);
        a[i] = rng.gen::<u128>() as u128;
    }
    a
}


fn _update_hash_vals(mut hash_vals: Array1<u64>, a: &Array1<u128>, ngram: &VecDeque<usize>) -> Array1<u64> {

    // hash the vecdeque as a u128 
    let hash_a = RandomState::with_seed(123);
    let hash_b = RandomState::with_seed(456);
    let hash_val_a = hash_a.hash_one(ngram);
    let hash_val_b = hash_b.hash_one(ngram);
    let cur_hash = ((hash_val_a as u128) << 64) | (hash_val_b as u128);

    // then multiply by a (mod 2^128) and take top 64 most significant bits
    let phv: Array1<u64> = a.mapv(|x| (x.wrapping_mul(cur_hash) >> 64) as u64);
    hash_vals.zip_mut_with(&phv, |x, y| *x = std::cmp::min(*x, *y));

    hash_vals

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


