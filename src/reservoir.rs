/* Reservoir sampling */

use std::cmp::Ordering;
use serde_json::json;
use crate::utils::json_get;
use serde_json::Value;
use indicatif::ProgressBar;
use std::io::BufRead;
use anyhow::{Error, Result};
use std::path::PathBuf;
use mj_io::{
    build_pbar, expand_dirs, read_pathbuf_to_mem, write_mem_to_pathbuf,
};
use rayon::prelude::*;
use rand::prelude::*;
use rayon::current_num_threads;

use binary_heap_plus::*;
use tiktoken_rs::cl100k_base;


pub fn reservoir(input_dir: &PathBuf, key: &String, reservoir_size: usize, output_file: &PathBuf) -> Result<(), Error> {
	println!("Starting reservoir sampling...");


    let num_threads = current_num_threads();
    let all_files = expand_dirs(vec![input_dir.clone()], None).unwrap();
    let chunk_size = (all_files.len() + num_threads - 1) / num_threads;    
    let mut chunks: Vec<Vec<PathBuf>> = all_files.chunks(chunk_size).map(|c| c.to_vec()).collect();
    while chunks.len() < num_threads {
    	chunks.push(Vec::new());
    }

    let base_thread_res_size = reservoir_size / num_threads;
    let pbar = build_pbar(all_files.len(), "Paths");
    let full_res: Vec<Vec<Value>> = (0..num_threads).into_par_iter().map(|i| {
    	let res_size = if i < (reservoir_size % num_threads) {base_thread_res_size + 1} else {base_thread_res_size};
		thread_res(&chunks[i], key, res_size, &pbar).unwrap()
    }).collect();



    let full_res: Vec<Value> = full_res.into_iter().flat_map(|k| k).collect();
    let json_res = json!(full_res);
    let output_contents = serde_json::to_vec(&json_res).unwrap();
    write_mem_to_pathbuf(&output_contents, output_file).unwrap();


	Ok(())
}


pub fn thread_res(input_paths: &Vec<PathBuf>, key: &String, reservoir_size: usize, pbar: &ProgressBar) -> Result<Vec<Value>, Error> {
	let mut cur_res: Vec<Value> = Vec::new();
	let mut total_seen: usize = 0;
	let mut rng = rand::rng();
	input_paths.into_iter().for_each(|p| {
		let contents = read_pathbuf_to_mem(&p).unwrap();
		for line in contents.lines() {
			// Only process if we need to access this data 
			total_seen += 1;
			let rand_idx = rng.random_range(0..=total_seen);
			if cur_res.len() < reservoir_size || rand_idx < reservoir_size {
				let line = line.unwrap();
				let json_line: Value = serde_json::from_str(&line).unwrap();
				let item = json_get(&json_line, key).unwrap().clone();
				if cur_res.len() < reservoir_size {
					cur_res.push(item);
				} else {
					cur_res[rand_idx] = item;
				}
			}		
		}
		pbar.inc(1);
	});
	Ok(cur_res)
}


/*==================================================================
=                      Weighted Reservoir Sampling                 =
==================================================================*/
// Only use tiktoken cl100k for weights 


pub fn token_weighted_reservoir(input_dir: &PathBuf, score_key: &String, text_key: &String, reservoir_size: usize, output_file: &PathBuf) -> Result<(), Error> {
	println!("Starting reservoir sampling...");


    let num_threads = current_num_threads();
    let all_files = expand_dirs(vec![input_dir.clone()], None).unwrap();
    let chunk_size = (all_files.len() + num_threads - 1) / num_threads;    
    let mut chunks: Vec<Vec<PathBuf>> = all_files.chunks(chunk_size).map(|c| c.to_vec()).collect();
    while chunks.len() < num_threads {
    	chunks.push(Vec::new());
    }

    let base_thread_res_size = reservoir_size / num_threads;
    let pbar = build_pbar(all_files.len(), "Paths");
    let full_res: Vec<Vec<WeightedItem>> = (0..num_threads).into_par_iter().map(|i| {
    	let res_size = if i < (reservoir_size % num_threads) {base_thread_res_size + 1} else {base_thread_res_size};
		token_weighted_thread_res(&chunks[i], score_key, text_key, res_size, &pbar).unwrap()
    }).collect();

    let mut full_res: Vec<WeightedItem> = full_res.into_iter().flat_map(|k| k).collect();
    full_res.par_sort_by(|a, b| a.value.partial_cmp(&b.value).unwrap());    
    let total_weight : usize = full_res.par_iter().map(|w| w.weight).sum();
    let mut cum_weight = 0;
    let mut percentiles: Vec<Value> = Vec::new();
    for item in full_res.into_iter() {
        cum_weight += item.weight;
        let percentile = ((cum_weight as f64 - item.weight as f64 / 2.0) / total_weight as f64) * 100.0;
        percentiles.push(json!({"percentile": percentile, "value": item.value}));
    }

    let output_contents = serde_json::to_vec(&percentiles).unwrap();
    write_mem_to_pathbuf(&output_contents, output_file).unwrap();


	Ok(())
}




#[derive(Clone, Debug)]
struct WeightedItem {
    value: f64,
    log_key: f64,
    weight: usize,
}
impl PartialEq for WeightedItem {
    fn eq(&self, other: &Self) -> bool {
        self.log_key == other.log_key
    }
}
impl Eq for WeightedItem {}

impl PartialOrd for WeightedItem {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for WeightedItem {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Compare by log_key (lower number = higher priority)
        self.log_key.partial_cmp(&other.log_key).unwrap_or(Ordering::Greater)
    }
}


fn token_weighted_thread_res(
    input_paths: &Vec<PathBuf>, 
    score_key: &String,
    text_key: &String,
    reservoir_size: usize, 
    pbar: &ProgressBar
) -> Result<Vec<WeightedItem>, Error> {
    // Create min-heap ordered by log_key using closure comparator

    let mut heap: BinaryHeap<WeightedItem, MinComparator> = BinaryHeap::new_min();
    
    let mut rng = rand::rng();
    let tokenizer = cl100k_base().unwrap();

    input_paths.into_iter().for_each(|p| {
        let contents = read_pathbuf_to_mem(&p).unwrap();
        for line in contents.lines() {
            let line = line.unwrap();
            let json_line: Value = serde_json::from_str(&line).unwrap();
            let value = json_get(&json_line, score_key).unwrap().as_f64().unwrap();
            let text = json_get(&json_line, text_key).unwrap().clone();
            let text = text.as_str().unwrap();
            let weight = tokenizer.encode_with_special_tokens(text).len();

            // Generate log-space key: log(U) / weight
            let u: f64 = rng.random();
            let log_key = u.ln() / (weight as f64);
            
            let weighted_item = WeightedItem { value, log_key, weight };
            
            if heap.len() < reservoir_size {
                heap.push(weighted_item);
            } else if let Some(min_item) = heap.peek() {
                if log_key > min_item.log_key {
                    heap.pop(); // Remove minimum
                    heap.push(weighted_item); // Add new item
                }
            }
        }
        pbar.inc(1);
    });
    
    Ok(heap.into_vec())
}
