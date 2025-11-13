/* Reservoir sampling */

use std::fs;
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


pub fn reservoir_sample(input_dir: &PathBuf, output_file: &PathBuf, key: &String, reservoir_size: usize, token_weighted: bool, text_key: Option<String>) -> Result<(), Error> {
	println!("Starting reservoir sampling...");
	if !token_weighted {
		unweighted_reservoir(input_dir, key, reservoir_size, output_file).unwrap();
	} else {
		token_weighted_reservoir(input_dir, key, &text_key.unwrap(), reservoir_size, output_file).unwrap();
	}
	Ok(())
}


/*==========================================================================
=                           Unweighted Reservoir Sampling                  =
==========================================================================*/

fn unweighted_reservoir(input_dir: &PathBuf, key: &String, reservoir_size: usize, output_file: &PathBuf) -> Result<(), Error> {


    let all_files = expand_dirs(vec![input_dir.clone()], None).unwrap();
    let num_files = all_files.len();

    let chunks_targets = get_chunks_targets(all_files, reservoir_size).unwrap();
    let pbar = build_pbar(num_files, "Paths");

    let full_res: Vec<(Vec<Value>, usize)> = chunks_targets.into_par_iter().map(|(pvec, target_size)| {
        thread_res(&pvec, key, target_size, &pbar).unwrap()
    }).collect();


    let total_seen = full_res.par_iter().map(|k| k.1).sum::<usize>();
    let full_res: Vec<Value> = full_res.into_iter().flat_map(|k| k.0).collect();
    let json_res = json!(full_res);
    let output_contents = serde_json::to_vec(&json_res).unwrap();
    write_mem_to_pathbuf(&output_contents, output_file).unwrap();
    println!("Made a reservoir of size {:?} from {:?} documents total", full_res.len(), total_seen);

	Ok(())
}


fn thread_res(input_paths: &Vec<PathBuf>, key: &String, reservoir_size: usize, pbar: &ProgressBar) -> Result<(Vec<Value>, usize), Error> {
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
	Ok((cur_res, total_seen))
}


fn get_chunks_targets(all_paths: Vec<PathBuf>, reservoir_size: usize) -> Result<Vec<(Vec<PathBuf>, usize)>, Error> {
    let num_threads = current_num_threads();    
    let mut chunks: Vec<Vec<PathBuf>> = (0..num_threads).map(|_i| Vec::new()).collect();
    let mut total_size = 0;
    all_paths.into_iter().enumerate().for_each(|(i, p)| {
        total_size += fs::metadata(&p).unwrap().len();
        chunks[i % num_threads].push(p);
    });
    let chunks: Vec<Vec<PathBuf>> = chunks.into_iter().filter(|v| v.len() > 0).collect();
    let chunks_targets = chunks.into_par_iter().map(|pvec| {
        let chunk_size: usize = pvec.iter().map(|p| fs::metadata(p).unwrap().len() as usize).sum();
        let target_size = ((reservoir_size as f64) * ((chunk_size as f64) / (total_size as f64))) as usize;        
        (pvec, target_size)
    }).collect();
    Ok(chunks_targets)
}

/*==================================================================
=                      Weighted Reservoir Sampling                 =
==================================================================*/
// Only use tiktoken cl100k for weights 


fn token_weighted_reservoir(input_dir: &PathBuf, score_key: &String, text_key: &String, reservoir_size: usize, output_file: &PathBuf) -> Result<(), Error> {
    let all_files = expand_dirs(vec![input_dir.clone()], None).unwrap();
    let num_files = all_files.len();
    let chunks_targets = get_chunks_targets(all_files, reservoir_size).unwrap();

    let pbar = build_pbar(num_files, "Paths");
    let full_res: Vec<Vec<WeightedItem>> = chunks_targets.into_par_iter().map(|(pvec, res_size)| {
        token_weighted_thread_res(&pvec, score_key, text_key, res_size, &pbar).unwrap()
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