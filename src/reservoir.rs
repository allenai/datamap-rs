/* Reservoir sampling */

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





pub fn reservoir(input_dir: &PathBuf, key: &String, reservoir_size: usize, output_file: &PathBuf) -> Result<(), Error> {
	println!("Starting reservoir sampling...");


    let num_threads = current_num_threads();
    let all_files = expand_dirs(vec![input_dir.clone()], None).unwrap();
    let chunk_size = (all_files.len() + num_threads - 1) / num_threads;    
    let chunks: Vec<Vec<PathBuf>> = all_files.chunks(chunk_size).map(|c| c.to_vec()).collect();

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