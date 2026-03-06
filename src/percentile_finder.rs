/* Reservoir sampling */

use serde_json::json;
use std::io::BufRead;
use anyhow::{Error, Result};
use std::path::PathBuf;
use mj_io::{
    build_pbar, expand_dirs, read_pathbuf_to_mem, write_mem_to_pathbuf,
};
use rayon::prelude::*;
use rand::prelude::*;
use tiktoken_rs::{cl100k_base};

pub fn percentile_finder(input_dir: &PathBuf, output_file: &PathBuf, score_key: &String, text_key: &String, tokenizer: &String, num_buckets: usize, subsample_rate: f32) -> Result<(), Error> {
    let mut docs: Vec<(usize, f32)> = match tokenizer.as_str() {
        "cl100k" => {
            let tokenizer = cl100k_base().unwrap();
            gather_counts(input_dir, score_key, text_key, subsample_rate, |text| {
                tokenizer.encode_with_special_tokens(text).len()
            }).unwrap()
        }, 
        "bytes" => {
            gather_counts(input_dir, score_key, text_key, subsample_rate, |text| {
                text.len()
            }).unwrap()
        },
        _ => {
            panic!("Unsupported tokenizer {:}", tokenizer)
        }
    };

    let total_count: usize = docs.par_iter().map(|tup| tup.0).sum::<usize>();
    docs.par_sort_by(|a,b| a.1.partial_cmp(&b.1).unwrap());

    let mut score_breaks: Vec<f32> = Vec::new();    
    // Add minimum score
    score_breaks.push(docs[0].1);
    
    // Find break points
    let interval = total_count as f64 / num_buckets as f64;
    let mut next_target = interval;
    let mut cur_seen = 0f64;
    
    for (tok_count, score) in &docs {
        cur_seen += *tok_count as f64;
        
        // Check if we've crossed one or more targets
        while cur_seen >= next_target && score_breaks.len() < num_buckets {
            score_breaks.push(*score);
            next_target += interval;
        }
    }
    
    // Add maximum score
    score_breaks.push(docs.last().unwrap().1);




    let output_json = json!(score_breaks);
    let output_contents = serde_json::to_vec(&output_json).unwrap();
    write_mem_to_pathbuf(&output_contents, output_file).unwrap();
    
    Ok(())

}

fn gather_counts<F>(
    input_dir: &PathBuf, 
    score_key: &String, 
    text_key: &String, 
    subsample_rate: f32,
    length_fn: F
) -> Result<Vec<(usize, f32)>, Error> 
where
    F: Fn(&str) -> usize + Sync + Send,
{
    let all_files = expand_dirs(vec![input_dir.clone()], None).unwrap();
    let pbar = build_pbar(all_files.len(), "Paths");
    let output: Vec<(usize, f32)> = all_files.into_par_iter().flat_map(|p| {
        let mut rng = rand::rng();
        let contents = read_pathbuf_to_mem(&p).unwrap();
        let mut path_output: Vec<(usize, f32)> = Vec::new();
        for line in contents.lines() {
            if subsample_rate < 1.0 && rng.random::<f32>() > subsample_rate {
                continue;
            }
            let line = line.unwrap();
            let text_bytes_binding = gjson::get(&line, text_key);
            let text_bytes = text_bytes_binding.str();
            let length = length_fn(text_bytes);
            let score = gjson::get(&line, score_key).f32();
            path_output.push((length, score));
        }
        pbar.inc(1);
        path_output
    }).collect();
    Ok(output)
}
