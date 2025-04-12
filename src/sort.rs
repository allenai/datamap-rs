use rand::Rng;
use anyhow::{Error, Result};
use dashmap::DashMap;
use std::{
    fs::{self, create_dir_all, File, OpenOptions},
    hash::{DefaultHasher, Hash, Hasher},
    io::{BufWriter, Write, Read, BufRead},
    os::unix::fs::OpenOptionsExt,
    path::PathBuf,
    sync::{Arc, Mutex},
    time::Instant,
};
use serde_json;
use rayon::prelude::*;
use crate::utils::json_get;
use mj_io::{expand_dirs, read_pathbuf_to_mem, write_mem_to_pathbuf, build_pbar, get_output_filename};
use zstd::stream::Encoder;


/*
Single node sorting:
- Assumes we can load a dataset onto a single device
- But then loads jsonl files and sorts them based on their sort-key
- And then writes them to new files with max_size (in uncompressed bytes)


Uses an intermediary data store to wirte intermediate files. The assumption is that 
data can't fit in memory, but CAN fit on disk.


*/
const SHARD_SIZE: usize = 100000000; // Shards of 100MB by default

macro_rules! time_it {
    ($label:expr, $block:block) => {{
        println!("Staring {}", $label);
        let start = std::time::Instant::now();
        let result = $block;
        println!("Finished {} in {:?} secs", $label, start.elapsed().as_secs());
        result
    }};
}



pub fn single_node_sort(input_dir: &PathBuf, working_dir: &PathBuf, output_dir: &PathBuf, sort_key: &String, max_size: usize) -> Result<(), Error> {
	let start_main = Instant::now();

	let input_files = expand_dirs(vec![input_dir.clone()], None).unwrap();
	let total_size = input_files.iter().map(|p| fs::metadata(p).unwrap().len()).sum::<u64>() as usize;
	let num_shards = total_size / SHARD_SIZE + 1; 

	let gen_writer = GenWriter::new(working_dir, num_shards, "intermed");

	let pbar = build_pbar(input_files.len(), "Input files");
	input_files.into_par_iter().for_each(|p| {

		let contents = read_pathbuf_to_mem(&p).unwrap();
		for line in contents.lines() {
			let line = line.unwrap();
			let json_val = serde_json::from_str(&line).unwrap();
			let sort_val = json_get(&json_val, sort_key);
			let shard_num = if let Some(sort_val) = sort_val {
				let mut hasher = DefaultHasher::new();
				sort_val.hash(&mut hasher);
				hasher.finish() as usize % num_shards
			} else {
				let random_usize: usize = rand::random::<u64>().try_into().unwrap();
				random_usize % num_shards
			};
			let mut row = line.as_bytes().to_vec();
			row.push(b'\n');
			gen_writer.write_line(shard_num, row).unwrap();
		}
		pbar.inc(1);
	});
	gen_writer.finish();
	println!("Did sort in {:?} secs", start_main.elapsed().as_secs());


	Ok(())
}




/*==========================================================
=                        GEN WRITER STUFF                  =
==========================================================*/

pub struct GenWriter<'a> {
	pub writer: DashMap<usize, Arc<Mutex<Encoder<'a, File>>>>,
	#[allow(dead_code)]
	storage_loc: PathBuf,
	num_chunks: usize,
}

impl GenWriter<'_> {
	pub fn new(storage_loc: &PathBuf, num_chunks: usize, subext: &str) -> Self {
		let writer : DashMap<usize, Arc<Mutex<Encoder<File>>>> = DashMap::new();
		// Create writers
		println!("Opening {:?} writer files", num_chunks);
		for chunk in 0..num_chunks {
			let filename = GenWriter::get_filename(storage_loc, chunk, subext);
			if let Some(parent_dir) = filename.parent() {
		        if !parent_dir.exists() {
		            create_dir_all(parent_dir).unwrap()
		         }
		    }
			let ccwriter = Arc::new(
				Mutex::new(
				Encoder::new(
				OpenOptions::new()
				.append(true)
				.create(true)
				.mode(0o644)
				.open(filename)
				.unwrap(),
			3).unwrap()));


			writer.insert(chunk, ccwriter);
		}
		GenWriter { writer, storage_loc: storage_loc.clone(), num_chunks: num_chunks}
	}


	pub fn get_filename(storage_loc: &PathBuf, chunk: usize, subext: &str) -> PathBuf {
		storage_loc.clone()
			.join(format!("chunk_{:08}.{}.jsonl.zst", chunk, subext))
	}


	pub fn write_line(&self, key: usize, contents: Vec<u8>) -> Result<(), Error> {
		// hash the key and take mod num_chunks to get location

		let binding = self.writer.get(&key).unwrap();
		let mut cc_writer = binding.lock().unwrap();
		cc_writer.write_all(&contents).unwrap();
		
		Ok(())

	}

	pub fn finish(&self) -> Result<(), Error> {
		// Flushes all the open writers
		self.writer.par_iter()
			.for_each(|entry| entry.value().lock().unwrap().flush().unwrap());
		Ok(())
	}
}