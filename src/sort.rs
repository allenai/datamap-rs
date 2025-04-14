use std::sync::atomic::Ordering;
use std::sync::atomic::AtomicUsize;
use anyhow::{Error, Result};
use dashmap::DashMap;
use std::{
    fs::{self, create_dir_all, File, OpenOptions},
    hash::{DefaultHasher, Hash, Hasher},
    io::{Write, BufRead},
    os::unix::fs::OpenOptionsExt,
    path::PathBuf,
    sync::{Arc, Mutex},
    time::Instant,
};
use serde_json;
use rayon::{prelude::*, current_num_threads};
use crate::utils::json_get;
use mj_io::{expand_dirs, read_pathbuf_to_mem, write_mem_to_pathbuf, build_pbar};
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

	time_it!("Intermediate sort", {
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
		gen_writer.finish().unwrap();
		println!("Did sort in {:?} secs", start_main.elapsed().as_secs());
	});

	let intermed_files = expand_dirs(vec![working_dir.clone()], None).unwrap();
	let num_threads = current_num_threads();
	let chunk_size = (intermed_files.len() + num_threads - 1) / num_threads;
    let chunks: Vec<Vec<PathBuf>> = intermed_files.chunks(chunk_size).map(|c| c.to_vec()).collect();
    let global_chunk_id = AtomicUsize::new(0);
    time_it!("Final sort", {
    	let pbar = build_pbar(chunks.len(), "Chunks");
    	chunks.into_par_iter().for_each(|c| {
    		sort_chunk(c, output_dir, sort_key, max_size, &global_chunk_id).unwrap();
    		pbar.inc(1);
    	})
    });

    println!("Sorted and wrote {:?} new files in {:?} seconds", global_chunk_id.into_inner(), start_main.elapsed().as_secs());
	Ok(())
}



fn sort_chunk(chunk: Vec<PathBuf>, output_dir: &PathBuf, sort_key: &String, max_size: usize, global_chunk_id: &AtomicUsize) -> Result<(), Error>{
	
	let mut nonempties : Vec<(String, Vec<u8>)> = Vec::new();
	let mut empties: Vec<Vec<u8>> = Vec::new();

	chunk.iter().for_each(|p| {
		let contents = read_pathbuf_to_mem(p).unwrap();
		for line in contents.lines() {
			let line = line.unwrap();
			let json_line = serde_json::from_str(&line).unwrap();
			let sort_val = json_get(&json_line, sort_key).map(|val| val.clone());			
			drop(json_line);
			match sort_val {
				None => empties.push(line.as_bytes().to_vec()),
				Some(sort_val) => nonempties.push((sort_val.as_str().unwrap().to_string(), line.as_bytes().to_vec()))
			};
		}
	});

	// Make groups
	nonempties.sort_by(|a, b| a.0.cmp(&b.0));
	let get_group_size = |g: &Vec<Vec<u8>>| if g.len() == 0 {0} else {g.iter().map(|x| x.len()).sum::<usize>() + g.len() - 1};	
	let mut sorted_groups: Vec<(usize, Vec<Vec<u8>>)> = Vec::new();
	let mut cur_group: Vec<Vec<u8>> = Vec::new();
	let mut cur_group_id: Option<String> = None;
	nonempties.into_iter().for_each(|(a, b)| {
		if cur_group_id.is_none() {
			cur_group_id = Some(a.clone());
		}
    	if cur_group_id.as_ref().map_or(false, |id| a != *id) {
			sorted_groups.push((get_group_size(&cur_group), std::mem::take(&mut cur_group)));
			cur_group_id = Some(a);
			cur_group = Vec::new();
		}
		cur_group.push(b);
	});
	if cur_group.len() > 0 {
		sorted_groups.push((get_group_size(&cur_group), cur_group));
	}
	let mut small_groups: Vec<(usize, Vec<u8>)> = Vec::new();
	let mut big_groups: Vec<Vec<Vec<u8>>> = Vec::new();
	sorted_groups.into_iter().for_each(|g| {
		if g.0 <= max_size {
			small_groups.push((g.0, g.1.into_iter().flat_map(|mut el| {el.push(b'\n'); el}).collect()));
		} else {
			big_groups.push(g.1);
		}
	});

	
	// Make files:
	// Loop through small groups until almost too big, and then fill in w/ empties until too big
	let mut cur_contents: Vec<u8> = Vec::new();
	small_groups.into_iter().for_each(|(s, g)| {
		if cur_contents.len() + s > max_size {
			while empties.len() > 0 && cur_contents.len() < max_size {
				let last = empties.pop().unwrap();
				cur_contents.extend(last);
				cur_contents.push(b'\n');
			}
			let output_shard_name = get_output_shard_file_name(output_dir, global_chunk_id.fetch_add(1, Ordering::SeqCst), None);
			write_mem_to_pathbuf(&cur_contents, &output_shard_name).unwrap();
			cur_contents = Vec::new();
		}

		cur_contents.extend(g);
	});
	if cur_contents.len() > 0 {
		let output_shard_name = get_output_shard_file_name(output_dir, global_chunk_id.fetch_add(1, Ordering::SeqCst), None);
		write_mem_to_pathbuf(&cur_contents, &output_shard_name).unwrap();
	}	
	// And then make part'ed files for big groups
	big_groups.into_iter().for_each(|g| {
		let chunk_id = global_chunk_id.fetch_add(1, Ordering::SeqCst);
		let mut part_num = 0;
		let mut cur_contents: Vec<u8> = Vec::new();
		g.into_iter().for_each(|el| {
			cur_contents.extend(el);
			cur_contents.push(b'\n');
			if cur_contents.len() > max_size {
				let output_path = get_output_shard_file_name(output_dir, chunk_id, Some(part_num));
				write_mem_to_pathbuf(&cur_contents, &output_path).unwrap();
				part_num += 1;
				cur_contents = Vec::new();
			}
			if cur_contents.len() > 0 {
				let output_path = get_output_shard_file_name(output_dir, chunk_id, Some(part_num));
				write_mem_to_pathbuf(&cur_contents, &output_path).unwrap();				
			}
		});		
	});
	// And finally drain out the unaffiliated/groupless
	let mut cur_contents: Vec<u8> = Vec::new();
	empties.into_iter().for_each(|g| {
		cur_contents.extend(g);
		cur_contents.push(b'\n');
		if cur_contents.len() > max_size {
			let chunk_id = global_chunk_id.fetch_add(1, Ordering::SeqCst);
			let output_path = get_output_shard_file_name(output_dir, chunk_id, None);
			write_mem_to_pathbuf(&cur_contents, &output_path).unwrap();
		}
	});
	if cur_contents.len() > 0 {
		let chunk_id = global_chunk_id.fetch_add(1, Ordering::SeqCst);
		let output_path = get_output_shard_file_name(output_dir, chunk_id, None);
		write_mem_to_pathbuf(&cur_contents, &output_path).unwrap();		
	}

	Ok(())
}

fn get_output_shard_file_name(output_dir: &PathBuf, chunk_id: usize, part: Option<usize>) -> PathBuf {
	if let Some(part) = part {
		output_dir.clone().join(format!("sorted_shard_{:08}.part_{:03}.jsonl.zst", chunk_id, part))		
	} else {
		output_dir.clone().join(format!("sorted_shard_{:08}.jsonl.zst", chunk_id))
	}
}


/*==========================================================
=                        GEN WRITER STUFF                  =
==========================================================*/

pub struct GenWriter<'a> {
	pub writer: DashMap<usize, Arc<Mutex<Encoder<'a, File>>>>,
	#[allow(dead_code)]
	storage_loc: PathBuf,
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
		GenWriter { writer, storage_loc: storage_loc.clone() }
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