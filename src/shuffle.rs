use std::io::Write;
use std::io::BufRead;
use std::os::unix::fs::OpenOptionsExt;
use std::fs::OpenOptions;
use std::fs::create_dir_all;
use dashmap::DashMap;
use std::io::BufWriter;
use std::fs::File;
use anyhow::{Error, Result};
use std::path::PathBuf;
use std::time::Instant;
use mj_io::{
    build_pbar, expand_dirs, read_pathbuf_to_mem, write_mem_to_pathbuf,
};
use rayon::prelude::*;
use zstd::Encoder;
use std::sync::Mutex;
use rand::prelude::*;
use rand::seq::SliceRandom;


pub fn shuffle(input_dir: &PathBuf, working_dir: &PathBuf, output_dir: &PathBuf, num_files: usize) -> Result<(), Error> {

	let start_main = Instant::now();
	println!("Starting shuffle...");
	let input_files = expand_dirs(vec![input_dir.clone()], None).unwrap();
	let num_files = if num_files == 0 {
		input_files.len()
	} else {
		num_files
	};
	let num_files_u64 = num_files as u64;

	// Start phase 1 where we write the outputs in a streaming fashion to intermed files
	println!("Starting phase I shuffle...");
	let start_phase_1 = Instant::now();

	let writers: DashMap<u64, Mutex<Encoder<'static, BufWriter<File>>>> = DashMap::new();
	(0..num_files).into_par_iter().for_each(|i| {
		let shard_name = working_dir.clone().join(format!("intermed_shard_{:08}.jsonl.zst", i));
		let writer = make_intermed_writer(shard_name).unwrap();
		writers.insert(i as u64, writer);
	});

	let pbar = build_pbar(input_files.len(), "Input paths");
	input_files.into_par_iter().for_each(|p| {
		let mut rng = rand::rng();
		let contents = read_pathbuf_to_mem(&p).unwrap();
		for line in contents.lines() {
			let line = line.unwrap();
			let line_bytes = line.as_bytes();
   		    let writer_id: u64 = rng.random::<u64>() % num_files_u64;
   		 	let writer_ref = writers.get_mut(&writer_id).unwrap();
   		 	let mut encoder = writer_ref.lock().unwrap();
   		 	encoder.write_all(line_bytes).unwrap();
   		 	encoder.write(&vec![b'\n']).unwrap();
		}
		pbar.inc(1);
		});
		writers.into_iter().for_each(|(_, v)| {
			let mut encoder = v.into_inner().unwrap();
			encoder.flush().unwrap();
			encoder.finish().unwrap();
		});
	println!("Finished phase I in {:?} secs", start_phase_1.elapsed().as_secs());



	// Start phase 2 where we loop over intermediate files and shuffle the lines within each to make outputs
	let start_phase_2 = Instant::now();
	println!("Starting phase II shuffle...");
	let intermed_files = expand_dirs(vec![working_dir.clone()], None).unwrap();
	let pbar = build_pbar(intermed_files.len(), "Intermed files");
	intermed_files.into_par_iter().for_each(|p| {
		let mut shuffled_lines: Vec<Vec<u8>> = Vec::new();
		let contents = read_pathbuf_to_mem(&p).unwrap();
		for line in contents.lines() {
			let line = line.unwrap();
			let mut line_bytes = line.as_bytes().to_vec();
			line_bytes.push(b'\n');
			shuffled_lines.push(line_bytes);
		}
		let mut rng = rand::rng();
		shuffled_lines.shuffle(&mut rng);
		let shuffled_contents: Vec<u8> = shuffled_lines.into_iter().flat_map(|v| v).collect();
		let shard_num = extract_shard_number(&p).unwrap();
		let output_name = output_dir.clone().join(format!("shuffled_shard_{:08}.jsonl.zst", shard_num));
		write_mem_to_pathbuf(&shuffled_contents, &output_name).unwrap();
		pbar.inc(1);
	});
	println!("Finished phase II in {:?} secs", start_phase_2.elapsed().as_secs());


	println!("Shuffled in {:?} secs", start_main.elapsed().as_secs());
	Ok(())
}


fn extract_shard_number(shard_path: &PathBuf) -> Option<usize> {
    // Get the filename as a string
    let filename = shard_path.file_name()?.to_str()?;
    
    // Use a regex to extract the number
    let re = regex::Regex::new(r"intermed_shard_(\d{8})\.jsonl\.zst").ok()?;
    let captures = re.captures(filename)?;
    
    // Parse the captured number as usize
    captures.get(1)?.as_str().parse::<usize>().ok()
}


fn make_intermed_writer(shard_name: PathBuf) -> Result<Mutex<Encoder<'static, BufWriter<File>>>, Error> {
    // Make parent dir if not exists
    if let Some(parent_dir) = shard_name.parent() {
        if !parent_dir.exists() {
            create_dir_all(parent_dir).unwrap()
        }
    }
    let buf_writer = BufWriter::new(
        OpenOptions::new()
            .append(true)
            .create(true)
            .mode(0o644)
            .open(shard_name)
            .unwrap(),
    );

    let writer = Mutex::new(Encoder::new(buf_writer, 3).unwrap());
    Ok(writer)
}