
/*============================================================
=                            RESHARD                         =
============================================================*/
use zstd::Encoder;
use std::panic;
use rand::Rng;
use std::cmp::max;
use std::collections::HashMap;
use std::fs;
use std::fs::{create_dir_all, File, OpenOptions};
use std::io::{BufRead, BufWriter, Write};
use std::os::unix::fs::OpenOptionsExt;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

use anyhow::{ensure, Error, Result};
use rayon::current_num_threads;
use rayon::prelude::*;

use indicatif::ProgressBar;
use mj_io::{
    build_pbar, expand_dirs, get_output_filename, read_pathbuf_to_mem,
};

pub fn reshard(
    input_dir: &PathBuf,
    output_dir: &PathBuf,
    max_lines: usize,
    max_size: usize,
    subsample: f32,
    keep_dirs: bool,
    delete_after_read: bool,
) -> Result<(), Error> {
    let start_main = Instant::now();

    ensure!(
        max(max_lines, max_size) > 0,
        "Either max_lines or max_size must be provided!"
    );
    let max_lines = if max_lines == 0 {
        usize::MAX
    } else {
        max_lines
    };
    let max_size = if max_size == 0 { usize::MAX } else { max_size };

    let num_threads = current_num_threads();
    let all_files = expand_dirs(vec![input_dir.clone()], None).unwrap();
    let pbar = build_pbar(all_files.len(), "Files");
    let chunk_size = (all_files.len() + num_threads - 1) / num_threads;

    let chunks: Vec<Vec<PathBuf>> = if keep_dirs {
        // group by dir, and then maybe split up dirs if they're too big (to balance thread load)
        let mut dir_groups: HashMap<PathBuf, Vec<PathBuf>> = HashMap::new();

        // Group files by their parent directory
        for file in all_files {
            if let Some(parent) = file.parent().map(|p| p.to_path_buf()) {
                dir_groups.entry(parent).or_default().push(file);
            } else {
                // Handle files with no parent (e.g., root files)
                dir_groups.entry(PathBuf::from(".")).or_default().push(file);
            }
        }

        // Convert HashMap to Vec<Vec<PathBuf>> and split large groups
        dir_groups
            .into_values()
            .flat_map(|files| {
                if files.len() <= chunk_size {
                    vec![files]
                } else {
                    // Split large directories into multiple chunks
                    files.chunks(chunk_size).map(|c| c.to_vec()).collect()
                }
            })
            .collect()
    } else {
        all_files.chunks(chunk_size).map(|c| c.to_vec()).collect()
    };
    let out_num = AtomicUsize::new(0);
    chunks.par_iter().for_each(|chunk| {
        reshard_chunk(
            chunk,
            input_dir,
            output_dir,
            &out_num,
            max_lines,
            max_size,
            &pbar,
            subsample,
            keep_dirs,
            delete_after_read,
        )
        .unwrap();
    });

    println!(
        "Finished reshard in {:?} seconds | Wrote {:?} new shards",
        start_main.elapsed().as_secs(),
        out_num.fetch_add(0, Ordering::SeqCst)
    );
    Ok(())
}

fn reshard_chunk(
    chunk: &Vec<PathBuf>,
    input_dir: &PathBuf,
    output_dir: &PathBuf,
    out_num: &AtomicUsize,
    max_lines: usize,
    max_size: usize,
    pbar: &ProgressBar,
    subsample: f32,
    keep_dirs: bool,
    delete_after_read: bool,
) -> Result<(), Error> {
    // Quick assert: if keep dirs, all parents should be the same, and then we modify the output dir to be the "parent dir"
    let output_dir: PathBuf = if keep_dirs {
        let chunk_parents: Vec<Option<PathBuf>> = chunk
            .iter()
            .map(|file| file.parent().map(|p| p.to_path_buf()))
            .collect();
        let parent_example = &chunk_parents[0];
        assert!(chunk_parents.iter().all(|x| x == parent_example));
        get_output_filename(&parent_example.as_ref().unwrap(), input_dir, output_dir).unwrap()
    } else {
        output_dir.clone()
    };

    // faster strat: keep an open writer and append until full
    let get_new_writer = |out_num: &AtomicUsize| -> Result<Box<dyn std::io::Write>, Error> {
        let shard_id = out_num.fetch_add(1, Ordering::SeqCst);
        let shard = get_reshard_name(&output_dir, shard_id).unwrap();
        let writer = make_shard_writer(shard).unwrap();
        let auto_finisher = writer.auto_finish();
        Ok(Box::new(auto_finisher))
    };

    let mut rng = rand::rng();
    let mut writer = get_new_writer(out_num).unwrap();

    let mut cur_lines = 0;
    let mut cur_size = 0;
    for path in chunk {
        let data = match panic::catch_unwind(|| read_pathbuf_to_mem(path)) {
            Ok(Ok(data)) => data,
            Ok(Err(e)) => {
                eprintln!("Error reading file {:?}: {}", path, e);
                continue;
            }
            Err(_) => {
                eprintln!("Panic occurred while reading file {:?}", path);
                continue;
            }
        };        
        for line in data.lines() {
            if subsample == 0.0 || (subsample > 0.0 && rng.random::<f32>() < subsample) {
                let line = line.unwrap();
                let line = line.as_bytes();
                cur_lines += 1;
                cur_size += line.len();
                writer.write_all(&line).unwrap();
                writer.write(vec![b'\n'].as_slice()).unwrap();
                if cur_lines >= max_lines || cur_size >= max_size {
                    writer.flush().unwrap();
                    drop(writer);
                    writer = get_new_writer(out_num).unwrap();
                    cur_lines = 0;
                    cur_size = 0;
                }
            }
        }
        if cur_lines >= max_lines || cur_size >= max_size {
            writer.flush().unwrap();
            drop(writer);
            writer = get_new_writer(out_num).unwrap();
            cur_lines = 0;
            cur_size = 0;
        }
        pbar.inc(1);

        if delete_after_read {
            fs::remove_file(path).unwrap();
        }
    }

    writer.flush().unwrap();
    //writer.do_finish().unwrap();

    Ok(())
}

fn get_reshard_name(output_dir: &PathBuf, shard_id: usize) -> Result<PathBuf, Error> {
    let basename = PathBuf::from(format!("shard_{:08}.jsonl.zst", shard_id));
    let output_file = output_dir.clone().join(basename);

    Ok(output_file)
}

fn make_shard_writer(shard_name: PathBuf) -> Result<Encoder<'static, BufWriter<File>>, Error> {
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

    let writer = Encoder::new(buf_writer, 3).unwrap();
    Ok(writer)
}