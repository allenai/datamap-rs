// External crates


use std::os::unix::fs::OpenOptionsExt;
use std::fs::{File, create_dir_all, OpenOptions};
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::PathBuf;
use std::time::Instant;
use std::cmp::max;
use std::sync::atomic::{AtomicUsize, Ordering};
use rand::Rng;

use serde_json;
use serde_yaml;
use anyhow::{Error, Result, ensure};
use clap::{Parser, Subcommand};
use rayon::prelude::*;
use rayon::current_num_threads;
use mj_io::{expand_dirs, read_pathbuf_to_mem, write_mem_to_pathbuf, build_pbar};
use indicatif::ProgressBar;

//use crate::map_fxn::{CompiledProcessor, precompile_processor};
/*
Map Config layout:

pipeline: list with:
    [{name, 
     kwargs: {arg1: val1, ...}},
    ]

*/
/*============================================================
=                            ARGS                            =
============================================================*/


#[derive(Parser)]
#[clap(author, version, about, long_about = None)]
struct ArgParser {
    #[clap(subcommand)]
    command: Commands,

    #[arg(long, default_value_t=0)]
    threads: usize,
}


#[derive(Subcommand, Debug)]
enum Commands {
    #[clap(arg_required_else_help = true)]
    Map {
        #[arg(required=true, long)]
        input_dir: PathBuf,

        #[arg(required=true, long)]
        output_dir: PathBuf,

        #[arg(required=true, long)]
        config: PathBuf
    },


    Reshard {
        #[arg(required=true, long)]
        input_dir: PathBuf,

        #[arg(required=true, long)]
        output_dir: PathBuf,

        #[arg(long, default_value_t=0)]
        max_lines: usize,

        #[arg(long, default_value_t=0)]
        max_size: usize,

        #[arg(long, default_value_t=0.0)]
        subsample: f32,
    },

}


/*============================================================
=                            UTILITIES                       =
============================================================*/

fn get_output_file_name(input_dir: &PathBuf, output_dir: &PathBuf, input_file: &PathBuf) -> Result<PathBuf, Error> {
    let binding = input_file.clone();
    let basename = binding.strip_prefix(input_dir).unwrap();
    let output_file = output_dir.clone().join(basename);

    Ok(output_file)
}

fn parse_config(config: &PathBuf) -> Result<serde_json::Value, Error> {
    // Handle either .yaml or .json config and return a Json value

    let file = File::open(config).unwrap();
    let reader = BufReader::new(file);

    let ext = config.extension().unwrap().to_str().unwrap();
    let parsed_config : serde_json::Value = match ext {
        "json" => {
            serde_json::from_reader(reader).unwrap()
        },
        "yaml" => {
            let yaml_value: serde_yaml::Value = serde_yaml::from_reader(reader).unwrap();
            serde_json::to_value(yaml_value).unwrap()
        }
        _ => {
            return Err(Error::msg(format!("Weird config format: {:?}", config)));
        }
    };

    Ok(parsed_config.into())
}



/*============================================================
=                            GENERAL MAP                     =
============================================================*/


fn gen_map(input_dir: &PathBuf, output_dir: &PathBuf, config: &PathBuf) -> Result<(), Error> {
    /* Generic mapping/filtration function. 

    Processes each *.jsonl.* in input_dir and makes an identically named copy in output_dir
    with the changes specified in the config applied
    */



    Ok(())    
}





/*============================================================
=                            RESHARD                         =
============================================================*/

fn reshard(input_dir: &PathBuf, output_dir: &PathBuf, max_lines: usize, max_size: usize, subsample: f32) -> Result<(), Error> {
    let start_main = Instant::now();

    ensure!(max(max_lines, max_size) > 0, "Either max_lines or max_size must be provided!");
    let max_lines = if max_lines == 0 {
        usize::MAX
    } else {
        max_lines
    };
    let max_size = if max_size == 0 {
        usize::MAX
    } else {
        max_size
    };

    let num_threads = current_num_threads();    
    let all_files = expand_dirs(vec![input_dir.clone()], None).unwrap();
    let pbar = build_pbar(all_files.len(), "Files");
    let chunk_size = (all_files.len() + num_threads - 1) / num_threads;
    let chunks: Vec<Vec<PathBuf>> = all_files.chunks(chunk_size).map(|c| c.to_vec()).collect();
    let out_num = AtomicUsize::new(0);
    chunks.par_iter().for_each(|chunk| {
        reshard_chunk2(chunk, output_dir, &out_num, max_lines, max_size, &pbar, subsample).unwrap();
    });

    println!("Finished reshard in {:?} seconds | Wrote {:?} new shards", start_main.elapsed().as_secs(), out_num.fetch_add(0, Ordering::SeqCst));
    Ok(())
}


fn reshard_chunk(chunk: &Vec<PathBuf>, output_dir: &PathBuf, out_num: &AtomicUsize, max_lines: usize, max_size: usize, pbar: &ProgressBar, subsample: f32) -> Result<(), Error> {

    let mut cur_data: Vec<u8> = Vec::new();
    let mut cur_lines = 0;
    let mut cur_size = 0;
    for path in chunk {
        let data = read_pathbuf_to_mem(path).unwrap();
        for line in data.lines() {
            let line = line.unwrap();
            let line = line.as_bytes();


            cur_lines += 1;
            cur_size += line.len();
            cur_data.extend(line);
            cur_data.push(b'\n');
            if cur_lines >= max_lines || cur_size >= max_size {
                write_new_shard(cur_data, output_dir, out_num).unwrap();
                cur_data = Vec::new();
                cur_lines = 0;
                cur_size = 0;
            }
        }
        pbar.inc(1);
    }
    if cur_data.len() > 0 {
        write_new_shard(cur_data, output_dir, out_num).unwrap();
    }

    Ok(())
}

fn reshard_chunk2(chunk: &Vec<PathBuf>, output_dir: &PathBuf, out_num: &AtomicUsize, max_lines: usize, max_size: usize, pbar: &ProgressBar, subsample: f32) -> Result<(), Error> {
    // faster strat: keep an open writer and append until full
    let get_new_writer = |out_num: &AtomicUsize| -> Result<BufWriter<File>, Error> {
        let shard_id = out_num.fetch_add(1, Ordering::SeqCst);
        let shard = get_reshard_name(output_dir, shard_id).unwrap();
        let writer = make_shard_writer(shard).unwrap();        
        Ok(writer)
    };
    let mut rng = rand::rng();
    let mut writer = get_new_writer(out_num).unwrap();

    let mut cur_lines = 0;
    let mut cur_size = 0;
    for path in chunk {
        let data = read_pathbuf_to_mem(path).unwrap();
        for line in data.lines() {
            if subsample > 0.0 &&  rng.random::<f32>() < subsample {
                let line = line.unwrap();
                let line = line.as_bytes();
                cur_lines += 1;
                cur_size += line.len();
                writer.write_all(&line).unwrap();
                writer.write(vec![b'\n'].as_slice()).unwrap();
                if cur_lines >= max_lines || cur_size >= max_size {
                    writer.flush().unwrap();
                    writer = get_new_writer(out_num).unwrap();
                }
            }
        }
        pbar.inc(1);
    }

    writer.flush().unwrap();
    Ok(())
}


fn get_reshard_name(output_dir: &PathBuf, shard_id: usize) -> Result<PathBuf, Error> {
    let basename = PathBuf::from(format!("shard_{:08}.jsonl.zst", shard_id));
    let output_file = output_dir.clone().join(basename);

    Ok(output_file)
}

fn make_shard_writer(shard_name: PathBuf) -> Result<BufWriter<File>, Error> {

    // Make parent dir if not exists
    if let Some(parent_dir) = shard_name.parent() {
        if !parent_dir.exists() {
            create_dir_all(parent_dir).unwrap()
         }    
    }

    let writer = BufWriter::new(
            OpenOptions::new()
            .append(true)
            .create(true)
            .mode(0o644)
            .open(shard_name)
            .unwrap()
    );
    Ok(writer)
}

fn write_new_shard(data: Vec<u8>, output_dir: &PathBuf, out_num: &AtomicUsize) -> Result<(), Error> {
    let shard_id = out_num.fetch_add(1, Ordering::SeqCst);
    let basename = PathBuf::from(format!("shard_{:08}.jsonl.zst", shard_id));
    let output_file = output_dir.clone().join(basename);

    write_mem_to_pathbuf(&data, &output_file).unwrap();
    Ok(())
}



/*============================================================
=                            MAIN                            =
============================================================*/
#[allow(unreachable_patterns)]
fn main() {
    let args = ArgParser::parse();
    let threads = args.threads;
    if threads != 0 {
        std::env::set_var("RAYON_NUM_THREADS", threads.to_string());
    }

    let result = match &args.command {
        Commands::Map{input_dir, output_dir, config} => {
            gen_map(input_dir, output_dir, config)
        },


        Commands::Reshard{input_dir, output_dir, max_lines, max_size, subsample} => {
            reshard(input_dir, output_dir, *max_lines, *max_size, *subsample)
        },


        _ => {Ok(())}
    };
    result.unwrap();
}

