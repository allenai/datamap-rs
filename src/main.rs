// External crates


use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::PathBuf;
use std::time::Instant;
use std::cmp::max;
use std::sync::atomic::{AtomicUsize, Ordering};

use serde_json;
use serde_yaml;
use anyhow::{Error, Result, ensure};
use clap::{Parser, Subcommand};
use rayon::prelude::*;
use rayon::current_num_threads;
use mj_io::{expand_dirs, read_pathbuf_to_mem, write_mem_to_pathbuf, build_pbar};
use rand::Rng;
use phf::phf_map;
use uuid::Uuid;


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
    }



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

static MAP_FXNS : phf::Map<&'static str, fn(String, &serde_json::Value) -> Result<String, Error>> = phf_map! {
    "subsample" => subsample_line,
    "len_filter" => len_filter_line,
    "add_id" => add_id_line
};


fn gen_map(input_dir: &PathBuf, output_dir: &PathBuf, config: &PathBuf) -> Result<(), Error> {
    let start_main = Instant::now();

    let all_files = expand_dirs(vec![input_dir.clone()], None).unwrap();
    let json_config = parse_config(config).unwrap();
    

    let pbar = build_pbar(all_files.len(), "Files");
    all_files.par_iter().for_each(|p| {
        let output_file = get_output_file_name(input_dir, output_dir, p).unwrap();
        gen_map_single(p, &output_file, &json_config).unwrap();
        pbar.inc(1);
    });


    println!("Finishing map({:?}) in {:?} seconds", json_config.get("function").unwrap().as_str().unwrap(), start_main.elapsed().as_secs());

    Ok(())    
}



fn gen_map_single(input_file: &PathBuf, output_file: &PathBuf, json_config: &serde_json::Value) -> Result<(), Error> {
    let data = read_pathbuf_to_mem(input_file).unwrap();
    let mut output_bytes: Vec<u8> = Vec::new();

    let fxn_name = json_config.get("function").unwrap().as_str().unwrap();
    let process_line = MAP_FXNS[fxn_name];

    for line in data.lines() {
        let line = line.unwrap();
        let output_line = process_line(line, json_config).unwrap();
        if output_line.len() > 0 {
            output_bytes.extend(output_line.as_bytes());
            output_bytes.push(b'\n');
        }
    }

    if output_bytes.len() > 0 {
        write_mem_to_pathbuf(&output_bytes, output_file).unwrap();
    }

    Ok(())
}



/*============================================================
=                            SUBSAMPLE                       =
============================================================*/



fn subsample_line(line: String, config: &serde_json::Value) -> Result<String, Error> {
    let mut rng = rand::rng();
    let random_float = rng.random::<f64>();
    let ratio = config.get("ratio").unwrap().as_f64().unwrap();

    let output = if random_float <= ratio {
        line
    } else {
        String::new()
    };

    Ok(output)
}


/*============================================================
=                            LEN FILTER                      =
============================================================*/

fn len_filter_line(line: String, config: &serde_json::Value) -> Result<String, Error> {
    let min_len = match config.get("min_len") {
        Some(min_len) => min_len.as_u64().unwrap() as usize,
        None => 0
    };

    let max_len = match config.get("max_len") {
        Some(max_len) => max_len.as_u64().unwrap() as usize,
        None => usize::MAX
    };

    let json_obj : serde_json::Value = serde_json::from_str(&line).unwrap();
    let textlen = json_obj.get("text").unwrap().as_str().unwrap().len();

    let output = if textlen <= max_len && textlen >= min_len {
        line
    } else {
        String::new()
    };
    Ok(output)

}


/*============================================================
=                            ADD ID                          =
============================================================*/

fn add_id_line(line: String, config: &serde_json::Value) -> Result<String, Error> {
    let mut json_obj : serde_json::Value = serde_json::from_str(&line).unwrap();
    let id_key = match config.get("id_key") {
        Some(id_key) => id_key.to_string(),
        None => "id".to_string()
    };

    json_obj[id_key] = serde_json::Value::String(Uuid::new_v4().to_string());

    let new_line = json_obj.to_string();
    Ok(new_line)
}



/*============================================================
=                            RESHARD                         =
============================================================*/

fn reshard(input_dir: &PathBuf, output_dir: &PathBuf, max_lines: usize, max_size: usize) -> Result<(), Error> {
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
    let chunk_size = (all_files.len() + num_threads - 1) / num_threads;
    let chunks: Vec<Vec<PathBuf>> = all_files.chunks(chunk_size).map(|c| c.to_vec()).collect();
    let out_num = AtomicUsize::new(0);
    chunks.par_iter().for_each(|chunk| {
        reshard_chunk(chunk, output_dir, &out_num, max_lines, max_size).unwrap();
    });

    println!("Finished reshard in {:?} seconds | Wrote {:?} new shards", start_main.elapsed().as_secs(), out_num.fetch_add(0, Ordering::SeqCst));
    Ok(())
}


fn reshard_chunk(chunk: &Vec<PathBuf>, output_dir: &PathBuf, out_num: &AtomicUsize, max_lines: usize, max_size: usize) -> Result<(), Error> {

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
    }
    if cur_data.len() > 0 {
        write_new_shard(cur_data, output_dir, out_num).unwrap();
    }

    Ok(())
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
        Commands::Reshard{input_dir, output_dir, max_lines, max_size} => {
            reshard(input_dir, output_dir, *max_lines, *max_size)
        }
        _ => {Ok(())}
    };
    result.unwrap();
}

