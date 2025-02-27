// External crates


use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::PathBuf;
use std::time::Instant;

use serde_json;
use serde_yaml;
use anyhow::{Error, Result};
use clap::{Parser, Subcommand};
use rayon::prelude::*;
use mj_io::{expand_dirs, read_pathbuf_to_mem, write_mem_to_pathbuf, build_pbar};
use rand::Rng;
use phf::phf_map;



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
        }
        _ => {Ok(())}
    };
    result.unwrap();
}

