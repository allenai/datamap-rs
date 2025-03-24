// External crates


use std::collections::HashMap;
use crate::serde_json::Value;
use dashmap::DashMap;
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
use mj_io::{expand_dirs, read_pathbuf_to_mem, write_mem_to_pathbuf, build_pbar, get_output_filename};
use indicatif::ProgressBar;


pub mod map_fxn; 
pub mod utils;
use datamap_rs::map_fxn::{PipelineProcessor};
pub use map_fxn::DataProcessor;
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
        config: PathBuf,

        #[arg(long)]
        err_dir: Option<PathBuf>,


        #[arg(long)]
        debug_stash: Option<PathBuf>
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
    },

}


/*============================================================
=                            UTILITIES                       =
============================================================*/


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


fn save_debug_stuff(debug_file: &PathBuf, debug_collector: DashMap<usize, Vec<Value>>) -> Result<(), Error> {
    let debug_data : HashMap<usize, Vec<Value>> = debug_collector.into_iter().map(|(k, v)| (k, v)).collect();
    let debug_bytes = serde_json::to_vec(&debug_data).unwrap();
    write_mem_to_pathbuf(&debug_bytes, debug_file).unwrap();
    Ok(())
}

fn print_global_stats_stuff(start_time: Instant, global_timer: DashMap<usize, AtomicUsize>, global_filter: DashMap<usize, usize>, processor: &PipelineProcessor) -> () {
    // Timing info
    let total_time = start_time.elapsed().as_secs();
    let step_times: HashMap<usize, usize> = global_timer.into_iter().map(|(k,v)| (k, v.into_inner())).collect();
    let total_step_time = step_times.values().sum::<usize>();
    let step_fracs: HashMap<usize, f64> = step_times.iter().map(|(k,v)| (*k, *v as f64 / total_step_time as f64)).collect();

    // Filtering info 
    let total_docs: usize = global_filter.iter().map(|e| *e.value()).sum::<usize>();
    let mut remaining_docs: usize = total_docs;

    // Print things
    println!("Finishing map in {:?} seconds", total_time);
    println!("Processed {:?} total documents", total_docs);
    println!("-------------------------------------------");
    for (i, el) in processor.pipeline.iter().enumerate() {
        println!("Step {:?} | {:?}", i, el);

        let step_time_pct = step_fracs.get(&i).unwrap();
        println!("\t Spent {:.2}% of processing time in this step", step_time_pct * 100.0);

        let filter_entry = global_filter.get(&i).unwrap();
        let removed_in_this_step = filter_entry.value();

        let remaining_remove_pct = *removed_in_this_step as f32 / f32::max(0.0, remaining_docs as f32) * 100.0;
        let total_remove_pct = *removed_in_this_step as f32 / f32::max(0.0, total_docs as f32) * 100.0;
        remaining_docs -= removed_in_this_step;
        println!("\t Removed {:?} docs | {:.2}% of pool | {:.2}% of remaining", removed_in_this_step, remaining_remove_pct, total_remove_pct);
    }

    println!("FINAL:");
    println!("\t {:?} docs survived | {:.2} of pool", remaining_docs, remaining_docs as f32 / f32::max(0.0, total_docs as f32) * 100.0);

    ()
}


/*============================================================
=                            GENERAL MAP                     =
============================================================*/


fn gen_map(input_dir: &PathBuf, output_dir: &PathBuf, config: &PathBuf, err_dir: Option<PathBuf>, debug_stash: Option<PathBuf>) -> Result<(), Error> {
    /* Generic mapping/filtration function. 

    Processes each *.jsonl.* in input_dir and makes an identically named copy in output_dir
    with the changes specified in the config applied
    */

    // Setup data handlers
    let start_main = Instant::now();
    let all_files = expand_dirs(vec![input_dir.clone()], None).unwrap();
    let json_config = parse_config(config).unwrap();
    let processor = PipelineProcessor::new(&json_config).unwrap();

    // Setup logging utils
    let global_timer: DashMap<usize, AtomicUsize> = DashMap::new();
    let global_filter: DashMap<usize, usize> = DashMap::new();
    let debug_collector: Option<DashMap<usize, Vec<Value>>>  = if let Some(ref _x) = debug_stash {
        Some(DashMap::new())
    } else {
        None
    };
    for i in 0..processor.pipeline.len() {
        global_timer.insert(i, AtomicUsize::new(0));
        global_filter.insert(i,0);
    }
    global_filter.insert(usize::MAX, 0);
    let err_count: AtomicUsize = AtomicUsize::new(0);

    // Loop over files
    let pbar = build_pbar(all_files.len(), "Files");
    all_files.par_iter().for_each(|p| {
        let output_file = get_output_filename(p, input_dir, output_dir).unwrap();        
        let err_file: Option<PathBuf> = if let Some(err_dir_real) = &err_dir {
            Some(get_output_filename(p, input_dir, &err_dir_real).unwrap())
        } else {
            None
        };
        gen_map_single(p, &output_file, err_file, &processor, &global_timer, &global_filter, &debug_collector, &err_count).unwrap();
        pbar.inc(1);
    });

    // Finalize logging
    if !debug_stash.is_none() {
        save_debug_stuff(&debug_stash.unwrap(), debug_collector.unwrap()).unwrap();
    }
    print_global_stats_stuff(start_main, global_timer, global_filter, &processor);

    Ok(())    
}



fn gen_map_single(input_file: &PathBuf, output_file: &PathBuf, err_file: Option<PathBuf>, processor: &PipelineProcessor, 
                  global_timer: &DashMap<usize, AtomicUsize>, global_filter: &DashMap<usize, usize>,
                  debug_collector: &Option<DashMap<usize, Vec<Value>>>, err_count: &AtomicUsize) -> Result<(), Error> {
    /* Single-file mapping/filtration function

    Processes the contents of a single file, using file-centric mappers specified in the config and writes to output file
    */

    // Setup for processing
    let data = read_pathbuf_to_mem(input_file).unwrap();
    let lines: Vec<_> = data.lines().map(|el| el.unwrap()).collect();
    let debug_mode = if let Some(ref _x) = debug_collector {true} else {false};

    // Process data
    let (output_lines, err_lines, timing_info, filter_info) = processor.process_lines(lines, debug_mode).unwrap();
    let err_lines_len = err_lines.len();
    let mut output_bytes: Vec<u8> = Vec::new();
    output_lines.into_iter().for_each(|(k, v)| {
        if k == usize::MAX {
            for line in  v {
                output_bytes.extend(serde_json::to_vec(&line).unwrap());
                output_bytes.push(b'\n')
            }
        } else if let Some(debug_data) = debug_collector {        
            debug_data.entry(k).or_default().extend(v)
        }
    });

    // Save outputs
    if output_bytes.len() > 0 {
        write_mem_to_pathbuf(&output_bytes, output_file).unwrap();
    }

    if let Some(err_file_real) = err_file {
        let mut err_bytes: Vec<u8> = Vec::new();
        err_lines.into_iter().for_each(|line| {
            err_bytes.extend(line.as_bytes());
            err_bytes.push(b'\n');
        });
        if err_bytes.len() > 0 {
            write_mem_to_pathbuf(&err_bytes, &err_file_real).unwrap();
        }
    }


    // Do logging stuff
    let _ = err_count.fetch_add(err_lines_len, Ordering::SeqCst);
    timing_info.iter().for_each(|(k,v)| {
        global_timer.get(k).unwrap().fetch_add(*v as usize, Ordering::SeqCst);
    });

    filter_info.iter().for_each(|(k, v)| {
        global_filter.entry(*k)
            .and_modify(|gv| *gv += v);
    });


    Ok(())
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
    let pbar = build_pbar(all_files.len(), "Files");
    let chunk_size = (all_files.len() + num_threads - 1) / num_threads;
    let chunks: Vec<Vec<PathBuf>> = all_files.chunks(chunk_size).map(|c| c.to_vec()).collect();
    let out_num = AtomicUsize::new(0);
    chunks.par_iter().for_each(|chunk| {
        reshard_chunk(chunk, output_dir, &out_num, max_lines, max_size, &pbar).unwrap();
    });

    println!("Finished reshard in {:?} seconds | Wrote {:?} new shards", start_main.elapsed().as_secs(), out_num.fetch_add(0, Ordering::SeqCst));
    Ok(())
}


fn reshard_chunk(chunk: &Vec<PathBuf>, output_dir: &PathBuf, out_num: &AtomicUsize, max_lines: usize, max_size: usize, pbar: &ProgressBar) -> Result<(), Error> {

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
        Commands::Map{input_dir, output_dir, config, err_dir, debug_stash} => {
            gen_map(input_dir, output_dir, config, err_dir.clone(), debug_stash.clone())
        },


        Commands::Reshard{input_dir, output_dir, max_lines, max_size} => {
            reshard(input_dir, output_dir, *max_lines, *max_size)
        },


        _ => {Ok(())}
    };
    result.unwrap();
}

