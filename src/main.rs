// External crates


use std::os::unix::fs::OpenOptionsExt;
use std::fs::{File, create_dir_all, OpenOptions};
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::collections::HashMap;
use crate::serde_json::Value;
use dashmap::DashMap;
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

use zstd::{Encoder};
use mj_io::{expand_dirs, read_pathbuf_to_mem, write_mem_to_pathbuf, build_pbar, get_output_filename};
use indicatif::ProgressBar;


pub mod map_fxn; 
pub mod group_map_fxn;
pub mod utils;
use datamap_rs::map_fxn::{PipelineProcessor};
pub use map_fxn::DataProcessor;
use datamap_rs::group_map_fxn::{GroupPipelineProcessor};
pub use group_map_fxn::GroupDataProcessor;


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

fn write_output_lines(output_values: Vec<Value>, output_file: &PathBuf) -> Result<(), Error> {
    if output_values.len() == 0 {
        return Ok(());
    } 

    let mut output_bytes: Vec<u8> = Vec::new();
    output_values.into_iter().for_each(|v| {
        output_bytes.extend(serde_json::to_vec(&v).unwrap());
        output_bytes.push(b'\n')
    });

    write_mem_to_pathbuf(&output_bytes, output_file)
}


fn print_global_stats_stuff(start_time: Instant, global_timer: DashMap<usize, AtomicUsize>, global_filter: DashMap<usize, AtomicUsize>, 
                            global_group_timer: DashMap<(usize, usize), AtomicUsize>, global_group_filter: DashMap<(usize, usize), AtomicUsize>,
                            processor: &PipelineProcessor, group_processor: &GroupPipelineProcessor) -> () {
    // Timing info
    let total_time = start_time.elapsed().as_secs();
    let step_times : HashMap<usize, usize> = global_timer.into_iter().map(|(k,v)| (k, v.into_inner())).collect();
    let group_step_times : HashMap<(usize, usize), usize> = global_group_timer.into_iter().map(|(k,v)| (k, v.into_inner())).collect();

    let mut total_step_time = 0;
    total_step_time += step_times.values().sum::<usize>();
    total_step_time += group_step_times.values().sum::<usize>();

    let step_fracs: HashMap<usize, f64> = step_times.iter().map(|(k,v)| (*k, *v as f64 / total_step_time as f64)).collect();
    let group_step_fracs: HashMap<(usize, usize), f64> = group_step_times.iter().map(|(k,v)| (*k, *v as f64 / total_step_time as f64)).collect();


    // Filtering info 
    let step_docs : HashMap<usize, usize> = global_filter.into_iter().map(|(k,v)| (k, v.into_inner())).collect();
    let group_step_docs : HashMap<(usize, usize), usize> = global_group_filter.into_iter().map(|(k,v)| (k, v.into_inner())).collect();
    let mut total_docs = 0;
    total_docs += step_docs.values().sum::<usize>();
    total_docs += group_step_docs.values().sum::<usize>();
    let mut remaining_docs: usize = total_docs;

    // Print things
    println!("Finishing map in {:?} seconds", total_time);
    println!("Processed {:?} total documents", total_docs);
    if processor.pipeline.len() > 0 {
        println!("-------------------------------------------");        
        println!("---------SINGLE DOC PIPELINE---------------");
        println!("-------------------------------------------");    
        for (i, el) in processor.pipeline.iter().enumerate() {
            println!("Step {:?} | {:?}", i, el);
            let step_time_pct = step_fracs.get(&i).unwrap();
            println!("\t Spent {:.2}% of processing time in this step", step_time_pct * 100.0);

            let filter_entry = step_docs.get(&i).unwrap();
            let removed_in_this_step = filter_entry;

            let remaining_remove_pct = *removed_in_this_step as f32 / f32::max(0.0, remaining_docs as f32) * 100.0;
            let total_remove_pct = *removed_in_this_step as f32 / f32::max(0.0, total_docs as f32) * 100.0;
            remaining_docs -= removed_in_this_step;
            println!("\t Removed {:?} docs | {:.2}% of remaining | {:.2}% of pool", removed_in_this_step, remaining_remove_pct, total_remove_pct);
        }
    }

    if group_processor.group_pipelines.len() > 0 {
        println!("-------------------------------------------");        
        println!("---------SINGLE DOC PIPELINE---------------");
        println!("-------------------------------------------");   
        for (pipeline_num, pipeline) in group_processor.group_pipelines.iter().enumerate() {
            for (step_num, step) in pipeline.iter().enumerate() {
                println!("Group Step ({:?}, {:?}) | {:?}", pipeline_num, step_num, step);
                let step_time_pct = group_step_fracs.get(&(pipeline_num, step_num)).unwrap();
                println!("\t Spet {:.2}% of processing time in this step", step_time_pct * 100.0);

                
                let filter_entry = group_step_docs.get(&(pipeline_num, step_num)).unwrap();
                let removed_in_this_step = filter_entry;

                let remaining_remove_pct = *removed_in_this_step as f32 / f32::max(0.0, remaining_docs as f32) * 100.0;
                let total_remove_pct = *removed_in_this_step as f32 / f32::max(0.0, total_docs as f32) * 100.0;
                remaining_docs -= removed_in_this_step;
                println!("\t Removed {:?} docs | {:.2}% of remaining | {:.2}% of pool", removed_in_this_step, remaining_remove_pct, total_remove_pct);       
            }     
        }        
    }

    println!("FINAL:");
    println!("\t {:?} docs survived | {:.2}% of pool", remaining_docs, remaining_docs as f32 / f32::max(0.0, total_docs as f32) * 100.0);

    ()
}

fn agg_global<T>(local: &HashMap<T, usize>, global: &DashMap<T, AtomicUsize>) -> ()
where
    T: Eq + std::hash::Hash + Clone,
{
    for (k, v) in local {
        global
            .entry(k.clone())
            .or_insert(AtomicUsize::new(0))
            .fetch_add(*v, Ordering::SeqCst);
    }
}


/*============================================================
=                            GENERAL MAP                     =
============================================================*/



fn gen_map(input_dir: &PathBuf, output_dir: &PathBuf, config: &PathBuf, err_dir: Option<PathBuf>) -> Result<(), Error> {
    /* Generic mapping/filtration function. 

    Processes each *.jsonl.* in input_dir and makes an identically named copy in output_dir
    with the changes specified in the config applied
    */

    // Setup data handlers
    let start_main = Instant::now();
    let all_files = expand_dirs(vec![input_dir.clone()], None).unwrap();
    let json_config = parse_config(config).unwrap();
    let processor = PipelineProcessor::new(&json_config).unwrap();
    let group_processor = GroupPipelineProcessor::new(&json_config).unwrap();

    // Setup logging utils
    let global_timer: DashMap<usize, AtomicUsize> = DashMap::new();
    let global_filter: DashMap<usize, AtomicUsize> = DashMap::new();
    let global_group_timer: DashMap<(usize, usize), AtomicUsize> = DashMap::new();
    let global_group_filter: DashMap<(usize, usize), AtomicUsize> = DashMap::new();


    // Loop over input files
    let pbar = build_pbar(all_files.len(), "Files");
    all_files.par_iter().for_each(|p| {
        //let output_file = get_output_filename(p, input_dir, output_dir).unwrap();        
        let err_file: Option<PathBuf> = if let Some(err_dir_real) = &err_dir {
            Some(get_output_filename(p, input_dir, &err_dir_real).unwrap())
        } else {
            None
        };
        gen_map_single(p, input_dir, output_dir, err_file, &processor, &group_processor, 
                       &global_timer, &global_filter, &global_group_timer, &global_group_filter).unwrap();
        pbar.inc(1);
    });

    print_global_stats_stuff(start_main, global_timer, global_filter, global_group_timer, global_group_filter, &processor, &group_processor);
    Ok(())    
}


fn gen_map_single(input_file: &PathBuf, input_dir: &PathBuf, output_dir: &PathBuf, err_file: Option<PathBuf>, processor: &PipelineProcessor, 
                  group_processor: &GroupPipelineProcessor,
                  global_timer: &DashMap<usize, AtomicUsize>, global_filter: &DashMap<usize, AtomicUsize>,
                  global_group_timer: &DashMap<(usize, usize), AtomicUsize>, global_group_filter: &DashMap<(usize, usize), AtomicUsize>) -> Result<(), Error> {
    /* Single-file mapping/filtration function

    Processes the contents of a single file, using file-centric mappers specified in the config and writes to output file
    */
    
    
    // Setup for processing
    let data = read_pathbuf_to_mem(input_file).unwrap();
    let values: Vec<Value> = data.lines().map(|el| serde_json::from_str(&el.unwrap()).unwrap()).collect();

    // Process data (individual line pipeline step) and pass to group processors
    let (mut output_lines, err_lines, timing_info, filter_info) = processor.process_lines(values).unwrap();    
    let passed_lines : Vec<Value> = if let Some(passed_lines) = output_lines.remove(&usize::MAX) {
        passed_lines
    } else {
        vec![]
    };


    // Now do the "group" pipeline, if it exists
    let (group_output, group_errs, group_timing_info, group_filter_info) = group_processor.process_lines(passed_lines).unwrap();


    // Now save everything:
    // - output_lines has the "single document" filtered things | keys here single_doc steps
    // - err_lines + group_errs has all errored documents 
    // - group_output has (group_pipeline_num, step_num)

    // Save errs first
    if let Some(err_file_real) = err_file {
        let mut err_bytes: Vec<u8> = Vec::new();
        err_lines.into_iter().for_each(|line| {
            err_bytes.extend(line.as_bytes());
            err_bytes.push(b'\n');
        });
        group_errs.into_iter().for_each(|val| {
            err_bytes.extend(serde_json::to_vec(&val).unwrap());
            err_bytes.push(b'\n');
        });
        if err_bytes.len() > 0 {
            write_mem_to_pathbuf(&err_bytes, &err_file_real).unwrap();
        }

    }

    // Then save all the outputs --> single filter first, then multi-filter
    output_lines.into_iter().for_each(|(k,v)| {
        let step_output_dir = output_dir.clone().join(format!("step_{:02}", k));
        let output_file = get_output_filename(input_file, input_dir, &step_output_dir).unwrap();
        write_output_lines(v, &output_file).unwrap();
    });
    group_output.into_iter().for_each(|(k, v)| {
        let step_output_dir = if k.0 < usize::MAX {
            output_dir.clone().join(format!("group_{:02}_step_{:02}", k.0, k.1))
        } else {
            output_dir.clone().join("step_final")
        };
        let output_file = get_output_filename(input_file, input_dir, &step_output_dir).unwrap();
        write_output_lines(v, &output_file).unwrap();
    });

    // Bubble up logging stuff to outside thread
    agg_global::<usize>(&timing_info, global_timer);
    agg_global::<usize>(&filter_info, global_filter);
    agg_global::<(usize, usize)>(&group_timing_info, global_group_timer);
    agg_global::<(usize, usize)>(&group_filter_info, global_group_filter);


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
        reshard_chunk(chunk, output_dir, &out_num, max_lines, max_size, &pbar, subsample).unwrap();
    });

    println!("Finished reshard in {:?} seconds | Wrote {:?} new shards", start_main.elapsed().as_secs(), out_num.fetch_add(0, Ordering::SeqCst));
    Ok(())
}


fn reshard_chunk(chunk: &Vec<PathBuf>, output_dir: &PathBuf, out_num: &AtomicUsize, max_lines: usize, max_size: usize, pbar: &ProgressBar, subsample: f32) -> Result<(), Error> {
    // faster strat: keep an open writer and append until full
    let get_new_writer = |out_num: &AtomicUsize| -> Result<Encoder<BufWriter<File>>, Error> {
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
            if subsample == 0.0 || (subsample > 0.0 &&  rng.random::<f32>() < subsample) {
                let line = line.unwrap();
                let line = line.as_bytes();
                cur_lines += 1;
                cur_size += line.len();
                writer.write_all(&line).unwrap();
                writer.write(vec![b'\n'].as_slice()).unwrap();
                if cur_lines >= max_lines || cur_size >= max_size {
                    writer.flush().unwrap();
                    writer.do_finish().unwrap();
                    writer = get_new_writer(out_num).unwrap();
                    cur_lines = 0; 
                    cur_size = 0;
                }
            }
        }
        pbar.inc(1);
    }

    writer.flush().unwrap();
    writer.do_finish().unwrap();

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
            .unwrap()
    );

    let writer = Encoder::new(buf_writer, 3)
        .unwrap();
    Ok(writer)
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
        Commands::Map{input_dir, output_dir, config, err_dir} => {
            gen_map(input_dir, output_dir, config, err_dir.clone())
        },

        Commands::Reshard{input_dir, output_dir, max_lines, max_size, subsample} => {
            reshard(input_dir, output_dir, *max_lines, *max_size, *subsample)
        },

        _ => {Ok(())}
    };
    result.unwrap();
}

