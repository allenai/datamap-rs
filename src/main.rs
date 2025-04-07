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

use zstd::Encoder;
use mj_io::{expand_dirs, read_pathbuf_to_mem, write_mem_to_pathbuf, build_pbar, get_output_filename};
use indicatif::ProgressBar;


pub mod map_fxn;
pub mod utils;
use datamap_rs::map_fxn::PipelineProcessor;
pub use map_fxn::DataProcessor;
use utils::get_tokenizer;

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

    PartitionByLength {
        #[arg(required=true, long)]
        input_dir: PathBuf,

        #[arg(required=true, long)]
        output_dir: PathBuf,

        //supported tokenizers:
        // - o200k_base: GPT-4o models, o1 models
        // - cl100k_base: ChatGPT models, text-embedding-ada-002
        // - p50k_base: Code models, text-davinci-002, text-davinci-003
        // - p50k_edit: Use for edit models like text-davinci-edit-001, code-davinci-edit-001
        // - r50k_base/gpt2: GPT-3 models like davinci

        #[arg(long, default_value_t=String::from("cl100k_base"))]
        tokenizer_name: String,

        #[arg(long, default_value_t=4096)]
        min_length: usize,

        #[arg(long, default_value_t=1_048_576)]
        max_length: usize,
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
        println!("\t Removed {:?} docs | {:.2}% of remaining | {:.2}% of pool", removed_in_this_step, remaining_remove_pct, total_remove_pct);
    }

    println!("FINAL:");
    println!("\t {:?} docs survived | {:.2}% of pool", remaining_docs, remaining_docs as f32 / f32::max(0.0, total_docs as f32) * 100.0);

    ()
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

    // Setup logging utils
    let global_timer: DashMap<usize, AtomicUsize> = DashMap::new();
    let global_filter: DashMap<usize, usize> = DashMap::new();
    for i in 0..processor.pipeline.len() {
        global_timer.insert(i, AtomicUsize::new(0));
        global_filter.insert(i,0);
    }
    global_filter.insert(usize::MAX, 0);
    let err_count: AtomicUsize = AtomicUsize::new(0);

    // Loop over input files
    let pbar = build_pbar(all_files.len(), "Files");
    all_files.par_iter().for_each(|p| {
        //let output_file = get_output_filename(p, input_dir, output_dir).unwrap();
        let err_file: Option<PathBuf> = if let Some(err_dir_real) = &err_dir {
            Some(get_output_filename(p, input_dir, &err_dir_real).unwrap())
        } else {
            None
        };
        gen_map_single(p, input_dir, output_dir, err_file, &processor, &global_timer, &global_filter, &err_count).unwrap();
        pbar.inc(1);
    });

    print_global_stats_stuff(start_main, global_timer, global_filter, &processor);
    Ok(())
}



fn gen_map_single(input_file: &PathBuf, input_dir: &PathBuf, output_dir: &PathBuf, err_file: Option<PathBuf>, processor: &PipelineProcessor,
                  global_timer: &DashMap<usize, AtomicUsize>, global_filter: &DashMap<usize, usize>,
                  err_count: &AtomicUsize) -> Result<(), Error> {
    /* Single-file mapping/filtration function

    Processes the contents of a single file, using file-centric mappers specified in the config and writes to output file
    */

    // Setup for processing
    let data = read_pathbuf_to_mem(input_file).unwrap();
    let lines: Vec<_> = data.lines().map(|el| el.unwrap()).collect();

    // Process data
    let (output_lines, err_lines, timing_info, filter_info) = processor.process_lines(lines).unwrap();
    let err_lines_len = err_lines.len();

    output_lines.into_iter().for_each(|(k, v)| {
        let step_output_dir = if k < usize::MAX {
            output_dir.clone().join(format!("step_{:02}", k))
        } else {
            output_dir.clone().join("step_final")
        };
        let output_file = get_output_filename(input_file, input_dir, &step_output_dir).unwrap();
        write_output_lines(v, &output_file).unwrap();
    });


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

    let num_threads: usize = current_num_threads();
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
=                    PARTITION BY LENGTH                     =
============================================================*/

fn partition_by_length(
    input_dir: &PathBuf,
    output_dir: &PathBuf,
    tokenizer_name: String,
    min_length: usize,
    max_length: usize,
) -> Result<(), Error> {

    // Setup data handlers
    let start_main = Instant::now();
    let all_files = expand_dirs(vec![input_dir.clone()], None).unwrap();

    // Loop over input files
    let pbar = build_pbar(all_files.len(), "Files");

    all_files.par_iter().for_each(|p| {
        partition_by_length_single(
            p,
            input_dir,
            output_dir,
            tokenizer_name.clone(),
            min_length,
            max_length
        ).unwrap();
        pbar.inc(1);
    });

    println!("Finished partition by length in {:?} seconds", start_main.elapsed().as_secs());
    Ok(())
}


fn partition_by_length_single(
    input_file: &PathBuf,
    input_dir: &PathBuf,
    output_dir: &PathBuf,
    tokenizer_name: String,
    min_length: usize,
    max_length: usize,
) -> Result<(), Error> {


    let data = read_pathbuf_to_mem(input_file).unwrap();
    let tokenizer = get_tokenizer(&tokenizer_name).unwrap();

    let log_min_length = (min_length as f32).log2().floor() as usize;
    let log_max_length = (max_length as f32).log2().ceil() as usize;

    // create a dictionary of vectors of lines, each key is a power of 2 from min_length to max_length
    let mut lines_by_length = HashMap::new();
    for i in log_min_length..=log_max_length {
        lines_by_length.insert(i, Vec::new());
    }

    for line in data.lines() {
        // read the line, decode it to json dict
        let line = line.unwrap();
        let mut row: serde_json::Value = serde_json::from_str(&line).unwrap();

        let text = row["text"].as_str().unwrap();

        // check if length of the text in characters is at least min_length;
        // if not, skip this row
        if text.len() < min_length {
            continue;
        }

        // get the length of the text in tokens
        let length_in_tokens = tokenizer.encode_with_special_tokens(text).len();

        let closest_power_of_2 = match (length_in_tokens as f32).log2() {
             x if x > log_max_length as f32 => log_max_length,  // we cap at max_length
             x => x.floor() as usize,   // round down to nearest integer
        };

        if closest_power_of_2 < log_min_length {
            continue;  // we skip if the length is less than min_length
        }

        // add the length of the row to metadata
        // Check if "metadata" exists and is an object, if not create it
        if !row.get("metadata").map_or(false, |m| m.is_object()) {
            row["metadata"] = serde_json::json!({});
        }

        // Add a new key to metadata (for example, "new_key" with value "new_value")
        row["metadata"][format!("len_{}", tokenizer_name)] = serde_json::json!(length_in_tokens);

        // add to the correct bucket
        lines_by_length.get_mut(&closest_power_of_2).unwrap().push(row);
    }

    // write the lines to the output files
    for (length, rows) in lines_by_length {
        let suffix = format!("length_2e{:02}", length);
        let output_file = get_output_filename(
            input_file,
            input_dir,
            &output_dir.join(suffix)
        ).unwrap();
        write_output_lines(rows, &output_file).unwrap();
    }

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
        Commands::Map{input_dir, output_dir, config, err_dir} => {
            gen_map(input_dir, output_dir, config, err_dir.clone())
        },
        Commands::Reshard{input_dir, output_dir, max_lines, max_size, subsample} => {
            reshard(input_dir, output_dir, *max_lines, *max_size, *subsample)
        },
        Commands::PartitionByLength{
            input_dir,
            output_dir,
            tokenizer_name,
            min_length,
            max_length
        } => {
            partition_by_length(
                input_dir,
                output_dir,
                tokenizer_name.clone(),
                min_length.clone(),
                max_length.clone()
            )
        },
        _ => {Ok(())}
    };
    result.unwrap();
}
