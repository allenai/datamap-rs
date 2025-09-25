// External crates

use std::fs;
use serde_json::Value;
use dashmap::DashMap;
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;

use anyhow::{Error, Result};
use clap::{Parser, Subcommand};
use rayon::prelude::*;
use serde_json;
use serde_yaml;

use mj_io::{
    build_pbar, expand_dirs, get_output_filename, read_pathbuf_to_mem, write_mem_to_pathbuf,
};
pub mod map_fxn;
pub mod partition;
pub mod utils;
pub mod groupfilter;
pub use map_fxn::DataProcessor;
use datamap_rs::map_fxn::PipelineProcessor;
use datamap_rs::partition::partition;
use datamap_rs::reshard::reshard;
use datamap_rs::groupfilter::{group, group_filter};

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

    #[arg(long, default_value_t = 0)]
    threads: usize,
}

#[derive(Subcommand, Debug)]
enum Commands {
    #[clap(arg_required_else_help = true)]
    Map {
        #[arg(required = true, long)]
        input_dir: PathBuf,

        #[arg(required = true, long)]
        output_dir: PathBuf,

        #[arg(required = true, long)]
        config: PathBuf,

        #[arg(long)]
        err_dir: Option<PathBuf>,

        #[arg(long)]
        delete_after_read: bool,
    },

    Reshard {
        #[arg(required = true, long)]
        input_dir: PathBuf,

        #[arg(required = true, long)]
        output_dir: PathBuf,

        #[arg(long, default_value_t = 0)]
        max_lines: usize,

        #[arg(long, default_value_t = 0)]
        max_size: usize,

        #[arg(long, default_value_t = 0.0)]
        subsample: f32,

        #[arg(long)]
        keep_dirs: bool,

        #[arg(long)]
        delete_after_read: bool,
    },

    Partition {
        #[arg(required = true, long)]
        input_dir: PathBuf,

        #[arg(required = true, long)]
        output_dir: PathBuf,

        #[arg(required = true, long)]
        config: PathBuf,
    },

    Group {
        #[arg(required = true, long)]
        input_dir: PathBuf,

        #[arg(required = true, long)]
        group_dir: PathBuf,

        #[arg(required = true, long)]
        config: PathBuf,        

        #[arg(long)]
        subext: Option<String>,
    },

    GroupFilter {
        #[arg(required = true, long)]
        input_dir: PathBuf,

        #[arg(required = true, long)]
        output_dir: PathBuf,

        #[arg(required = true, long)]
        config: PathBuf,        

    }

}

/*============================================================
=                            UTILITIES                       =
============================================================*/

fn parse_config(config: &PathBuf) -> Result<serde_json::Value, Error> {
    // Handle either .yaml or .json config and return a Json value

    let file = File::open(config).unwrap();
    let reader = BufReader::new(file);

    let ext = config.extension().unwrap().to_str().unwrap();
    let parsed_config: serde_json::Value = match ext {
        "json" => serde_json::from_reader(reader).unwrap(),
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

fn print_global_stats_stuff(
    start_time: Instant,
    global_timer: DashMap<usize, AtomicUsize>,
    global_filter: DashMap<usize, usize>,
    processor: &PipelineProcessor,
) -> () {
    // Timing info
    let total_time = start_time.elapsed().as_secs();
    let step_times: HashMap<usize, usize> = global_timer
        .into_iter()
        .map(|(k, v)| (k, v.into_inner()))
        .collect();
    let total_step_time = step_times.values().sum::<usize>();
    let step_fracs: HashMap<usize, f64> = step_times
        .iter()
        .map(|(k, v)| (*k, *v as f64 / total_step_time as f64))
        .collect();

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
        println!(
            "\t Spent {:.2}% of processing time in this step",
            step_time_pct * 100.0
        );

        let filter_entry = global_filter.get(&i).unwrap();
        let removed_in_this_step = filter_entry.value();

        let remaining_remove_pct =
            *removed_in_this_step as f32 / f32::max(0.0, remaining_docs as f32) * 100.0;
        let total_remove_pct =
            *removed_in_this_step as f32 / f32::max(0.0, total_docs as f32) * 100.0;
        remaining_docs -= removed_in_this_step;
        println!(
            "\t Removed {:?} docs | {:.2}% of remaining | {:.2}% of pool",
            removed_in_this_step, remaining_remove_pct, total_remove_pct
        );
    }

    println!("FINAL:");
    println!(
        "\t {:?} docs survived | {:.2}% of pool",
        remaining_docs,
        remaining_docs as f32 / f32::max(0.0, total_docs as f32) * 100.0
    );

    ()
}

/*============================================================
=                            GENERAL MAP                     =
============================================================*/

fn gen_map(
    input_dir: &PathBuf,
    output_dir: &PathBuf,
    config: &PathBuf,
    err_dir: Option<PathBuf>,
    delete_after_read: bool,
) -> Result<(), Error> {
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
        global_filter.insert(i, 0);
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
        let processor_clone = &processor;
        gen_map_single(
            p,
            input_dir,
            output_dir,
            err_file,
            &processor_clone,
            &global_timer,
            &global_filter,
            &err_count,
        )
        .unwrap();
        if delete_after_read {
            fs::remove_file(p).unwrap();
        }
        pbar.inc(1);
    });

    print_global_stats_stuff(start_main, global_timer, global_filter, &processor);
    Ok(())
}

fn gen_map_single(
    input_file: &PathBuf,
    input_dir: &PathBuf,
    output_dir: &PathBuf,
    err_file: Option<PathBuf>,
    processor: &PipelineProcessor,
    global_timer: &DashMap<usize, AtomicUsize>,
    global_filter: &DashMap<usize, usize>,
    err_count: &AtomicUsize,
) -> Result<(), Error> {
    /* Single-file mapping/filtration function

    Processes the contents of a single file, using file-centric mappers specified in the config and writes to output file
    */

    // Setup for processing
    let data = read_pathbuf_to_mem(input_file).unwrap();
    let lines: Vec<_> = data.lines().map(|el| el.unwrap()).collect();

    // Process data
    let (output_lines, err_lines, timing_info, filter_info) =
        processor.process_lines(lines).unwrap();
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
    timing_info.iter().for_each(|(k, v)| {
        global_timer
            .get(k)
            .unwrap()
            .fetch_add(*v as usize, Ordering::SeqCst);
    });

    filter_info.iter().for_each(|(k, v)| {
        global_filter.entry(*k).and_modify(|gv| *gv += v);
    });

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
        Commands::Map {
            input_dir,
            output_dir,
            config,
            err_dir,
            delete_after_read,

        } => gen_map(input_dir, output_dir, config, err_dir.clone(), *delete_after_read),
        Commands::Reshard {
            input_dir,
            output_dir,
            max_lines,
            max_size,
            subsample,
            keep_dirs,
            delete_after_read,
        } => reshard(
            input_dir,
            output_dir,
            *max_lines,
            *max_size,
            *subsample,
            *keep_dirs,
            *delete_after_read,
        ),
        Commands::Partition {
            input_dir,
            output_dir,
            config,
        } => partition(input_dir, output_dir, config),
        Commands::Group {
            input_dir,
            group_dir,
            config,
            subext
        } => group(input_dir, group_dir, config, subext.clone()),
        Commands::GroupFilter {
            input_dir,
            output_dir,
            config
        } => group_filter(input_dir, output_dir, config),
        _ => Ok(()),
    };
    result.unwrap();
}
