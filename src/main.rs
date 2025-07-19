// External crates

use serde_json::json;
use dashmap::DashSet;
use std::panic;
use crate::serde_json::Value;
use dashmap::DashMap;
use rand::Rng;
use std::cmp::max;
use std::collections::HashMap;
use std::fs;
use std::fs::{create_dir_all, File, OpenOptions};
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::os::unix::fs::OpenOptionsExt;
use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Instant;
use tiktoken_rs::get_bpe_from_model;


use anyhow::{ensure, Error, Result};
use clap::{Parser, Subcommand};
use rayon::current_num_threads;
use rayon::prelude::*;
use serde_json;
use serde_yaml;

use indicatif::ProgressBar;
use mj_io::{
    build_pbar, expand_dirs, get_output_filename, read_pathbuf_to_mem, write_mem_to_pathbuf,
};
use zstd::Encoder;
pub mod map_fxn;
pub mod partition;
pub mod utils;
use datamap_rs::map_fxn::PipelineProcessor;
use datamap_rs::partition::partition;
use datamap_rs::utils::json_get;
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

    UrlHunt {
        #[arg(required = true, long)]
        input_dir: PathBuf,

        #[arg(required = true, long)]
        output_dir: PathBuf,
        
        #[arg(required=true, long)]        
        url_json: PathBuf,
    },
    UrlScan {
        #[arg(required = true, long)]
        gold_dir: PathBuf,

        #[arg(required = true, long)]
        raw_dir: PathBuf,
        
        #[arg(required=true, long)]        
        output_dir: PathBuf,
    },


    FrontierRequest {
        #[arg(required=true, long)]
        input_dir: PathBuf,

        #[arg(required=true, long)]
        output_dir: PathBuf,        
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
=                            RESHARD                         =
============================================================*/

fn reshard(
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

/*============================================================
=                           URL HUNT                         =
============================================================*/
fn url_hunt(input_dir: &PathBuf, output_dir: &PathBuf, url: &PathBuf) -> Result<(), Error> {
    let start_main = Instant::now();
    println!("URL hunt for {:?}", input_dir);

    println!("Loading url targets");
    let urls = read_pathbuf_to_mem(url).unwrap().into_inner().into_inner();
    let urls: Vec<String> = serde_json::from_value(serde_json::from_str(&String::from_utf8(urls).unwrap()).unwrap()).unwrap();
    let url_counts : DashMap<String, usize> = urls.into_par_iter().map(|v| (v, 0)).collect();
    let url_len = url_counts.len();

    let all_files = expand_dirs(vec![input_dir.clone()], None).unwrap();

    println!("Searching for urls...");
    let pbar = build_pbar(all_files.len(), "Paths");
    let found_docs: Vec<Vec<u8>> = all_files.into_par_iter().flat_map(|p| {
        let output = collect_url_docs(p, &url_counts).unwrap();
        pbar.inc(1);
        output
    }).collect();
    

    println!("Making outputs...");
    const OUTPUT_SIZE: usize = 256_000_000;
    let mut shard_num = 0;
    let mut cur_vec: Vec<u8> = Vec::new();
    found_docs.into_iter().for_each(|v| {
        cur_vec.extend(v);
        cur_vec.push(b'\n');
        if cur_vec.len() >= OUTPUT_SIZE {
            let output_path = output_dir.clone().join(format!("shard_{:08}.jsonl.zst", shard_num));
            write_mem_to_pathbuf(&cur_vec, &output_path).unwrap();
            cur_vec.clear();
            shard_num += 1;
        }
    });
    if cur_vec.len() > 0 {
        let output_path = output_dir.clone().join(format!("shard_{:08}.jsonl.zst", shard_num));
        write_mem_to_pathbuf(&cur_vec, &output_path).unwrap();        
    }

    let frequencies: DashMap<usize, usize> = DashMap::new();
    url_counts.into_par_iter().for_each(|(_k,v)| {
        frequencies.entry(v).and_modify(|x| *x += 1).or_insert(1);
    });
    
    println!("Finished url hunt in {:?} secs", start_main.elapsed().as_secs());
    let zero_freq = frequencies.get(&0).map_or(0 , |v| *v);
    println!("Searched for {:?} urls | found {:?} of them ", url_len, url_len - zero_freq);
    println!("Frequencies (url-hit-count, num-times-this-occurred): {:?} frequencies", frequencies);
    Ok(())
}

fn collect_url_docs(p: PathBuf, url_counts: &DashMap<String, usize>) -> Result<Vec<Vec<u8>>, Error> {
    let mut output: Vec<Vec<u8>> = Vec::new();

    let contents = read_pathbuf_to_mem(&p).unwrap();
    for line in contents.lines() {
        let line = line.unwrap();
        let line_json: Value = serde_json::from_str(&line).unwrap();
        let mut line_url = json_get(&line_json, "metadata.WARC-Target-URI");
        line_url = if let Some(line_url) = line_url {
            Some(line_url)
        } else {
            json_get(&line_json, "metadata.warc_url")
        };
        if line_url.is_none() {
            continue
        }
        let line_url = line_url.unwrap().as_str().unwrap().to_string();
        if let Some(mut cur_count) = url_counts.get_mut(&line_url) {
            *cur_count += 1;
            output.push(serde_json::to_vec(&line_json).unwrap());
        }        
    }
    Ok(output)
}

fn url_scan(gold_dir: &PathBuf, raw_dir: &PathBuf, output_dir: &PathBuf) -> Result<(), Error> {
    let start_main = Instant::now();
    println!("Starting url scan...");


    let gold_paths = expand_dirs(vec![gold_dir.clone()], None).unwrap();
    let gold_pbar = build_pbar(gold_paths.len(), "Gold paths");
    let gold_items: DashSet<(String, String)> = DashSet::new();
    gold_paths.par_iter().for_each(|p| {
        collect_gold_items(p, &gold_items).unwrap();
        gold_pbar.inc(1);
    });

    let raw_paths = expand_dirs(vec![raw_dir.clone()], None).unwrap();
    let raw_pbar = build_pbar(raw_paths.len(), "Raw paths");
    let full_kept = AtomicUsize::new(0);
    raw_paths.par_iter().for_each(|p| {
        let output_path = get_output_filename(p, raw_dir, output_dir).unwrap();
        let kept_docs = keep_gold_docs(p, &gold_items, &output_path).unwrap();
        full_kept.fetch_add(kept_docs, Ordering::SeqCst);
        raw_pbar.inc(1);
    });

    let full_kept = full_kept.into_inner();

    println!("Finished url scan in {:?} secs", start_main.elapsed().as_secs());
    println!("Saw {:?} of the {:?} gold docs | {:.2}", full_kept, gold_items.len(), (100.0 * full_kept as f32 / gold_items.len() as f32));


    Ok(())
}

fn collect_gold_items(p: &PathBuf, gold_items: &DashSet<(String, String)>) -> Result<(), Error> {
    let contents = read_pathbuf_to_mem(&p).unwrap();
    for line in contents.lines() {
        let line = line.unwrap();
        let line_json: Value = serde_json::from_str(&line).unwrap();
        let item = get_urlscan_item(&line_json).unwrap();
        gold_items.insert(item);
    }
    Ok(())

}

fn keep_gold_docs(p: &PathBuf, gold_items: &DashSet<(String, String)>, output_path: &PathBuf) -> Result<usize, Error> {
    let mut kept_docs = 0;
    let mut output: Vec<u8> = Vec::new();
    let contents = read_pathbuf_to_mem(&p).unwrap();

    for line in contents.lines() {
        let line = line.unwrap();
        let line_json: Value = serde_json::from_str(&line).unwrap();
        let item = get_urlscan_item(&line_json).unwrap();
        if gold_items.contains(&item) {
            kept_docs += 1;
            output.extend(serde_json::to_vec(&line_json).unwrap());
            output.push(b'\n');
        }
    }

    if kept_docs > 0 {
        write_mem_to_pathbuf(&output, output_path).unwrap();
    }

    Ok(kept_docs)
}


fn get_urlscan_item(doc: &Value) -> Result<(String, String), Error> {
    let cc_path = doc.get("url").unwrap().as_str().unwrap().to_string();
    let timestamp = doc.get("timestamp").unwrap().as_str().unwrap().to_string();    
    Ok((cc_path, timestamp))
}

/*============================================================
=                          FRONTIER                          =
============================================================*/

const REWRITE_TEMPLATE: &str = "Task:
- Carefully analyze the provided text to extract key facts, concrete details, important numbers,
and core concepts.
- Remove any irrelevant or noisy information, and reorganize the content into a logically structured,
information-dense, and concise version that is easy to learn from. Output only the refined text.
- Strive to maintain the original length as much as possible (avoid excessive shortening).
- Refine multiple choice questions and answers if any.
Text:
{}
Just output the refined text, no other text.
";

fn frontier_request(input_dir: &PathBuf, output_dir: &PathBuf) -> Result<(), Error> {
    let start_main = Instant::now();
    println!("Making frontier requests...");

    let input_paths = expand_dirs(vec![input_dir.clone()], None).unwrap();
    let pbar = build_pbar(input_paths.len(), "Paths");
    let total_reqs = AtomicUsize::new(0);
    input_paths.par_iter().for_each(|p| {
        let output_path = get_output_filename(p, input_dir, output_dir).unwrap();
        let base_output_path = get_base_path(&output_path).unwrap();
        let path_reqs = make_frontier_req(p, &base_output_path).unwrap();
        total_reqs.fetch_add(path_reqs, Ordering::SeqCst);
        pbar.inc(1);
    });


    println!("Made {:?} frontier requests in {:?} secs", total_reqs.into_inner(), start_main.elapsed().as_secs());
    Ok(())
}


fn make_frontier_req(p: &PathBuf, base_output_path: &PathBuf) -> Result<usize, Error> {
    let contents = read_pathbuf_to_mem(p).unwrap();
    let bpe = get_bpe_from_model("gpt-4")?;

    let mut req_count = 0;
    let mut all_reqs : Vec<Value> = Vec::new();

    for line in contents.lines() {
        let line = line.unwrap();
        let line_json: Value = serde_json::from_str(&line).unwrap();
        let text = line_json.get("text").unwrap().as_str().unwrap().to_string();
        if bpe.encode_with_special_tokens(&text).len() > 35_000 {
            continue
        }

        let request = json!({
            "custom_id": line_json.get("id").unwrap().as_str().unwrap().to_string(),
            "method": "POST", 
            "url": "/v1/chat/completions",
            "body": {
                "model": "Qwen/Qwen3-32B",
                "messages": [
                    {
                        "role": "system",
                        "content": "You are a helpful assistant."
                    },
                    {
                        "role": "user", 
                        "content": format!("Task:
- Carefully analyze the provided text to extract key facts, concrete details, important numbers,
and core concepts.
- Remove any irrelevant or noisy information, and reorganize the content into a logically structured,
information-dense, and concise version that is easy to learn from. Output only the refined text.
- Strive to maintain the original length as much as possible (avoid excessive shortening).
- Refine multiple choice questions and answers if any.
Text:
{}
Just output the refined text, no other text.
", text)
                    }
                ],
                "max_tokens": 4096
            }
        });
        all_reqs.push(request);
        req_count += 1;    
    }
    let mut chunk_num = 0;
    for chunk in all_reqs.chunks(1000) {
        let mut req_content : Vec<u8> = Vec::new();
        for el in chunk {
            req_content.extend(serde_json::to_vec(&el).unwrap());
            req_content.push(b'\n');            
        }
        let output_path = create_suffixed_path(base_output_path, chunk_num);
        println!("Trying output path {:?}", output_path);
        write_mem_to_pathbuf(&req_content, &output_path).unwrap();
        chunk_num += 1;
    }
    Ok(req_count)

}

fn get_base_path(input: &PathBuf) -> Option<PathBuf> {
    let parent = input.parent()?;
    let stem = input.file_stem()?.to_str()?;
    
    // Remove .jsonl if it's part of the stem
    let base_name = if let Some(base) = stem.strip_suffix(".jsonl") {
        base
    } else {
        stem
    };
    
    Some(parent.join(base_name))
}

fn create_suffixed_path(base: &PathBuf, i: i32) -> PathBuf {
    let suffixed_name = format!("{}_{:04}.jsonl", base.file_name().unwrap().to_str().unwrap(), i);
    base.parent().unwrap_or(&PathBuf::new()).join(suffixed_name)
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
        } => gen_map(input_dir, output_dir, config, err_dir.clone()),
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
        Commands::UrlHunt {
            input_dir,
            output_dir,
            url_json
        } => url_hunt(input_dir, output_dir, url_json),
        Commands::UrlScan {
            gold_dir, raw_dir, output_dir            
        } => url_scan(gold_dir, raw_dir, output_dir),
        Commands::FrontierRequest {
            input_dir, output_dir
        } => frontier_request(input_dir, output_dir),
        _ => Ok(()),
    };
    result.unwrap();
}
