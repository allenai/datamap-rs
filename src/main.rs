// External crates

use xxhash_rust::xxh3::xxh3_64;
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
use datamap_rs::utils::{json_get, json_set};
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

        #[arg(required=true, long)]
        flavor: String,          

        #[arg(long, default_value_t=String::from("text"))]
        text_key: String,

        #[arg(long)]
        id_key: Option<String>
    },


    FrontierMerge {
        #[arg(required=true, long)]
        og_dir: PathBuf,

        #[arg(required=true, long)]
        frontier_dir: PathBuf,

        #[arg(required=true, long)]
        og_id: String,

        #[arg(required=true, long)]
        output_dir: PathBuf,          

        #[arg(long, default_value_t=String::from("original_text"))]
        old_text_loc: String
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
/*
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
*/


fn make_message(text: &str, flavor: &str) -> Result<Value, Error> {

    let message = match flavor {
        "MIND_2STUDENT" => {
            json!([{"role": "system", "content": "You are a helpful AI assistant"},
                   {"role": "user", "content": "Convert the context above as a multi-turn discussions between two students who are working on their assignment related to the given context. Make sure that their discussions strictly adhere to the context above and remains faithful to information in the context. If there are any mathematical calculations that need to be performed, please perform them. Other than that, please DONOT add any new information/reference other than the context. DONOT assume the ability to call any code or tools.\n".to_owned() + text}])
        },
        "MIND_PROBLEM_SOLVING" => {
            json!([{"role": "system", "content": "You are a helpful AI assistant"},
                   {"role": "user", "content": "Convert the context above as a multi-turn problem-solving conversation where participants analyze challenges or scenarios presented in the content and brainstorm solutions within the context of the provided material, avoiding speculation or unrelated discussions. Make sure that their conversation strictly adhere to the context above and remains faithful to information in the context. If there are any mathematical calculations that need to be performed, please perform them. Other than that, please DONOT add any new information/reference other than the context. DONOT assume the ability to call any code or tools.\n".to_owned() + text}])
        },        
        "swallowcode_sgcr" => {
            json!([{"role": "system", "content": "You are a smart software engineer. Please evaluate the following code on a scale of 1 to 10 based on the following criteria:\n
1. Are variable names descriptive and consistent with naming conventions?
2. Are comments and doc-strings appropriately written to explain the purpose and functionality of the code?
3. Are type annotations used effectively where applicable?
4. Are functions appropriately modularized, with well-defined responsibilities and clear separation of concerns?
5. Are variables' lifetimes intentionally managed, avoiding frequent reassignment or overly long scopes?
6. Is error handling implemented appropriately where necessary?
7. Is the code properly indented and follows standard formatting guidelines?
8. Do comments provide context and rationale, rather than merely describing what the code does?
9. Are functions and classes designed with clear, single responsibilities?
10. Is the code formatted in a way that enhances readability?\n\n
And provide suggestions for improvement based on the evaluation criteria. You can also provide an improved version of the code like the following style:\n
### Evaluation: 7\n\n
### Suggestions:\n
    Provide specific, actionable suggestions to improve the code based on the evaluation criteria.\n\n
### Improved Code:\n
Provide a revised version of the code incorporating the suggested improvements.\n
```python\n
def improved_function(arg1: int, arg2: str) -> str:
    # Your improved code here
    pass
```\n\n
"}, 
            {"role": "user", "content": text}])
        },
    "swallowcode_sgcr_javascript" => {
    json!([{"role": "system", "content": "You are a smart software engineer. Please evaluate the following JavaScript code on a scale of 1 to 10 based on the following criteria:\n
1. Are variable names descriptive and consistent with naming conventions (camelCase for variables/functions, PascalCase for classes/constructors)?\n
2. Are comments and JSDoc annotations appropriately written to explain the purpose and functionality of the code?\n
3. Are modern JavaScript features (const/let, arrow functions, destructuring) used appropriately?\n
4. Are functions appropriately modularized, with well-defined responsibilities and clear separation of concerns?\n
5. Are variables' lifetimes intentionally managed, avoiding frequent reassignment or overly long scopes?\n
6. Is error handling implemented appropriately using try/catch blocks or proper Promise rejection handling?\n
7. Is the code properly indented and follows standard formatting guidelines (semicolons, spacing)?\n
8. Do comments provide context and rationale, rather than merely describing what the code does?\n
9. Are functions and classes designed with clear, single responsibilities?\n
10. Is the code formatted in a way that enhances readability?\n\n
And provide suggestions for improvement based on the evaluation criteria. You can also provide an improved version of the code like the following style:\n
### Evaluation: 7\n\n
### Suggestions:\n
    Provide specific, actionable suggestions to improve the code based on the evaluation criteria.\n\n
### Improved Code:\n
Provide a revised version of the code incorporating the suggested improvements.\n
```javascript\n
/**
 * Your improved code here
 * @param {number} arg1 - Description
 * @param {string} arg2 - Description
 * @returns {string} Description
 */
function improvedFunction(arg1, arg2) {
    // Your improved code here
}
```\n\n
"},
        {"role": "user", "content": text}])

    },
    "swallowcode_sgcr_java" => {
    json!([{"role": "system", "content": "You are a smart software engineer. Please evaluate the following Java code on a scale of 1 to 10 based on the following criteria:\n
1. Are variable names descriptive and consistent with naming conventions (camelCase for variables/methods, PascalCase for classes)?\n
2. Are JavaDoc comments appropriately written to explain the purpose and functionality of classes and methods?\n
3. Are access modifiers (public, private, protected) used appropriately to encapsulate data?\n
4. Are methods appropriately modularized, with well-defined responsibilities and clear separation of concerns?\n
5. Are variables' lifetimes intentionally managed, with appropriate scope and minimal mutability?\n
6. Is exception handling implemented appropriately with specific exception types and proper try-catch blocks?\n
7. Is the code properly indented and follows standard formatting guidelines (braces, spacing)?\n
8. Do comments provide context and rationale, rather than merely describing what the code does?\n
9. Are classes and methods designed with clear, single responsibilities following SOLID principles?\n
10. Is the code formatted in a way that enhances readability and follows Java conventions?\n\n
And provide suggestions for improvement based on the evaluation criteria. You can also provide an improved version of the code like the following style:\n
### Evaluation: 7\n\n
### Suggestions:\n
    Provide specific, actionable suggestions to improve the code based on the evaluation criteria.\n\n
### Improved Code:\n
Provide a revised version of the code incorporating the suggested improvements.\n
```java\n
/**
 * Your improved code here
 * @param arg1 Description of parameter
 * @param arg2 Description of parameter
 * @return Description of return value
 */
public String improvedMethod(int arg1, String arg2) {
    // Your improved code here
    return ;
}
```\n\n
"},
        {"role": "user", "content": text}])

    },
    "swallowcode_sgcr_ruby" => {
    json!([{"role": "system", "content": "You are a smart software engineer. Please evaluate the following Ruby code on a scale of 1 to 10 based on the following criteria:\n
1. Are variable names descriptive and consistent with Ruby naming conventions (snake_case for variables/methods, PascalCase for classes)?\n
2. Are comments appropriately written to explain the purpose and functionality of the code?\n
3. Are Ruby idioms and conventions used effectively (blocks, symbols, proper use of ? and ! methods)?\n
4. Are methods appropriately modularized, with well-defined responsibilities and clear separation of concerns?\n
5. Are variables' lifetimes intentionally managed, with appropriate scope and minimal unnecessary instance variables?\n
6. Is error handling implemented appropriately using rescue blocks and specific exception types?\n
7. Is the code properly indented and follows standard Ruby formatting guidelines (2-space indentation)?\n
8. Do comments provide context and rationale, rather than merely describing what the code does?\n
9. Are classes and methods designed with clear, single responsibilities following Ruby principles?\n
10. Is the code formatted in a way that enhances readability and follows Ruby style guide?\n\n
And provide suggestions for improvement based on the evaluation criteria. You can also provide an improved version of the code like the following style:\n
### Evaluation: 7\n\n
### Suggestions:\n
    Provide specific, actionable suggestions to improve the code based on the evaluation criteria.\n\n
### Improved Code:\n
Provide a revised version of the code incorporating the suggested improvements.\n
```ruby\n
# Your improved code here
# @param arg1 [Integer] Description of parameter
# @param arg2 [String] Description of parameter
# @return [String] Description of return value
def improved_method(arg1, arg2)
  # Your improved code here
end
```\n\n
"},
        {"role": "user", "content": text}])

    },
    "swallowcode_sgcr_c_sharp" => {
    json!([{"role": "system", "content": r#"You are a smart software engineer. Please evaluate the following C# code on a scale of 1 to 10 based on the following criteria:\n
1. Are variable names descriptive and consistent with C# naming conventions (camelCase for local variables, PascalCase for methods/properties/classes)?\n
2. Are XML documentation comments appropriately written to explain the purpose and functionality of public members?\n
3. Are access modifiers and properties used appropriately to encapsulate data and follow OOP principles?\n
4. Are methods appropriately modularized, with well-defined responsibilities and clear separation of concerns?\n
5. Are variables' lifetimes intentionally managed, with appropriate scope and proper disposal of resources using 'using' statements?\n
6. Is exception handling implemented appropriately with specific exception types and proper try-catch-finally blocks?\n
7. Is the code properly indented and follows standard C# formatting guidelines (braces, spacing)?\n
8. Do comments provide context and rationale, rather than merely describing what the code does?\n
9. Are classes and methods designed with clear, single responsibilities following SOLID principles?\n
10. Is the code formatted in a way that enhances readability and follows C# conventions?\n\n
And provide suggestions for improvement based on the evaluation criteria. You can also provide an improved version of the code like the following style:\n
### Evaluation: 7\n\n
### Suggestions:\n
    Provide specific, actionable suggestions to improve the code based on the evaluation criteria.\n\n
### Improved Code:\n
Provide a revised version of the code incorporating the suggested improvements.\n
```csharp\n
/// <summary>
/// Your improved code here
/// </summary>
/// <param name="arg1">Description of parameter</param>
/// <param name="arg2">Description of parameter</param>
/// <returns>Description of return value</returns>
public string ImprovedMethod(int arg1, string arg2)
{
    // Your improved code here
    return string.Empty;
}
```\n\n
"#},
        {"role": "user", "content": text}])

    },
    "swallowcode_sgcr_go" => {
    json!([{"role": "system", "content": r#"You are a smart software engineer. Please evaluate the following Go code on a scale of 1 to 10 based on the following criteria:\n
1. Are variable names descriptive and consistent with Go naming conventions (camelCase for unexported, PascalCase for exported)?\n
2. Are comments appropriately written, especially for exported functions and packages, following Go documentation conventions?\n
3. Are Go idioms used effectively (multiple return values, error handling, channels, goroutines where appropriate)?\n
4. Are functions appropriately modularized, with well-defined responsibilities and clear separation of concerns?\n
5. Are variables' lifetimes intentionally managed, with appropriate scope and minimal unnecessary global variables?\n
6. Is error handling implemented appropriately using Go's explicit error return pattern?\n
7. Is the code properly formatted using 'go fmt' standards (tabs for indentation, proper spacing)?\n
8. Do comments provide context and rationale, rather than merely describing what the code does?\n
9. Are functions and types designed with clear, single responsibilities following Go principles?\n
10. Is the code formatted in a way that enhances readability and follows Go conventions?\n\n
And provide suggestions for improvement based on the evaluation criteria. You can also provide an improved version of the code like the following style:\n
### Evaluation: 7\n\n
### Suggestions:\n
    Provide specific, actionable suggestions to improve the code based on the evaluation criteria.\n\n
### Improved Code:\n
Provide a revised version of the code incorporating the suggested improvements.\n
```go\n
// ImprovedFunction does something specific and useful
// arg1 represents the description of first parameter
// arg2 represents the description of second parameter
// Returns a string and an error
func ImprovedFunction(arg1 int, arg2 string) (string, error) {
    // Your improved code here
    return "", nil
}
```\n\n
"#},
        {"role": "user", "content": text}])

    },
    "swallowcode_sgcr_shell" => {
    json!([{"role": "system", "content": r#"You are a smart software engineer. Please evaluate the following Bash script on a scale of 1 to 10 based on the following criteria:\n
1. Are variable names descriptive and consistent with Bash conventions (uppercase for environment/global vars, lowercase for local vars)?\n
2. Are comments appropriately written to explain the purpose and functionality of the script and complex sections?\n
3. Are Bash best practices followed (proper quoting, use of [[ ]] instead of [ ], parameter expansion)?\n
4. Are functions appropriately modularized, with well-defined responsibilities and clear separation of concerns?\n
5. Are variables properly scoped using 'local' keyword in functions and avoiding unnecessary global variables?\n
6. Is error handling implemented appropriately using 'set -e', exit codes, and proper error checking?\n
7. Is the script properly indented and follows standard formatting guidelines (consistent spacing, line breaks)?\n
8. Do comments provide context and rationale, rather than merely describing what the code does?\n
9. Are functions designed with clear, single responsibilities and reusability in mind?\n
10. Is the script formatted in a way that enhances readability and maintainability?\n\n
And provide suggestions for improvement based on the evaluation criteria. You can also provide an improved version of the code like the following style:\n
### Evaluation: 7\n\n
### Suggestions:\n
    Provide specific, actionable suggestions to improve the code based on the evaluation criteria.\n\n
### Improved Code:\n
Provide a revised version of the code incorporating the suggested improvements.\n
```bash\n
#!/bin/bash
set -euo pipefail

# Your improved script here
# Function: improved_function
# Description: Brief description of what this function does
# Arguments:
#   $1 - Description of first argument
#   $2 - Description of second argument
# Returns: Description of return value/exit code
improved_function() {
    local arg1="$1"
    local arg2="$2"
    
    # Your improved code here
}
```\n\n
"#},
        {"role": "user", "content": text}])

    }

    "swallowcode_scor" => {json!([{"role": "system", "content": "You are a smart software engineer. Please change a given code into self-contained and well-structured code following the below best practices and pythonic way.
1. Use meaningful variable and function names.
2. Write a clear and concise docstring for the function.
3. Use type hints for the function signature.
4. Write a clear and concise comment for the code block.
5. Ensure the code is self-contained and does not depend on external variables.
6. Ensure the code is well-structured and easy to read.
7. Ensure the code is free of errors and runs correctly.
8. Ensure the code is optimized and does not have redundant operations.
9. Ensure the algorithm and data structures are efficient and concise.

If given code is not self-contained or too simple, please change it to a more educational and useful code.
"}, 
        {"role": "user", "content": text}])
        },    

        "swallowmath" => {
            json!([{"role": "system", "content": "You are an intelligent math tutor. You are given the following math problem and answer with some unnecessary parts. Please remove the unneeded parts of the questions. For example, the date of the question submitted, the answer date, the privacy policy, the footer, the header, etc, should be removed. However, please keep the main question and answer.\nIf questions or answers lack some information or are not elaborate, please make them more informative and easy to understand. If needed, please add more detail about the step-by-step calculation process.\n\nHere is the example:"},
                   {"role": "user", "content": "UBS Interview Question: What is the sum of 1-40?... | Glassdoor\n\n# What is the sum of 1-40?\n\n0\n\n820\n\nInterview Candidate on Feb 11, 2010\n2\n\nWe can have the sum by using the following formula\nN * (N + 1) / 2\n\nSo we have:\n40 * (40 + 1) / 2 = 820\n\nblue on Mar 21, 2010\n0\n\nSince he isn't asking to sum all the numbers from 1 to 40:\n\nsum of 1 and 40 = 41\nOR\nsum of 1 + -40 = -39\n \nEvandro on Dec 30, 2010\n3\n\nAny sum like this is easy. take the lowest and highest... 1 +40 = 41, then the next highest and the next lowest. 2 + 39 = 41. Realize that there are 20 such pairs. 20 * 41 =  820.\nFor the numbers 1 - 100, 1 + 100 = 101, 2 + 99 = 101; there are 50 such pairs, so 50 * 101 = 5050\n\nscienceguy on Jan 11, 2011\n1\n\nhttp://brainteaserbible.com/interview-brainteaser-sum-of-the-num bers-from-1-to-50\n\nan87 on Jun 13, 2011 "},
                   {"role": "assistant", "content": "What is the sum of 1-40?\n\nWe can have the sum by using the following formula\nN * (N + 1) / 2\n\nSo we have:\n40 * (40 + 1) / 2 = 820\n\n#### 820"},
                   {"role": "user", "content": text}
            ])
        }
        _ => json!({})
    };

    Ok(message)
}



fn frontier_request(input_dir: &PathBuf, output_dir: &PathBuf, flavor: &str, text_key: &String, id_key: &Option<String>) -> Result<(), Error> {
    let start_main = Instant::now();
    println!("Making frontier requests...");

    let input_paths = expand_dirs(vec![input_dir.clone()], None).unwrap();
    let pbar = build_pbar(input_paths.len(), "Paths");
    let total_reqs = AtomicUsize::new(0);
    input_paths.par_iter().for_each(|p| {
        let output_path = get_output_filename(p, input_dir, output_dir).unwrap();
        let base_output_path = get_base_path(&output_path).unwrap();
        let path_reqs = make_frontier_req(p, &base_output_path, flavor, text_key, id_key).unwrap();
        total_reqs.fetch_add(path_reqs, Ordering::SeqCst);
        pbar.inc(1);
    });


    println!("Made {:?} frontier requests in {:?} secs", total_reqs.into_inner(), start_main.elapsed().as_secs());
    Ok(())
}


fn make_frontier_req(p: &PathBuf, base_output_path: &PathBuf, flavor: &str, text_key: &String, id_key: &Option<String>) -> Result<usize, Error> {
    let contents = read_pathbuf_to_mem(p).unwrap();
    let bpe = get_bpe_from_model("gpt-4")?;

    let mut req_count = 0;
    let mut all_reqs : Vec<Value> = Vec::new();


    let model = match flavor {
        "swallowcode_scor" => "Qwen/Qwen2.5-Coder-32B-Instruct",
        "swallowcode_sgcr" => "Qwen/Qwen2.5-Coder-32B-Instruct",
        s if s.starts_with("swallowcode_sgcr") => "Qwen/Qwen3-Coder-30B-A3B-Instruct",
        "swallowmath" => "Qwen/Qwen3-32B",
        "MIND_PROBLEM_SOLVING" => "Qwen/Qwen3-32B",
        "MIND_2STUDENT" => "Qwen/Qwen3-32B",
        _ => panic!("{}", format!("Unknown flavor {:?}", flavor))
    };


    let id_key = if let Some(id_key_inner) = id_key {
        id_key_inner.as_str()
    } else { match flavor {
        "swallowcode_scor" => "blob_id",
        "swallowcode_sgcr" => "blob_id",
        s if s.starts_with("swallowcode_sgcr") => "blob_id",
        "swallowmath" => "<<HASH>>",
        _ => "id"
        }
    };




    for line in contents.lines() {
        let line = line.unwrap();
        let line_json: Value = serde_json::from_str(&line).unwrap();
        let text = json_get(&line_json, text_key).unwrap().as_str().unwrap().to_string();
        if bpe.encode_with_special_tokens(&text).len() > 35_000 {
            continue
        }

        let custom_id = if id_key == "<<HASH>>" {
            format!("{}", xxh3_64(text.as_bytes()))
        } else {
            line_json.get(id_key).unwrap().as_str().unwrap().to_string()
        };

        let request = json!({
            "custom_id": custom_id,
            "method": "POST", 
            "url": "/v1/chat/completions",
            "body": {
                "model": model,
                "messages": make_message(&text, flavor).unwrap(),
                "max_tokens": 16384
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


fn frontier_merge(og_dir: &PathBuf, frontier_dir: &PathBuf, og_id: &String, output_dir: &PathBuf, old_text_loc: &String) -> Result<(), Error> {
    /* Looks for original files in 'og_dir' and frontier output files in 'frontier_dir', moves the elements in og_dir.text field -> og_dir.original_text
       and replaces with the new frontier text
    */
    let start_main = Instant::now();
    println!("Merging frontier requests");
    let og_files = expand_dirs(vec![og_dir.clone()], None).unwrap();
    let frontier_files = expand_dirs(vec![frontier_dir.clone()], None).unwrap();

    println!("Making frontier req map");
    let start_frontier_main = Instant::now();
    let frontier_map : DashMap<String, String> = DashMap::new();
    let pbar = build_pbar(frontier_files.len(), "Frontier files");
    frontier_files.into_par_iter().for_each(|p| {
        frontier_req_map(&p, &frontier_map).unwrap();
        pbar.inc(1);
    });
    println!("Made frontier map in {:?} secs| {:?} entries", start_frontier_main.elapsed().as_secs(), frontier_map.len());

    println!("Making matches...");
    let pbar = build_pbar(og_files.len(), "Og files");
    let num_matches = AtomicUsize::new(0);
    let num_og_docs = AtomicUsize::new(0);
    og_files.par_iter().for_each(|p| {
        let output_path = get_output_filename(p, og_dir, output_dir).unwrap();
        let (num_path_matches, num_path_docs) = merge_frontier_file(p, og_id, &frontier_map, &output_path, &old_text_loc).unwrap();
        num_matches.fetch_add(num_path_matches, Ordering::SeqCst);
        num_og_docs.fetch_add(num_path_docs, Ordering::SeqCst);
        pbar.inc(1);
    });  
    let num_matches = num_matches.into_inner();
    let num_og_docs = num_og_docs.into_inner();

    println!("Merged frontier files in {:?} secs", start_main.elapsed().as_secs());
    println!("Saw {:?} original docs | Saw {:?} completed requests | Made {:?} matches", 
             num_og_docs, frontier_map.len(), num_matches);
    Ok(())
}


fn frontier_req_map(frontier_file: &PathBuf, frontier_map: &DashMap<String, String>) -> Result<(), Error> {
    let contents = read_pathbuf_to_mem(frontier_file).unwrap();
    for line in contents.lines() {
        let line = line.unwrap();
        let line_json: Value = serde_json::from_str(&line).unwrap();
        let status_code = json_get(&line_json, "response.status_code").unwrap().as_u64().unwrap();
        if status_code != 200 {
            continue;
        }
        let custom_id = json_get(&line_json, "custom_id").unwrap().as_str().unwrap().to_string();
        let choices = json_get(&line_json, "response.body.choices").unwrap();
        if let Value::Array(array) = choices {
            let first_choice: &Value = array.first().unwrap();
            if let Some(message_content) = json_get(&first_choice, "message.content") {
                if let Some(content_str) = message_content.as_str() {
                    let content = content_str.to_string();
                    frontier_map.insert(custom_id, content.clone());
                }
            }
        }
    }
    Ok(())
}

fn merge_frontier_file(p: &PathBuf, og_id: &String, frontier_map: &DashMap<String, String>, output_path: &PathBuf, old_text_loc: &String) -> Result<(usize, usize), Error> {
    let mut num_path_matches = 0;
    let mut num_path_docs = 0;
    let mut output_contents: Vec<u8> = Vec::new();
    let contents = read_pathbuf_to_mem(p).unwrap();

    for line in contents.lines() {
        num_path_docs += 1;
        let line = line.unwrap();
        let line_json : Value = serde_json::from_str(&line).unwrap();
        let original_text = json_get(&line_json, "text").unwrap().as_str().unwrap().to_string();

        let line_id = if og_id == "<<HASH>>" {
            format!("{}", xxh3_64(original_text.as_bytes()))
        } else {
            json_get(&line_json, og_id).unwrap().as_str().unwrap().to_string()
        };
        if !frontier_map.contains_key(&line_id) {
            continue;
        }
        let mut line_json = line_json.clone();
        //json_set(/* &mut serde_json::Value */, /* &std::string::String */, line_json);
        json_set(&mut line_json, old_text_loc, json!(original_text)).unwrap();
        json_set(&mut line_json, &String::from("text"), json!(*frontier_map.get(&line_id).unwrap())).unwrap();
        num_path_matches += 1;
        output_contents.extend(serde_json::to_vec(&line_json).unwrap());
        output_contents.push(b'\n');
    }

    if output_contents.len() > 0 {
        write_mem_to_pathbuf(&output_contents, output_path).unwrap();
    }

    Ok((num_path_matches, num_path_docs))
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
            input_dir, output_dir, flavor, text_key, id_key
        } => frontier_request(input_dir, output_dir, flavor, text_key, id_key),
        Commands::FrontierMerge {
            og_dir, frontier_dir, og_id, output_dir, old_text_loc
        } => frontier_merge(og_dir, frontier_dir, og_id, output_dir, old_text_loc),    
        _ => Ok(()),
    };
    result.unwrap();
}
