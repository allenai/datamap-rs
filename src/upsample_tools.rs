use std::fs::OpenOptions;
use std::io::Write;
use std::fs::create_dir_all;
use std::fs::File;
use rand::Rng;
use std::collections::HashMap;
use anyhow::{Error, Result};
use dashmap::DashMap;
use std::{
    io::BufRead,
    os::unix::fs::OpenOptionsExt,
    path::PathBuf,
    sync::{Arc, Mutex},
    time::Instant,
};
use serde_json;
use rayon::prelude::*;
use crate::utils::json_get;
use mj_io::{expand_dirs, read_pathbuf_to_mem, write_mem_to_pathbuf, build_pbar};
use zstd::stream::Encoder;
use serde::{Deserialize, Serialize};

/* Tools used for upsampling.
In two parts: 
1. Get reservoir sampling for percentiles
2. Filter data points into percentile groups (sharding as we go)

And then have a joint command to do both
*/


#[derive(Debug, Serialize, Deserialize)]
struct UpsampleConfig {
	name: String,
	value: String,
	default_value: Option<f32>, // defaults to 0	
	percentile_groups: Vec<f32>, // e.g. [0.25, 0.50, 0.75] -> splits into [[0.0, 0.25), [0.25, 0.5), [0.5, 0.75), [0.75, 1]]
	#[serde(default="default_max_file_size")]
	max_file_size: usize,
	#[serde(default="default_reservoir_size")]
	reservoir_size: usize,


}


fn default_max_file_size() -> usize {
	256_000_000
}


fn default_reservoir_size() -> usize {
	1_000_000
}

/*======================================================
=                    RESERVOIR SAMPLING                =
======================================================*/

pub fn reservoir_sample(input_dir: &PathBuf, output_path: &Option<PathBuf>, config_path: &PathBuf) -> Result<Vec<f32>, Error> {
	println!("Starting build of reservoir...");
	let start_time = Instant::now();
	let config_contents = read_pathbuf_to_mem(config_path).unwrap();
	let config: UpsampleConfig = serde_yaml::from_reader(config_contents).unwrap();		
	let default: f32 = if let Some(default) = config.default_value {
		default
	} else {
		0.0
	};

	let input_paths = expand_dirs(vec![input_dir.clone()], None).unwrap();
	let pbar = build_pbar(input_paths.len(), "Paths");
	let thread_count = rayon::current_num_threads().clamp(0, input_paths.len());
    let chunk_size = (input_paths.len() + thread_count - 1) / thread_count; // Ceiling division
    let chunks: Vec<Vec<PathBuf>> = input_paths.chunks(chunk_size)
       .map(|chunk| chunk.to_vec())
       .collect();
    let mut chunk_reservoir_sizes: Vec<usize> = (0..thread_count).map(|_| config.reservoir_size / thread_count).collect();
    let to_add = config.reservoir_size - chunk_reservoir_sizes.iter().sum::<usize>();
    for i in 0..to_add {
    	chunk_reservoir_sizes[i] += 1;
    }

    let mut reservoir: Vec<f32> = (0..thread_count).into_par_iter().flat_map(|i| {
    	reservoir_sample_chunk(&chunks[i], chunk_reservoir_sizes[i], &config.value, &default, &pbar).unwrap()
    }).collect::<Vec<f32>>();
    reservoir.par_sort_by(|a, b| a.partial_cmp(b).unwrap());

    if let Some(output_path) = output_path {
    	let json_reservoir = serde_json::to_vec(&reservoir).unwrap();
    	write_mem_to_pathbuf(&json_reservoir, &output_path).unwrap();
    }

    println!("Made reservoir in {:?} secs", start_time.elapsed().as_secs());

	Ok(reservoir)

}


fn reservoir_sample_chunk(input_paths: &Vec<PathBuf>, reservoir_size: usize, reservoir_key: &String, default_val: &f32, pbar: &indicatif::ProgressBar) -> Result<Vec<f32>, Error> {
	let mut reservoir: Vec<f32> = Vec::new();
	let mut item_num = 0;
	let mut rng = rand::rng();
	input_paths.iter().for_each(|p| {
		let contents = read_pathbuf_to_mem(p).unwrap();
		for line in contents.lines() {
			let j = if item_num < reservoir_size {
				usize::MAX
			} else {
				rng.random_range(0..=item_num)
			};
			if j < usize::MAX && j >= reservoir_size {
				item_num += 1;
				continue;
			}
			let line = line.unwrap();
			let value : serde_json::Value = serde_json::from_str(&line).unwrap();
			let gathered_value = json_get(&value, reservoir_key);
			let res_value = if let Some(res_value) = gathered_value {
				res_value.as_f64().unwrap() as f32
			} else {
				*default_val
			};
			if j == usize::MAX {
				reservoir.push(res_value);
			} else {
				reservoir[j] = res_value;
			}

			item_num += 1;
		}
		pbar.inc(1);
	});


	Ok(reservoir)
}

/*=======================================================
=                     PARTITIONING                      =
=======================================================*/


pub fn percentile_partition(input_dir: &PathBuf, output_dir: &PathBuf, reservoir_path: &Option<PathBuf>, reservoir: &Option<Vec<f32>>, config_path: &PathBuf) -> Result<(), Error> {
	println!("Starting partition...");
	let start_time = Instant::now();
	let config_contents = read_pathbuf_to_mem(config_path).unwrap();
	let config: UpsampleConfig = serde_yaml::from_reader(config_contents).unwrap();		
	let input_paths = expand_dirs(vec![input_dir.clone()], None).unwrap();

	let reservoir: Vec<f32> = if reservoir.is_some() {
		reservoir.clone().unwrap()
	} else {
		let reservoir_path = reservoir_path.clone().unwrap();
		let res_contents = read_pathbuf_to_mem(&reservoir_path).unwrap().into_inner().into_inner();
		let reservoir_json = serde_json::from_slice(&res_contents).unwrap();
		reservoir_json
	};

	let percentile_values: Vec<f32> = config.percentile_groups.iter()
		.map(|p| reservoir[(((reservoir.len() as f32) * p).round() as usize).clamp(0, reservoir.len() - 1)])
		.collect();
	let counter: DashMap<usize, usize> = DashMap::new();
	let writer = GenWriter::new(output_dir, config.max_file_size);
	let pbar = build_pbar(input_paths.len(), "Paths");

	input_paths.par_iter().for_each(|p| {
		percentile_partition_path(p, &writer, &percentile_values, &config, &counter).unwrap();
		pbar.inc(1);
	});

	// 
	println!("Finished partition in {:?} seconds", start_time.elapsed().as_secs());
	println!("Put this many docs in each group");
	counter.into_iter().for_each(|(k, v)| {
		if k == 0 {
			println!("[0.0, {:?}) | {:?} docs", config.percentile_groups[0], v);
		} else if k == config.percentile_groups.len() + 1 {
			println!("[{:?}, 1.0] | {:?} docs", config.percentile_groups[config.percentile_groups.len() -1], v);
		} else {
			println!("[{:?}, {:?}) | {:?} docs", config.percentile_groups[k-1], config.percentile_groups[k], v);
		}
	});


	Ok(())
}

fn percentile_partition_path(input_path: &PathBuf, writer: &GenWriter, percentile_values: &Vec<f32>, config: &UpsampleConfig, counter: &DashMap<usize, usize>) -> Result<(), Error> {
	let mut subcounter: HashMap<usize, usize> = HashMap::new();
	let mut partitioned_contents: HashMap<usize, Vec<u8>> = HashMap::new();
	let contents = read_pathbuf_to_mem(input_path).unwrap();
	for line in contents.lines() {		
		let line = line.unwrap();
		let value : serde_json::Value = serde_json::from_str(&line).unwrap();
		let gathered_value = json_get(&value, &config.value);
		let res_value = if let Some(res_value) = gathered_value {
			res_value.as_f64().unwrap() as f32
		} else {
			config.default_value.unwrap_or(0.0)
		};
		let bucket = f32_to_bucket(percentile_values, res_value);
		*subcounter.entry(bucket).or_insert(0) += 1;	
		let mut value_bytes = line.as_bytes().to_vec();
		value_bytes.push(b'\n');
		partitioned_contents.entry(bucket).or_default().extend(value_bytes);
	}

	partitioned_contents.into_iter().for_each(|(k, v)| {
		writer.write_contents(k, v).unwrap();
		*counter.entry(k).or_insert(0) += subcounter.get(&k).unwrap();
	});


	Ok(())
}

fn f32_to_bucket(bucket_bounds: &Vec<f32>, value: f32) -> usize {
	// linear scan of percentile bounds to the right bucket index
	if value < bucket_bounds[0] {
		return 0;
	}
	for j in 0..bucket_bounds.len() - 1 {
		if bucket_bounds[j] <= value && value < bucket_bounds[j+1] {
			return j + 1
		}
	}

	bucket_bounds.len() + 1
}


/*=============================================================
=                         SINGLE CALL STUFF                   =
=============================================================*/

pub fn full_percentile_partition(input_dir: &PathBuf, output_dir: &PathBuf, config_path: &PathBuf) -> Result<(), Error> {
	let reservoir = reservoir_sample(input_dir, &None, config_path).unwrap();
	percentile_partition(input_dir, output_dir, &None, &Some(reservoir), config_path).unwrap();
	Ok(())
}



/*==========================================================
=                        GEN WRITER STUFF                  =
==========================================================*/

pub struct GenWriter<'a> {
	pub writer: DashMap<usize, Arc<Mutex<WriterInfo<'a>>>>,
	#[allow(dead_code)]
	storage_loc: PathBuf,	
	max_len: usize
}

pub struct WriterInfo<'a> {
	encoder: Option<Encoder<'a, File>>,
	bytes_written: usize,
	file_idx: usize,
}
	

impl<'a> GenWriter<'a> {
	pub fn new(storage_loc: &PathBuf, max_len: usize) -> Self {
		let writer : DashMap<usize, Arc<Mutex<WriterInfo<'a>>>> = DashMap::new();

		GenWriter { writer, storage_loc: storage_loc.clone(), max_len}
	}


	pub fn get_filename(storage_loc: &PathBuf, bucket: &usize, file_idx: usize) -> PathBuf {
		storage_loc.clone()
				.join(format!("bucket_{:04}", bucket))
				.join(format!("shard_{:08}.jsonl.zst", file_idx))
	}

    fn create_new_encoder(&self, key: usize, file_idx: usize) -> Encoder<'a, File> {
		let new_filename = GenWriter::get_filename(&self.storage_loc, &key, file_idx)	;

        if let Some(parent_dir) = new_filename.parent() {
            if !parent_dir.exists() {
                create_dir_all(parent_dir).unwrap()
            }
        }
        
        Encoder::new(
            OpenOptions::new()
            .append(true)
            .create(true)
            .mode(0o644)
            .open(new_filename)
            .unwrap(),
        3).unwrap()
    }	



    pub fn write_contents(&self, key: usize, contents: Vec<u8>) -> Result<(), Error> {
        // Get or create the writer for this key
        let writer_arc = self.writer.entry(key).or_insert_with(|| {

            let filename = GenWriter::get_filename(&self.storage_loc, &key, 0);
            if let Some(parent_dir) = filename.parent() {
                if !parent_dir.exists() {
                    create_dir_all(parent_dir).unwrap()
                }
            }
            let writer_info = WriterInfo {
                encoder: Some(self.create_new_encoder(key, 0)),
                bytes_written: 0,
                file_idx: 0,
            };
            Arc::new(Mutex::new(writer_info))
        });

        let mut writer_info = writer_arc.lock().unwrap();
        writer_info.bytes_written += contents.len();

        if writer_info.encoder.is_none() {
        	writer_info.encoder = Some(self.create_new_encoder(key, writer_info.file_idx));
        }



		if let Some(encoder) = &mut writer_info.encoder {
			encoder.write_all(&contents).unwrap();
			if writer_info.bytes_written >= self.max_len {
				let mut old_encoder = writer_info.encoder.take().unwrap();
				old_encoder.flush().unwrap();
				old_encoder.finish().unwrap();
				writer_info.file_idx += 1;
				writer_info.encoder = None;
				writer_info.bytes_written = 0;
			}
		}
		
		Ok(())
    }


	pub fn finish(self) -> Result<(), Error> {
		// Flushes all the open writers
		self.writer.into_par_iter()
			.for_each(|(_, value)| {
				match Arc::try_unwrap(value) {
					Ok(mutex) => {
						let mut writer_info = mutex.into_inner().unwrap();
						if writer_info.bytes_written > 0 {
							let mut encoder = writer_info.encoder.take().unwrap();
							encoder.flush().unwrap();
							encoder.finish().unwrap();
						}
					},
					_ => panic!("WHAT?")
				}
		});
		Ok(())
	}
}

