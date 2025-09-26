use std::sync::atomic::Ordering;
use std::sync::atomic::AtomicUsize;
use std::collections::{HashMap, HashSet};
use anyhow::{Error, Result};
use dashmap::DashMap;
use std::{
    fs::{create_dir_all, File, OpenOptions},
    io::{Write, BufRead},
    os::unix::fs::OpenOptionsExt,
    path::PathBuf,
    sync::{Arc, Mutex},
    time::Instant,
};
use serde_json;
use rayon::prelude::*;
use crate::utils::json_get;
use mj_io::{expand_dirs, read_pathbuf_to_mem, build_pbar};
use zstd::stream::Encoder;
use serde::{Deserialize, Serialize};


/*================================================================
=                            DISCRETE PARTITION                  =
================================================================*/


#[derive(Debug, Serialize, Deserialize)]
struct DiscretePartitionConfig {
	name: String,
	partition_key: String,
	choices: Vec<String>,
	#[serde(default="default_max_file_size")]
	max_file_size: usize,
}


fn default_max_file_size() -> usize {
	256_000_000
}




pub fn discrete_partition(input_dir: &PathBuf, output_dir: &PathBuf, config_path: &PathBuf) -> Result<(), Error> {
	let start_main = Instant::now();
	println!("Starting partition operation");
	let input_paths = expand_dirs(vec![input_dir.clone()], None).unwrap();
	let config_contents = read_pathbuf_to_mem(config_path).unwrap();
	let config: DiscretePartitionConfig = serde_yaml::from_reader(config_contents).unwrap();


	let writer = GenWriter::new_category_writer(output_dir, &config.choices, config.max_file_size);
	let global_counts: DashMap<Option<String>, AtomicUsize> = DashMap::new();
	let pbar = build_pbar(input_paths.len(), "Paths");
	input_paths.par_iter().for_each(|p| {

		let local_counts = partition_single_path(p, &config, &writer).unwrap();
		local_counts.into_iter().for_each(|(k, v)| {
		    global_counts.entry(k).or_insert_with(|| AtomicUsize::new(0)).fetch_add(v, Ordering::Relaxed);
		});
		pbar.inc(1);
	});
	writer.finish().unwrap();
	println!("Finished partition in {:?} secs", start_main.elapsed().as_secs());
	let global_counts: HashMap<Option<String>, usize> = global_counts
		.into_par_iter()
		.map(|(k,v)| {
			(k, v.into_inner())
		}).collect();
	let total_values: usize = global_counts.iter().map(|(_k,v)| *v).sum();
	println!("Saw {:?} documents...", total_values);
	global_counts.into_iter().for_each(|(k,v)| {
		let printkey: String = if k.is_none() {
			String::from("None")
		} else {
			k.unwrap()
		};
		println!("Saw {:?} documents with type {:?}", v, printkey);
	});

	Ok(())
}


fn partition_single_path(path: &PathBuf, config: &DiscretePartitionConfig, writer: &GenWriter) -> Result<HashMap<Option<String>, usize>, Error> {

	let contents = read_pathbuf_to_mem(path).unwrap();
	let mut partitioned_bytes: HashMap<Option<String>, Vec<u8>> = HashMap::new();
	let mut counts: HashMap<Option<String>, usize> = HashMap::new();
	let WriterConfig::Category {ref full_choices} = writer.config else {panic!("should never happen")};

	for line in contents.lines() {
		let line = line.unwrap();
		let json_value = serde_json::from_str(&line).unwrap();
		let partition_value = json_get(&json_value, &config.partition_key).unwrap().as_str().unwrap().to_string();
		let key: &Option<String> = if full_choices.contains(&Some(partition_value.clone())) {
			&Some(partition_value)
		} else {
			&None
		};
		let append_vec = partitioned_bytes.entry(key.clone()).or_default();
		*counts.entry(key.clone()).or_insert(0) += 1;
		append_vec.extend(line.as_bytes());
		append_vec.push(b'\n');
	}
	partitioned_bytes.into_iter().for_each(|(key, val)| {
		writer.write_contents(WriterKey::Category(key), val).unwrap();
	});

	Ok(counts)	
}

/*=============================================================
=                        PERCENTILE PARTITION                 =
=============================================================*/
#[derive(Debug, Serialize, Deserialize)]
struct PercentilePartitionConfig {
	name: String,
	value: String,
	default_value: Option<f64>, // defaults to 0	
	range_groups: Vec<f64>, // e.g. [0.25, 0.50, 0.75] -> splits into [[0.0, 0.25), [0.25, 0.5), [0.5, 0.75), [0.75, 1]]
	#[serde(default="default_max_file_size")]
	max_file_size: usize,
	#[serde(default="default_bucket_name")]
	bucket_name: String


}


fn default_bucket_name() -> String {
	String::from("bucket")
}


pub fn range_partition(input_dir: &PathBuf, output_dir: &PathBuf, reservoir_path: &Option<PathBuf>, reservoir: &Option<Vec<f64>>, config_path: &PathBuf, weighted_percentiles: &bool) -> Result<(), Error> {
	println!("Starting partition...");
	let start_time = Instant::now();
	let config_contents = read_pathbuf_to_mem(config_path).unwrap();
	let config: PercentilePartitionConfig = serde_yaml::from_reader(config_contents).unwrap();		
	let input_paths = expand_dirs(vec![input_dir.clone()], None).unwrap();





	let percentile_values = if *weighted_percentiles {
		// reservoir is [{percentile, value: f64}, ...]
		let reservoir_path = reservoir_path.clone().unwrap();
		let res_contents = read_pathbuf_to_mem(&reservoir_path).unwrap().into_inner().into_inner();
		let reservoir_json: Vec<serde_json::Value> = serde_json::from_slice(&res_contents).unwrap();
		let mut pct_vals : Vec<f64> = Vec::new();
		let mut pct_idx = 0;
		for i in 0..(reservoir_json.len() - 1) {
			let pct_lo = json_get(&reservoir_json[i], "percentile").unwrap().as_f64().unwrap();
			let pct_hi = json_get(&reservoir_json[i+1], "percentile").unwrap().as_f64().unwrap();
			if (pct_lo <= config.range_groups[pct_idx] * 100.0) && (pct_hi > config.range_groups[pct_idx] * 100.0) {
				pct_vals.push(json_get(&reservoir_json[i], "value").unwrap().as_f64().unwrap());
				pct_idx += 1;			
			}
			if pct_idx >= config.range_groups.len() {
				break;
			}
		}
		pct_vals
	} else { // reservoir is just a vec<f64>
		let reservoir: Vec<f64> = if reservoir.is_some() {
			reservoir.clone().unwrap()
		} else {
			let reservoir_path = reservoir_path.clone().unwrap();
			let res_contents = read_pathbuf_to_mem(&reservoir_path).unwrap().into_inner().into_inner();
			let reservoir_json = serde_json::from_slice(&res_contents).unwrap();
			reservoir_json
		};
		config.range_groups.iter()
		.map(|p| reservoir[(((reservoir.len() as f64) * p).round() as usize).clamp(0, reservoir.len() - 1)])
		.collect()
	};

	println!("Percentile values are {:?}", percentile_values);
	//println!("PCT VAL {:?}", percentile_values);
	let counter: DashMap<usize, usize> = DashMap::new();
	let writer = GenWriter::new_bucket_writer(output_dir, config.max_file_size, &config.bucket_name);
	let pbar = build_pbar(input_paths.len(), "Paths");

	input_paths.par_iter().for_each(|p| {
		percentile_partition_path(p, &writer, &percentile_values, &config, &counter).unwrap();
		pbar.inc(1);
	});
	writer.finish().unwrap();
	// 
	println!("Finished partition in {:?} seconds", start_time.elapsed().as_secs());
	println!("Put this many docs in each group");
    let mut keys: Vec<usize> = counter.iter().map(|entry| entry.key().clone()).collect();
    keys.sort();	
	keys.into_iter().for_each(|k| {
		let binding = counter.get(&k).unwrap();
	    let v = binding.value();
		if k == 0 {
			println!("[0.0, {:?}) | {:?} score upper bound |{:?} docs", config.range_groups[0], percentile_values[0], v);
		} else if k == config.range_groups.len() + 1 {
			println!("[{:?}, 1.0] | {:?} score upper bound |{:?} docs", config.range_groups[config.range_groups.len() -1], 1.0, v);
		} else {
			println!("[{:?}, {:?}) | {:?} score upper bound |{:?} docs", config.range_groups[k-1], config.range_groups[k], percentile_values[k], v);
		}
	});


	Ok(())
}

fn percentile_partition_path(input_path: &PathBuf, writer: &GenWriter, percentile_values: &Vec<f64>, config: &PercentilePartitionConfig, counter: &DashMap<usize, usize>) -> Result<(), Error> {
	let mut subcounter: HashMap<usize, usize> = HashMap::new();
	let mut partitioned_contents: HashMap<usize, Vec<u8>> = HashMap::new();
	let contents = read_pathbuf_to_mem(input_path).unwrap();
	for line in contents.lines() {		
		let line = line.unwrap();
		let value : serde_json::Value = serde_json::from_str(&line).unwrap();
		let gathered_value = json_get(&value, &config.value);
		let res_value = if let Some(res_value) = gathered_value {
			res_value.as_f64().unwrap() as f64
		} else {
			config.default_value.unwrap_or(0.0)
		};
		let bucket = f64_to_bucket(percentile_values, res_value);
		*subcounter.entry(bucket).or_insert(0) += 1;	
		let mut value_bytes = line.as_bytes().to_vec();
		value_bytes.push(b'\n');
		partitioned_contents.entry(bucket).or_default().extend(value_bytes);
	}

	partitioned_contents.into_iter().for_each(|(k, v)| {
		writer.write_contents(WriterKey::Bucket(k), v).unwrap();
		*counter.entry(k).or_insert(0) += subcounter.get(&k).unwrap();
	});


	Ok(())
}

fn f64_to_bucket(bucket_bounds: &Vec<f64>, value: f64) -> usize {
	// linear scan of percentile bounds to the right bucket index
	if value < bucket_bounds[0] {
		return 0;
	}
	for j in 0..bucket_bounds.len() - 1 {
		if bucket_bounds[j] <= value && value < bucket_bounds[j+1] {
			return j + 1
		}
	}
	bucket_bounds.len()
}





/*==========================================================
=                        GEN WRITER STUFF                  =
==========================================================*/


// Generic key type that can be either String-based or numeric
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum WriterKey {
    Category(Option<String>),
    Bucket(usize),
}

pub struct GenWriter<'a> {
    pub writer: DashMap<WriterKey, Arc<Mutex<WriterInfo<'a>>>>,
    storage_loc: PathBuf,
    max_len: usize,
    config: WriterConfig,
}

pub struct WriterInfo<'a> {
    encoder: Option<Encoder<'a, File>>, // You'll need to import your Encoder type
    bytes_written: usize,
    file_idx: usize,
}

#[derive(Clone)]
pub enum WriterConfig {
    Category {
        full_choices: HashSet<Option<String>>,
    },
    Bucket {
        bucket_name: String,
    },
}

impl<'a> GenWriter<'a> {
    // Constructor for category-based writer (Version 1)
    pub fn new_category_writer(
        storage_loc: &PathBuf, 
        choices: &Vec<String>, 
        max_len: usize
    ) -> Self {
        let writer = DashMap::new();
        let mut full_choices: HashSet<Option<String>> = choices
            .iter()
            .map(|c| Some(c.clone()))
            .collect();
        full_choices.insert(None);
        
        println!("Opening {:?} writer files", full_choices.len());
        
        GenWriter {
            writer,
            storage_loc: storage_loc.clone(),
            max_len,
            config: WriterConfig::Category { full_choices },
        }
    }

    // Constructor for bucket-based writer (Version 2)
    pub fn new_bucket_writer(
        storage_loc: &PathBuf,
        max_len: usize,
        bucket_name: &String
    ) -> Self {
        let writer = DashMap::new();
        
        GenWriter {
            writer,
            storage_loc: storage_loc.clone(),
            max_len,
            config: WriterConfig::Bucket {
                bucket_name: bucket_name.to_string(),
            },
        }
    }

    pub fn get_filename(&self, key: &WriterKey, file_idx: usize) -> PathBuf {
        match (&self.config, key) {
            (WriterConfig::Category { .. }, WriterKey::Category(choice)) => {
                if choice.is_none() {
                    self.storage_loc.join(format!("no_category.{:08}.jsonl.zst", file_idx))
                } else {
                    self.storage_loc.join(format!(
                        "chunk_{}.{:08}.jsonl.zst",
                        choice.as_ref().unwrap(),
                        file_idx
                    ))
                }
            }
            (WriterConfig::Bucket { bucket_name }, WriterKey::Bucket(bucket_num)) => {
                self.storage_loc
                    .join(format!("{}_{:04}", bucket_name, bucket_num))
                    .join(format!("shard_{:08}.jsonl.zst", file_idx))
            }
            _ => panic!("Mismatched writer config and key type"),
        }
    }

    fn create_new_encoder(&self, key: &WriterKey, file_idx: usize) -> Encoder<'a, File> {
        let new_filename = self.get_filename(key, file_idx);

        if let Some(parent_dir) = new_filename.parent() {
            if !parent_dir.exists() {
                create_dir_all(parent_dir).unwrap();
            }
        }

        Encoder::new(
            OpenOptions::new()
                .append(true)
                .create(true)
                .mode(0o644)
                .open(new_filename)
                .unwrap(),
            3,
        )
        .unwrap()
    }

    pub fn write_contents(&self, key: WriterKey, contents: Vec<u8>) -> Result<(), Box<dyn std::error::Error>> {
        let writer_arc = self.writer.entry(key.clone()).or_insert_with(|| {
            let filename = self.get_filename(&key, 0);
            if let Some(parent_dir) = filename.parent() {
                if !parent_dir.exists() {
                    create_dir_all(parent_dir).unwrap();
                }
            }
            let writer_info = WriterInfo {
                encoder: Some(self.create_new_encoder(&key, 0)),
                bytes_written: 0,
                file_idx: 0,
            };
            Arc::new(Mutex::new(writer_info))
        });

        let mut writer_info = writer_arc.lock().unwrap();
        writer_info.bytes_written += contents.len();

        if writer_info.encoder.is_none() {
            writer_info.encoder = Some(self.create_new_encoder(&key, writer_info.file_idx));
        }

        if let Some(encoder) = &mut writer_info.encoder {
            encoder.write_all(&contents)?;
            if writer_info.bytes_written >= self.max_len {
                let mut old_encoder = writer_info.encoder.take().unwrap();
                old_encoder.flush()?;
                old_encoder.finish()?;
                writer_info.file_idx += 1;
                writer_info.encoder = None;
                writer_info.bytes_written = 0;
            }
        }

        Ok(())
    }

    // Convenience methods for the different key types
    pub fn write_category_contents(
        &self,
        category: Option<String>,
        contents: Vec<u8>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        self.write_contents(WriterKey::Category(category), contents)
    }

    pub fn write_bucket_contents(
        &self,
        bucket: usize,
        contents: Vec<u8>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        self.write_contents(WriterKey::Bucket(bucket), contents)
    }

    pub fn finish(self) -> Result<(), Box<dyn std::error::Error>> {
        self.writer.into_par_iter().for_each(|(_, value)| {
            match Arc::try_unwrap(value) {
                Ok(mutex) => {
                    let mut writer_info = mutex.into_inner().unwrap();
                    if writer_info.bytes_written > 0 {
                        if let Some(mut encoder) = writer_info.encoder.take() {
                            encoder.flush().unwrap();
                            encoder.finish().unwrap();
                        }
                    }
                }
                Err(_) => panic!("Failed to unwrap Arc - multiple references still exist"),
            }
        });
        Ok(())
    }
}

