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

/*
Tools for partitioning a dataset across categories or across ranges (like quantile bucketing).

These are treated separately:

Discrete (Categorical) Partitioning:
- Takes an input dataset and the config holds the key we're partitioning on as well as optional known categories
	- If categories are known beforehand, will bucket anything not matching these categories into a separate bucket
	- If categories are not known beforehand, will create one bucket per category as it's seen
- Output files are stored like chunk_{category}_{filenum}.jsonl.zst



Range Partitioning:
- Takens an input dataset and the config holds the key, a default value, 
  and either a list of the range groups we want OR a pointer to a reservoir sample and number of desired buckets

- Output files are stored like 
	bucket_{bucket_num}/shard_{:08}.jsonl.zst
	
*/


/*================================================================
=                            DISCRETE PARTITION                  =
================================================================*/


#[derive(Debug, Serialize, Deserialize)]
struct DiscretePartitionConfig {
	name: String,
	partition_key: String,
	choices: Option<Vec<String>>,
	#[serde(default="default_max_file_size")]
	max_file_size: usize,
}


fn default_max_file_size() -> usize {
	256_000_000
}




pub fn discrete_partition(input_dir: &PathBuf, output_dir: &PathBuf, config_opt: &Option<PathBuf>, partition_key: &Option<String>) -> Result<(), Error> {
	let start_main = Instant::now();
	println!("Starting partition operation");
	let input_paths = expand_dirs(vec![input_dir.clone()], None).unwrap();

	let config: DiscretePartitionConfig = if let Some(config_path) = config_opt {
		let config_contents = read_pathbuf_to_mem(config_path).unwrap();
		serde_yaml::from_reader(config_contents).unwrap()
	} else {
		DiscretePartitionConfig {name: String::from("Discrete partition"),
							     partition_key: partition_key.clone().unwrap(), 
							     choices: None,
							 	 max_file_size: default_max_file_size()}
	};


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
	for line in contents.lines() {
		let line = line.unwrap();
		let json_value = serde_json::from_str(&line).unwrap();
		let partition_value = json_get(&json_value, &config.partition_key).unwrap();


		let key = match partition_value {
			serde_json::Value::Null => &None,
			_ => {

				let str_key = partition_value.as_str().unwrap().to_string();
				&if let Some(valid_choices) = &config.choices {
					if valid_choices.contains(&str_key) {
						Some(str_key)
					} else {
						None
					}
				} else {
					Some(str_key)
				}
			}
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
	range_groups: Option<Vec<f64>>, // e.g. [0.25, 0.50, 0.75] -> splits into [[0.0, 0.25), [0.25, 0.5), [0.5, 0.75), [0.75, 1]]
	reservoir_path: Option<PathBuf>,
	num_buckets: Option<usize>,
	#[serde(default="default_max_file_size")]
	max_file_size: usize,
	#[serde(default="default_bucket_name")]
	bucket_name: String


}


fn default_bucket_name() -> String {
	String::from("bucket")
}


pub fn range_partition(input_dir: &PathBuf, output_dir: &PathBuf, config_path: &PathBuf) -> Result<(), Error> {
	println!("Starting partition...");
	let start_time = Instant::now();
	let config_contents = read_pathbuf_to_mem(config_path).unwrap();
	let config: PercentilePartitionConfig = serde_yaml::from_reader(config_contents).unwrap();		
	let input_paths = expand_dirs(vec![input_dir.clone()], None).unwrap();

	let ranges: Vec<f64> = if let Some(ref range_groups) = config.range_groups {
		range_groups.to_vec()
	} else if let Some(ref res_path) = config.reservoir_path {
		let reservoir_content = read_pathbuf_to_mem(&res_path).unwrap().into_inner().into_inner();
		let mut reservoir_data: Vec<f64> = serde_json::from_slice(&reservoir_content).unwrap();
		reservoir_data.sort_unstable_by(|a,b| a.total_cmp(b));
		let num_buckets = config.num_buckets.unwrap();
		(1..num_buckets).map(|i| {
			let index = (i * reservoir_data.len()) / num_buckets;
			if index < reservoir_data.len() {
				reservoir_data[index] 				
			} else {
				reservoir_data[reservoir_data.len() - 1]
			}
		})
		.collect()
	} else {
		panic!("Need either range groups or a reservoir");
	};
	println!("Range groups are {:?}", ranges);


	let counter: DashMap<usize, usize> = DashMap::new(); // counts range group -> num docs
	let writer = GenWriter::new_bucket_writer(output_dir, config.max_file_size, &config.bucket_name);
	let pbar = build_pbar(input_paths.len(), "Paths");

	input_paths.par_iter().for_each(|p| {
		percentile_partition_path(p, &writer, &ranges, &config, &counter).unwrap();
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
			println!("(-∞, {:?}) | {:?} docs", ranges[0], v);
		} else if k == ranges.len() {
			println!("[{:?}, ∞) | {:?} docs", ranges[ranges.len() -1], v);
		} else {
			println!("[{:?}, {:?}) | {:?} docs", ranges[k-1], ranges[k], v);
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
	match bucket_bounds.binary_search_by(|x| x.partial_cmp(&value).unwrap()) {
		Ok(index) => index,
		Err(index) => index
	}

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
        full_choices: Option<HashSet<Option<String>>>,
    },
    Bucket {
        bucket_name: String,
    },
}

impl<'a> GenWriter<'a> {
    // Constructor for category-based writer (Version 1)
    pub fn new_category_writer(
        storage_loc: &PathBuf, 
        choices: &Option<Vec<String>>, 
        max_len: usize
    ) -> Self {
        let writer = DashMap::new();

        let fake_config = &WriterConfig::Category {full_choices: None};
        let (full_choices, fc_len) = if let Some(choices) = choices {
        	let mut full_choices: HashSet<Option<String>> = HashSet::new();
        	for choice in choices {
        		full_choices.insert(Some(choice.clone()));
        	}
        	full_choices.insert(None);
        	let fc_len = full_choices.len();

        	for choice in &full_choices {
        		let key = WriterKey::Category(choice.clone());
				writer.entry(key.clone()).or_insert_with(|| {
		            let filename = GenWriter::get_filename(fake_config, &key, 0, storage_loc);
		            if let Some(parent_dir) = filename.parent() {
		                if !parent_dir.exists() {
		                    create_dir_all(parent_dir).unwrap();
		                }
		            }
		            let writer_info = WriterInfo {
		                encoder: Some(Self::create_new_encoder(fake_config, &key, 0, storage_loc)),
		                bytes_written: 0,
		                file_idx: 0,
		            };
		            Arc::new(Mutex::new(writer_info))
		        });  
			}
        	(Some(full_choices), fc_len)
       		        	
        } else {
        	(None, 0)
        };
        let gen_writer = GenWriter {
            writer,
            storage_loc: storage_loc.clone(),
            max_len,
            config: WriterConfig::Category { full_choices },
        };




        println!("Opening {:?} writer files", fc_len);        
        gen_writer
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

    pub fn get_filename(config: &WriterConfig, key: &WriterKey, file_idx: usize, storage_loc: &PathBuf) -> PathBuf {
        match (config, key) {
            (WriterConfig::Category { .. }, WriterKey::Category(choice)) => {
                if choice.is_none() {
                    storage_loc.join(format!("no_category.{:08}.jsonl.zst", file_idx))
                } else {
                    storage_loc.join(format!(
                        "chunk_{}.{:08}.jsonl.zst",
                        choice.as_ref().unwrap(),
                        file_idx
                    ))
                }
            }
            (WriterConfig::Bucket { bucket_name }, WriterKey::Bucket(bucket_num)) => {
                storage_loc
                    .join(format!("{}_{:04}", bucket_name, bucket_num))
                    .join(format!("shard_{:08}.jsonl.zst", file_idx))
            }
            _ => panic!("Mismatched writer config and key type"),
        }
    }

    fn create_new_encoder(config: &WriterConfig, key: &WriterKey, file_idx: usize, storage_loc: &PathBuf) -> Encoder<'a, File> {
        let new_filename = GenWriter::get_filename(config, key, file_idx, storage_loc);

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
    	let writer_arc = match (&self.config, &key) {
    		(WriterConfig::Category { full_choices }, WriterKey::Category(choice)) => {
    			if let Some(og_choices) = full_choices { // Choices are prespecified -- either we match or key=None
    				let proper_key = if og_choices.contains(&choice) {
    					key.clone()
    				} else {
    					WriterKey::Category(None)
    				};
    				&self.writer.get_mut(&proper_key).unwrap()
    			} else { // Choices are not prespecified, always match, otherwise create a new thing
					&self.writer.entry(key.clone()).or_insert_with(|| {
			            let filename = GenWriter::get_filename(&self.config, &key, 0, &self.storage_loc);
			            if let Some(parent_dir) = filename.parent() {
			                if !parent_dir.exists() {
			                    create_dir_all(parent_dir).unwrap();
			                }
			            }
			            let writer_info = WriterInfo {
			                encoder: Some(GenWriter::create_new_encoder(&self.config, &key, 0, &self.storage_loc)),
			                bytes_written: 0,
			                file_idx: 0,
			            };
			            Arc::new(Mutex::new(writer_info))
			        })    				
    			}
    		},
    		(WriterConfig::Bucket { .. }, WriterKey::Bucket(..)) => {
				&self.writer.entry(key.clone()).or_insert_with(|| {
		            let filename = GenWriter::get_filename(&self.config, &key, 0, &self.storage_loc);
		            if let Some(parent_dir) = filename.parent() {
		                if !parent_dir.exists() {
		                    create_dir_all(parent_dir).unwrap();
		                }
		            }
		            let writer_info = WriterInfo {
		                encoder: Some(GenWriter::create_new_encoder(&self.config, &key, 0, &self.storage_loc)),
		                bytes_written: 0,
		                file_idx: 0,
		            };
		            Arc::new(Mutex::new(writer_info))
		        })    	    			
    		}
    		_ => panic!("Mismatched writer config and key type"),
    	};


        let mut writer_info = writer_arc.lock().unwrap();
        writer_info.bytes_written += contents.len();

        if writer_info.encoder.is_none() {
            writer_info.encoder = Some(GenWriter::create_new_encoder(&self.config, &key, writer_info.file_idx, &self.storage_loc));
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

