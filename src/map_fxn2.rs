use aho_corasick::AhoCorasick;
use std::io::BufRead;
use std::path::PathBuf;
use rand::rng;
use serde_json;
use serde_json::{json, Value};
use anyhow::{Error, Result, ensure, anyhow};
use rand::Rng;
use uuid::Uuid;
use crate::utils::{get_default, json_get, json_set};
use serde::Serialize;
use once_cell::sync::Lazy;
use std::collections::{HashMap, HashSet};

use url::Url;
use mj_io::read_pathbuf_to_mem;
use fasttext::FastText;
use unicode_segmentation::UnicodeSegmentation;


/*================================================================================
=                            PIPELINE PROCESSING                                 =
================================================================================*/

type ProcessorConstructor = fn(&Value) -> Result<Box<dyn AnyDataProcessor>, Error>;


macro_rules! register_processor {
    ($map:expr, $name:expr, $processor_type:ty) => {
        $map.insert($name, |config| {
            let processor = <$processor_type>::new(config).unwrap();
            Ok(Box::new(processor) as Box<dyn AnyDataProcessor>)
        });
    };
}


// Static map of processor types to their constructor wrapper functions
static PROCESSOR_CONSTRUCTORS: Lazy<HashMap<&'static str, ProcessorConstructor>> = Lazy::new(|| {
    let mut m: HashMap<&'static str, ProcessorConstructor> = HashMap::new();

    
    register_processor!(m, "line_len_filter", LineLenFilterJson);
    register_processor!(m, "subsample", SubsampleFilterJson);
    register_processor!(m, "url_filter", UrlSubstringFilterJson);
    register_processor!(m, "fasttext_anno", FastTextAnnotator);
    register_processor!(m, "float_filter", FloatFilter);
    register_processor!(m, "page_len_filter", PageLenFilter);
    register_processor!(m, "word_len_filter", WordLengthFilter);
    register_processor!(m, "symbol_ratio_filter", SymbolRatioFilter);
    register_processor!(m, "ellipsis_line_ratio_filter", EllipsisLineRatioFilter);
    register_processor!(m, "alphabetic_word_ratio_filter", AlphabeticWordRatioFilter);

    // Add more processor types as needed
    
    m
});



pub trait AnyDataProcessor: Send + Sync {
    fn process(&self, data: Value) -> Result<Option<Value>, Error>;
    fn name(&self) -> String;
}

impl<T> AnyDataProcessor for T 
where
    T: DataProcessor + Send + Sync + serde::Serialize,
{
    fn process(&self, data: Value) -> Result<Option<Value>, Error> {
        // Just delegate to the underlying DataProcessor implementation
        DataProcessor::process(self, data)
    }
    
    fn name(&self) -> String {
        // Delegate to the underlying name implementation
        DataProcessor::name(self)
    }
}

pub struct PipelineProcessor {
    pipeline: Vec<Box<dyn AnyDataProcessor>>
}

impl PipelineProcessor {
    // Create an empty pipeline
    pub fn new(config: &Value) -> Result<Self, Error> {
    	let mut pipeline : Vec<Box<dyn AnyDataProcessor>> = Vec::<Box<dyn AnyDataProcessor>>::new(); 
    	let pipeline_configs = config.get("pipeline").unwrap().as_array().unwrap();
    	for subconfig in pipeline_configs {
    		let subconfig_name = subconfig.get("name").unwrap().as_str().unwrap();
    		let default_json = json!({});
    		let subconfig_kwargs: &Value = subconfig.get("kwargs").or(Some(&default_json)).unwrap();
    		let constructor = PROCESSOR_CONSTRUCTORS[subconfig_name];
    		pipeline.push(constructor(subconfig_kwargs).unwrap());
    	}
        Ok(Self { pipeline })
    }

	pub fn process(&self, data: Value) -> Result<Option<Value>, Error> {
	    let mut current_data = Some(data);
	    
	    for processor in &self.pipeline {
	        if let Some(data_value) = current_data {
	            current_data = processor.process(data_value).unwrap();
	        } else {
	            return Ok(None);
	        }
	    }
	    
	    Ok(current_data)
	}

}



/*================================================================================
=                            DATA PROCESSOR TRAIT                                =
================================================================================*/
/*
New plan:
	- each data processing "unit" operates on a single line of a jsonl.
	- the signature for processing takes in data and some extra configs, but also has maybe some precached data 
		(precached data is nice for things that need to be loaded like banlists or a fasttext classifier)
	- this is specified in the pipeline with the kwargs argument in the config yaml
	- signatures are always a (json, config) -> Result<Option<Value>, Error>
*/

trait DataProcessor {
    type CachedData: Default;

    // Load anything we need to help here
    fn initialize_cache(config: Value) -> Result<Self::CachedData, Error> {
    	Ok(Default::default())
    }

    // Initialize and return Self with cached data
    fn new(config: &Value) -> Result<Self, Error> 
    where
        Self: Sized;
    
    // Process method that all implementations must provide
    fn process(&self, data: Value) -> Result<Option<Value>, Error>;
    
    // Optional method to retrieve processor metadata

    fn name(&self) -> String 
        where 
        Self: Serialize + Sized
    {
    	serde_json::to_string_pretty(&self).unwrap()
    }
}

/*================================================================================
=                            DATA PROCESSOR VARIANTS                             =
================================================================================*/
#[derive(Serialize)]
struct LineLenFilterJson {
	min_len: usize,
	max_len: usize,
	text_field: String,
}
impl DataProcessor for LineLenFilterJson {
	type CachedData = ();

	fn new(config: &Value) -> Result<Self, Error> {
		let min_len = get_default(config, "min_len", 0); 
		let max_len = get_default(config, "max_len", usize::MAX);
		let text_field = get_default(config, "text_field", String::from("text"));
		Ok(Self {min_len, max_len, text_field})
	}


	fn process(&self, data: Value) -> Result<Option<Value>, Error> {
		let text_len = data.get(&self.text_field).unwrap().as_str().unwrap().len();
		if self.min_len <= text_len && text_len <= self.max_len {
			Ok(Some(data))
		} else {
			Ok(None)
		}

	}

}


#[derive(Serialize)]
struct AddIdJson {
	id_key: String
}
impl DataProcessor for AddIdJson {
	type CachedData = ();

	fn new(config: &Value) -> Result<Self, Error> {
		let id_key = get_default(config, "id_key", String::from("id")); 
		Ok(Self {id_key})
	}


	fn process(&self, mut data: Value) -> Result<Option<Value>, Error> {
		data[&self.id_key] = Value::String(Uuid::new_v4().to_string());
	    Ok(Some(data))		

	}
}



#[derive(Serialize)]
struct SantaCoderPLFilterJson {
	pl_key : String
}
impl DataProcessor for SantaCoderPLFilterJson {
	type CachedData = ();

	fn new(config: &Value) -> Result<Self, Error> {
		let pl_key = get_default(config, "pl_key", String::from("metadata.language"));
		Ok(Self {pl_key})
	}


	fn process(&self, data: Value) -> Result<Option<Value>, Error> {
		let pl = json_get(&data, &self.pl_key).unwrap();
		if pl == "Python" || pl == "Java" || pl == "Javascript" {
			Ok(Some(data))
		} else {
			Ok(None)
		}


	}
}


#[derive(Serialize)]
struct SubsampleFilterJson {
	subsample_rate : f64
}
impl DataProcessor for SubsampleFilterJson {
	type CachedData = ();

	fn new(config: &Value) -> Result<Self, Error> {
		let subsample_rate = get_default(config, "subsample_rate", 1.0 as f64);
		Ok(Self {subsample_rate})
	}


	fn process(&self, data: Value) -> Result<Option<Value>, Error> {
		let random_float = rng().random::<f64>();
		if random_float <= self.subsample_rate {
			Ok(Some(data))
		} else {
			Ok(None)
		}		
	}
}


#[derive(Serialize)]
struct UrlSubstringFilterJson {
	url_key: String,
	ignore_chars: Vec<String>,
	num_banned_substrs: usize,
	exact_domain_match: bool, 
	match_substrings: bool,
	case_sensitive: bool,
	banlist: HashSet<String>,
	#[serde(skip)]
	ac_banlist: Option<AhoCorasick>
}
impl DataProcessor for UrlSubstringFilterJson {
	type CachedData = ();

	fn new(config: &Value) -> Result<Self, Error> {
		let url_key = config.get("url_key").unwrap().to_string();
		let ignore_chars = get_default(config, "ignore_chars", Vec::new()).into_iter().map(|el| el.to_string()).collect();
		let num_banned_substrs = get_default(config, "num_banned_substrs", 1);
		let exact_domain_match = get_default(config, "exact_domain_match", false);
		let match_substrings = get_default(config, "match_substrings", true);
		let case_sensitive = get_default(config, "case_sensitive", false);

		let banlist_file = PathBuf::from(config.get("banlist_file").unwrap().as_str().unwrap());
		let banlist_data = read_pathbuf_to_mem(&banlist_file).unwrap();
		let banlist: HashSet<String> = banlist_data.lines()
			.map(|line| if case_sensitive { line.unwrap().to_lowercase() } else {line.unwrap()})
			.collect();

		let ac_banlist = if !exact_domain_match {
			let banlist_vec : Vec<String> = banlist.clone().into_iter().map(|v| v).collect();
			Some(AhoCorasick::new(banlist_vec).unwrap())
		} else {
			None
		};

		Ok(Self {url_key, ignore_chars, num_banned_substrs, exact_domain_match, match_substrings, case_sensitive, banlist, ac_banlist})
	}


	fn process(&self, data: Value) -> Result<Option<Value>, Error> {
		// Process url
		let mut url = json_get(&data, &self.url_key).unwrap().to_string();
		url = if self.exact_domain_match {Url::parse(&url).unwrap().to_string()} else {url};
		url = if self.case_sensitive { url.to_lowercase() } else { url };
		for c in &self.ignore_chars {
			url = url.replace(c, "");
		}

		// Exact match case
		if self.exact_domain_match {
			if self.banlist.contains(&url) {
				return Ok(None);
			} else {
				return Ok(Some(data));
			}

		} 

		// Nonexact case 
		let ac_banlist = self.ac_banlist.as_ref().unwrap(); 
		let match_count = ac_banlist.find_iter(&url).collect::<Vec<_>>().len();
		if match_count < self.num_banned_substrs {
			Ok(Some(data))
		} else {
			Ok(None)
		}
	}
}


#[derive(Serialize)]
struct FastTextAnnotator {
	fast_text_file: String,
	text_field: String,
	output_field: String,
	k: i32,
	threshold: f32,
	#[serde(skip)]
	model: FastText
}

impl DataProcessor for FastTextAnnotator {
	type CachedData = ();

	fn new(config: &Value) -> Result<Self, Error> {
		let fast_text_file = config.get("fast_text_file").unwrap().to_string();
		let text_field = get_default(config, "text_field", String::from("text"));
		let output_field = get_default(config, "output_field", String::from("metadata.fasttext"));
		let k = get_default(config, "k", 10 as usize) as i32;
		let threshold = get_default(config, "threshold", 0.0) as f32;

		let mut model = FastText::new();
		model.load_model(&fast_text_file).unwrap();
		Ok(Self {fast_text_file, text_field, output_field, k, threshold, model})	
	}


	fn process(&self, mut data: Value) -> Result<Option<Value> ,Error> {

		let text = json_get(&data, &self.text_field).unwrap().to_string();
		let predictions = self.model.predict(&text, self.k, self.threshold).unwrap();

		let mut map = serde_json::Map::new();
		for pred in predictions {
			map.insert(pred.label.clone(), json!(pred.prob));
		}
		let pred_json = Value::Object(map);

		json_set(&mut data, &self.output_field, pred_json).unwrap();

		Ok(Some(data))
	}
}


#[derive(Serialize)]
struct FloatFilter {
	float_field: String,
	lower_bound: f32,
	upper_bound: f32,
	default: f32,
}

impl DataProcessor for FloatFilter {
	type CachedData = ();
	fn new(config: &Value) -> Result<Self, Error> {
		let float_field = config.get("float_field").unwrap().to_string();
		let lower_bound = get_default(config, "lower_bound", 0.0 as f64) as f32;
		let upper_bound = get_default(config, "upper_bound", f32::MAX as f64) as f32;
		let default = get_default(config, "default", 0.0 as f64) as f32;

		Ok(Self {float_field, lower_bound, upper_bound, default})
	}

	fn process(&self, data: Value) -> Result<Option<Value>, Error> {

		let val = if let Some(json_val) = json_get(&data, &self.float_field) {
			json_val.as_f64().unwrap() as f32
		} else {
			self.default
		};

		if self.lower_bound <= val && val <= self.upper_bound {
			Ok(Some(data))
		} else {
			Ok(None)
		}
	}
}



#[derive(Serialize)]
struct PageLenFilter {
	text_field: String,
	length_type: String,
	lower_bound: usize,
	upper_bound: usize,
	ignore_punctuation: bool,
}

impl DataProcessor for PageLenFilter {
	type CachedData = ();
	fn new(config: &Value) -> Result<Self, Error> {
		let text_field = get_default(config, "text_field", String::from("text"));
		let length_type = config.get("length_type").unwrap().to_string();		
		ensure!(["word", "sentence", "line", "paragraph", "char"].contains(&&length_type.as_str()),
				format!("Length type must be one of {{word, sentence, line, paragraph, char}} and not {:?}", length_type)
			);

		let lower_bound = get_default(config, "lower_bound", 1 as usize);
		let upper_bound = get_default(config, "upper_bound", usize::MAX);
		let ignore_punctuation = get_default(config, "ignore_punctuation", true);

		Ok(Self {text_field, length_type, lower_bound, upper_bound, ignore_punctuation})
	}

	fn process(&self, data: Value) -> Result<Option<Value>, Error> {
		let text = json_get(&data, &self.text_field).unwrap().to_string();
		let len = match self.length_type.as_str() {
			"word" => {
				text.as_str().split_word_bounds()
					.filter(|v| v.len() > 0 && (v.chars().next().unwrap().is_alphanumeric() || !self.ignore_punctuation))
					.collect::<Vec<_>>().len()
			},
			_ => {
				return Err(anyhow!("Only implemented for words for now!"))
			}
		};

		if self.lower_bound <= len && len <= self.upper_bound {
			Ok(Some(data))
		} else {
			Ok(None)
		}
	}

}


#[derive(Serialize)]
struct WordLengthFilter {
	text_field: String,
	lower_bound: f32,
	upper_bound: f32
}

impl DataProcessor for WordLengthFilter {
	type CachedData = ();
	fn new(config: &Value) -> Result<Self, Error> {
		let text_field = get_default(config, "text_field", String::from("text"));
		let lower_bound = get_default(config, "lower_bound", 0.0 as f64) as f32;
		let upper_bound = get_default(config, "upper_bound", f32::MAX as f64) as f32;		
		Ok(Self {text_field, lower_bound, upper_bound})
	}

	fn process(&self, data: Value) -> Result<Option<Value>, Error> {
		let text = json_get(&data, &self.text_field).unwrap().to_string();
		let word_lens: Vec<usize> = text.split_whitespace().map(|v| v.len()).collect();

		let avg_word_len = word_lens.iter().sum::<usize>() as f32 / word_lens.len() as f32;

		if self.lower_bound <= avg_word_len && avg_word_len <= self.upper_bound {
			Ok(Some(data))
		} else {
			Ok(None)
		}	
	}
}


#[derive(Serialize)]
struct SymbolRatioFilter {
	text_field: String,
	max_symbol_to_word_ratio: f32,
}

impl DataProcessor for SymbolRatioFilter {
	type CachedData = ();
	fn new(config: &Value) -> Result<Self, Error> {

		let text_field = get_default(config, "text_field", String::from("text"));
		let max_symbol_to_word_ratio = get_default(config, "max_symbol_to_word_ratio", f32::MAX as f64) as f32;
		Ok(Self {text_field, max_symbol_to_word_ratio})
	}

	fn process(&self, data: Value) -> Result<Option<Value>, Error> {
		let text = json_get(&data, &self.text_field).unwrap().to_string();
		let symbols = vec!["#", "...", ". . .", "\u{2026}"];
		let mut num_symbols = 0;
		for symbol in symbols.iter() {
			num_symbols += text.matches(symbol).count();
		}

		let num_words = text.split_whitespace().collect::<Vec<_>>().len();

		let symbol_to_word_ratio = num_symbols as f32 / num_words as f32;


		if symbol_to_word_ratio <= self.max_symbol_to_word_ratio {
			Ok(Some(data))
		} else {
			Ok(None)
		}	
	}
}

#[derive(Serialize)]
struct EllipsisLineRatioFilter {
	text_field: String,
	max_ratio: f32,
}

impl DataProcessor for EllipsisLineRatioFilter {
	type CachedData = ();
	fn new(config: &Value) -> Result<Self, Error> {
		let text_field = get_default(config, "text_field", String::from("text"));
		let max_ratio = get_default(config, "max_ratio", f32::MAX as f64) as f32;
		Ok(Self {text_field, max_ratio})
	}

	fn process(&self, data: Value) -> Result<Option<Value>, Error> {
		let text = json_get(&data, &self.text_field).unwrap().to_string();
		let lines: Vec<&str> = text.lines().filter(|line| line.len() > 0).collect();

	    let ellipsis_count = lines.iter()
	        .filter(|line| {
	            line.ends_with("...") || line.ends_with(". . .") || line.ends_with("\u{2026}")
	        })
	        .count();


	    let ratio = ellipsis_count as f32 / lines.len() as f32;
		if ratio <= self.max_ratio {
			Ok(Some(data))
		} else {
			Ok(None)
		}	
	}
}


#[derive(Serialize)]
struct AlphabeticWordRatioFilter {
	text_field: String,
	max_ratio: f32,
}

impl DataProcessor for AlphabeticWordRatioFilter {
	type CachedData = ();
	fn new(config: &Value) -> Result<Self, Error> {
		let text_field = get_default(config, "text_field", String::from("text"));
		let max_ratio = get_default(config, "max_ratio", f32::MAX as f64) as f32;
		Ok(Self {text_field, max_ratio})
	}

	fn process(&self, data: Value) -> Result<Option<Value>, Error> {
		let text = json_get(&data, &self.text_field).unwrap().to_string();
		let words = text.split_whitespace().collect::<Vec<_>>();
		let total_words = words.len() as f32;
		let non_alpha_words = words.into_iter().filter(|w| !w.chars().any(|c| c.is_alphabetic())).collect::<Vec<_>>().len();

		let ratio = non_alpha_words as f32 / total_words;

		if ratio <= self.max_ratio {
			Ok(Some(data))
		} else {
			Ok(None)
		}	
	}
}

#[derive(Serialize)]
struct StopWordFilter {
	text_field: String,
	count_unique: bool,
	min_stop_word: usize,
	stop_words: HashSet<String>,
}

impl DataProcessor for StopWordFilter {
	type CachedData = ();
	fn new(config: &Value) -> Result<Self, Error> {
		let text_field = get_default(config, "text_field", String::from("text"));
		let count_unique = get_default(config, "count_unique", false);
		let min_stop_word = get_default(config, "min_stop_word", 2);
		let stop_words: HashSet<String> = [
        "the", "be", "to", "of", "and", "that", "have", "with"
	    ].into_iter().map(|w| String::from(w)).collect();

		Ok(Self {text_field, count_unique, min_stop_word, stop_words})
	}

	fn process(&self, data: Value) -> Result<Option<Value>, Error> {
		let text = json_get(&data, &self.text_field).unwrap().to_string();
		let words: Vec<_>= text.split_whitespace().map(|w| w.to_lowercase()).collect();
		if self.count_unique {
			let mut occur_stop_words = HashSet::new();
			for word in words {
				if self.stop_words.contains(&word) {
					occur_stop_words.insert(word);

					if occur_stop_words.len() >= self.min_stop_word {
						return Ok(Some(data))
					}
				}
			}
		} else {
			let mut count = 0;
			for word in words {
				if self.stop_words.contains(&word) {
					count += 1;
					if count >= self.min_stop_word {
						return Ok(Some(data))
					}
				}
			}
		}
		Ok(None)
	}

}


