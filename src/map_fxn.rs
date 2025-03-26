use std::time::Instant;
use std::hash::{Hash, Hasher};
use std::collections::VecDeque;
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
use regex::Regex;
use fxhash::{FxHasher};


use derivative::Derivative; 
//use mj_io::build_pbar;

/*================================================================================
=                            PIPELINE PROCESSING                                 =
================================================================================*/
type TimingInfo = HashMap<usize, u128>;
type FilterInfo = HashMap<usize, usize>;

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

    
    register_processor!(m, "text_len_filter", TextLenFilter);
    register_processor!(m, "subsample", SubsampleFilter);
    register_processor!(m, "santcoder_pl_filter", SantaCoderPLFilter);
    register_processor!(m, "add_id", AddIdModifier);
    register_processor!(m, "url_substring_filter", UrlSubstringFilter);
    register_processor!(m, "newline_removal_modifier", NewlineRemovalModifier);
    register_processor!(m, "fasttext_annotator", FastTextAnnotator);
    register_processor!(m, "float_filter", FloatFilter);
    register_processor!(m, "page_len_filter", PageLenFilter);
    register_processor!(m, "word_len_filter", WordLenFilter);
    register_processor!(m, "symbol_ratio_filter", SymbolRatioFilter);
    register_processor!(m, "bullet_filter", BulletFilter);
    register_processor!(m, "ellipsis_line_ratio_filter", EllipsisLineRatioFilter);
    register_processor!(m, "alphabetic_word_ratio_filter", AlphabeticWordRatioFilter);
    register_processor!(m, "stop_word_filter", StopWordFilter);
    register_processor!(m, "massive_web_repetition_filter", MassiveWebRepetitionFilter);
    register_processor!(m, "word_count_adder", WordCountAdder);
    register_processor!(m, "ratio_line_modifier", RatioLineModifier);
    register_processor!(m, "regex_line_modifier", RegexLineModifier);
    register_processor!(m, "line_len_modifier", LineLenModifier);
    register_processor!(m, "substring_line_modifier", SubstringLineModifier);
    register_processor!(m, "word_removal_ratio_filter", WordRemovalRatioFilter);
    register_processor!(m, "madlad400_sentence_filter", Madlad400SentenceFilter);
    // Add more processor types as needed
    
    m
});



pub trait AnyDataProcessor: Send + Sync + std::fmt::Debug  {
    fn process(&self, data: Value) -> Result<Option<Value>, Error>;
}

impl<T> AnyDataProcessor for T 
where
    T: DataProcessor + Send + Sync + serde::Serialize + std::fmt::Debug,
{
    fn process(&self, data: Value) -> Result<Option<Value>, Error> {
        // Just delegate to the underlying DataProcessor implementation
        DataProcessor::process(self, data)
    }
    

}

#[derive(Debug)]
pub struct PipelineProcessor {
    pub pipeline: Vec<Box<dyn AnyDataProcessor>>
}

impl PipelineProcessor {
    // Create an empty pipeline
    pub fn new(config: &Value) -> Result<Self, Error> {
    	let mut pipeline : Vec<Box<dyn AnyDataProcessor>> = Vec::<Box<dyn AnyDataProcessor>>::new(); 
    	let text_field = get_default(&config, "text_field", String::from("text"));

    	let pipeline_configs = config.get("pipeline").unwrap().as_array().unwrap();
    	for subconfig in pipeline_configs {
    		let subconfig_name = subconfig.get("name").unwrap().as_str().unwrap();
    		let default_json = json!({});
    		let mut subconfig_kwargs: Value = subconfig.get("kwargs").or(Some(&default_json)).unwrap().clone();
    		json_set(&mut subconfig_kwargs, &String::from("text_field"), serde_json::Value::String(text_field.clone())).unwrap();
    		let constructor = PROCESSOR_CONSTRUCTORS[subconfig_name];
    		pipeline.push(constructor(&subconfig_kwargs).unwrap());
    	}
        Ok(Self { pipeline })
    }

	pub fn process(&self, data: Value, timing_info: &mut TimingInfo, filter_info: &mut FilterInfo) -> Result<(usize, Option<Value>), Error> {
		/*
		General data processor for the pipeline: 
			Takes in a Value and some extra logging info. Will maybe modify the json and then spit it back out with a (usize, .) prefixing it
			If the usize is less than usize::MAX, then this document got filtered and should not be included in outputs
			else, the thing that gets output passes the map and should be included in outputs
		*/

		let og_copy = data.clone();
	    let mut current_data = data;
		
		let mut filter_step = 0;
	    for processor in &self.pipeline {
	    	let start_step = Instant::now();
	    	let proc_result = processor.process(current_data)?;
	        *timing_info.entry(filter_step).or_insert(0 as u128) += start_step.elapsed().as_nanos();

	    	match proc_result {
	    		Some(data_value) => current_data = data_value,
	    		None => {
	    			*filter_info.entry(filter_step).or_insert(0 as usize) += 1;
	    			return Ok((filter_step, Some(og_copy)));
	    		}

	    	}

	        filter_step += 1;
	    }
	    *filter_info.entry(usize::MAX).or_insert(0 as usize) += 1;
	    Ok((usize::MAX, Some(current_data)))
	}

	pub fn process_lines(&self, lines: Vec<String>) -> Result<(HashMap<usize, Vec<Value>>, Vec<String>, TimingInfo, FilterInfo), Error> {
		let mut timing_info = TimingInfo::new();
		let mut filter_info = FilterInfo::new();
		let mut output_lines: HashMap<usize, Vec<Value>> = HashMap::new();
		let mut err_lines: Vec<String> = Vec::new();		
		for line in lines {

			let json_line = serde_json::from_str(&line).unwrap();
			let process_out = self.process(json_line, &mut timing_info, &mut filter_info);
			match process_out {
				Ok((step_out, json_result)) => {
					if let Some(json_out) = json_result {
						output_lines.entry(step_out)
							.or_insert_with(Vec::new)
							.push(json_out);
						}},
				Err(_e) => {
					err_lines.push(line.clone())
				}
			};
		};

		Ok((output_lines, err_lines, timing_info, filter_info))
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

pub trait DataProcessor {
    // Initialize and return Self with cached data
    fn new(config: &Value) -> Result<Self, Error> 
    where
        Self: Sized;
    
    // Process method that all implementations must provide
    fn process(&self, data: Value) -> Result<Option<Value>, Error>;
    

}

/*================================================================================
=                            DATA PROCESSOR VARIANTS                             =
================================================================================*/
#[derive(Serialize, Debug)]
pub struct TextLenFilter {
	// Filters to only keep docs that have text length in range [lower_bound, upper_bound]
	text_field: String,	
	lower_bound: usize,
	upper_bound: usize,
}
 impl DataProcessor for TextLenFilter {
	fn new(config: &Value) -> Result<Self, Error> {
		let lower_bound = get_default(config, "lower_bound", 0); 
		let upper_bound = get_default(config, "upper_bound", usize::MAX);
		let text_field = get_default(config, "text_field", String::from("text"));
		Ok(Self {text_field, lower_bound, upper_bound})
	}


	fn process(&self, data: Value) -> Result<Option<Value>, Error> {
		let text = json_get(&data, &self.text_field).unwrap().as_str().unwrap();
		let text_len = text.len();
		if self.lower_bound <= text_len && text_len <= self.upper_bound {
			Ok(Some(data))
		} else {
			Ok(None)
		}
	}
}


#[derive(Serialize, Debug)]
pub struct AddIdModifier {
	// Adds a uuidv4 value to the id_key field
	pub id_key: String
}
impl DataProcessor for AddIdModifier {
	fn new(config: &Value) -> Result<Self, Error> {
		let id_key = get_default(config, "id_key", String::from("id")); 
		Ok(Self {id_key})
	}


	fn process(&self, mut data: Value) -> Result<Option<Value>, Error> {
		let id = Uuid::new_v4().to_string();
		json_set(&mut data, &self.id_key, Value::String(id)).unwrap();
	    Ok(Some(data))		

	}
}



#[derive(Serialize, Debug)]
pub struct SantaCoderPLFilter {
	// Filters to collect only documents tha have pl_key in [Python, Java, Javascript]
	pub pl_key : String
}
impl DataProcessor for SantaCoderPLFilter {
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


#[derive(Serialize, Debug)]
struct SubsampleFilter {
	// Keeps a random subsample_rate fraction of the documens
	subsample_rate : f64
}
impl DataProcessor for SubsampleFilter {
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

#[derive(Derivative)]
#[derivative(Debug)]
#[derive(Serialize)]
pub struct UrlSubstringFilter {
	/* Filters by the url. 
	Stealing docs from DCLM:
    ignore_chars -- A list of characters to ignore (e.g., ['.', "-"]) as they are typically used to bypass
            detectors for fradulent/inappropriate webpages
    num_banned_substrs -- Number of num_banned_substrs within the banlist that must be present
            to be filtered out. Refinedweb uses this for "softer" banlist items (e.g., "webcam", "escort")
    exact_domain_match -- Whether to extract the domain from the page url and check for an exact match (e.g., when
    set to False, "le.com" being in banlist would lead to "google.com" being banned)
    match_substrings -- When True, the banlist items only need to be a substring. When False, items must exist 
            in between word boundaries. Note this is only used when exact_domain_match is False. 
    case_sensitive -- Whether to check for case sensitivity (RefinedWeb sets this to be True)

	*/

	pub url_key: String,
	pub ignore_chars: Vec<String>,
	pub num_banned_substrs: usize,
	pub exact_domain_match: bool, 
	pub match_substrings: bool,
	pub case_sensitive: bool,
    #[derivative(Debug="ignore")]
	pub banlist: HashSet<String>, // Key for this is banlist_file
    #[derivative(Debug="ignore")]	
	#[serde(skip)]
	pub ac_banlist: Option<AhoCorasick>
}
impl DataProcessor for UrlSubstringFilter {
	fn new(config: &Value) -> Result<Self, Error> {
		let case_sensitive = get_default(config, "case_sensitive", false);

		let banlist_file = PathBuf::from(config.get("banlist_file").unwrap().as_str().unwrap());
		let banlist_data = read_pathbuf_to_mem(&banlist_file).unwrap();
		let banlist: HashSet<String> = banlist_data.lines()
			.map(|line| if case_sensitive { line.unwrap().to_lowercase() } else {line.unwrap()})
			.collect();

		UrlSubstringFilter::construct_w_explicit_banlist(config, banlist)		


	}


	fn process(&self, data: Value) -> Result<Option<Value>, Error> {
		// Process url
		let mut url = json_get(&data, &self.url_key).unwrap().as_str().unwrap().to_string();
		url = if self.exact_domain_match {
			let parsed_url = Url::parse(&url)?;
			let host_str = parsed_url.host_str().ok_or_else(|| anyhow!("URL has no host component"))?;
			host_str.to_string()
		} else {
			url
		};
		url = if !self.case_sensitive { url.to_lowercase() } else { url };
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
		let ac_banlist = self.ac_banlist.as_ref().ok_or(anyhow!("AC Banlist"))?; 

		if self.match_substrings {
			let match_count = ac_banlist.find_iter(&url).collect::<Vec<_>>().len();
			if match_count < self.num_banned_substrs {
				Ok(Some(data))
			} else {
				Ok(None)
			}
		} else {
		    let matches: Vec<_> = ac_banlist.find_iter(&url).collect();
		    
		    // Filter matches to only keep those at word boundaries
		    let valid_matches = matches.into_iter().filter(|mat| {
		        let start = mat.start();
		        let end = mat.end();
		        
		        let is_start_boundary = start == 0 || !url[..start].chars().last().unwrap().is_alphanumeric();
		        let is_end_boundary = end == url.len() || !url[end..].chars().next().unwrap().is_alphanumeric();
		        
		        is_start_boundary && is_end_boundary
		    }).collect::<Vec<_>>();
		    
		    if valid_matches.len() < self.num_banned_substrs {
		        Ok(Some(data))
		    } else {
		        Ok(None)
		    }
		}
	}
}


impl UrlSubstringFilter {
	pub fn construct_w_explicit_banlist(config: &Value, banlist: HashSet<String>) -> Result<Self, Error> {

		let url_key = config.get("url_key").unwrap().as_str().unwrap().to_string();
		let ignore_chars = get_default(config, "ignore_chars", Vec::new()).into_iter().map(|el| el.as_str().unwrap().to_string()).collect();
		let num_banned_substrs = get_default(config, "num_banned_substrs", 1);
		let exact_domain_match = get_default(config, "exact_domain_match", false);
		let match_substrings = get_default(config, "match_substrings", true);
		let case_sensitive = get_default(config, "case_sensitive", false);



		let ac_banlist = if !exact_domain_match {
			let banlist_vec : Vec<String> = banlist.clone().into_iter().map(|v| v).collect();
			Some(AhoCorasick::new(banlist_vec).unwrap())
		} else {
			None
		};
		Ok(Self {url_key, ignore_chars, num_banned_substrs, exact_domain_match, match_substrings, case_sensitive, banlist, ac_banlist})

	}
}


#[derive(Serialize, Debug)]
pub struct NewlineRemovalModifier {
	// Modifies the doc by controlling for maximum number of consecutive newlines
	pub text_field: String,
	pub max_consecutive: usize
}
impl DataProcessor for NewlineRemovalModifier {
	fn new(config: &Value) -> Result<Self, Error> {
		let text_field = get_default(config, "text_field", String::from("text"));
		let max_consecutive = get_default(config, "max_consecutive", 2);
		Ok(Self {text_field, max_consecutive})	
	}


	fn process(&self, mut data: Value) -> Result<Option<Value> ,Error> {

		let text = json_get(&data, &self.text_field).unwrap().as_str().unwrap().to_string();
	    let pattern = Regex::new(&format!(r"\n{{{},}}", self.max_consecutive + 1)).unwrap();
	    let replacement = "\n".repeat(self.max_consecutive);
	    let new_text = pattern.replace_all(&text, replacement.as_str()).to_string();
	    json_set(&mut data, &self.text_field, serde_json::Value::String(new_text)).unwrap();

		Ok(Some(data))
	}
}




#[derive(Serialize, Debug)]
pub struct FastTextAnnotator {
	// Enriches the data with the top k predictions from a fast text classifier
	pub fast_text_file: String,
	pub text_field: String,
	pub output_field: String,
	pub k: i32,
	pub threshold: f32,
	#[serde(skip)]
	pub model: FastText
}

impl DataProcessor for FastTextAnnotator {
	fn new(config: &Value) -> Result<Self, Error> {

		let fast_text_file = config.get("fast_text_file").unwrap().as_str().unwrap().to_string();
		let text_field = get_default(config, "text_field", String::from("text"));
		let output_field = get_default(config, "output_field", String::from("metadata.fasttext"));
		let k = get_default(config, "k", 10 as usize) as i32;
		let threshold = get_default(config, "threshold", 0.0) as f32;
		let mut model = FastText::new();
		model.load_model(&fast_text_file).unwrap();
		Ok(Self {fast_text_file, text_field, output_field, k, threshold, model})	
	}


	fn process(&self, mut data: Value) -> Result<Option<Value> ,Error> {

		let text = json_get(&data, &self.text_field).unwrap().as_str().unwrap().to_string().replace("\n", " ");
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


#[derive(Serialize, Debug)]
pub struct FloatFilter {
	// Filters to only keep docs that have float in doc.float_field in range [lower_bound, upper_bound] (or ![lower_bound, upper_bound])
	pub float_field: String,
	pub lower_bound: f32,
	pub upper_bound: f32,
	pub negate: bool, // if this is true, collect only lines that do NOT meet the criteria
	pub default: f32,
}

impl DataProcessor for FloatFilter {
	fn new(config: &Value) -> Result<Self, Error> {
		let float_field = config.get("float_field").unwrap().as_str().unwrap().to_string();
		let lower_bound = get_default(config, "lower_bound", 0.0 as f64) as f32;
		let upper_bound = get_default(config, "upper_bound", f32::MAX as f64) as f32;
		let negate = get_default(config, "negate", false);
		let default = get_default(config, "default", 0.0 as f64) as f32;

		Ok(Self {float_field, lower_bound, upper_bound, negate, default})
	}

	fn process(&self, data: Value) -> Result<Option<Value>, Error> {
		let val = if let Some(json_val) = json_get(&data, &self.float_field) {
			json_val.as_f64().ok_or(anyhow!("Float field {:?} | {:?} is not a number?", self.float_field, json_val))? as f32
		} else {
			self.default
		};
		let mut passes = self.lower_bound <= val && val <= self.upper_bound;
		if self.negate {
			passes = !passes
		}

		if passes {
			Ok(Some(data))
		} else {
			Ok(None)
		}
	}
}



#[derive(Serialize, Debug)]
pub struct PageLenFilter {
	/*
    This function measures page length according to a specified atomic unit (e.g., char, word, sentence,
    line, paragraph).
    If the length is less than `min_length`, it returns None
    If the length is greater/equal to `min_length`, it returns the original JSON object.
    */	
	pub text_field: String,
	pub length_type: String,
	pub lower_bound: usize,
	pub upper_bound: usize,
	pub ignore_punctuation: bool,
}

impl DataProcessor for PageLenFilter {
	fn new(config: &Value) -> Result<Self, Error> {
		let text_field = get_default(config, "text_field", String::from("text"));
		let length_type = config.get("length_type").unwrap().as_str().unwrap().to_string();		
		ensure!(["word", "sentence", "line", "paragraph", "char"].contains(&&length_type.as_str()),
				format!("Length type must be one of {{word, sentence, line, paragraph, char}} and not {:?}", length_type)
			);

		let lower_bound = get_default(config, "lower_bound", 1 as usize);
		let upper_bound = get_default(config, "upper_bound", usize::MAX);
		let ignore_punctuation = get_default(config, "ignore_punctuation", true);

		Ok(Self {text_field, length_type, lower_bound, upper_bound, ignore_punctuation})
	}

	fn process(&self, data: Value) -> Result<Option<Value>, Error> {
		let text = json_get(&data, &self.text_field).unwrap().as_str().unwrap().to_string();
		let len = match self.length_type.as_str() {
			"word" => {
				text.as_str().split_word_bounds()
					.filter(|v| v.trim().len() > 0 && (v.chars().next().unwrap().is_alphanumeric() || !self.ignore_punctuation))
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


#[derive(Serialize, Debug)]
pub struct WordLenFilter {
	// Filters according to average word length
	pub text_field: String,
	pub lower_bound: f32,
	pub upper_bound: f32
}

impl DataProcessor for WordLenFilter {
	fn new(config: &Value) -> Result<Self, Error> {
		let text_field = get_default(config, "text_field", String::from("text"));
		let lower_bound = get_default(config, "lower_bound", 0.0 as f64) as f32;
		let upper_bound = get_default(config, "upper_bound", f32::MAX as f64) as f32;		
		Ok(Self {text_field, lower_bound, upper_bound})
	}

	fn process(&self, data: Value) -> Result<Option<Value>, Error> {
		let text = json_get(&data, &self.text_field).unwrap().as_str().unwrap().to_string();
		let word_lens: Vec<usize> = text.split_whitespace().map(|v| v.len()).collect();

		let avg_word_len = word_lens.iter().sum::<usize>() as f32 / word_lens.len() as f32;

		if self.lower_bound <= avg_word_len && avg_word_len <= self.upper_bound {
			Ok(Some(data))
		} else {
			Ok(None)
		}	
	}
}


#[derive(Serialize, Debug)]
pub struct SymbolRatioFilter {
	// Filters the doc by how many symbols (see symbols var) appear relative to other words
	pub text_field: String,
	pub max_symbol_to_word_ratio: f32,
}

impl DataProcessor for SymbolRatioFilter {
	fn new(config: &Value) -> Result<Self, Error> {

		let text_field = get_default(config, "text_field", String::from("text"));
		let max_symbol_to_word_ratio = get_default(config, "max_symbol_to_word_ratio", f32::MAX as f64) as f32;
		Ok(Self {text_field, max_symbol_to_word_ratio})
	}

	fn process(&self, data: Value) -> Result<Option<Value>, Error> {
		let text = json_get(&data, &self.text_field).unwrap().as_str().unwrap().to_string();
		let symbols = vec!["#", "...", ". . .", "\u{2026}"];
		let mut num_symbols = 0;
		for symbol in symbols.iter() {
			num_symbols += text.matches(symbol).count();
		}

		let num_words = text.replace(". . .", "...").split_whitespace().collect::<Vec<_>>().len();
		let symbol_to_word_ratio = num_symbols as f32 / std::cmp::max(num_words, 1) as f32;


		if symbol_to_word_ratio <= self.max_symbol_to_word_ratio {
			Ok(Some(data))
		} else {
			Ok(None)
		}	
	}
}


#[derive(Serialize, Debug)]
pub struct BulletFilter {
	// Filters the doc by how many lines starting with bullets appear relative to other lines
	pub text_field: String,
	pub max_bullet_ratio: f32,
}

impl DataProcessor for BulletFilter {
	fn new(config: &Value) -> Result<Self, Error> {

		let text_field = get_default(config, "text_field", String::from("text"));
		let max_bullet_ratio = get_default(config, "max_bullet_ratio", f32::MAX as f64) as f32;
		Ok(Self {text_field, max_bullet_ratio})
	}

	fn process(&self, data: Value) -> Result<Option<Value>, Error> {
		let text = json_get(&data, &self.text_field).unwrap().as_str().unwrap().to_string();
		let lines: Vec<&str> = text.split('\n').collect();
	   let bullet_count = lines.iter()
	        .filter(|line| {
	            line.starts_with('●') || 
	            line.starts_with('•') || 
	            line.starts_with('*') || 
	            line.starts_with('-')
	        })
	        .count();		
	    if bullet_count as f32 / lines.len() as f32 > self.max_bullet_ratio {
	    	Ok(None)
	    } else {
		    Ok(Some(data))
		}
	}
}





#[derive(Serialize, Debug)]
pub struct EllipsisLineRatioFilter {
	// Filters the doc by what fraction of lines end with an ellipsis
	pub text_field: String,
	pub max_ratio: f32,
}

impl DataProcessor for EllipsisLineRatioFilter {
	fn new(config: &Value) -> Result<Self, Error> {
		let text_field = get_default(config, "text_field", String::from("text"));
		let max_ratio = get_default(config, "max_ratio", f32::MAX as f64) as f32;
		Ok(Self {text_field, max_ratio})
	}

	fn process(&self, data: Value) -> Result<Option<Value>, Error> {
		let text = json_get(&data, &self.text_field).unwrap().as_str().unwrap().to_string();
		let lines: Vec<&str> = text.lines().filter(|line| line.len() > 0).collect();

	    let ellipsis_count = lines.iter()
	        .filter(|line| {
	            line.ends_with("...") || line.ends_with(". . .") || line.ends_with("\u{2026}")
	        })
	        .count();


	    let ratio = ellipsis_count as f32 / std::cmp::max(lines.len(), 1) as f32;
		if ratio <= self.max_ratio {
			Ok(Some(data))
		} else {
			Ok(None)
		}	
	}
}


#[derive(Serialize, Debug)]
pub struct AlphabeticWordRatioFilter {
	// Filters the doc by what fraction of words are NOT alphanumeric
	pub text_field: String,
	pub max_ratio: f32,
}

impl DataProcessor for AlphabeticWordRatioFilter {
	fn new(config: &Value) -> Result<Self, Error> {
		let text_field = get_default(config, "text_field", String::from("text"));
		let max_ratio = get_default(config, "max_ratio", f32::MAX as f64) as f32;
		Ok(Self {text_field, max_ratio})
	}

	fn process(&self, data: Value) -> Result<Option<Value>, Error> {

		let text = json_get(&data, &self.text_field).unwrap().as_str().unwrap().to_string();
		let words = text.split_whitespace().collect::<Vec<_>>();
		if words.len() == 1 {
			return Ok(None);
		}
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

#[derive(Serialize, Debug)]
pub struct StopWordFilter {
	// Filters to only include docs that have min_stop_words stopwords
	pub text_field: String,
	pub count_unique: bool,
	pub min_stop_word: usize,
	/////////
	pub stop_words: HashSet<String>,
}

impl DataProcessor for StopWordFilter {
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
		if self.min_stop_word == 0 {
			return Ok(Some(data));
		}
		let text = json_get(&data, &self.text_field).unwrap().as_str().unwrap().to_string();
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


#[derive(Serialize, Debug)]
pub struct MassiveWebRepetitionFilter {
	// Fancy repetition thing from Gopher
	pub text_field: String,
}

impl DataProcessor for MassiveWebRepetitionFilter {
	fn new(config: &Value) -> Result<Self, Error> {
		let text_field = get_default(config, "text_field", String::from("text"));
		Ok(Self { text_field })		
	}
	fn process(&self, data: Value) -> Result<Option<Value>, Error> {
		let text = json_get(&data, &self.text_field).unwrap().as_str().unwrap().to_string();
		let lines: Vec<&str> = text.split('\n').filter(|w| w.len() > 0).collect();
		let pars: Vec<&str> = text.split("\n\n").filter(|w| w.len() > 0).collect();
		let words: Vec<&str> = text.unicode_words().collect();	
		
		let flow_args = vec![((&lines, 1, false), 0.3),
						     ((&pars, 1, false), 0.3),
						     ((&lines, 1, true), 0.2),
						     ((&pars, 1, true), 0.2),
						     ((&words, 2, true), 0.2),
						     ((&words, 3, true), 0.18),
						     ((&words, 4, true), 0.16),
						     ((&words, 5, true), 0.15),
						     ((&words, 6, true), 0.14),
						     ((&words, 7, true), 0.13),
						     ((&words, 8, true), 0.12),
						     ((&words, 9, true), 0.11),
						     ((&words, 10, true), 0.10)
						    ];
		for (arglist, upper_bound) in flow_args.into_iter() {
			let rep_frac = MassiveWebRepetitionFilter::_rep_counter_fraction(arglist.0, arglist.1, arglist.2).unwrap();
			if rep_frac > upper_bound {
				return Ok(None);
			}
		}
		
		Ok(Some(data))
	}



}

impl MassiveWebRepetitionFilter {
	pub fn _rep_counter_fraction<'a>(elements: &'a Vec<&'a str>, ngram_size: usize, weighted: bool,) -> Result<f32, Error> {
		let mut ngram : VecDeque<&'a str> = VecDeque::with_capacity(ngram_size); // temp to hold current "ngram"
		let mut ngram_char_len = 0; // temp to current ngram len?

		let mut ngram_counts: HashMap<(u64, usize), Vec<usize>> = HashMap::new(); //(ngram_hash, ngram_char_len) -> [idxs where this ngram starts, ...]
		let total_elements = elements.len(); 
		let mut total_ngrams = 0;
		let total_charlen = elements.iter().map(|v| v.len()).sum::<usize>(); 

		for (idx, &element) in elements.iter().enumerate() {
			ngram.push_back(element);
			ngram_char_len += element.len();
			if ngram.len() >= ngram_size { // if enough "elements" to make ngram
				// hash ngram and add it to counts

				let mut hasher = FxHasher::default();
				ngram.hash(&mut hasher);
				let hash_val: u64 = hasher.finish();
				ngram_counts.entry((hash_val, ngram_char_len)).or_insert(Vec::new()).push((idx +1) - ngram_size);

				total_ngrams += 1;				
				ngram_char_len -= ngram.pop_front().unwrap().len();
			}
		}

		// Special cases: either 0 or 1 ngrams
		if total_ngrams == 0 {
			if ngram_size == 1 { return Ok(1.0); } else { return Ok(0.0);}
		} else if total_ngrams == 1 {
			return Ok(0.0);
		} 


		let repeat_frac = if ngram_size == 1 {	
			// Single ngram case:
			if weighted {
				// no ngrams, weighted => get total charlen of elements repeated > 1x, divide by total charlen
				let total_repeat_len = ngram_counts.iter()
					.filter_map(|(k,v)| if v.len() > 1 { Some(k.1 * v.len()) } else { None })
					.sum::<usize>();
				total_repeat_len as f32 / total_charlen as f32
			} else {
				// no ngrams, unweighted => get total repeated elements >1x, divide by total elements
				let total_repeats = ngram_counts.iter()
					.filter_map(|(_k,v)| if v.len() > 1 { Some(v.len()) } else { None })
					.sum::<usize>();
				total_repeats as f32 / total_elements as f32
			}
		} else { 
			// Ngram size > 1 case:
			// If ngram size is >= 4, juts find the ngram that occurs most-often and use this to generate indexes 
			// otherwise, find ALL ngrams that occur > 1
			// Use these to generate element indices that are repeated and then count charlen / total_charlen

			let repeated_start_idxs: Vec<usize> = if ngram_size <= 4 {
				let most_common = ngram_counts.iter()
				    .filter(|(_k,v)| v.len() > 1) // only select ngrams that repeat
					.max_by(|a, b| {  // take max of (#repeats, ngramCharLen)
						let value_cmp = a.1.len().cmp(&b.1.len());
						if value_cmp == std::cmp::Ordering::Equal {
							a.0.1.cmp(&b.0.1)
						} else {
							value_cmp
						}
					});
				if let Some(most_common_pair) = most_common {
					most_common_pair.1.to_vec()				
				} else {
					Vec::new()
				}
			} else {
				ngram_counts.into_values()
					.filter(|v| v.len() > 1)
					.flat_map(|v| v)
					.collect()
			};
			let repeat_element_idxs: HashSet<usize> = repeated_start_idxs.iter()
				.flat_map(|v| (*v..(v+ngram_size)).collect::<Vec<usize>>())
				.collect();

			let repeat_len = repeat_element_idxs.iter().map(|idx| elements[*idx].len()).sum::<usize>();				
			repeat_len as f32 / total_charlen as f32
		};
		

		Ok(repeat_frac)

	}
}

#[derive(Serialize, Debug)]
pub struct WordCountAdder {
	// Adds a field which is the count of how many words are in the text_field
	pub text_field: String,
	pub word_count_field: String
}
impl DataProcessor for WordCountAdder {
	fn new(config: &Value) -> Result<Self, Error> {
		let text_field = get_default(config, "text_field", String::from("text"));
		let word_count_field = get_default(config, "word_count_field", String::from("original_word_count"));

		Ok(Self { text_field, word_count_field })
	}

	fn process(&self, mut data: Value) -> Result<Option<Value>, Error> {
		let text = json_get(&data, &self.text_field).unwrap().as_str().unwrap().to_string();

		let word_count = text.unicode_words().collect::<Vec<_>>().len();
		json_set(&mut data, &self.word_count_field, word_count.into()).unwrap();

		Ok(Some(data))
	}
}




#[derive(Serialize, Debug)]
pub struct RatioLineModifier {
	// Modifies docs to keep only lines that have not-too-many uppercase chars or digits
	pub text_field: String,
	pub upper_bound: f32,
	pub check: String,
}

impl DataProcessor for RatioLineModifier {
	fn new(config: &Value) -> Result<Self, Error> {
		let text_field = get_default(config, "text_field", String::from("text"));
		let upper_bound = json_get(config, "upper_bound").unwrap().as_f64().unwrap() as f32;
		let check = json_get(config, "check").unwrap().as_str().unwrap().to_string();
		ensure!(["uppercase", "numeric"].contains(&&check.as_str()),
				format!("Check must be one of {{uppercase, numeric}} and not {:?}", check)
			);


		Ok(Self {text_field, upper_bound, check})
	}

	fn process(&self, mut data: Value) -> Result<Option<Value>, Error> {
		let text = json_get(&data, &self.text_field).unwrap().as_str().unwrap().to_string();
		let lines: Vec<&str> = text.split('\n').collect();


		let mut passing_lines : Vec<&str> = Vec::new();
		for line in lines {
			let line_len = std::cmp::max(line.len(), 1) as f32;
			let count = if &self.check == "uppercase" {
				line.chars().filter(|v| v.is_uppercase()).collect::<Vec<_>>().len() as f32
			} else {
				line.chars().filter(|v| v.is_digit(10)).collect::<Vec<_>>().len() as f32
			};
			if count / line_len <= self.upper_bound {
				passing_lines.push(line)
			}
		}


		json_set(&mut data, &self.text_field, serde_json::Value::String(passing_lines.join("\n"))).unwrap();

		Ok(Some(data))
	}
}

#[derive(Serialize, Debug)]
pub struct RegexLineModifier {
	// Modifies lines to only keep those that don't have any regex matches
	// Note that we automatically lowercase the text we query!
	pub text_field: String,
	pub regex_string: String, // 
	#[serde(skip)]
	pub regex: Regex,
}

impl DataProcessor for RegexLineModifier {
	fn new(config: &Value) -> Result<Self, Error> {
		let counter_regex = r"^\W*\d(?:,|\.|\d)*(?:K|k|M|m|B|b)?\s+(?:likes|shares|comments|retweets|reposts|quotes|bookmarks|upvotes|downvotes|downloads|views|followers)\W*$".to_string();
		let text_field = get_default(config, "text_field", String::from("text"));
		let regex_string = get_default(config, "regex", counter_regex);
		let regex = Regex::new(&regex_string).unwrap();

		Ok(Self { text_field, regex_string, regex })

	}

	fn process(&self, mut data: Value) -> Result<Option<Value>, Error> {
		let text = json_get(&data, &self.text_field).unwrap().as_str().unwrap().to_string();
		let lines: Vec<&str> = text.split('\n').collect();


		let passing_lines: Vec<_> = lines.iter()
			.filter(|line| !self.regex.is_match(&line.to_lowercase()))
			.map(|&l| l)
			.collect();
		if passing_lines.len() == 0 {
			return Ok(None);
		}

		json_set(&mut data, &self.text_field, serde_json::Value::String(passing_lines.join("\n"))).unwrap();

		Ok(Some(data))
	}
}



#[derive(Serialize, Debug)]
pub struct LineLenModifier {
	// Modifes lines to only keep those that have >= lower_bound words
	pub text_field: String,
	pub lower_bound: usize
}

impl DataProcessor for LineLenModifier {
	fn new(config: &Value) -> Result<Self, Error> {
		let text_field = get_default(config, "text_field", String::from("text"));
		let lower_bound = get_default(config, "lower_bound", 0);


		Ok(Self { text_field, lower_bound})

	}

	fn process(&self, mut data: Value) -> Result<Option<Value>, Error> {
		let text = json_get(&data, &self.text_field).unwrap().as_str().unwrap().to_string();
		let lines: Vec<&str> = text.split('\n').collect();


		let passing_lines: Vec<_> = lines.iter()
			.filter(|line| line.unicode_words().collect::<Vec<_>>().len() >= self.lower_bound)
			.map(|&l| l)
			.collect();
		if passing_lines.len() == 0 {
			return Ok(None);
		}

		json_set(&mut data, &self.text_field, serde_json::Value::String(passing_lines.join("\n"))).unwrap();

		Ok(Some(data))
	}
}


#[derive(Serialize, Debug)]
pub struct SubstringLineModifier {
	// Modifies lines to only keep those that don't have any words from the banlist (or just removes those words themselves)
	pub text_field: String, 
	pub banlist: String,
	pub max_len: usize,
	pub remove_substring_only: bool,
	pub location: String
}

impl DataProcessor for SubstringLineModifier {
	fn new(config: &Value) -> Result<Self, Error> {
		let text_field = get_default(config, "text_field", String::from("text"));
		let banlist = config.get("banlist").unwrap().as_str().unwrap().to_string();
		let max_len = get_default(config, "max_len", usize::MAX);
		let remove_substring_only = get_default(config, "remove_substring_only", true);
		let location = get_default(config, "location", String::from("any"));

		Ok(Self { text_field, banlist, max_len, remove_substring_only, location})

	}

	fn process(&self, mut data: Value) -> Result<Option<Value>, Error> {
		let text = json_get(&data, &self.text_field).unwrap().as_str().unwrap().to_string();
		let lines: Vec<&str> = text.split('\n').collect();
	    let (pattern, replacement) = match self.location.as_str() {
	        "prefix" => (format!(r"^(?:{banlist})\s?", banlist = self.banlist), ""),
	        "suffix" => (format!(r"\s?(?:{banlist})$", banlist = self.banlist), ""),
	        _ => (format!(r"\s?(?:{banlist})\s?", banlist = self.banlist), " ")
	    };

	    let regex = Regex::new(&pattern).unwrap();


		let mut passing_lines: Vec<String> = Vec::new();

		for line in lines {
			let line = line.to_string();
			if self.max_len == usize::MAX || line.unicode_words().collect::<Vec<_>>().len() <= self.max_len {

				if self.remove_substring_only {
					let cleaned = regex.replace_all(&line, replacement).to_string();
					if !cleaned.trim().is_empty() {
						passing_lines.push(cleaned);
					}
				} else {
					if regex.is_match(&line) { continue ;}
					passing_lines.push(line);

				}
			} else {
				passing_lines.push(line);
			}
		}	
		json_set(&mut data, &self.text_field, serde_json::Value::String(passing_lines.join("\n"))).unwrap();

		Ok(Some(data))
	}
}


#[derive(Serialize, Debug)]
pub struct WordRemovalRatioFilter {
	// Only keeps docs that haven't removed too many words (from a previous, old, word_count_field)
	pub text_field: String,
	pub word_count_field: String,
	pub upper_bound: f32
}

impl DataProcessor for WordRemovalRatioFilter {
	fn new(config: &Value) -> Result<Self, Error> {
		let text_field = get_default(config, "text_field", String::from("text"));
		let word_count_field = get_default(config, "word_count_field", String::from("original_word_count"));
		let upper_bound = get_default(config, "upper_bound", 1.0) as f32;
		Ok(Self { text_field, word_count_field, upper_bound})

	}

	fn process(&self, data: Value) -> Result<Option<Value>, Error> {
		let text = json_get(&data, &self.text_field).unwrap().as_str().unwrap().to_string();
		let old_word_count: usize = json_get(&data, &self.word_count_field).unwrap().as_u64().unwrap() as usize;
		let cur_word_count: usize = text.unicode_words().collect::<Vec<_>>().len();

		let removed_ratio = ((old_word_count - cur_word_count) as f32) / old_word_count as f32;
		if removed_ratio <= self.upper_bound {
			Ok(Some(data))
		} else {
			Ok(None)
		}
	}
}


#[derive(Derivative)]
#[derivative(Debug)]
#[derive(Serialize)]
pub struct Madlad400SentenceFilter {
	// Does the madlad400 sec2.3 filter : https://openreview.net/pdf?id=Y45ZCxslFx
	pub text_field: String,
	pub sentence_lower_bound: usize, // defaults to 5
	pub sentence_question_upper_bound: f32, // defaults to 20%


	// document consistency 
	pub fast_text_file: String, // path to fasttext model
	#[serde(skip)]
	pub model: FastText,
	pub langid_field: String, // field where the document level language is 

	// list case
	pub case_upper_bound: f32, // defaults to 0.50
	pub case_tok_lower_bound: usize, // defaults to 12

	// abnormal lengths
	pub char_len_lower_bound: usize, // defaults to 20
	pub char_len_upper_bound: usize, // defaults to 500 

	// technical chars
	pub tech_lower_bound: f32, // defaults to 0.20
    #[derivative(Debug="ignore")]	
    #[serde(skip)]
    pub tech_charset: HashSet<char>,

	// cursed regxes 
	pub cursed_regex_file: String, // path to cursed strings // last 4 are regexes
    #[derivative(Debug="ignore")]	
    #[serde(skip)]
	pub cursed_inclusions: AhoCorasick,
    #[derivative(Debug="ignore")]	
    #[serde(skip)]
	pub cursed_regexes: Vec<Regex>,

}

impl DataProcessor for Madlad400SentenceFilter {
	fn new(config: &Value) -> Result<Self, Error> {
		let text_field = get_default(config, "text_field", String::from("text"));
		let sentence_lower_bound = get_default(config, "sentence_lower_bound", 5);
		let sentence_question_upper_bound = get_default(config, "sentence_question_upper_bound", 0.20) as f32;

		let fast_text_file = config.get("fast_text_file").unwrap().as_str().unwrap().to_string();
		let mut model = FastText::new();
		model.load_model(&fast_text_file).unwrap();
		let langid_field = config.get("langid_field").unwrap().as_str().unwrap().to_string();

		let case_upper_bound = get_default(config, "case_upper_bound", 0.50) as f32;
		let case_tok_lower_bound = get_default(config, "case_tok_lower_bound", 12);

		let char_len_lower_bound = get_default(config, "char_len_lower_bound", 20);
		let char_len_upper_bound = get_default(config, "char_len_upper_bound", 500);

		let tech_lower_bound = get_default(config, "tech_lower_bound", 0.20) as f32;
		let tech_charset: HashSet<char> = ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 
                               '{', '}', '+', '/', '(', ')', '>'].into_iter().collect();

		let cursed_regex_file = config.get("cursed_regex_file").unwrap().as_str().unwrap().to_string();
		let cursed_regex_data = read_pathbuf_to_mem(&PathBuf::from(cursed_regex_file.clone())).unwrap();
		let cursed_regex_lines: Vec<_> = cursed_regex_data.lines().map(|l| l.unwrap()).collect();
		let cursed_inclusions = AhoCorasick::new(&cursed_regex_lines[..cursed_regex_lines.len() - 4]).unwrap();
		let mut cursed_regexes: Vec<Regex> = Vec::new();
		for el in &cursed_regex_lines[cursed_regex_lines.len() - 4..] {
			cursed_regexes.push(Regex::new(el).unwrap());
		}
		Ok(Self { text_field, sentence_lower_bound, sentence_question_upper_bound, 
				  fast_text_file, model, langid_field,
				  case_upper_bound, case_tok_lower_bound,
				  char_len_lower_bound, char_len_upper_bound,
				  tech_lower_bound, tech_charset,
				  cursed_regex_file, cursed_inclusions, cursed_regexes})			
	}

	fn process(&self, data: Value) -> Result<Option<Value>, Error> {

		// Setup for filtering
		let text = json_get(&data, &self.text_field).unwrap().as_str().unwrap().to_string();		
		let sentence_splitter = Regex::new(r"[.!?]+\s+").unwrap();
		let sentences: Vec<_> = sentence_splitter.split(&text).filter(|s| s.trim().len() > 0 ).collect();
		let num_sentences = sentences.len();
		if num_sentences < self.sentence_lower_bound {
			return Ok(None);
		}

		let doc_lang = json_get(&data, &self.langid_field).unwrap().as_object().unwrap()
			.iter()
			.max_by(|(_, a), (_, b)| (&(a.as_f64().unwrap())).partial_cmp(&(b.as_f64().unwrap())).unwrap_or(std::cmp::Ordering::Equal)).unwrap().0;

		let sentence_threshold = num_sentences as f32 * self.sentence_question_upper_bound;
		let mut sus_sentences = 0;
		//let mut sus_levels = vec![0,0,0,0,0];

		// Loop through sentences
		for sentence in sentences {
			// Stop to exit early maybe
			if sus_sentences as f32 > sentence_threshold {
				return Ok(None);
			}

			// Check abnormal len sentences
			if self.abnormal_len_sentence(sentence).unwrap() {
				//sus_levels[0] += 1;
				sus_sentences += 1;
				continue;
			}

			// Then check technical character counts
			if self.technical_characters(sentence).unwrap() {
				//sus_levels[1] += 1;
				sus_sentences += 1;
				continue;
			}

			// Then check case
			if self.list_case(sentence).unwrap() {
				//sus_levels[2] += 1;
				sus_sentences += 1;
				continue;
			}

			// Then do cursed regex stuff
			if self.check_cursed_regexes(sentence).unwrap() {
				//sus_levels[3] += 1;
				sus_sentences += 1;
				continue;
			}

			// And finally langid
			if self.document_consistency(sentence, doc_lang).unwrap() {
				//sus_levels[4] += 1;
				sus_sentences += 1;
				continue
			}
		}


		// If too many questionable setences, filter out
		//println!("Sus sentences {:?}", sus_levels);
		if sus_sentences as f32 > sentence_threshold {
			Ok(None)
		} else {
			Ok(Some(data))
		}		
	}
}

impl Madlad400SentenceFilter {
	// Individual checks. Returns True if the sentence IS questionable!
	pub fn abnormal_len_sentence(&self, sentence: &str) -> Result<bool, Error> {
		Ok(sentence.len() < self.char_len_lower_bound || sentence.len() > self.char_len_upper_bound)
	}

	pub fn technical_characters(&self, sentence: &str) -> Result<bool, Error> {
		let technical_chars = sentence.chars().filter(|c| self.tech_charset.contains(c)).count();
		Ok((technical_chars as f32) > sentence.len() as f32 * self.tech_lower_bound)
	}

	pub fn list_case(&self, sentence: &str) -> Result<bool, Error> {
		// List case : we treat "tokens" here as words
		let words: Vec<&str> = sentence.unicode_words().collect();
		if words.len() < self.case_tok_lower_bound {
			return Ok(false);
		}
		let cap_counts = words.iter().filter(|w| {
			if let Some(first_char) = w.chars().next() {
				first_char.is_uppercase()
			} else {
				false
			}
		})
		.count();

		Ok(cap_counts as f32 > words.len() as f32 * self.case_upper_bound)		
	}

	pub fn check_cursed_regexes(&self, sentence: &str) -> Result<bool, Error> {
		if let Some(_) = self.cursed_inclusions.find_iter(sentence).next() {
			return Ok(true)
		}
		let has_curse = self.cursed_regexes.iter().any(|re| {
			if let Some(_) = re.find(sentence) {
				true
			} else {
				false
			}
		});
		Ok(has_curse)	
	}

	pub fn document_consistency(&self, sentence: &str, doc_lang: &str) -> Result<bool, Error> {
		// Do langid 
		let sentence_lang_preds = &self.model.predict(&sentence.replace("\n", " "), 1, 0.0).unwrap();
		if sentence_lang_preds.len() == 0 {
			return Ok(true);
		}
		let sentence_lang = &sentence_lang_preds.iter()
			.max_by(|a, b| (&a.prob).partial_cmp(&b.prob).unwrap_or(std::cmp::Ordering::Equal))
			.unwrap()
			.label;
		Ok(sentence_lang != doc_lang)	
	}
}



