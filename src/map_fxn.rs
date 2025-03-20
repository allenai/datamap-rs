use std::time::Instant;
use std::hash::{Hash, Hasher};
use std::collections::VecDeque;
use std::hash::DefaultHasher;
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
    register_processor!(m, "url_filter", UrlSubstringFilterJson);
    register_processor!(m, "newline_removal_modifier", NewlineRemovalModifier);
    register_processor!(m, "fasttext_anno", FastTextAnnotator);
    register_processor!(m, "float_filter", FloatFilter);
    register_processor!(m, "page_len_filter", PageLenFilter);
    register_processor!(m, "word_len_filter", WordLengthFilter);
    register_processor!(m, "symbol_ratio_filter", SymbolRatioFilter);
    register_processor!(m, "bullet_filter", BulletFilter);
    register_processor!(m, "ellipsis_line_ratio_filter", EllipsisLineRatioFilter);
    register_processor!(m, "alphabetic_word_ratio_filter", AlphabeticWordRatioFilter);
    register_processor!(m, "stop_word_filter", StopWordFilter);
    register_processor!(m, "massive_web_repetition_filter", MassiveWebRepetitionFilter);
    register_processor!(m, "word_count_adder", WordCountAdder);
    register_processor!(m, "ratio_line_modifier", RatioLineModifier);
    register_processor!(m, "regex_line_modifier", RegexLineModifier);
    register_processor!(m, "line_length_modifier", LineLengthModifier);
    register_processor!(m, "substring_line_modifier", SubstringLineModifier);
    register_processor!(m, "word_removal_ratio_filter", WordRemovalRatioFilter);
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

	pub fn process(&self, data: Value, timing_info: &mut TimingInfo, filter_info: &mut FilterInfo) -> Result<Option<Value>, Error> {
	    let mut current_data = data;
		
		let mut filter_step = 0;
	    for processor in &self.pipeline {
	    	let start_step = Instant::now();
	    	let proc_data = processor.process(current_data).unwrap();
	        *timing_info.entry(filter_step).or_insert(0 as u128) += start_step.elapsed().as_nanos();

	        if let Some(data_value) = proc_data {
	        	current_data = data_value;
	        } else {
	        	*filter_info.entry(filter_step).or_insert(0 as usize) += 1;
	        	return Ok(None);
	        }
	        filter_step += 1;
	    }
	    *filter_info.entry(usize::MAX).or_insert(0 as usize) += 1;
	    Ok(Some(current_data))
	}

	pub fn process_lines(&self, lines: Vec<String>) -> Result<(Vec<Value>, TimingInfo, FilterInfo), Error> {
		let mut timing_info = TimingInfo::new();
		let mut filter_info = FilterInfo::new();
		let mut output_lines: Vec<Value> = Vec::new();
		for line in lines {
			let json_line = serde_json::from_str(&line).unwrap();
			if let Some(json_out) = self.process(json_line, &mut timing_info, &mut filter_info).unwrap() {
				output_lines.push(json_out);
			}
		};

		Ok((output_lines, timing_info, filter_info))
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
struct TextLenFilter {
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
struct AddIdModifier {
	// Adds a uuidv4 value to the id_key field
	id_key: String
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
struct SantaCoderPLFilter {
	// Filters to collect only documents tha have pl_key in [Python, Java, Javascript]
	pl_key : String
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


#[derive(Serialize, Debug)]
struct UrlSubstringFilterJson {
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

	url_key: String,
	ignore_chars: Vec<String>,
	num_banned_substrs: usize,
	exact_domain_match: bool, 
	match_substrings: bool,
	case_sensitive: bool,
	banlist: HashSet<String>, // Key for this is banlist_file
	#[serde(skip)]
	ac_banlist: Option<AhoCorasick>
}
impl DataProcessor for UrlSubstringFilterJson {
	fn new(config: &Value) -> Result<Self, Error> {
		let url_key = config.get("url_key").unwrap().as_str().unwrap().to_string();
		let ignore_chars = get_default(config, "ignore_chars", Vec::new()).into_iter().map(|el| el.as_str().unwrap().to_string()).collect();
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
		let mut url = json_get(&data, &self.url_key).unwrap().as_str().unwrap().to_string();
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





#[derive(Serialize, Debug)]
struct NewlineRemovalModifier {
	// Modifies the doc by controlling for maximum number of consecutive newlines
	text_field: String,
	max_consecutive: usize
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
struct FastTextAnnotator {
	// Enriches the data with the top k predictions from a fast text classifier
	fast_text_file: String,
	text_field: String,
	output_field: String,
	k: i32,
	threshold: f32,
	#[serde(skip)]
	model: FastText
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

		let text = json_get(&data, &self.text_field).unwrap().as_str().unwrap().to_string();
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
struct FloatFilter {
	// Filters to only keep docs that have float in doc.float_field in range [lower_bound, upper_bound]
	float_field: String,
	lower_bound: f32,
	upper_bound: f32,
	default: f32,
}

impl DataProcessor for FloatFilter {
	fn new(config: &Value) -> Result<Self, Error> {
		let float_field = config.get("float_field").unwrap().as_str().unwrap().to_string();
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



#[derive(Serialize, Debug)]
struct PageLenFilter {
	/*
    This function measures page length according to a specified atomic unit (e.g., char, word, sentence,
    line, paragraph).
    If the length is less than `min_length`, it returns None
    If the length is greater/equal to `min_length`, it returns the original JSON object.
    */	
	text_field: String,
	length_type: String,
	lower_bound: usize,
	upper_bound: usize,
	ignore_punctuation: bool,
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


#[derive(Serialize, Debug)]
struct WordLengthFilter {
	// Filters according to average word length
	text_field: String,
	lower_bound: f32,
	upper_bound: f32
}

impl DataProcessor for WordLengthFilter {
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
struct SymbolRatioFilter {
	// Filters the doc by how many symbols (see symbols var) appear relative to other words
	text_field: String,
	max_symbol_to_word_ratio: f32,
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

		let num_words = text.split_whitespace().collect::<Vec<_>>().len();

		let symbol_to_word_ratio = num_symbols as f32 / num_words as f32;


		if symbol_to_word_ratio <= self.max_symbol_to_word_ratio {
			Ok(Some(data))
		} else {
			Ok(None)
		}	
	}
}


#[derive(Serialize, Debug)]
struct BulletFilter {
	// Filters the doc by how many symbols (see symbols var) appear relative to other words
	text_field: String,
	max_bullet_ratio: f32,
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
struct EllipsisLineRatioFilter {
	// Filters the doc by what fraction of lines end with an ellipsis
	text_field: String,
	max_ratio: f32,
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


	    let ratio = ellipsis_count as f32 / lines.len() as f32;
		if ratio <= self.max_ratio {
			Ok(Some(data))
		} else {
			Ok(None)
		}	
	}
}


#[derive(Serialize, Debug)]
struct AlphabeticWordRatioFilter {
	// Filters the doc by what fraction of words are NOT alphanumeric
	text_field: String,
	max_ratio: f32,
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
struct StopWordFilter {
	// Filters to only include docs that have min_stop_words stopwords
	text_field: String,
	count_unique: bool,
	min_stop_word: usize,
	/////////
	stop_words: HashSet<String>,
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
struct MassiveWebRepetitionFilter {
	// Fancy repetition thing from Gopher
	text_field: String,
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
		let words: Vec<&str> = text.split_word_bounds().collect();	
		
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
	fn _rep_counter_fraction(elements: &Vec<&str>, ngram_size: usize, weighted: bool,) -> Result<f32, Error> {
		let mut ngram : VecDeque<String> = VecDeque::with_capacity(ngram_size);
		let mut ngram_counts: HashMap<(u64, usize), Vec<usize>> = HashMap::new();
		let elements_len = elements.len();
		let mut ngram_char_len = 0;
		let mut total_ngrams = 0;
		let total_len = elements.iter().map(|v| v.len()).sum::<usize>();

		for (idx, element) in elements.iter().enumerate() {
			ngram.push_back(element.to_string());
			ngram_char_len += element.len();
			if ngram.len() >= ngram_size {
				total_ngrams += 1;
				let mut hasher = DefaultHasher::new();
				ngram.hash(&mut hasher);
				let hash_val: u64 = hasher.finish();
				ngram_counts.entry((hash_val, ngram_char_len)).or_insert(Vec::new()).push(idx);
				ngram_char_len -= ngram.pop_front().unwrap().len();
			}
		}


		// Special cases:
		if ngram_size == 1 && total_ngrams == 0 {
			return Ok(1.0);
		} else if ngram_size == 1 && total_ngrams == 1 {
			return Ok(0.0);
		} else if ngram_size > 1 && total_ngrams == 0 {
			return Ok(0.0)
		}

		let repeat_frac = if ngram_size == 1 {	
			if weighted {
				// no ngrams, weighted => get total charlen of elements repeated > 1x, divide by total charlen
				let total_repeat_len = ngram_counts.iter()
					.map(|(k,v)| if v.len() > 1 { k.1 * v.len() } else { 0 })
					.sum::<usize>();
				total_repeat_len as f32 / total_len as f32
			} else {
				// no ngrams, unweighted => get total repeated elements >1x, divide by total elements
				let total_repeats = ngram_counts.iter()
					.map(|(_k,v)| if v.len() > 1 { v.len() } else { 0 })
					.sum::<usize>();
				total_repeats as f32 / elements_len as f32
			}
		} else { 
			if ngram_size <= 4 {
				// Get most common repeated ngram (max by (v, ngram.len)), divide by total charlen
				let most_common = ngram_counts.iter()
					.max_by(|a, b| {
						let value_cmp = a.1.len().cmp(&b.1.len());
						if value_cmp == std::cmp::Ordering::Equal {
							a.0.1.cmp(&b.0.1)
						} else {
							value_cmp
						}
					})
					.map(|(k, v)| k.1 *v.len()).unwrap();
				most_common as f32 / total_len as f32

			} else {
				// Get full set of indices for which repeats occur
				let repeat_idxs : HashSet<usize> = ngram_counts.values().filter(|v| v.len() > 1).flat_map(|v| v.clone()).collect();
				let repeat_len = repeat_idxs.into_iter().map(|i| elements[i].len()).sum::<usize>();
				repeat_len as f32 / total_len as f32
			}
		};

		Ok(repeat_frac)

	}
}

#[derive(Serialize, Debug)]
struct WordCountAdder {
	// Adds a field which is the count of how many words are in the text_field
	text_field: String,
	word_count_field: String
}
impl DataProcessor for WordCountAdder {
	fn new(config: &Value) -> Result<Self, Error> {
		let text_field = get_default(config, "text_field", String::from("text"));
		let word_count_field = get_default(config, "word_count_field", String::from("original_word_count"));

		Ok(Self { text_field, word_count_field })
	}

	fn process(&self, mut data: Value) -> Result<Option<Value>, Error> {
		let text = json_get(&data, &self.text_field).unwrap().as_str().unwrap().to_string();
		let word_count = text.split_word_bounds().collect::<Vec<_>>().len();
		json_set(&mut data, &self.word_count_field, word_count.into()).unwrap();

		Ok(Some(data))
	}
}




#[derive(Serialize, Debug)]
struct RatioLineModifier {
	// Modifies docs to keep only lines that have not-too-many uppercase chars or digits
	text_field: String,
	upper_bound: f32,
	check: String,
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
			let line_len = line.len() as f32;
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
struct RegexLineModifier {
	// Modifies lines to only keep those that don't have any regex matches
	text_field: String,
	regex_string: String, // 
	#[serde(skip)]

	regex: Regex,
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
struct LineLengthModifier {
	// Modifes lines to only keep those that have >= lower_bound words
	text_field: String,
	lower_bound: usize
}

impl DataProcessor for LineLengthModifier {
	fn new(config: &Value) -> Result<Self, Error> {
		let text_field = get_default(config, "text_field", String::from("text"));
		let lower_bound = get_default(config, "lower_bound", 0);


		Ok(Self { text_field, lower_bound})

	}

	fn process(&self, mut data: Value) -> Result<Option<Value>, Error> {
		let text = json_get(&data, &self.text_field).unwrap().as_str().unwrap().to_string();
		let lines: Vec<&str> = text.split('\n').collect();


		let passing_lines: Vec<_> = lines.iter()
			.filter(|line| line.split_word_bounds().collect::<Vec<_>>().len() >= self.lower_bound)
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
struct SubstringLineModifier {
	// Modifies lines to only keep those that don't have any words from the banlist (or just removes those words themselves)
	text_field: String, 
	banlist: String,
	max_len: usize,
	remove_substring_only: bool,
	location: String

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
	    let pattern = match self.location.as_str() {
	        "prefix" => format!(r"^(?:{banlist})\s?", banlist = self.banlist),
	        "suffix" => format!(r"\s?(?:{banlist})$", banlist = self.banlist),
	        _ => format!(r"\s?(?:{banlist})", banlist = self.banlist),
	    };

	    let regex = Regex::new(&pattern).unwrap();


		let mut passing_lines: Vec<String> = Vec::new();

		for line in lines {
			let line = line.to_string();
			if self.max_len == usize::MAX || line.split_word_bounds().collect::<Vec<_>>().len() <= self.max_len {

				if self.remove_substring_only {
					let cleaned = regex.replace(&line, "").to_string();
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
struct WordRemovalRatioFilter {
	// Only keeps docs that haven't removed too many words (from a previous, old, word_count_field)
	text_field: String,
	word_count_field: String,
	upper_bound: f32
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
		let cur_word_count: usize = text.split_word_bounds().collect::<Vec<_>>().len();

		let removed_ratio = ((old_word_count - cur_word_count) as f32) / old_word_count as f32;
		if removed_ratio <= self.upper_bound {
			Ok(Some(data))
		} else {
			Ok(None)
		}
	}
}
