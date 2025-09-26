
use std::cmp;
use std::time::Instant;
use crate::utils::{extract_subdomain, get_default, json_get, json_set, json_remove};
use aho_corasick::AhoCorasick;
use anyhow::{anyhow, ensure, Error, Result};
use once_cell::sync::Lazy;
use rand::rng;
use rand::Rng;
use serde::Serialize;
use serde_json;
use serde_json::{json, Value};
use std::collections::VecDeque;
use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::io::BufRead;
use std::path::PathBuf;
use uuid::Uuid;

use fasttext::FastText;
use fxhash::{FxHasher, FxHashMap};
use mj_io::read_pathbuf_to_mem;
use regex::Regex;
use unicode_segmentation::UnicodeSegmentation;
use url::Url;
use xxhash_rust::xxh3::{xxh3_128, xxh3_64};
use once_cell::sync::OnceCell;
use derivative::Derivative;

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
static PROCESSOR_CONSTRUCTORS: Lazy<HashMap<&'static str, ProcessorConstructor>> =
    Lazy::new(|| {
        let mut m: HashMap<&'static str, ProcessorConstructor> = HashMap::new();
        register_processor!(m, "non_null_filter", NonNullFilter);
        register_processor!(m, "text_len_filter", TextLenFilter);
        register_processor!(m, "subsample", SubsampleFilter);
        register_processor!(m, "santcoder_pl_filter", SantaCoderPLFilter);
        register_processor!(m, "add_id", AddIdModifier);
        register_processor!(m, "url_substring_filter", UrlSubstringFilter);
        register_processor!(m, "newline_removal_modifier", NewlineRemovalModifier);
        register_processor!(m, "fasttext_annotator", FastTextAnnotator);
        register_processor!(m, "float_filter", FloatFilter);
        register_processor!(m, "string_eq_filter", StringEqFilter);
        register_processor!(m, "page_len_filter", PageLenFilter);
        register_processor!(m, "word_len_filter", WordLenFilter);
        register_processor!(m, "symbol_ratio_filter", SymbolRatioFilter);
        register_processor!(m, "bullet_filter", BulletFilter);
        register_processor!(m, "ellipsis_line_ratio_filter", EllipsisLineRatioFilter);
        register_processor!(m, "alphabetic_word_ratio_filter", AlphabeticWordRatioFilter);
        register_processor!(m, "stop_word_filter", StopWordFilter);
        register_processor!(
            m,
            "massive_web_repetition_filter",
            MassiveWebRepetitionFilter
        );
        register_processor!(m, "word_count_adder", WordCountAdder);
        register_processor!(m, "ratio_line_modifier", RatioLineModifier);
        register_processor!(m, "regex_line_modifier", RegexLineModifier);
        register_processor!(m, "line_len_modifier", LineLenModifier);
        register_processor!(m, "substring_line_modifier", SubstringLineModifier);
        register_processor!(m, "word_removal_ratio_filter", WordRemovalRatioFilter);
        register_processor!(m, "madlad400_sentence_annotator", Madlad400SentenceAnnotator);
        register_processor!(m, "madlad400_rule_filter", Madlad400RuleFilter);
        // Add more processor types as needed
        register_processor!(m, "interval_filter", IntervalFilter);
        register_processor!(m, "dd_max_getter", DDMaxGetter);
        register_processor!(m, "hash_annotator", HashAnnotator);
        register_processor!(m, "max_extractor", MaxExtractor);
        register_processor!(m, "constant_annotator", ConstantAnnotator);
        register_processor!(m, "rename_modifier", RenameModifier);
        m
    });

pub trait AnyDataProcessor: Send + Sync + std::fmt::Debug {
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
    pub pipeline: Vec<Box<dyn AnyDataProcessor>>,
}

impl PipelineProcessor {
    // Create an empty pipeline
    pub fn new(config: &Value) -> Result<Self, Error> {
        let mut pipeline: Vec<Box<dyn AnyDataProcessor>> = Vec::<Box<dyn AnyDataProcessor>>::new();
        let text_field = get_default(&config, "text_field", String::from("text"));

        let pipeline_configs = config.get("pipeline").unwrap().as_array().unwrap();
        for subconfig in pipeline_configs {
            let subconfig_name = subconfig.get("name").unwrap().as_str().unwrap();
            let default_json = json!({});
            let mut subconfig_kwargs: Value = subconfig
                .get("kwargs")
                .or(Some(&default_json))
                .unwrap()
                .clone();
            json_set(
                &mut subconfig_kwargs,
                &String::from("text_field"),
                serde_json::Value::String(text_field.clone()),
            )
            .unwrap();
            let constructor = PROCESSOR_CONSTRUCTORS[subconfig_name];
            pipeline.push(constructor(&subconfig_kwargs).unwrap());

        }
        Ok(Self { pipeline })
    }

    pub fn process(
        &self,
        data: Value,
        _timing_info: &mut TimingInfo,
        _filter_info: &mut FilterInfo,
    ) -> Result<(usize, Option<Value>), Error> {
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
            *_timing_info.entry(filter_step).or_insert(0 as u128) += start_step.elapsed().as_nanos();

            match proc_result {
                Some(data_value) => current_data = data_value,
                None => {
                    *_filter_info.entry(filter_step).or_insert(0 as usize) += 1;
                    return Ok((filter_step, Some(og_copy)));
                }
            }

            filter_step += 1;
        }
        *_filter_info.entry(usize::MAX).or_insert(0 as usize) += 1;
        Ok((usize::MAX, Some(current_data)))
    }

    pub fn process_lines(
        &self,
        lines: Vec<String>,
        filename: &PathBuf,
    ) -> Result<
        (
            HashMap<usize, Vec<Value>>,
            Vec<String>,
            TimingInfo,
            FilterInfo,
        ),
        Error,
    > {
        let mut timing_info = TimingInfo::new();
        let mut filter_info = FilterInfo::new();
        let mut output_lines: HashMap<usize, Vec<Value>> = HashMap::new();
        let mut err_lines: Vec<String> = Vec::new();
        for (line_num, line) in lines.into_iter().enumerate() {        
            let json_parse_result = serde_json::from_str(&line);
            match json_parse_result {
                Ok(json_line) => {
                    let process_out = self.process(json_line, &mut timing_info, &mut filter_info);
                    match process_out {
                        Ok((step_out, json_result)) => {
                            if let Some(json_out) = json_result {
                                output_lines
                                    .entry(step_out)
                                    .or_insert_with(Vec::new)
                                    .push(json_out);
                            }
                        }
                        Err(_e) => err_lines.push(line.clone()),
                    };                    
                },
                Err(_e) => {
                    println!("Error parsing json in {:?}:{:?}", filename, line_num);
                    err_lines.push(line.clone())
                }
            };
        }

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
pub struct NonNullFilter {

}
impl DataProcessor for NonNullFilter {
    fn new(_config: &Value) -> Result<Self, Error> {
        Ok(Self { })
    }

    fn process(&self, data: Value) -> Result<Option<Value>, Error> {
        if data.is_null() {
            Ok(None)
        }
        else {
            Ok(Some(data))
        }
    }
}


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
        Ok(Self {
            text_field,
            lower_bound,
            upper_bound,
        })
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
    pub id_key: String,
}
impl DataProcessor for AddIdModifier {
    fn new(config: &Value) -> Result<Self, Error> {
        let id_key = get_default(config, "id_key", String::from("id"));
        Ok(Self { id_key })
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
    pub pl_key: String,
}
impl DataProcessor for SantaCoderPLFilter {
    fn new(config: &Value) -> Result<Self, Error> {
        let pl_key = get_default(config, "pl_key", String::from("metadata.language"));
        Ok(Self { pl_key })
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
    subsample_rate: f64,
}
impl DataProcessor for SubsampleFilter {
    fn new(config: &Value) -> Result<Self, Error> {
        let subsample_rate = get_default(config, "subsample_rate", 1.0 as f64);
        Ok(Self { subsample_rate })
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
    pub alt_url_key: String, // Alternate key in case first one is missing

    // Main modes of operation
    pub exact_domain_match: bool,
    pub exact_subdomain_match: bool,
    pub exact_url_match: bool,
    pub exact_part_match: bool,
    pub match_substrings: bool,

    // Modifiers
    pub case_sensitive: bool,
    pub ignore_chars: Vec<String>,
    pub num_banned_substrs: usize,

    // Internal storage
    #[derivative(Debug = "ignore")]
    pub banlist: HashSet<String>, // Key for this is banlist_file
    #[derivative(Debug = "ignore")]
    #[serde(skip)]
    pub ac_banlist: Option<AhoCorasick>,


    #[derivative(Debug = "ignore")]
    #[serde(skip)]
    pub part_splitter: Option<Regex>,
}

impl DataProcessor for UrlSubstringFilter {
    fn new(config: &Value) -> Result<Self, Error> {
        let case_sensitive = get_default(config, "case_sensitive", false);

        let banlist_file = PathBuf::from(config.get("banlist_file").unwrap().as_str().unwrap());
        let banlist_data = read_pathbuf_to_mem(&banlist_file).unwrap();
        let banlist: HashSet<String> = banlist_data
            .lines()
            .map(|line| {
                if case_sensitive {
                    line.unwrap().to_lowercase()
                } else {
                    line.unwrap()
                }
            })
            .collect();

        UrlSubstringFilter::construct_w_explicit_banlist(config, banlist)
    }

    fn process(&self, data: Value) -> Result<Option<Value>, Error> {
        // Process url

        let url_val = if let Some(url_val) = json_get(&data, &self.url_key) {
            url_val
        } else {
            if let Some(url_val) = json_get(&data, &self.alt_url_key) {
                url_val
            } else {
                return Ok(None);
            }
        };
        let mut url = url_val.as_str().unwrap().to_string();

        // Extract domain/subdomain if exact match case
        url = if self.exact_domain_match {
            let parsed_url = Url::parse(&url)?;
            let host_str = parsed_url
                .host_str()
                .ok_or_else(|| anyhow!("URL has no host component"))?;
            host_str.to_string()
        } else if self.exact_subdomain_match {
            let subdomain_match = extract_subdomain(&url)?;
            if let Some(subdomain) = subdomain_match {
                subdomain
            } else {
                return Ok(Some(data));
            }
        } else {
            url
        };

        // Handle downcasing + ignore_chars
        url = if !self.case_sensitive {
            url.to_lowercase()
        } else {
            url
        };
        for c in &self.ignore_chars {
            url = url.replace(c, "");
        }

        // Exact match case
        if self.exact_domain_match || self.exact_subdomain_match || self.exact_url_match {
            if self.banlist.contains(&url) {
                return Ok(None);
            } else {
                return Ok(Some(data));
            }
        }

        // Exact part match
        if self.exact_part_match {
            if let Some(ref splitter) = self.part_splitter {
                for part in splitter.split(&url) {
                    if !part.is_empty() && self.banlist.contains(part) {
                        return Ok(None)
                    }
                }
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
            let valid_matches = matches
                .into_iter()
                .filter(|mat| {
                    let start = mat.start();
                    let end = mat.end();

                    let is_start_boundary =
                        start == 0 || !url[..start].chars().last().unwrap().is_alphanumeric();
                    let is_end_boundary =
                        end == url.len() || !url[end..].chars().next().unwrap().is_alphanumeric();

                    is_start_boundary && is_end_boundary
                })
                .collect::<Vec<_>>();

            if valid_matches.len() < self.num_banned_substrs {
                Ok(Some(data))
            } else {
                Ok(None)
            }
        }
    }
}

impl UrlSubstringFilter {
    pub fn construct_w_explicit_banlist(
        config: &Value,
        banlist: HashSet<String>,
    ) -> Result<Self, Error> {
        let url_key = config.get("url_key").unwrap().as_str().unwrap().to_string();
        let alt_url_key = get_default(config, "alt_url_key", String::from("ALT_URL_KEY"));
        let ignore_chars = get_default(config, "ignore_chars", Vec::new())
            .into_iter()
            .map(|el| el.as_str().unwrap().to_string())
            .collect();
        let num_banned_substrs = get_default(config, "num_banned_substrs", 1);
        let exact_domain_match = get_default(config, "exact_domain_match", false);
        let exact_subdomain_match = get_default(config, "exact_subdomain_match", false);
        let exact_url_match = get_default(config, "exact_url_match", false);
        let exact_part_match = get_default(config, "exact_part_match", false);
        let match_substrings = get_default(config, "match_substrings", true);
        let case_sensitive = get_default(config, "case_sensitive", false);

        let ac_banlist =
            if exact_domain_match | exact_subdomain_match | exact_url_match | exact_part_match {
                None
            } else {
                let banlist_vec: Vec<String> = banlist.clone().into_iter().map(|v| v).collect();
                Some(AhoCorasick::new(banlist_vec).unwrap())
            };

        let part_splitter = if exact_part_match {
            Some(Regex::new(r"[^a-zA-Z0-9]+").unwrap())
        } else {
            None
        };

        Ok(Self {
            url_key,
            alt_url_key,
            exact_domain_match,
            exact_subdomain_match,
            exact_url_match,
            exact_part_match,
            match_substrings,
            case_sensitive,
            ignore_chars,
            num_banned_substrs,
            banlist,
            ac_banlist,
            part_splitter
        })
    }
}

#[derive(Serialize, Debug)]
pub struct NewlineRemovalModifier {
    // Modifies the doc by controlling for maximum number of consecutive newlines
    pub text_field: String,
    pub max_consecutive: usize,
}
impl DataProcessor for NewlineRemovalModifier {
    fn new(config: &Value) -> Result<Self, Error> {
        let text_field = get_default(config, "text_field", String::from("text"));
        let max_consecutive = get_default(config, "max_consecutive", 2);
        Ok(Self {
            text_field,
            max_consecutive,
        })
    }

    fn process(&self, mut data: Value) -> Result<Option<Value>, Error> {
        let text = json_get(&data, &self.text_field)
            .unwrap()
            .as_str()
            .unwrap()
            .to_string();
        let pattern = Regex::new(&format!(r"\n{{{},}}", self.max_consecutive + 1)).unwrap();
        let replacement = "\n".repeat(self.max_consecutive);
        let new_text = pattern.replace_all(&text, replacement.as_str()).to_string();
        json_set(
            &mut data,
            &self.text_field,
            serde_json::Value::String(new_text),
        )
        .unwrap();

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
    pub model: FastText,
}

impl DataProcessor for FastTextAnnotator {
    fn new(config: &Value) -> Result<Self, Error> {
        let fast_text_file = config
            .get("fast_text_file")
            .unwrap()
            .as_str()
            .unwrap()
            .to_string();
        let text_field = get_default(config, "text_field", String::from("text"));
        let output_field = get_default(config, "output_field", String::from("metadata.fasttext"));
        let k = get_default(config, "k", 10 as usize) as i32;
        let threshold = get_default(config, "threshold", 0.0) as f32;
        let mut model = FastText::new();
        model.load_model(&fast_text_file).unwrap();
        Ok(Self {
            fast_text_file,
            text_field,
            output_field,
            k,
            threshold,
            model,
        })
    }

    fn process(&self, mut data: Value) -> Result<Option<Value>, Error> {
        let mut text = json_get(&data, &self.text_field)
            .unwrap()
            .as_str()
            .unwrap()
            .to_string()
            .replace("\n", " ");
        text.push_str("\n");

        let predictions = match self.model.predict(&text, self.k, self.threshold) {
			Ok(preds) => preds,
			Err(_e) => {
				// If prediction fails, drop this document by returning None, this can happen for some bad utf bytes etc that happen very rarely
				return Ok(None);
			}
		};

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
        let float_field = config
            .get("float_field")
            .unwrap()
            .as_str()
            .unwrap()
            .to_string();
        let lower_bound = get_default(config, "lower_bound", 0.0 as f64) as f32;
        let upper_bound = get_default(config, "upper_bound", f32::MAX as f64) as f32;
        let negate = get_default(config, "negate", false);
        let default = get_default(config, "default", 0.0 as f64) as f32;

        Ok(Self {
            float_field,
            lower_bound,
            upper_bound,
            negate,
            default,
        })
    }

    fn process(&self, data: Value) -> Result<Option<Value>, Error> {
        let val = if let Some(json_val) = json_get(&data, &self.float_field) {
            json_val.as_f64().ok_or(anyhow!(
                "Float field {:?} | {:?} is not a number?",
                self.float_field,
                json_val
            ))? as f32
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
pub struct StringEqFilter {
    // Filters based on string equality
    pub str_field: String,
    pub eq: String,
    pub keep_matches: bool  // defaults to true, which means we keep docs that have this trait; o/w docs that don't
}

impl DataProcessor for StringEqFilter {
    fn new(config: &Value) -> Result<Self, Error> {
        let str_field = config
            .get("str_field")
            .unwrap()
            .as_str()
            .unwrap()
            .to_string();
        let eq = config
            .get("eq")
            .unwrap()
            .as_str()
            .unwrap()
            .to_string();
        let keep_matches = get_default(config, "keep_matches", true);

        Ok(Self {str_field, eq, keep_matches})
    }

    fn process(&self, data: Value) -> Result<Option<Value>, Error> {
        let val = json_get(&data, &self.str_field).unwrap().as_str().unwrap().to_string();

        if (&val == &self.eq) == self.keep_matches {
            return Ok(Some(data));
        }
        Ok(None)
    }
}


#[derive(Serialize, Debug)]
pub struct PageLenFilter {
    pub text_field: String,
    pub length_type: LengthType,
    pub lower_bound: usize,
    pub upper_bound: usize,
    pub ignore_punctuation: bool,
}

#[derive(Serialize, Debug, Clone, Copy, PartialEq)]
pub enum LengthType {
    Word,
    Sentence,
    Line,
    Paragraph,
    Char,
}

impl std::str::FromStr for LengthType {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "word" => Ok(LengthType::Word),
            "sentence" => Ok(LengthType::Sentence),
            "line" => Ok(LengthType::Line),
            "paragraph" => Ok(LengthType::Paragraph),
            "char" => Ok(LengthType::Char),
            _ => Err(anyhow!(
                "Length type must be one of {{word, sentence, line, paragraph, char}} and not {:?}",
                s
            )),
        }
    }
}

impl DataProcessor for PageLenFilter {
    fn new(config: &Value) -> Result<Self, Error> {
        let text_field = get_default(config, "text_field", String::from("text"));

        let length_type_str = config
            .get("length_type")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow!("length_type is required and must be a string"))?;

        let length_type = length_type_str.parse::<LengthType>()?;

        let lower_bound = get_default(config, "lower_bound", 1_usize);
        let upper_bound = get_default(config, "upper_bound", usize::MAX);
        let ignore_punctuation = get_default(config, "ignore_punctuation", true);

        Ok(Self {
            text_field,
            length_type,
            lower_bound,
            upper_bound,
            ignore_punctuation,
        })
    }

    fn process(&self, data: Value) -> Result<Option<Value>, Error> {
        let text = json_get(&data, &self.text_field)
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow!("Text field '{}' not found or not a string", self.text_field))?;

        let len = self.calculate_length(text)?;
        if self.lower_bound <= len && len <= self.upper_bound {
            Ok(Some(data))
        } else {
            Ok(None)
        }
    }
}

impl PageLenFilter {
    fn calculate_length(&self, text: &str) -> Result<usize, Error> {
        match self.length_type {
            LengthType::Word => Ok(self.count_words(text)),
            LengthType::Char => Ok(if self.ignore_punctuation {
                text.chars().filter(|c| c.is_alphanumeric()).count()
            } else {
                text.chars().count()
            }),
            LengthType::Line => Ok(text.lines().count()),
            LengthType::Sentence => Ok(self.count_sentences(text)),
            LengthType::Paragraph => Ok(self.count_paragraphs(text)),
        }
    }
    fn count_words(&self, text: &str) -> usize {
        if !text.is_ascii() {
            return self.count_words_uni(text);
        }

        let mut count = 0;
        let mut in_word = false;

        for &byte in text.as_bytes() {
            if self.ignore_punctuation {
                let is_word_char = byte.is_ascii_alphanumeric();
                if is_word_char && !in_word {
                    count += 1;
                    in_word = true;
                } else if !is_word_char {
                    in_word = false;
                }
            } else {
                // Count alphanumeric sequences and individual punctuation as separate words
                if byte.is_ascii_alphanumeric() {
                    if !in_word {
                        count += 1;
                        in_word = true;
                    }
                } else if byte.is_ascii_punctuation() {
                    if in_word {
                        in_word = false;
                    }
                    count += 1; // Each punctuation mark is a separate word
                } else {
                    // Whitespace or other characters
                    in_word = false;
                }
            }
        }
        count
    }



    fn count_words_uni(&self, text: &str) -> usize {
        if self.ignore_punctuation {
            text.unicode_words().count()
        } else {
            text.split_word_bounds()
                .filter(|s| !s.trim().is_empty())
                .count()
        }
    }

    fn count_sentences(&self, text: &str) -> usize {
        text.chars()
            .filter(|&c| matches!(c, '.' | '!' | '?'))
            .count()
            .max(1) // At least 1 sentence if text is non-empty
    }

    fn count_paragraphs(&self, text: &str) -> usize {
        text.split("\n\n")
            .filter(|p| !p.trim().is_empty())
            .count()
            .max(1) // At least 1 paragraph if text is non-empty
    }
}


#[derive(Serialize, Debug)]
pub struct WordLenFilter {
    // Filters according to average word length
    pub text_field: String,
    pub lower_bound: f32,
    pub upper_bound: f32,
}

impl DataProcessor for WordLenFilter {
    fn new(config: &Value) -> Result<Self, Error> {
        let text_field = get_default(config, "text_field", String::from("text"));
        let lower_bound = get_default(config, "lower_bound", 0.0 as f64) as f32;
        let upper_bound = get_default(config, "upper_bound", f32::MAX as f64) as f32;
        Ok(Self {
            text_field,
            lower_bound,
            upper_bound,
        })
    }

    fn process(&self, data: Value) -> Result<Option<Value>, Error> {
        let text = json_get(&data, &self.text_field)
            .unwrap()
            .as_str()
            .unwrap()
            .to_string();
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
        let max_symbol_to_word_ratio =
            get_default(config, "max_symbol_to_word_ratio", f32::MAX as f64) as f32;
        Ok(Self {
            text_field,
            max_symbol_to_word_ratio,
        })
    }

    fn process(&self, data: Value) -> Result<Option<Value>, Error> {
        let text = json_get(&data, &self.text_field)
            .unwrap()
            .as_str()
            .unwrap()
            .to_string();
        let symbols = vec!["#", "...", ". . .", "\u{2026}"];
        let mut num_symbols = 0;
        for symbol in symbols.iter() {
            num_symbols += text.matches(symbol).count();
        }

        let num_words = text
            .replace(". . .", "...")
            .split_whitespace()
            .collect::<Vec<_>>()
            .len();
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
        Ok(Self {
            text_field,
            max_bullet_ratio,
        })
    }

    fn process(&self, data: Value) -> Result<Option<Value>, Error> {
        let text = json_get(&data, &self.text_field)
            .unwrap()
            .as_str()
            .unwrap()
            .to_string();
        let lines: Vec<&str> = text.split('\n').collect();
        let bullet_count = lines
            .iter()
            .filter(|line| {
                line.starts_with('●')
                    || line.starts_with('•')
                    || line.starts_with('*')
                    || line.starts_with('-')
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
        Ok(Self {
            text_field,
            max_ratio,
        })
    }

    fn process(&self, data: Value) -> Result<Option<Value>, Error> {
        let text = json_get(&data, &self.text_field)
            .unwrap()
            .as_str()
            .unwrap()
            .to_string();
        let lines: Vec<&str> = text.lines().filter(|line| line.len() > 0).collect();

        let ellipsis_count = lines
            .iter()
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
        Ok(Self {
            text_field,
            max_ratio,
        })
    }

    fn process(&self, data: Value) -> Result<Option<Value>, Error> {
        let text = json_get(&data, &self.text_field)
            .unwrap()
            .as_str()
            .unwrap()
            .to_string();
        let words = text.split_whitespace().collect::<Vec<_>>();
        if words.len() == 1 {
            return Ok(None);
        }
        let total_words = words.len() as f32;
        let non_alpha_words = words
            .into_iter()
            .filter(|w| !w.chars().any(|c| c.is_alphabetic()))
            .collect::<Vec<_>>()
            .len();

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
    pub text_field: String,
    pub count_unique: bool,
    pub min_stop_word: usize,
    // Use &'static str for better performance
    pub stop_words: HashSet<&'static str>,
}

impl DataProcessor for StopWordFilter {
    fn new(config: &Value) -> Result<Self, Error> {
        let text_field = get_default(config, "text_field", String::from("text"));
        let count_unique = get_default(config, "count_unique", false);
        let min_stop_word = get_default(config, "min_stop_word", 2);

        // Use &'static str to avoid String allocations
        let stop_words: HashSet<&'static str> =
            ["the", "be", "to", "of", "and", "that", "have", "with"]
            .into_iter()
            .collect();

        Ok(Self {
            text_field,
            count_unique,
            min_stop_word,
            stop_words,
        })
    }

    fn process(&self, data: Value) -> Result<Option<Value>, Error> {
        // Early return optimization
        if self.min_stop_word == 0 {
            return Ok(Some(data));
        }

        let text = json_get(&data, &self.text_field).unwrap().as_str().unwrap();

        let meets_threshold = if self.count_unique {
            self.has_unique_stop_words(text)
        } else {
            self.has_enough_stop_words(text)
        };

        if meets_threshold {
            Ok(Some(data))
        } else {
            Ok(None)
        }
    }
}

impl StopWordFilter {
    // Return boolean instead of moving data
    fn has_unique_stop_words(&self, text: &str) -> bool {
        let mut unique_stop_words = HashSet::new();

        // Avoid collecting into Vec, process words as iterator
        for word in text.split_whitespace() {
            let word_lower = word.to_lowercase();
            if self.stop_words.contains(word_lower.as_str()) {
                unique_stop_words.insert(word_lower);
                if unique_stop_words.len() >= self.min_stop_word {
                    return true;
                }
            }
        }
        false
    }

    fn has_enough_stop_words(&self, text: &str) -> bool {
        let mut count = 0;

        // Process words as iterator without collecting
        for word in text.split_whitespace() {
            let word_lower = word.to_lowercase();
            if self.stop_words.contains(word_lower.as_str()) {
                count += 1;
                if count >= self.min_stop_word {
                    return true;
                }
            }
        }
        false
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
        let text = json_get(&data, &self.text_field)
            .unwrap()
            .as_str()
            .unwrap()
            .to_string();
        let lines: Vec<&str> = text.split('\n').filter(|w| w.len() > 0).collect();
        let pars: Vec<&str> = text.split("\n\n").filter(|w| w.len() > 0).collect();
        let words: Vec<&str> = text.unicode_words().collect();

        let flow_args = vec![
            ((&lines, 1, false), 0.3),
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
            ((&words, 10, true), 0.10),
        ];
        for (arglist, upper_bound) in flow_args.into_iter() {
            let rep_frac =
                MassiveWebRepetitionFilter::_rep_counter_fraction(arglist.0, arglist.1, arglist.2)
                    .unwrap();
            if rep_frac > upper_bound {
                return Ok(None);
            }
        }

        Ok(Some(data))
    }
}

impl MassiveWebRepetitionFilter {
    pub fn _rep_counter_fraction<'a>(
        elements: &'a Vec<&'a str>,
        ngram_size: usize,
        weighted: bool,
    ) -> Result<f32, Error> {
        let mut rolling_hash = CompatibleRollingHash::new(ngram_size);
        let mut ngram_counts: FxHashMap<(u64, usize), Vec<usize>> = FxHashMap::default(); //(ngram_hash, ngram_char_len) -> [idxs where this ngram starts, ...]
        let total_elements = elements.len();
        let mut total_ngrams = 0;
        let total_charlen = elements.iter().map(|v| v.len()).sum::<usize>();


        for (idx, &element) in elements.iter().enumerate() {
            rolling_hash.roll(element);

            if rolling_hash.is_full() {
                let hash_val = rolling_hash.get_hash();
                let char_len = rolling_hash.get_char_length();

                ngram_counts
                    .entry((hash_val, char_len))
                    .or_insert_with(Vec::new)
                    .push(idx + 1 - ngram_size);

                total_ngrams += 1;
            }
        }

        // Special cases: either 0 or 1 ngrams
        if total_ngrams == 0 {
            if ngram_size == 1 {
                return Ok(1.0);
            } else {
                return Ok(0.0);
            }
        } else if total_ngrams == 1 {
            return Ok(0.0);
        }

        let repeat_frac = if ngram_size == 1 {
            // Single ngram case:
            if weighted {
                // no ngrams, weighted => get total charlen of elements repeated > 1x, divide by total charlen
                let total_repeat_len = ngram_counts
                    .iter()
                    .filter_map(|(k, v)| {
                        if v.len() > 1 {
                            Some(k.1 * v.len())
                        } else {
                            None
                        }
                    })
                    .sum::<usize>();
                total_repeat_len as f32 / total_charlen as f32
            } else {
                // no ngrams, unweighted => get total repeated elements >1x, divide by total elements
                let total_repeats = ngram_counts
                    .iter()
                    .filter_map(|(_k, v)| if v.len() > 1 { Some(v.len()) } else { None })
                    .sum::<usize>();
                total_repeats as f32 / total_elements as f32
            }
        } else {
            // Ngram size > 1 case:
            // If ngram size is >= 4, juts find the ngram that occurs most-often and use this to generate indexes
            // otherwise, find ALL ngrams that occur > 1
            // Use these to generate element indices that are repeated and then count charlen / total_charlen

            let repeated_start_idxs: Vec<usize> = if ngram_size <= 4 {
                let most_common = ngram_counts
                    .iter()
                    .filter(|(_k, v)| v.len() > 1) // only select ngrams that repeat
                    .max_by(|a, b| {
                        // take max of (#repeats, ngramCharLen)
                        let value_cmp = a.1.len().cmp(&b.1.len());
                        if value_cmp == std::cmp::Ordering::Equal {
                            a.0 .1.cmp(&b.0 .1)
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
                ngram_counts
                    .into_values()
                    .filter(|v| v.len() > 1)
                    .flat_map(|v| v)
                    .collect()
            };
            let repeat_element_idxs: HashSet<usize> = repeated_start_idxs
                .iter()
                .flat_map(|v| (*v..(v + ngram_size)).collect::<Vec<usize>>())
                .collect();

            let repeat_len = repeat_element_idxs
                .iter()
                .map(|idx| elements[*idx].len())
                .sum::<usize>();
            repeat_len as f32 / total_charlen as f32
        };

        Ok(repeat_frac)
    }
}

/// Alternative: True rolling hash that matches original hash values
/// This version computes the same hash as the original but still optimizes other aspects
struct CompatibleRollingHash<'a> {
    window: VecDeque<&'a str>,
    window_size: usize,
    char_length: usize,
}

impl<'a> CompatibleRollingHash<'a> {
    fn new(window_size: usize) -> Self {
        Self {
            window: VecDeque::with_capacity(window_size),
            window_size,
            char_length: 0,
        }
    }

    fn roll(&mut self, new_element: &'a str) -> Option<&'a str> {
        // Add new element
        self.window.push_back(new_element);
        self.char_length += new_element.len();

        // Remove oldest if window is full
        if self.window.len() > self.window_size {
            let removed = self.window.pop_front().unwrap();
            self.char_length -= removed.len();
            Some(removed)
        } else {
            None
        }
    }

    fn get_hash(&self) -> u64 {
        // Hash the entire VecDeque to match original
        let mut hasher = FxHasher::default();
        self.window.hash(&mut hasher);
        hasher.finish()
    }

    fn get_char_length(&self) -> usize {
        self.char_length
    }

    fn is_full(&self) -> bool {
        self.window.len() >= self.window_size
    }
}




#[derive(Serialize, Debug)]
pub struct WordCountAdder {
    // Adds a field which is the count of how many words are in the text_field
    pub text_field: String,
    pub word_count_field: String,
}
impl DataProcessor for WordCountAdder {
    fn new(config: &Value) -> Result<Self, Error> {
        let text_field = get_default(config, "text_field", String::from("text"));
        let word_count_field = get_default(
            config,
            "word_count_field",
            String::from("original_word_count"),
        );

        Ok(Self {
            text_field,
            word_count_field,
        })
    }

    fn process(&self, mut data: Value) -> Result<Option<Value>, Error> {
        let text = json_get(&data, &self.text_field)
            .unwrap()
            .as_str()
            .unwrap();
        let word_count = text.unicode_words().count();
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
        let check = json_get(config, "check")
            .unwrap()
            .as_str()
            .unwrap()
            .to_string();
        ensure!(
            ["uppercase", "numeric"].contains(&&check.as_str()),
            format!(
                "Check must be one of {{uppercase, numeric}} and not {:?}",
                check
            )
        );

        Ok(Self {
            text_field,
            upper_bound,
            check,
        })
    }

    fn process(&self, mut data: Value) -> Result<Option<Value>, Error> {
        let text = json_get(&data, &self.text_field)
            .unwrap()
            .as_str()
            .unwrap()
            .to_string();
        let lines: Vec<&str> = text.split('\n').collect();

        let mut passing_lines: Vec<&str> = Vec::new();
        for line in lines {
            if line.len() == 0 {
                passing_lines.push(line);
                continue;
            }
            let line_len = std::cmp::max(line.len(), 1) as f32;
            let count = if &self.check == "uppercase" {
                line.chars()
                    .filter(|v| v.is_uppercase())
                    .collect::<Vec<_>>()
                    .len() as f32
            } else {
                line.chars()
                    .filter(|v| v.is_digit(10))
                    .collect::<Vec<_>>()
                    .len() as f32
            };
            if count / line_len <= self.upper_bound {
                passing_lines.push(line)
            }
        }

        json_set(
            &mut data,
            &self.text_field,
            serde_json::Value::String(passing_lines.join("\n")),
        )
        .unwrap();

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

        Ok(Self {
            text_field,
            regex_string,
            regex,
        })
    }

    fn process(&self, mut data: Value) -> Result<Option<Value>, Error> {
        let text = json_get(&data, &self.text_field)
            .unwrap()
            .as_str()
            .unwrap()
            .to_string();
        let lines: Vec<&str> = text.split('\n').collect();

        let passing_lines: Vec<_> = lines
            .iter()
            .filter(|line| !self.regex.is_match(&line.to_lowercase()))
            .map(|&l| l)
            .collect();
        if passing_lines.len() == 0 {
            return Ok(None);
        }

        json_set(
            &mut data,
            &self.text_field,
            serde_json::Value::String(passing_lines.join("\n")),
        )
        .unwrap();

        Ok(Some(data))
    }
}

#[derive(Serialize, Debug)]
pub struct LineLenModifier {
    // Modifes lines to only keep those that have >= lower_bound words
    pub text_field: String,
    pub lower_bound: usize,
}

impl DataProcessor for LineLenModifier {
    fn new(config: &Value) -> Result<Self, Error> {
        let text_field = get_default(config, "text_field", String::from("text"));
        let lower_bound = get_default(config, "lower_bound", 0);

        Ok(Self {
            text_field,
            lower_bound,
        })
    }

    fn process(&self, mut data: Value) -> Result<Option<Value>, Error> {
        let text = json_get(&data, &self.text_field)
            .unwrap()
            .as_str()
            .unwrap()
            .to_string();
        let lines: Vec<&str> = text.split('\n').collect();

        let passing_lines: Vec<_> = lines
            .iter()
            .filter(|line| line.unicode_words().collect::<Vec<_>>().len() >= self.lower_bound || line.len() == 0)
            .map(|&l| l)
            .collect();
        if passing_lines.iter().map(|v| v.len()).sum::<usize>() == 0 {
            return Ok(None);
        }

        json_set(
            &mut data,
            &self.text_field,
            serde_json::Value::String(passing_lines.join("\n")),
        )
        .unwrap();

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
    pub location: String,
    #[serde(skip)]
    regex: OnceCell<Regex>

}

impl DataProcessor for SubstringLineModifier {
    fn new(config: &Value) -> Result<Self, Error> {
        let text_field = get_default(config, "text_field", String::from("text"));
        let banlist = config.get("banlist").unwrap().as_str().unwrap().to_string();
        let max_len = get_default(config, "max_len", usize::MAX);
        let remove_substring_only = get_default(config, "remove_substring_only", true);
        let location = get_default(config, "location", String::from("any"));

        Ok(Self {
            text_field,
            banlist,
            max_len,
            remove_substring_only,
            location,
            regex: OnceCell::new(),
        })
    }

    fn process(&self, mut data: Value) -> Result<Option<Value>, Error> {
        let text = json_get(&data, &self.text_field)
            .unwrap()
            .as_str()
            .unwrap();

        // Get or compile regex once
        let regex = self.regex.get_or_try_init(|| {
            let (pattern, _) = match self.location.as_str() {
                "prefix" => (format!(r"^(?:{})\s?", self.banlist), ""),
                "suffix" => (format!(r"\s?(?:{})$", self.banlist), ""),
                _ => (format!(r"\s?(?:{})\s?", self.banlist), " "),
            };
            Regex::new(&pattern)
        })?;

        let replacement = match self.location.as_str() {
            "prefix" | "suffix" => "",
            _ => " ",
        };

        // Use iterator with filter_map for better performance
        let processed_lines: Vec<String> = text
            .lines()
            .filter_map(|line| {
                // Skip empty lines processing if they should be kept as-is
                if line.is_empty() {
                    return Some(String::new());
                }

                // Check max_len constraint first (cheaper operation)
                if self.max_len != usize::MAX {
                    let word_count = line.unicode_words().count();
                    if word_count > self.max_len {
                        return Some(line.to_string());
                    }
                }

                if self.remove_substring_only {
                    let cleaned = regex.replace_all(line, replacement);
                    // Only keep non-empty trimmed lines
                    if !cleaned.trim().is_empty() {
                        Some(cleaned.into_owned())
                    } else {
                        None
                    }
                } else {
                    // If regex matches, skip the line (return None)
                    if regex.is_match(line) {
                        None
                    } else {
                        Some(line.to_string())
                    }
                }
            })
            .collect();

        json_set(
            &mut data,
            &self.text_field,
            serde_json::Value::String(processed_lines.join("\n")),
        )?;

        Ok(Some(data))
    }
}

#[derive(Serialize, Debug)]
pub struct WordRemovalRatioFilter {
    // Only keeps docs that haven't removed too many words (from a previous, old, word_count_field)
    pub text_field: String,
    pub word_count_field: String,
    pub upper_bound: f32,
}

impl DataProcessor for WordRemovalRatioFilter {
    fn new(config: &Value) -> Result<Self, Error> {
        let text_field = get_default(config, "text_field", String::from("text"));
        let word_count_field = get_default(
            config,
            "word_count_field",
            String::from("original_word_count"),
        );
        let upper_bound = get_default(config, "upper_bound", 1.0) as f32;
        Ok(Self {
            text_field,
            word_count_field,
            upper_bound,
        })
    }

    fn process(&self, data: Value) -> Result<Option<Value>, Error> {
        let text = json_get(&data, &self.text_field)
            .unwrap()
            .as_str()
            .unwrap()
            .to_string();
        let old_word_count: usize = json_get(&data, &self.word_count_field)
            .unwrap()
            .as_u64()
            .unwrap() as usize;
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
pub struct Madlad400SentenceAnnotator {
    // Does the madlad400 sec2.3 filter : https://openreview.net/pdf?id=Y45ZCxslFx
    // But just annotates
    pub text_field: String,
    pub sentence_lower_bound: usize,        // defaults to 5
    pub sentence_question_upper_bound: f32, // defaults to 20%
    pub annotation_key: String, // defaults to metadata.madlad
    pub rules_to_include: Vec<usize>, // If empty, includes ALL rules. Otherwise just counts the rules here

    // document consistency
    pub fast_text_file: String, // path to fasttext model
    #[serde(skip)]
    pub model: FastText,
    pub langid_field: String, // field where the document level language is

    // list case
    pub case_upper_bound: f32,       // defaults to 0.50
    pub case_tok_lower_bound: usize, // defaults to 12

    // abnormal lengths
    pub char_len_lower_bound: usize, // defaults to 20
    pub char_len_upper_bound: usize, // defaults to 500

    // technical chars
    pub tech_lower_bound: f32, // defaults to 0.20
    #[derivative(Debug = "ignore")]
    #[serde(skip)]
    pub tech_charset: HashSet<char>,

    // cursed regxes
    pub cursed_regex_file: String, // path to cursed strings // last 4 are regexes
    #[derivative(Debug = "ignore")]
    #[serde(skip)]
    pub cursed_inclusions: AhoCorasick,
    #[derivative(Debug = "ignore")]
    #[serde(skip)]
    pub cursed_regexes: Vec<Regex>,
}

impl DataProcessor for Madlad400SentenceAnnotator {
    fn new(config: &Value) -> Result<Self, Error> {
        let text_field = get_default(config, "text_field", String::from("text"));
        let sentence_lower_bound = get_default(config, "sentence_lower_bound", 5);
        let sentence_question_upper_bound =
            get_default(config, "sentence_question_upper_bound", 0.20) as f32;

        let annotation_key = get_default(config, "annotation_key", String::from("metadata.madlad"));
        let rules_to_include: Vec<usize> = get_default(config, "rules_to_include", vec![])
            .into_iter().map(|v| v.as_u64().unwrap() as usize).collect::<Vec<usize>>();
        let fast_text_file = config
            .get("fast_text_file")
            .unwrap()
            .as_str()
            .unwrap()
            .to_string();
        let mut model = FastText::new();
        model.load_model(&fast_text_file).unwrap();
        let langid_field = config
            .get("langid_field")
            .unwrap()
            .as_str()
            .unwrap()
            .to_string();

        let case_upper_bound = get_default(config, "case_upper_bound", 0.50) as f32;
        let case_tok_lower_bound = get_default(config, "case_tok_lower_bound", 12);

        let char_len_lower_bound = get_default(config, "char_len_lower_bound", 20);
        let char_len_upper_bound = get_default(config, "char_len_upper_bound", 500);

        let tech_lower_bound = get_default(config, "tech_lower_bound", 0.20) as f32;
        let tech_charset: HashSet<char> = [
            '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '{', '}', '+', '/', '(', ')', '>',
        ]
        .into_iter()
        .collect();

        let cursed_regex_file = config
            .get("cursed_regex_file")
            .unwrap()
            .as_str()
            .unwrap()
            .to_string();
        let cursed_regex_data =
            read_pathbuf_to_mem(&PathBuf::from(cursed_regex_file.clone())).unwrap();
        let cursed_regex_lines: Vec<_> = cursed_regex_data.lines().map(|l| l.unwrap()).collect();
        let cursed_inclusions =
            AhoCorasick::new(&cursed_regex_lines[..cursed_regex_lines.len() - 4]).unwrap();
        let mut cursed_regexes: Vec<Regex> = Vec::new();
        for el in &cursed_regex_lines[cursed_regex_lines.len() - 4..] {
            cursed_regexes.push(Regex::new(el).unwrap());
        }
        Ok(Self {
            text_field,
            sentence_lower_bound,
            sentence_question_upper_bound,
            annotation_key,
            rules_to_include,
            fast_text_file,
            model,
            langid_field,
            case_upper_bound,
            case_tok_lower_bound,
            char_len_lower_bound,
            char_len_upper_bound,
            tech_lower_bound,
            tech_charset,
            cursed_regex_file,
            cursed_inclusions,
            cursed_regexes,
        })
    }

    fn process(&self, mut data: Value) -> Result<Option<Value>, Error> {
        // Setup for filtering
        let text = json_get(&data, &self.text_field)
            .unwrap()
            .as_str()
            .unwrap()
            .to_string();
        let sentence_splitter = Regex::new(r"[.!?]+\s+").unwrap();

        let rules_to_include: HashSet<usize> = if self.rules_to_include.len() == 0 {
            vec![1,2,3,4,5].into_iter().map(|v| v).collect()
        } else {
            self.rules_to_include.iter().map(|v| *v).collect()
        };

        let sentences: Vec<_> = sentence_splitter
            .split(&text)
            .filter(|s| s.trim().len() > 0)
            .collect();
        let num_sentences = sentences.len();
        let madlad_status = self.annotation_key.clone() + "_status";
        let mut tracker: FxHashMap<&str, Vec<usize>> = FxHashMap::default();
        tracker.entry("num_sentences").or_default().push(num_sentences);

        if num_sentences < self.sentence_lower_bound {
            json_set(&mut data, &madlad_status, json!("killed:too_short")).unwrap();
            return Ok(Some(data));
        }

        let doc_lang = json_get(&data, &self.langid_field)
            .unwrap()
            .as_object()
            .unwrap()
            .iter()
            .max_by(|(_, a), (_, b)| {
                (&(a.as_f64().unwrap()))
                    .partial_cmp(&(b.as_f64().unwrap()))
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
            .unwrap()
            .0;

        // Tracker maps rule -> sentence ids for which this pops

        let mut sus_sentences: HashSet<usize> = HashSet::new();
        let sentence_threshold = num_sentences as f32 * self.sentence_question_upper_bound;
        // Loop through sentences


        for (sentence_num, sentence) in sentences.into_iter().enumerate() {
            // And finally langid
            if rules_to_include.contains(&1) && self.document_consistency(sentence, doc_lang).unwrap() {
                tracker.entry("rule.1").or_default().push(sentence_num);
                sus_sentences.insert(sentence_num);
            }

            // Then check case
            if rules_to_include.contains(&2) && self.list_case(sentence).unwrap() {
                tracker.entry("rule.2").or_default().push(sentence_num);
                sus_sentences.insert(sentence_num);

            }

            // Check abnormal len sentences
            if rules_to_include.contains(&3) && self.abnormal_len_sentence(sentence).unwrap() {
                tracker.entry("rule.3").or_default().push(sentence_num);
                sus_sentences.insert(sentence_num);

            }


            // Then check technical character counts
            if rules_to_include.contains(&4) && self.technical_characters(sentence).unwrap() {
                tracker.entry("rule.4").or_default().push(sentence_num);
                sus_sentences.insert(sentence_num);

            }


            // Then do cursed regex stuff
            if rules_to_include.contains(&5) && self.check_cursed_regexes(sentence).unwrap() {
                tracker.entry("rule.5").or_default().push(sentence_num);
                sus_sentences.insert(sentence_num);

            }
        }

        let tracker_json: Value = json!(tracker);
        if sus_sentences.len() as f32 > sentence_threshold {
            json_set(&mut data, &madlad_status, json!("killed:too_many_sus_sentences")).unwrap();
        } else {
            json_set(&mut data, &madlad_status, json!("survived")).unwrap();
        }
        json_set(&mut data, &self.annotation_key, tracker_json).unwrap();
        Ok(Some(data))

    }
}

impl Madlad400SentenceAnnotator {
    // Individual checks. Returns True if the sentence IS questionable!
    pub fn abnormal_len_sentence(&self, sentence: &str) -> Result<bool, Error> {
        Ok(
            sentence.len() < self.char_len_lower_bound
                || sentence.len() > self.char_len_upper_bound,
        )
    }

    pub fn technical_characters(&self, sentence: &str) -> Result<bool, Error> {
        let technical_chars = sentence
            .chars()
            .filter(|c| self.tech_charset.contains(c))
            .count();
        Ok((technical_chars as f32) > sentence.len() as f32 * self.tech_lower_bound)
    }

    pub fn list_case(&self, sentence: &str) -> Result<bool, Error> {
        // List case : we treat "tokens" here as words
        let words: Vec<&str> = sentence.unicode_words().collect();
        if words.len() < self.case_tok_lower_bound {
            return Ok(false);
        }
        let cap_counts = words
            .iter()
            .filter(|w| {
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
            return Ok(true);
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
        let sentence_lang_preds = &self
            .model
            .predict(&sentence.replace("\n", " "), 1, 0.0)
            .unwrap();
        if sentence_lang_preds.len() == 0 {
            return Ok(true);
        }
        let sentence_lang = &sentence_lang_preds
            .iter()
            .max_by(|a, b| {
                (&a.prob)
                    .partial_cmp(&b.prob)
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
            .unwrap()
            .label;
        Ok(sentence_lang != doc_lang)
    }
}



#[derive(Derivative)]
#[derivative(Debug)]
#[derive(Serialize)]
pub struct Madlad400RuleFilter {
    // Filters based on the madlad rules
    // Removes if too_short OR if any of the rule filters applies
    pub annotation_key: String, // defaults to metadata.madlad
    pub status_key: String, // defaults to metadata.madlad_status
    pub remove_too_short: bool, // remove if status is too short, defaults to false
    pub rules_to_remove: Vec<Vec<usize>>,
    pub threshold: f64, // defaults to 0.2
}

impl DataProcessor for Madlad400RuleFilter {
    fn new(config: &Value) -> Result<Self, Error> {

        let annotation_key = get_default(config, "annotation_key", String::from("metadata.madlad"));
        let status_key = get_default(config, "status_key", String::from("metadata.madlad_status"));
        let remove_too_short = get_default(config, "remove_too_short", false);
        let rules_to_remove = get_default(config, "rules_to_remove", Vec::new());
        let rules_to_remove: Vec<Vec<usize>> = if rules_to_remove.len() == 0 {
            Vec::new()
        } else {
            rules_to_remove.into_iter().map(|v| v.as_array().unwrap().into_iter().map(|k| k.clone().as_u64().unwrap() as usize).collect::<Vec<usize>>()).collect::<Vec<Vec<usize>>>()
        };

        let threshold = get_default(config, "threshold", 0.2);

        Ok(Self {
            annotation_key,
            status_key,
            remove_too_short,
            rules_to_remove,
            threshold
        })
    }

    fn process(&self, data: Value) -> Result<Option<Value>, Error> {
    	let status: String = json_get(&data, &self.status_key).unwrap().as_str().unwrap().to_string();

    	if status == "killed:too_short" {
    		if self.remove_too_short {
    			return Ok(None);
    		} else {
    			return Ok(Some(data));
    		}

    	}


        let annotation_data: HashMap<String, Vec<usize>> = serde_json::from_value(json_get(&data, &self.annotation_key).unwrap().clone()).unwrap();
        let num_sentences = annotation_data.get("num_sentences").unwrap()[0];
        let sus_threshold = num_sentences as f64 * &self.threshold;
        for rule in &self.rules_to_remove {
            let mut sus_sentences: HashSet<usize> = HashSet::new();
            for subrule in rule {
                let key = format!("rule.{:}", subrule);
                if let Some(sentence_ids) = annotation_data.get(&key) {
                    for sentence_id in sentence_ids {
                        sus_sentences.insert(*sentence_id);
                    }
                }
            }
            if sus_sentences.len() as f64 >= sus_threshold {
                return Ok(None);
            }
        }


        Ok(Some(data))

    }
}


#[derive(Derivative)]
#[derivative(Debug)]
#[derive(Serialize)]
pub struct IntervalFilter {
    pub text_field: String, // defaults to global text field, or "text"
    pub interval_field: String, // Required! If intervals don't exist, doc is left as is
    pub fuzzy_merge: bool, // defaults to false

    pub merge_fuzziness: f64, // only necessary if fuzzy_merge is true
    pub output_text_field: String, // defaults to text field if not present
}

impl DataProcessor for IntervalFilter {
    fn new(config: &Value) -> Result<Self, Error> {
        let text_field = get_default(config, "text_field", String::from("text_field"));
        let interval_field = json_get(config, "interval_field").unwrap().as_str().unwrap().to_string();
        let fuzzy_merge = get_default(config, "fuzzy_merge", false);
        let merge_fuzziness = get_default(config, "merge_fuzziness", 1.0 as f64);
        let output_text_field = get_default(config, "output_text_field", text_field.clone());
        Ok(Self {text_field, interval_field, fuzzy_merge, merge_fuzziness, output_text_field})
    }

    fn process(&self, mut data: Value) -> Result<Option<Value>, Error> {

        // Collect things we need frorm the data
        let text = json_get(&data, &self.text_field).unwrap().as_str().unwrap().to_string();
        let intervals: Vec<(usize, usize)> = if let Some(base_intervals) = json_get(&data, &self.interval_field) {
            base_intervals.as_array().unwrap().iter().map(|interval| {
                let interval = interval.as_array().unwrap();
                (interval[0].as_u64().unwrap() as usize, interval[1].as_u64().unwrap() as usize)
            }).collect::<Vec<(usize, usize)>>()
        } else {
            return Ok(Some(data));
        };

        // Merge the intervals if that's a thing we need to do
        let intervals = if self.fuzzy_merge {
            fuzzy_interval_merge(intervals, self.merge_fuzziness)
        } else {
            intervals
        };


        // Scrub out the interval data from the text
        let mut output = String::with_capacity(text.len());
        let mut last_excluded = 0;
        for interval in intervals {
            let start = interval.0;
            let end = interval.1;
            output.push_str(&text[last_excluded..start]);
            last_excluded = end;
        }
        if last_excluded < text.len() {
            output.push_str(&text[last_excluded..]);
        }

        if output.len() == 0 {
            return Ok(None);
        }

        json_set(&mut data, &self.output_text_field, serde_json::Value::String(output)).unwrap();
        Ok(Some(data))
    }

}

fn fuzzy_interval_merge(intervals: Vec<(usize, usize)>, merge_fuzziness: f64) -> Vec<(usize, usize)> {
    let forward = fuzzy_sandwich_intervals(&intervals, true, merge_fuzziness);
    let backward = fuzzy_sandwich_intervals(&intervals, false, merge_fuzziness);
    merge_sorted_interval_pair(forward, backward)
}


fn merge_intervals(mut v: Vec<(usize, usize)>, already_sorted: bool) -> Vec<(usize, usize)>{
    if !already_sorted {
        v.sort_by_key(|(key, _)| key.clone());
    }
    let mut merged: Vec<(usize, usize)> = Vec::new();
    for (s, e) in v {
        if merged.len() == 0 {
            merged.push((s, e));
        } else if merged.last().unwrap().1 >= s {
            let (old_s, old_e) = merged.pop().unwrap();
            merged.push((old_s, cmp::max(e, old_e)));
        } else {
            merged.push((s, e));
        }
    }
    merged
}

fn merge_sorted_interval_pair(u: Vec<(usize, usize)>, w: Vec<(usize, usize)>) -> Vec<(usize, usize)> {
    // Given two sorted lists of intervals, does a merge of the pairs, and then unions all intervals
    let mut v : Vec<(usize, usize)> = Vec::new();
    let mut ui = 0;
    let mut wi = 0;
    while ui < u.len() && wi < w.len() {
        let (us, ue) = u[ui];
        let (ws, we) = w[wi];
        if us < ws || (us == ws && ue <= we){
            v.push((us, ue));
            ui += 1;
        } else {
            v.push((ws, we));
            wi += 1
        }
    }
    while ui < u.len() {
        v.push(u[ui]);
        ui += 1;
    }

    while wi < w.len() {
        v.push(w[wi]);
        wi += 1;
    }

    merge_intervals(v, true)
}


fn fuzzy_sandwich_intervals(v: &Vec<(usize, usize)>, foward: bool, threshold: f64) -> Vec<(usize, usize)> {
    // Given SORTED list of DISJOINT intervals, scans in the forward/!forward direction
    // And collects all intervals that:
    // 1. Start and end at an interval
    // 2. Have >=threshold of the range contained in an input interval
    // e.g. [(0,9), (10, 20)] -> [(0,20)] (when the threshold is <=0.95)

    let n = v.len();
    let iter_range : Vec<_> = if foward {
        (0..n).collect()
    } else {
        (0..n).rev().collect()
    };
    let mut output : Vec<(i32, i32, i32)> = Vec::new();
    for idx in iter_range {


        let (next_s, next_e) = v[idx];
        let next_s = next_s as i32;
        let next_e = next_e as i32;

        if output.len() == 0 {
            output.push((next_s, next_e, next_e - next_s));
            continue;
        }
        let (cur_s, cur_e, cur_w) = output.last().unwrap();
        let new_interval = (cmp::min(next_s, *cur_s as i32),
                            cmp::max(next_e, *cur_e as i32),
                            *cur_w  as i32 + next_e - next_s);
        if new_interval.2 as f64 >= (new_interval.1 - new_interval.0) as f64 * threshold {
            output.pop().unwrap();
            output.push(new_interval);
        } else {
            output.push((next_s, next_e, next_e - next_s));
        }
    }

    output
        .iter()
        .map(|(a,b, _)| (*a as usize, *b as usize))
        .collect()
}




#[derive(Serialize, Debug)]
pub struct DDMaxGetter {
    /* {attributes: {
        <prefix>_KEY : [[val]]
    }}
    of attributes keys that start with prefix, returns the max KEY
    */
    pub main_attribute: String, // default to "attributes"
    pub prefix: String,
    pub output_attribute: String,  // where the max KEY goes
}

impl DataProcessor for DDMaxGetter {
    fn new(config: &Value) -> Result<Self, Error> {
        let main_attribute = get_default(config, "main_attribute", String::from("attributes"));

        let prefix = json_get(config, "prefix").unwrap().as_str().unwrap().to_string();
        let output_attribute = json_get(config, "output_attribute").unwrap().as_str().unwrap().to_string();
        Ok(Self {
            main_attribute,
            prefix,
            output_attribute
        })

    }


    fn process(&self, mut data: Value) -> Result<Option<Value>, Error> {
        let input_dict = json_get(&data, &self.main_attribute).unwrap();
        // claude: loop over key,val pairs in input_dict
        // and for keys that start with prefix, get their value as a [[f64]] (or just an f64)

        let mut max_key = String::from("null");
        let mut max_val: f64 = -1.0;

        if let Value::Object(map) = input_dict {
            for (key, value) in map {
                if key.starts_with(&self.prefix) {

					// if the value is an array, get the first element of the first element (jake format)
					// if it is a siple float, just get the value; otherwise throw an error
                    let parsed_val = match value {
                        Value::Array(outer) => &outer[0][0].as_f64().unwrap(),
						Value::Number(num) => &num.as_f64().unwrap(),
						_ => panic!("Invalid value type: {:?}", value),
                    };
                    if *parsed_val > max_val {
                        max_key = key.clone();
                        max_val = *parsed_val;
                    }
                }
            }
        }

        json_set(&mut data, &self.output_attribute, serde_json::Value::String(max_key)).unwrap();
        Ok(Some(data))

    }
}

#[derive(Serialize, Debug)]
pub struct MaxExtractor {
    /*
    - main_attribute points to a dict with str->key floats
    - if the max value is >= lower bound (defaults to 0.0), sets the key to be the value of output_attribute
    */

    pub main_attribute: String,
    pub lower_bound: f64, // defaults to 0.0
    pub output_attribute: String,
    pub keep_nulls: bool, // defaults to true
}


impl DataProcessor for MaxExtractor {
    fn new(config: &Value) -> Result<Self, Error> {
        let main_attribute = json_get(config, "main_attribute").unwrap().as_str().unwrap().to_string();
        let lower_bound: f64 = get_default(config, "lower_bound", 0.0);
        let output_attribute = json_get(config, "output_attribute").unwrap().as_str().unwrap().to_string();
        let keep_nulls = get_default(config, "keep_nulls", true);
        Ok(Self {main_attribute, lower_bound, output_attribute, keep_nulls})
    }

    fn process(&self, mut data: Value) -> Result<Option<Value>, Error> {
        let mut max_key = String::from("");
        let mut max_val: f64 = f64::MIN;
        let input_dict = json_get(&data, &self.main_attribute).unwrap();
        if let Value::Object(map) = input_dict {
            for (key, value) in map {
                let value = value.as_f64().unwrap();
                if value >= max_val && value >= self.lower_bound {
                    max_key = key.to_string();
                    max_val = value;
                }                
            }
        }


        if max_key.len() > 0 {
            json_set(&mut data, &self.output_attribute, serde_json::Value::String(max_key)).unwrap();            
        } else {
            if !&self.keep_nulls {
                return Ok(None);
            }
        }
        Ok(Some(data))

    }
}



#[derive(Serialize, Debug)]
pub struct HashAnnotator {
    // Adds a hash id to
    pub hash_source: String, // field that gets hashed
    pub hash_destination: String, // where the target gets hashed and save
    pub num_bits: usize // defaults to 128
}

impl DataProcessor for HashAnnotator {
    fn new(config: &Value) -> Result<Self, Error> {
        let hash_source = get_default(config, "hash_source", String::from("text"));
        let hash_destination = get_default(config, "hash_destination", String::from("metadata.text_hash"));
        let num_bits = get_default(config, "num_bits", 128);

        assert!(num_bits == 64 || num_bits == 128);

        Ok(Self {
            hash_source,
            hash_destination,
            num_bits
        })
    }

    fn process(&self, mut data: Value) -> Result<Option<Value>, Error> {
        let text = json_get(&data, &self.hash_source)
            .unwrap()
            .as_str()
            .unwrap()
            .to_string();

        let hash_val = if self.num_bits == 128 {
            Value::from(xxh3_128(text.as_bytes()).to_string())
        } else {
            Value::from(xxh3_64(text.as_bytes()))
        };

        json_set(&mut data, &self.hash_destination, hash_val).unwrap();
        Ok(Some(data))
    }
}


#[derive(Serialize, Debug)]
pub struct ConstantAnnotator {
    // Adds a string into every json in a directory
    pub key: String, // location of where we save the constant
    pub value: String, // what we save    
}

impl DataProcessor for ConstantAnnotator {
    fn new(config: &Value) -> Result<Self, Error> {
        let key = json_get(config, "key").unwrap().as_str().unwrap().to_string();
        let value = json_get(config, "value").unwrap().as_str().unwrap().to_string();

        Ok(Self { key, value })
    }

    fn process(&self, mut data: Value) -> Result<Option<Value>, Error> {
        json_set(&mut data, &self.key, json!(&self.value)).unwrap();
        Ok(Some(data))
    }
}


#[derive(Serialize, Debug)]
pub struct RenameModifier {
    // Renames a field in the json
    pub old_field: String, // old field name
    pub new_field: String, // new field name  
}

impl DataProcessor for RenameModifier {
    fn new(config: &Value) -> Result<Self, Error> {
        let old_field = json_get(config, "old_field").unwrap().as_str().unwrap().to_string();
        let new_field = json_get(config, "new_field").unwrap().as_str().unwrap().to_string();

        Ok(Self { old_field, new_field })
    }

    fn process(&self, mut data: Value) -> Result<Option<Value>, Error> {
        let old_val = json_get(&data, &self.old_field).unwrap().clone();
        json_set(&mut data, &self.new_field, old_val).unwrap();
        json_remove(&mut data, &self.old_field).unwrap();

        Ok(Some(data))
    }
}

