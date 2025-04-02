// use regex::Regex;
use std::hash::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::time::Instant;
use serde_json;
use serde_json::{json, Value};
use anyhow::{Error, Result};
use crate::utils::{get_default, json_set, json_get};
use serde::Serialize;
use once_cell::sync::Lazy;
use std::collections::HashMap;
use rustpython_parser::{ast, Parse};

 


/*================================================================================
=                            PIPELINE PROCESSING                                 =
================================================================================*/
type GroupTimingInfo = HashMap<(usize, usize), usize>; // (group_id, step) -> time
type GroupFilterInfo = HashMap<(usize, usize), usize>; // (group_id, step) -> removed docs

type GroupProcessorConstructor = fn(&Value) -> Result<Box<dyn AnyGroupDataProcessor>, Error>;


macro_rules! register_group_processor {
    ($map:expr, $name:expr, $processor_type:ty) => {
        $map.insert($name, |config| {
            let processor = <$processor_type>::new(config).unwrap();
            Ok(Box::new(processor) as Box<dyn AnyGroupDataProcessor>)
        });
    };
}


// Static map of processor types to their constructor wrapper functions
static GROUP_PROCESSOR_CONSTRUCTORS: Lazy<HashMap<&'static str, GroupProcessorConstructor>> = Lazy::new(|| {
    let mut m: HashMap<&'static str, GroupProcessorConstructor> = HashMap::new();
   	register_group_processor!(m, "concatenate", Concatenate);
    // Add more processor types as needed
    
    m
});



pub trait AnyGroupDataProcessor: Send + Sync + std::fmt::Debug  {
    fn process_group(&self, data: Vec<Value>) ->  Result<(Vec<Value>, Vec<Value>, Vec<Value>), Error>;
}

impl<T> AnyGroupDataProcessor for T 
where
    T: GroupDataProcessor + Send + Sync + serde::Serialize + std::fmt::Debug,
{
    fn process_group(&self, data: Vec<Value>) -> Result<(Vec<Value>, Vec<Value>, Vec<Value>), Error> {
        // Just delegate to the underlying DataProcessor implementation
        GroupDataProcessor::process_group(self, data)
    }
    

}

#[derive(Debug)]
pub struct GroupPipelineProcessor {
    pub group_pipelines: Vec<Vec<Box<dyn AnyGroupDataProcessor>>>,
    pub group_keys: Vec<Vec<String>>,
}

impl GroupPipelineProcessor {
    /* Create an empty pipeline
    Config here is group_pipeline is a LIST of GROUP_OPS
    each GROUP_OP has a GROUP_KEY (which is itself a list)
    and a GROUP_OP_LIST (which is a list of fxns, operating as usual)
	*/
    pub fn new(config: &Value) -> Result<Self, Error> {
    	let global_default_text_field = get_default(&config, "text_field", String::from("text"));

    	let mut group_pipelines : Vec<Vec<Box<dyn AnyGroupDataProcessor>>> = Vec::new(); 
    	let mut group_keys: Vec<Vec<String>> = Vec::new();

    	let pipeline_configs = config.get("group_pipeline").unwrap().as_array().unwrap();
    	for subconfig in pipeline_configs {

    		let key = subconfig.get("group_key").unwrap().as_array().unwrap().into_iter().map(|v| v.as_str().unwrap().to_string()).collect();
    		group_keys.push(key);
    		let mut group_op_list: Vec<Box<dyn AnyGroupDataProcessor>> = Vec::new();
    		let group_ops_unparsed: Vec<Value> = subconfig.get("group_ops").unwrap().as_array().unwrap().to_vec();
    		for group_op in group_ops_unparsed {
    			let group_op_name = group_op.get("name").unwrap().as_str().unwrap();
    			let default_json = json!({});
    			let mut group_op_kwargs: Value = group_op.get("kwargs").or(Some(&default_json)).unwrap().clone();
	    		json_set(&mut group_op_kwargs, &String::from("text_field"), serde_json::Value::String(global_default_text_field.clone())).unwrap();
	    		let constructor = GROUP_PROCESSOR_CONSTRUCTORS[group_op_name];
	    		group_op_list.push(constructor(&group_op_kwargs).unwrap());
    		}
    		group_pipelines.push(group_op_list);
    	}
        Ok(Self { group_pipelines, group_keys })
    }


    pub fn process_group(&self, data: Vec<Value>, pipeline_num: usize, timing_info: &mut GroupTimingInfo, filter_info: &mut GroupFilterInfo) -> 
    	Result<(HashMap<usize, Vec<Value>>, Vec<Value>), Error> {
    		// Run through the full pipe

    		let pipeline = &self.group_pipelines[pipeline_num];
    		let mut filtered_lines : HashMap<usize, Vec<Value>> = HashMap::new();
    		let mut errored_lines: Vec<Value> = Vec::new();

    		let mut current_data = data;
    		for (filter_step, processor) in pipeline.iter().enumerate() {
    			let start_step = Instant::now();
    			let (proc_out, proc_removed, proc_erred) = processor.process_group(current_data)?; // proc_out should be (kept lines, removed lines, errored lines)
    			errored_lines.extend(proc_erred);
    			*filter_info.entry((pipeline_num, filter_step)).or_insert(0 as usize) += proc_removed.len();    			
    			filtered_lines.insert(filter_step, proc_removed);
    			*timing_info.entry((pipeline_num, filter_step)).or_insert(0 as usize) += start_step.elapsed().as_nanos() as usize;
    			current_data = proc_out;
    		}
    		filtered_lines.insert(usize::MAX, current_data);

    		Ok((filtered_lines, errored_lines))
    	}


	pub fn process_lines(&self, lines: Vec<Value>) -> Result<(HashMap<(usize, usize), Vec<Value>>, Vec<Value>, GroupTimingInfo, GroupFilterInfo), Error> {
		/* Processes all the group processes in order: 
			Will output:
				- {(group_id, group_step_id) -> files[] pulled out in this group}. (MAX, MAX) refers to the survivors
				- err_lines[], lines that errored 
				- filter_info: how many docs were removed in each step
				- timing_info: how much time was spent in each step of each group
		*/

		// Setup outputs + initial group
		let mut output_lines: HashMap<(usize, usize), Vec<Value>> = HashMap::new();
		let mut err_lines: Vec<Value> = Vec::new();				
		let mut timing_info = GroupTimingInfo::new();
		let mut filter_info = GroupFilterInfo::new();		
		let mut surviving_lines = lines;

		// process each pipeline in order
		for pipeline_num in 0..self.group_keys.len() {
			let current_key = &self.group_keys[pipeline_num];
			let mut new_survivors: Vec<Value> = Vec::new();
			let groups = self.make_group(surviving_lines, current_key).unwrap(); // make groups for this pipeline step

			for group in groups.into_values() { // process each group in order
				let (group_filters, group_errs) = self.process_group(group, pipeline_num, &mut timing_info, &mut filter_info).unwrap(); // do all the steps on that group
				err_lines.extend(group_errs);
				for (step_num, v) in group_filters.into_iter() {
					if step_num == usize::MAX {
						new_survivors.extend(v);
					} else {
						output_lines.entry((pipeline_num, step_num)).or_default().extend(v);
					}
				}
			}
			surviving_lines = new_survivors;
		};
		output_lines.insert((usize::MAX, usize::MAX), surviving_lines);


		Ok((output_lines, err_lines, timing_info, filter_info))
	}


	pub fn make_group(&self, lines: Vec<Value>, group_keys: &Vec<String>) -> Result<HashMap<u64, Vec<Value>>, Error> {
		let mut group: HashMap<u64, Vec<Value>> = HashMap::new();
		fn get_hash_val(obj: &Value, group_keys: &Vec<String>) -> u64 {
			let mut hasher = DefaultHasher::new();
			for k in group_keys {
				if let Some(val) = obj.get(k) {
					val.to_string().hash(&mut hasher);
				}
			}
			hasher.finish()
		}
		lines.into_iter().for_each(|obj| {
			let hash_val = get_hash_val(&obj, group_keys);
			group.entry(hash_val).or_default().push(obj);
		});
		Ok(group)
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

pub trait GroupDataProcessor {
    // Initialize and return Self with cached data
    fn new(config: &Value) -> Result<Self, Error> 
    where
        Self: Sized;
    
    // Process method that all implementations must provide
    fn process_group(&self, data: Vec<Value>) -> Result<(Vec<Value>, Vec<Value>, Vec<Value>), Error>;
    

}


/*================================================================================
=                            DATA PROCESSOR VARIANTS                             =
================================================================================*/
#[derive(Serialize, Debug)]
pub struct Identity {
	text_field: String,	
}
 impl GroupDataProcessor for Identity {
	fn new(config: &Value) -> Result<Self, Error> {
		let text_field = get_default(config, "text_field", String::from("text"));
		Ok(Self {text_field})
	}


	fn process_group(&self, data: Vec<Value>) -> Result<(Vec<Value>, Vec<Value>, Vec<Value>), Error>{
		Ok((data, vec![], vec![]))
	}
}

#[derive(Serialize, Debug)]
pub struct Concatenate {
	// Concatenates documents into a single string, joining with the join_str
	// For more specific concatenations, maybe make a custom function
	text_cat_field: String,
	join_string: String,
	keep_fields: Vec<String>,  // Keeps the values in this field from the first element in the group. If empty, keeps all but the text_cat_field -- this shouldn't have any funny nested stuff
}
 impl GroupDataProcessor for Concatenate {
	fn new(config: &Value) -> Result<Self, Error> {
		let text_cat_field = config.get("text_cat_field").unwrap().as_str().unwrap().to_string();
		let join_string = config.get("join_string").unwrap().as_str().unwrap().to_string();
		let keep_fields = get_default(config, "keep_fields", Vec::new()).into_iter().map(|el| el.as_str().unwrap().to_string()).collect();
		Ok(Self {text_cat_field, join_string, keep_fields})
	}


	fn process_group(&self, data: Vec<Value>) -> Result<(Vec<Value>, Vec<Value>, Vec<Value>), Error>{
		if let Some(_) = data.first() {} else {
			return Ok((data, vec![], vec![]));		
		}

		let mut reference = if self.keep_fields.len() == 0 {
			data.first().unwrap().clone()
		} else {
			let first = data.first().unwrap().clone();
			let mut reference = json!({});
			for k in &self.keep_fields {
				let val = json_get(&first, &k).unwrap();
				json_set(&mut reference, &k, val.clone()).unwrap();
			}
			reference
		};

		let cat_stringss: Vec<String> = data.into_iter().map(|v| json_get(&v, &self.text_cat_field).unwrap().as_str().unwrap().to_string()).collect();
		let joined_strings = cat_stringss.join(&self.join_string);
		json_set(&mut reference, &self.text_cat_field, serde_json::Value::String(joined_strings)).unwrap(); 
		Ok((vec![reference], vec![], vec![]))
	}
}

/*======================================================================
=                            IMPORT ORDERING                           =
======================================================================*/


pub fn extract_python_imports(content: &String, filename: &String) -> Result<Vec<String>, Error> {
	let mut imports : Vec<String> = Vec::new();
	let program = match ast::Suite::parse(content, filename) {
		Ok(ast) => ast,
		_ => return Ok(imports)
	};
	
	// Do a dumb thing where we only look at top-level imports 
	for stmt in program {
		match stmt {
			// Regular import statements (import x, import y as z)
			ast::Stmt::Import(import_stmt) => { 
				for alias in &import_stmt.names {
					imports.push(alias.name.to_string());
				}
			},

            // From import statements (from x import y, from . import z)
            ast::Stmt::ImportFrom(from_import) => {
                // Handle relative imports with dots

                let level_prefix = if let Some(level) = from_import.level {
                	".".repeat(level.to_usize())
                } else {
                	"".to_string()
                };
                
                // Get the module name (or empty string if none)
                let module_path = match &from_import.module {
                    Some(module) => {
                        if level_prefix.is_empty() {
                            module.as_str().to_string()
                        } else {
                            format!("{}{}", level_prefix, module)
                        }
                    },
                    None => level_prefix
                };
                
                // Format the imported names, handling aliases
                let imports_list: Vec<String> = from_import.names.iter()
                    .map(|alias| {
                        if let Some(asname) = &alias.asname {
                            format!("{} as {}", alias.name, asname)
                        } else {
                            alias.name.to_string()
                        }
                    })
                    .collect();
                
                imports.push(format!("from {} import {}", module_path, imports_list.join(", ")));
            },
            _ => {}

		}
	}

    Ok(imports)
}



