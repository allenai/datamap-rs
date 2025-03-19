/*

Library for specific map/filter functions:

The general signature of these should look like:

fn mapper(Vec<String>, serde_json::Value) -> Result<Vec<String>, Error>

where each element of the Vec<String> is a line of the jsonl object.

*/

use serde_json;
use serde_json::json;
use anyhow::{Error, Result};
use rand::Rng;
use phf::phf_map;
use uuid::Uuid;

use crate::dclm_mappers::{move_url_modifier};

pub static MAP_LINE_FXNS : phf::Map<&'static str, fn(String, &serde_json::Value) -> Result<String, Error>> = phf_map! {
	"SUBSAMPLE" => subsample_line,
};


pub static MAP_JSON_FXNS : phf::Map<&'static str, fn(serde_json::Value, &serde_json::Value) -> Result<Option<serde_json::Value>, Error>> = phf_map! {
	"LEN_FILTER" => len_filter_json,
	"ADD_ID" => add_id_json,
	"SANTACODER_PL_FILTER" => santacoder_pl_filter_json,
};

pub static MAP_JSONL_FXNS: phf::Map<&'static str, fn(Vec<String>, &serde_json::Value) -> Result<Vec<String>, Error>> = phf_map! {

};



/*===========================================================================
=                            GENERIC HELPERS                                =
===========================================================================*/


pub struct CompiledProcessor {
	groups: Vec<Vec<serde_json::Value>>,
}

impl CompiledProcessor {
	pub fn process(&self, mut lines: Vec<String>) -> Result<Vec<String>, Error> {
	    for group in self.groups.iter() {
	    	let group_type = group[0]["type"].as_str().unwrap();
	    	if group_type == "WHOLE_DOC" {
		        let fxn = MAP_JSONL_FXNS.get(&group[1]["name"].as_str().unwrap()).unwrap();
		        lines = fxn(lines, &group[1]).unwrap();
		    } else if group_type == "JSON_LINES" {
		    	let mut jsons: Vec<serde_json::Value> = lines.into_iter().map(|line| serde_json::from_str(&line).unwrap()).collect();
		    	for group_el in group[1..].iter() {
		    		let json_map_fxn = MAP_JSON_FXNS.get(&group_el.as_str().unwrap()).unwrap();
		    		jsons = jsons
		    			.into_iter()
		    			.filter_map(|j| {
		    				json_map_fxn(j, &group_el).unwrap()
		    			})
		    			.collect();
		    	}		    	
		    	lines = jsons.into_iter().map(|j| j.to_string()).collect();
		    } else if group_type == "RAW_LINES" {
		    	for group_el in group[1..].iter() {
		    		let line_map_fxn = MAP_LINE_FXNS.get(&group_el["name"].as_str().unwrap()).unwrap();
		    		lines = lines
		    			.into_iter()
		    			.map(|line| line_map_fxn(line, &group_el).unwrap())
		    			.filter(|l| l.len() > 0)
		    			.collect();
		    	}
		    }
	    }
	    Ok(lines)
	}
}



pub fn precompile_processor(json_config: &serde_json::Value) -> Result<CompiledProcessor, Error> {
	// First make groups 

	let mut groups: Vec<Vec<serde_json::Value>> = Vec::new();
	let mut cur: Vec<serde_json::Value> = Vec::new();
	let cur_group_type = String::new();
	let pipeline = json_config["pipeline"].as_array().unwrap().clone();

	for map_fxn in pipeline {
		let name = map_fxn["name"].as_str().unwrap();
		let group_type = if MAP_JSONL_FXNS.contains_key(name) {
			"WHOLE_DOC"
		} else if MAP_LINE_FXNS.contains_key(name) {
			"RAW_LINES"
		} else {
			"JSON_LINES"
		};

		if group_type == cur_group_type {
			cur.push(map_fxn)
		} else if cur.len() == 0 {
			cur = Vec::new();
			cur.push(json!({"type": group_type}));
			cur.push(map_fxn);
		} else {
			groups.push(cur);
			cur = Vec::new();
			cur.push(json!({"type": group_type}));
			cur.push(map_fxn);			
		}

	}

	if cur.len() > 0 {
		groups.push(cur);
	}


	let processor = CompiledProcessor { groups };
	Ok(processor)

}




/*================================================================================
=                            WHOLE DOC MAPPERS                                   =
================================================================================*/


/*================================================================================
=                            RAW LINE MAPPERS                                    =
================================================================================*/


fn subsample_line(line: String, config_json: &serde_json::Value) -> Result<String, Error> {
	let mut rng = rand::rng();
	let ratio = config_json.get("ratio").unwrap().as_f64().unwrap();
	let random_float = rng.random::<f64>();
	if random_float <= ratio {
		Ok(line)
	} else {
		Ok(String::new())
	}
}



/*================================================================================
=                            JSON MAPPERS                                        =
================================================================================*/


fn len_filter_json(json_obj: serde_json::Value, config_json: &serde_json::Value) -> Result<Option<serde_json::Value>, Error> {
    let min_len = match config_json.get("min_len") {
        Some(min_len) => min_len.as_u64().unwrap() as usize,
        None => 0
    };

    let max_len = match config_json.get("max_len") {
        Some(max_len) => max_len.as_u64().unwrap() as usize,
        None => usize::MAX
    };    
    let textlen = json_obj.get("text").unwrap().as_str().unwrap().len();

    if textlen <= max_len && textlen >= min_len {
    	Ok(Some(json_obj))
    } else {
    	Ok(None)
    }
}



fn add_id_json(mut json_obj: serde_json::Value, config_json: &serde_json::Value) -> Result<Option<serde_json::Value>, Error> {
    let id_key = match config_json.get("id_key") {
        Some(id_key) => id_key.to_string(),
        None => "id".to_string()
    };
    json_obj[&id_key] = serde_json::Value::String(Uuid::new_v4().to_string());
    Ok(Some(json_obj))

}


fn santacoder_pl_filter_json(json_obj: serde_json::Value, _config_json: &serde_json::Value) -> Result<Option<serde_json::Value>, Error> {
    let pl = json_obj.get("metadata").and_then(|m| m.get("language")).and_then(|l| l.as_str()).unwrap();
    if pl == "Python" || pl == "Java" || pl == "JavaScript"{
    	Ok(Some(json_obj))
    } else {
    	Ok(None)
    }
}











