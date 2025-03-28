
use serde_json;
use serde_json::json;
use anyhow::{Error, Result};
use rand::Rng;
use phf::phf_map;
use uuid::Uuid;
use url::Url;

//fn santacoder_pl_filter_json(json_obj: serde_json::Value, _config_json: &serde_json::Value) -> Result<Option<serde_json::Value>, Error> {

pub fn move_url_modifier(mut json_obj: serde_json::Value, _config_json: &serde_json::Value) -> Result<Option<serde_json::Value>, Error> {
	json_obj["url"] = json_obj["metadata"]["WARC-Target-URI"].clone();
	Ok(Some(json_obj))
}


pub fn url_substring_filter(json_obj: serde_json::Value, config_json: &serde_json::Value) -> Result<Option<serde_json::Value>, Error> {
	/*
	Cases towards banning urls:
	- exact domain match : just 
	- 

	*/
	let exact_domain_match = config_json.get("exact_domain_match").or(Some(&json!(false))).and_then(|v| v.as_bool()).unwrap();
	let match_substrings = config_json.get("match_substrings").or(Some(&json!(true))).and_then(|v| v.as_bool()).unwrap();
	let case_sensitive = config_json.get("case_sensitive").or(Some(&json!(false))).and_then(|v| v.as_bool()).unwrap();
	let ignore_chars: Vec<serde_json::Value> = config_json.get("ignore_chars")
		.or(Some(&json!(Vec::<serde_json::Value>::new())))
		.and_then(|v| v.as_array()).unwrap().to_vec();
	let ignore_chars: Vec<String> = ignore_chars.iter().map(|v| v.to_string()).collect();
	let num_banned_substrs = config_json.get("num_banned_substrs").or(Some(&json!(1))).and_then(|v| Some(v.as_u64().unwrap() as usize)).unwrap();
	let banlist = config_json.get("banlist").unwrap();


	// First get the url 
	let mut url = match exact_domain_match {
		true => Url::parse(&json_obj["url"].to_string()).unwrap().to_string(),
		false => json_obj["url"].to_string()
	};
	url = if case_sensitive { url.to_lowercase() } else { url };
	for r in ignore_chars {
		url = url.replace(&r, "");
	}

	if exact_domain_match {
		// Do the check here to see if url in banlist
	} else {
		// Check for presence of substrings? 
	}


	Ok(Some(json_obj))
}