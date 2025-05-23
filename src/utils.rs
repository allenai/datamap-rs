use anyhow::{anyhow, Error, Result};
use serde_json::{json, Value};
use url::Url;

/*================================================================================
=                            JSON GETTER METHODS                                 =
================================================================================*/

/// A trait for extracting values from JSON with type conversion
pub trait FromValue: Sized {
    /// Try to convert a JSON value to Self
    fn from_value(value: &Value) -> Option<Self>;
}

// Implement FromValue for common types
impl FromValue for String {
    fn from_value(value: &Value) -> Option<Self> {
        value.as_str().map(String::from)
    }
}

impl FromValue for usize {
    fn from_value(value: &Value) -> Option<Self> {
        value.as_u64().map(|v| v as usize)
    }
}

impl FromValue for u64 {
    fn from_value(value: &Value) -> Option<Self> {
        value.as_u64()
    }
}

impl FromValue for i64 {
    fn from_value(value: &Value) -> Option<Self> {
        value.as_i64()
    }
}

impl FromValue for f64 {
    fn from_value(value: &Value) -> Option<Self> {
        value.as_f64()
    }
}

impl FromValue for bool {
    fn from_value(value: &Value) -> Option<Self> {
        value.as_bool()
    }
}

impl FromValue for Vec<Value> {
    fn from_value(value: &Value) -> Option<Self> {
        value.as_array().cloned()
    }
}

/// Get a value from a JSON config with a default
pub fn get_default<T: FromValue>(config: &Value, key: &str, default: T) -> T {
    match config.get(key) {
        Some(value) => T::from_value(value).unwrap_or(default),
        None => default,
    }
}

pub fn json_get<'a>(data: &'a serde_json::Value, key: &str) -> Option<&'a Value> {
    let keys: Vec<&str> = key.split('.').collect();
    let mut current = data;

    for key in keys {
        match current.get(key) {
            Some(value) => current = value,
            None => return None,
        }
    }

    Some(current)
}

pub fn json_set(input: &mut Value, key: &String, val: Value) -> Result<(), Error> {
    let parts: Vec<&str> = key.split('.').collect();
    let mut current = input;

    for (i, &part) in parts.iter().enumerate() {
        if i == parts.len() - 1 {
            if current.is_object() {
                current[part] = val;
                return Ok(());
            } else {
                return Err(anyhow!("Weird nesting for setting json values"));
            }
        }
        if !current.is_object() {
            return Err(anyhow!("Weird nesting for setting json values"));
        }
        if !current.get(part).is_some() {
            current[part] = json!({});
        }
        current = &mut current[part];
    }
    Ok(())
}

/*====================================================================
=                            URL HELPERS                             =
====================================================================*/

pub fn extract_subdomain(url_str: &str) -> Result<Option<String>, Error> {
    let url = Url::parse(url_str)?;

    // Get the host
    let host = match url.host_str() {
        Some(host) => host,
        None => return Ok(None), // URL has no host component
    };

    // Split the host by dots
    let parts: Vec<&str> = host.split('.').collect();

    // If we have at least 3 parts (like in "sub.example.com"), the first part is a subdomain
    if parts.len() >= 3 {
        Ok(Some(parts[0].to_string()))
    } else {
        Ok(None) // No subdomain found
    }
}
