use serde_json::{Value, json};
use anyhow::{anyhow, Result, Error};
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

impl<T: FromValue> FromValue for Option<T> {
    // Implement this please 
    fn from_value(value: &Value) -> Option<Self> {
        if value.is_null() {
            Some(None)
        } else {
            Some(T::from_value(value))
        }
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
            None => {
                return None
            }
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

pub fn json_remove(data: &mut Value, key: &str) -> Result<(), Error> {
    let parts: Vec<&str> = key.split('.').collect();
    
    // Handle special case of a single key (no dots)
    if parts.len() == 1 {
        if data.is_object() {
            let obj = data.as_object_mut().unwrap();
            obj.remove(parts[0]);
            return Ok(());
        } else {
            return Err(anyhow!("Root is not an object"));
        }
    }
    
    // For nested keys, navigate to the parent object
    let parent_path = parts[..parts.len()-1].join(".");
    let field_to_remove = parts[parts.len()-1];
    
    // Get mutable reference to the parent object
    let parent = match json_get(data, &parent_path) {
        Some(_) => {
            // We need a mutable reference, so we'll navigate to it again
            let mut current = data;
            for &part in &parts[..parts.len()-1] {
                if !current.is_object() {
                    return Err(anyhow!("Path contains non-object element"));
                }
                current = &mut current[part];
            }
            current
        },
        None => return Err(anyhow!("Parent path not found: {}", parent_path))
    };
    
    // Remove the field from the parent object
    if parent.is_object() {
        let obj = parent.as_object_mut().unwrap();
        obj.remove(field_to_remove);
        Ok(())
    } else {
        Err(anyhow!("Parent is not an object"))
    }
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

