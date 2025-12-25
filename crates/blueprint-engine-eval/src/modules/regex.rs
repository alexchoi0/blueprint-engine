use std::collections::HashMap;
use std::sync::Arc;

use blueprint_engine_core::{
    validation::{get_string_arg, require_args},
    BlueprintError, NativeFunction, Result, Value,
};
use regex::Regex;
use tokio::sync::RwLock;

pub fn get_functions() -> Vec<NativeFunction> {
    vec![
        NativeFunction::new("regex_match", regex_match_fn),
        NativeFunction::new("regex_find_all", regex_find_all_fn),
        NativeFunction::new("regex_replace", regex_replace_fn),
        NativeFunction::new("regex_split", regex_split_fn),
    ]
}

async fn regex_match_fn(args: Vec<Value>, _kwargs: HashMap<String, Value>) -> Result<Value> {
    require_args("regex.regex_match", &args, 2)?;
    let pattern = get_string_arg("regex.regex_match", &args, 0)?;
    let text = get_string_arg("regex.regex_match", &args, 1)?;

    let re = Regex::new(&pattern).map_err(|e| BlueprintError::ValueError {
        message: format!("Invalid regex pattern: {}", e),
    })?;

    if let Some(captures) = re.captures(&text) {
        let mut groups: Vec<Value> = Vec::new();
        for cap in captures.iter() {
            match cap {
                Some(m) => groups.push(Value::String(Arc::new(m.as_str().to_string()))),
                None => groups.push(Value::None),
            }
        }
        Ok(Value::List(Arc::new(RwLock::new(groups))))
    } else {
        Ok(Value::None)
    }
}

async fn regex_find_all_fn(args: Vec<Value>, _kwargs: HashMap<String, Value>) -> Result<Value> {
    require_args("regex.regex_find_all", &args, 2)?;
    let pattern = get_string_arg("regex.regex_find_all", &args, 0)?;
    let text = get_string_arg("regex.regex_find_all", &args, 1)?;

    let re = Regex::new(&pattern).map_err(|e| BlueprintError::ValueError {
        message: format!("Invalid regex pattern: {}", e),
    })?;

    let matches: Vec<Value> = re
        .find_iter(&text)
        .map(|m| Value::String(Arc::new(m.as_str().to_string())))
        .collect();

    Ok(Value::List(Arc::new(RwLock::new(matches))))
}

async fn regex_replace_fn(args: Vec<Value>, kwargs: HashMap<String, Value>) -> Result<Value> {
    require_args("regex.regex_replace", &args, 3)?;
    let pattern = get_string_arg("regex.regex_replace", &args, 0)?;
    let text = get_string_arg("regex.regex_replace", &args, 1)?;
    let replacement = get_string_arg("regex.regex_replace", &args, 2)?;

    let replace_all = kwargs.get("all").map(|v| v.is_truthy()).unwrap_or(true);

    let re = Regex::new(&pattern).map_err(|e| BlueprintError::ValueError {
        message: format!("Invalid regex pattern: {}", e),
    })?;

    let result = if replace_all {
        re.replace_all(&text, replacement.as_str()).into_owned()
    } else {
        re.replace(&text, replacement.as_str()).into_owned()
    };

    Ok(Value::String(Arc::new(result)))
}

async fn regex_split_fn(args: Vec<Value>, kwargs: HashMap<String, Value>) -> Result<Value> {
    require_args("regex.regex_split", &args, 2)?;
    let pattern = get_string_arg("regex.regex_split", &args, 0)?;
    let text = get_string_arg("regex.regex_split", &args, 1)?;

    let limit = kwargs
        .get("limit")
        .and_then(|v| v.as_int().ok())
        .map(|n| n as usize)
        .unwrap_or(0);

    let re = Regex::new(&pattern).map_err(|e| BlueprintError::ValueError {
        message: format!("Invalid regex pattern: {}", e),
    })?;

    let parts: Vec<Value> = if limit > 0 {
        re.splitn(&text, limit)
            .map(|s| Value::String(Arc::new(s.to_string())))
            .collect()
    } else {
        re.split(&text)
            .map(|s| Value::String(Arc::new(s.to_string())))
            .collect()
    };

    Ok(Value::List(Arc::new(RwLock::new(parts))))
}
