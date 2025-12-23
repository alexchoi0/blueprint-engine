use std::collections::HashMap;
use std::sync::Arc;

use blueprint_engine_core::{BlueprintError, NativeFunction, Result, Value};
use regex::Regex;
use tokio::sync::RwLock;

use crate::eval::Evaluator;

pub fn register(evaluator: &mut Evaluator) {
    evaluator.register_native(NativeFunction::new("regex_match", regex_match_fn));
    evaluator.register_native(NativeFunction::new("regex_find_all", regex_find_all_fn));
    evaluator.register_native(NativeFunction::new("regex_replace", regex_replace_fn));
    evaluator.register_native(NativeFunction::new("regex_split", regex_split_fn));
}

async fn regex_match_fn(args: Vec<Value>, _kwargs: HashMap<String, Value>) -> Result<Value> {
    if args.len() != 2 {
        return Err(BlueprintError::ArgumentError {
            message: format!(
                "regex_match() takes exactly 2 arguments ({} given)",
                args.len()
            ),
        });
    }

    let pattern = args[0].as_string()?;
    let text = args[1].as_string()?;

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
    if args.len() != 2 {
        return Err(BlueprintError::ArgumentError {
            message: format!(
                "regex_find_all() takes exactly 2 arguments ({} given)",
                args.len()
            ),
        });
    }

    let pattern = args[0].as_string()?;
    let text = args[1].as_string()?;

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
    if args.len() != 3 {
        return Err(BlueprintError::ArgumentError {
            message: format!(
                "regex_replace() takes exactly 3 arguments ({} given)",
                args.len()
            ),
        });
    }

    let pattern = args[0].as_string()?;
    let text = args[1].as_string()?;
    let replacement = args[2].as_string()?;

    let replace_all = kwargs
        .get("all")
        .map(|v| v.is_truthy())
        .unwrap_or(true);

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
    if args.len() != 2 {
        return Err(BlueprintError::ArgumentError {
            message: format!(
                "regex_split() takes exactly 2 arguments ({} given)",
                args.len()
            ),
        });
    }

    let pattern = args[0].as_string()?;
    let text = args[1].as_string()?;

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
