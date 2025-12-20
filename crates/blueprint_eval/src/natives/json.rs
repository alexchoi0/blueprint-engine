use std::collections::HashMap;
use indexmap::IndexMap;
use std::sync::Arc;

use blueprint_core::{BlueprintError, NativeFunction, Result, Value};
use serde_json;
use tokio::sync::RwLock;

use crate::eval::Evaluator;

pub fn register(evaluator: &mut Evaluator) {
    evaluator.register_native(NativeFunction::new("json_encode", json_encode));
    evaluator.register_native(NativeFunction::new("json_decode", json_decode));

    evaluator.register_module_native("json", NativeFunction::new("encode", json_encode));
    evaluator.register_module_native("json", NativeFunction::new("decode", json_decode));
    evaluator.register_module_native("json", NativeFunction::new("dumps", json_encode));
    evaluator.register_module_native("json", NativeFunction::new("loads", json_decode));
}

async fn json_encode(args: Vec<Value>, kwargs: HashMap<String, Value>) -> Result<Value> {
    if args.len() != 1 {
        return Err(BlueprintError::ArgumentError {
            message: format!("json.encode() takes exactly 1 argument ({} given)", args.len()),
        });
    }

    let indent = kwargs
        .get("indent")
        .and_then(|v| v.as_int().ok())
        .map(|i| i as usize);

    let json_value = value_to_json(&args[0]).await?;

    let json_str = if let Some(spaces) = indent {
        let buf = Vec::new();
        let indent_bytes = vec![b' '; spaces];
        let formatter = serde_json::ser::PrettyFormatter::with_indent(&indent_bytes);
        let mut ser = serde_json::Serializer::with_formatter(buf, formatter);
        serde::Serialize::serialize(&json_value, &mut ser).map_err(|e| BlueprintError::JsonError {
            message: e.to_string(),
        })?;
        String::from_utf8(ser.into_inner()).map_err(|e| BlueprintError::JsonError {
            message: e.to_string(),
        })?
    } else {
        serde_json::to_string(&json_value).map_err(|e| BlueprintError::JsonError {
            message: e.to_string(),
        })?
    };

    Ok(Value::String(Arc::new(json_str)))
}

async fn json_decode(args: Vec<Value>, _kwargs: HashMap<String, Value>) -> Result<Value> {
    if args.len() != 1 {
        return Err(BlueprintError::ArgumentError {
            message: format!("json.decode() takes exactly 1 argument ({} given)", args.len()),
        });
    }

    let json_str = args[0].as_string()?;

    let json_value: serde_json::Value =
        serde_json::from_str(&json_str).map_err(|e| BlueprintError::JsonError {
            message: e.to_string(),
        })?;

    json_to_value(json_value)
}

async fn value_to_json(value: &Value) -> Result<serde_json::Value> {
    match value {
        Value::None => Ok(serde_json::Value::Null),
        Value::Bool(b) => Ok(serde_json::Value::Bool(*b)),
        Value::Int(i) => Ok(serde_json::json!(*i)),
        Value::Float(f) => {
            if f.is_finite() {
                Ok(serde_json::json!(*f))
            } else {
                Err(BlueprintError::JsonError {
                    message: "Cannot serialize infinity or NaN to JSON".into(),
                })
            }
        }
        Value::String(s) => Ok(serde_json::Value::String(s.as_ref().clone())),
        Value::List(l) => {
            let items = l.read().await;
            let mut arr = Vec::with_capacity(items.len());
            for item in items.iter() {
                arr.push(Box::pin(value_to_json(item)).await?);
            }
            Ok(serde_json::Value::Array(arr))
        }
        Value::Tuple(t) => {
            let mut arr = Vec::with_capacity(t.len());
            for item in t.iter() {
                arr.push(Box::pin(value_to_json(item)).await?);
            }
            Ok(serde_json::Value::Array(arr))
        }
        Value::Dict(d) => {
            let map = d.read().await;
            let mut obj = serde_json::Map::with_capacity(map.len());
            for (k, v) in map.iter() {
                obj.insert(k.clone(), Box::pin(value_to_json(v)).await?);
            }
            Ok(serde_json::Value::Object(obj))
        }
        _ => Err(BlueprintError::JsonError {
            message: format!("Cannot serialize {} to JSON", value.type_name()),
        }),
    }
}

fn json_to_value(json: serde_json::Value) -> Result<Value> {
    match json {
        serde_json::Value::Null => Ok(Value::None),
        serde_json::Value::Bool(b) => Ok(Value::Bool(b)),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(Value::Int(i))
            } else if let Some(f) = n.as_f64() {
                Ok(Value::Float(f))
            } else {
                Err(BlueprintError::JsonError {
                    message: "Invalid JSON number".into(),
                })
            }
        }
        serde_json::Value::String(s) => Ok(Value::String(Arc::new(s))),
        serde_json::Value::Array(arr) => {
            let mut items = Vec::with_capacity(arr.len());
            for item in arr {
                items.push(json_to_value(item)?);
            }
            Ok(Value::List(Arc::new(RwLock::new(items))))
        }
        serde_json::Value::Object(obj) => {
            let mut map = IndexMap::with_capacity(obj.len());
            for (k, v) in obj {
                map.insert(k, json_to_value(v)?);
            }
            Ok(Value::Dict(Arc::new(RwLock::new(map))))
        }
    }
}
