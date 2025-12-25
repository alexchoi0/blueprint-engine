use std::collections::HashMap;
use std::sync::Arc;

use blueprint_engine_core::{BlueprintError, Result, Value};

pub async fn len(args: Vec<Value>, _kwargs: HashMap<String, Value>) -> Result<Value> {
    if args.len() != 1 {
        return Err(BlueprintError::ArgumentError {
            message: format!("len() takes exactly 1 argument ({} given)", args.len()),
        });
    }

    let length = match &args[0] {
        Value::String(s) => s.chars().count() as i64,
        Value::List(l) => l.read().await.len() as i64,
        Value::Dict(d) => d.read().await.len() as i64,
        Value::Tuple(t) => t.len() as i64,
        Value::Set(s) => s.read().await.len() as i64,
        other => {
            return Err(BlueprintError::TypeError {
                expected: "string, list, dict, tuple, or set".into(),
                actual: other.type_name().into(),
            })
        }
    };

    Ok(Value::Int(length))
}

pub async fn type_of(args: Vec<Value>, _kwargs: HashMap<String, Value>) -> Result<Value> {
    if args.len() != 1 {
        return Err(BlueprintError::ArgumentError {
            message: format!("type() takes exactly 1 argument ({} given)", args.len()),
        });
    }

    Ok(Value::String(Arc::new(args[0].type_name().to_string())))
}

pub async fn hasattr(args: Vec<Value>, _kwargs: HashMap<String, Value>) -> Result<Value> {
    if args.len() != 2 {
        return Err(BlueprintError::ArgumentError {
            message: format!("hasattr() takes exactly 2 arguments ({} given)", args.len()),
        });
    }

    let attr_name = args[1].as_string()?;
    Ok(Value::Bool(args[0].has_attr(&attr_name)))
}

pub async fn getattr(args: Vec<Value>, _kwargs: HashMap<String, Value>) -> Result<Value> {
    if args.len() < 2 || args.len() > 3 {
        return Err(BlueprintError::ArgumentError {
            message: format!("getattr() takes 2 or 3 arguments ({} given)", args.len()),
        });
    }

    let attr_name = args[1].as_string()?;
    match args[0].get_attr(&attr_name) {
        Some(v) => Ok(v),
        None if args.len() == 3 => Ok(args[2].clone()),
        None => Err(BlueprintError::AttributeError {
            type_name: args[0].type_name().to_string(),
            attr: attr_name,
        }),
    }
}

pub async fn repr(args: Vec<Value>, _kwargs: HashMap<String, Value>) -> Result<Value> {
    if args.len() != 1 {
        return Err(BlueprintError::ArgumentError {
            message: format!("repr() takes exactly 1 argument ({} given)", args.len()),
        });
    }

    Ok(Value::String(Arc::new(args[0].repr())))
}
