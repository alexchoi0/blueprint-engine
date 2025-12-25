use std::collections::HashMap;
use std::sync::Arc;

use blueprint_engine_core::{
    validation::{get_int_arg, require_args, require_args_range},
    BlueprintError, NativeFunction, Result, Value,
};
use rand::RngCore;

pub fn get_functions() -> Vec<NativeFunction> {
    vec![
        NativeFunction::new("random_bytes", random_bytes_fn),
        NativeFunction::new("random_int", random_int_fn),
        NativeFunction::new("random_float", random_float_fn),
    ]
}

async fn random_bytes_fn(args: Vec<Value>, kwargs: HashMap<String, Value>) -> Result<Value> {
    require_args("random.random_bytes", &args, 1)?;
    let n = get_int_arg("random.random_bytes", &args, 0)? as usize;

    if n > 1024 * 1024 {
        return Err(BlueprintError::ArgumentError {
            message: "random_bytes() cannot generate more than 1MB at once".to_string(),
        });
    }

    let mut bytes = vec![0u8; n];
    rand::thread_rng().fill_bytes(&mut bytes);

    let output_hex = kwargs.get("hex").map(|v| v.is_truthy()).unwrap_or(false);

    if output_hex {
        Ok(Value::String(Arc::new(hex::encode(&bytes))))
    } else {
        // Return raw bytes as a string (each byte as a character)
        let s: String = bytes.into_iter().map(|b| b as char).collect();
        Ok(Value::String(Arc::new(s)))
    }
}

async fn random_int_fn(args: Vec<Value>, _kwargs: HashMap<String, Value>) -> Result<Value> {
    use rand::Rng;
    require_args_range("random.random_int", &args, 0, 2)?;

    match args.len() {
        0 => {
            let n: i64 = rand::thread_rng().gen();
            Ok(Value::Int(n))
        }
        1 => {
            let max = get_int_arg("random.random_int", &args, 0)?;
            if max <= 0 {
                return Err(BlueprintError::ArgumentError {
                    message: "random_int() max must be positive".to_string(),
                });
            }
            let n: i64 = rand::thread_rng().gen_range(0..max);
            Ok(Value::Int(n))
        }
        _ => {
            let min = get_int_arg("random.random_int", &args, 0)?;
            let max = get_int_arg("random.random_int", &args, 1)?;
            if min >= max {
                return Err(BlueprintError::ArgumentError {
                    message: "random_int() min must be less than max".to_string(),
                });
            }
            let n: i64 = rand::thread_rng().gen_range(min..max);
            Ok(Value::Int(n))
        }
    }
}

async fn random_float_fn(args: Vec<Value>, _kwargs: HashMap<String, Value>) -> Result<Value> {
    use rand::Rng;
    require_args("random.random_float", &args, 0)?;
    let n: f64 = rand::thread_rng().gen();
    Ok(Value::Float(n))
}
