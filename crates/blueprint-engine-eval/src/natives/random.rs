use std::collections::HashMap;
use std::sync::Arc;

use blueprint_engine_core::{BlueprintError, NativeFunction, Result, Value};
use rand::RngCore;

use crate::eval::Evaluator;

pub fn register(evaluator: &mut Evaluator) {
    evaluator.register_native(NativeFunction::new("random_bytes", random_bytes_fn));
    evaluator.register_native(NativeFunction::new("random_int", random_int_fn));
    evaluator.register_native(NativeFunction::new("random_float", random_float_fn));
}

async fn random_bytes_fn(args: Vec<Value>, kwargs: HashMap<String, Value>) -> Result<Value> {
    if args.len() != 1 {
        return Err(BlueprintError::ArgumentError {
            message: format!(
                "random_bytes() takes exactly 1 argument ({} given)",
                args.len()
            ),
        });
    }

    let n = args[0].as_int()? as usize;

    if n > 1024 * 1024 {
        return Err(BlueprintError::ArgumentError {
            message: "random_bytes() cannot generate more than 1MB at once".to_string(),
        });
    }

    let mut bytes = vec![0u8; n];
    rand::thread_rng().fill_bytes(&mut bytes);

    let output_hex = kwargs
        .get("hex")
        .map(|v| v.is_truthy())
        .unwrap_or(false);

    if output_hex {
        Ok(Value::String(Arc::new(hex::encode(&bytes))))
    } else {
        let encoded =
            base64::Engine::encode(&base64::engine::general_purpose::STANDARD, &bytes);
        Ok(Value::String(Arc::new(encoded)))
    }
}

async fn random_int_fn(args: Vec<Value>, _kwargs: HashMap<String, Value>) -> Result<Value> {
    use rand::Rng;

    match args.len() {
        0 => {
            let n: i64 = rand::thread_rng().gen();
            Ok(Value::Int(n))
        }
        1 => {
            let max = args[0].as_int()?;
            if max <= 0 {
                return Err(BlueprintError::ArgumentError {
                    message: "random_int() max must be positive".to_string(),
                });
            }
            let n: i64 = rand::thread_rng().gen_range(0..max);
            Ok(Value::Int(n))
        }
        2 => {
            let min = args[0].as_int()?;
            let max = args[1].as_int()?;
            if min >= max {
                return Err(BlueprintError::ArgumentError {
                    message: "random_int() min must be less than max".to_string(),
                });
            }
            let n: i64 = rand::thread_rng().gen_range(min..max);
            Ok(Value::Int(n))
        }
        _ => Err(BlueprintError::ArgumentError {
            message: format!(
                "random_int() takes 0-2 arguments ({} given)",
                args.len()
            ),
        }),
    }
}

async fn random_float_fn(args: Vec<Value>, _kwargs: HashMap<String, Value>) -> Result<Value> {
    use rand::Rng;

    if !args.is_empty() {
        return Err(BlueprintError::ArgumentError {
            message: format!(
                "random_float() takes no arguments ({} given)",
                args.len()
            ),
        });
    }

    let n: f64 = rand::thread_rng().gen();
    Ok(Value::Float(n))
}
