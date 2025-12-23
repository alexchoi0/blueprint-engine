use std::collections::HashMap;
use std::sync::Arc;

use base64::{engine::general_purpose, Engine as _};
use blueprint_engine_core::{BlueprintError, NativeFunction, Result, Value};
use percent_encoding::{percent_decode_str, utf8_percent_encode, NON_ALPHANUMERIC};

use crate::eval::Evaluator;

pub fn register(evaluator: &mut Evaluator) {
    evaluator.register_native(NativeFunction::new("base64_encode", base64_encode_fn));
    evaluator.register_native(NativeFunction::new("base64_decode", base64_decode_fn));
    evaluator.register_native(NativeFunction::new("hex_encode", hex_encode_fn));
    evaluator.register_native(NativeFunction::new("hex_decode", hex_decode_fn));
    evaluator.register_native(NativeFunction::new("url_encode", url_encode_fn));
    evaluator.register_native(NativeFunction::new("url_decode", url_decode_fn));
}

async fn base64_encode_fn(args: Vec<Value>, kwargs: HashMap<String, Value>) -> Result<Value> {
    if args.len() != 1 {
        return Err(BlueprintError::ArgumentError {
            message: format!(
                "base64_encode() takes exactly 1 argument ({} given)",
                args.len()
            ),
        });
    }

    let data = args[0].as_string()?;

    let url_safe = kwargs
        .get("url_safe")
        .map(|v| v.is_truthy())
        .unwrap_or(false);

    let encoded = if url_safe {
        general_purpose::URL_SAFE.encode(data.as_bytes())
    } else {
        general_purpose::STANDARD.encode(data.as_bytes())
    };

    Ok(Value::String(Arc::new(encoded)))
}

async fn base64_decode_fn(args: Vec<Value>, kwargs: HashMap<String, Value>) -> Result<Value> {
    if args.len() != 1 {
        return Err(BlueprintError::ArgumentError {
            message: format!(
                "base64_decode() takes exactly 1 argument ({} given)",
                args.len()
            ),
        });
    }

    let encoded = args[0].as_string()?;

    let url_safe = kwargs
        .get("url_safe")
        .map(|v| v.is_truthy())
        .unwrap_or(false);

    let decoded_bytes = if url_safe {
        general_purpose::URL_SAFE
            .decode(&*encoded)
            .map_err(|e| BlueprintError::ValueError {
                message: format!("Invalid base64 URL-safe string: {}", e),
            })?
    } else {
        general_purpose::STANDARD
            .decode(&*encoded)
            .map_err(|e| BlueprintError::ValueError {
                message: format!("Invalid base64 string: {}", e),
            })?
    };

    let decoded = String::from_utf8(decoded_bytes).map_err(|e| BlueprintError::ValueError {
        message: format!("Decoded base64 is not valid UTF-8: {}", e),
    })?;

    Ok(Value::String(Arc::new(decoded)))
}

async fn hex_encode_fn(args: Vec<Value>, _kwargs: HashMap<String, Value>) -> Result<Value> {
    if args.len() != 1 {
        return Err(BlueprintError::ArgumentError {
            message: format!(
                "hex_encode() takes exactly 1 argument ({} given)",
                args.len()
            ),
        });
    }

    let data = args[0].as_string()?;
    let encoded = hex::encode(data.as_bytes());

    Ok(Value::String(Arc::new(encoded)))
}

async fn hex_decode_fn(args: Vec<Value>, _kwargs: HashMap<String, Value>) -> Result<Value> {
    if args.len() != 1 {
        return Err(BlueprintError::ArgumentError {
            message: format!(
                "hex_decode() takes exactly 1 argument ({} given)",
                args.len()
            ),
        });
    }

    let encoded = args[0].as_string()?;

    let decoded_bytes = hex::decode(&*encoded).map_err(|e| BlueprintError::ValueError {
        message: format!("Invalid hex string: {}", e),
    })?;

    let decoded = String::from_utf8(decoded_bytes).map_err(|e| BlueprintError::ValueError {
        message: format!("Decoded hex is not valid UTF-8: {}", e),
    })?;

    Ok(Value::String(Arc::new(decoded)))
}

async fn url_encode_fn(args: Vec<Value>, _kwargs: HashMap<String, Value>) -> Result<Value> {
    if args.len() != 1 {
        return Err(BlueprintError::ArgumentError {
            message: format!(
                "url_encode() takes exactly 1 argument ({} given)",
                args.len()
            ),
        });
    }

    let data = args[0].as_string()?;
    let encoded = utf8_percent_encode(&data, NON_ALPHANUMERIC).to_string();

    Ok(Value::String(Arc::new(encoded)))
}

async fn url_decode_fn(args: Vec<Value>, _kwargs: HashMap<String, Value>) -> Result<Value> {
    if args.len() != 1 {
        return Err(BlueprintError::ArgumentError {
            message: format!(
                "url_decode() takes exactly 1 argument ({} given)",
                args.len()
            ),
        });
    }

    let encoded = args[0].as_string()?;

    let decoded = percent_decode_str(&encoded)
        .decode_utf8()
        .map_err(|e| BlueprintError::ValueError {
            message: format!("Invalid URL-encoded string: {}", e),
        })?;

    Ok(Value::String(Arc::new(decoded.into_owned())))
}
