use std::collections::HashMap;
use std::sync::Arc;

use blueprint_core::{BlueprintError, NativeFunction, Result, Value};
use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};

use crate::eval::Evaluator;

type HmacSha256 = Hmac<Sha256>;

pub fn register(evaluator: &mut Evaluator) {
    evaluator.register_native(NativeFunction::new("sha256", sha256_fn));
    evaluator.register_native(NativeFunction::new("hmac_sha256", hmac_sha256_fn));
}

async fn sha256_fn(args: Vec<Value>, _kwargs: HashMap<String, Value>) -> Result<Value> {
    if args.len() != 1 {
        return Err(BlueprintError::ArgumentError {
            message: format!("sha256() takes exactly 1 argument ({} given)", args.len()),
        });
    }

    let data = args[0].as_string()?;
    let mut hasher = Sha256::new();
    hasher.update(data.as_bytes());
    let result = hasher.finalize();

    Ok(Value::String(Arc::new(hex::encode(result))))
}

async fn hmac_sha256_fn(args: Vec<Value>, kwargs: HashMap<String, Value>) -> Result<Value> {
    if args.len() != 2 {
        return Err(BlueprintError::ArgumentError {
            message: format!("hmac_sha256() takes exactly 2 arguments ({} given)", args.len()),
        });
    }

    let key_str = args[0].as_string()?;
    let message = args[1].as_string()?;

    let key_is_hex = kwargs
        .get("key_hex")
        .map(|v| v.is_truthy())
        .unwrap_or(false);

    let key_bytes: Vec<u8> = if key_is_hex {
        hex::decode(&key_str).map_err(|e| BlueprintError::ValueError {
            message: format!("Invalid hex key: {}", e),
        })?
    } else {
        key_str.as_bytes().to_vec()
    };

    let mut mac = HmacSha256::new_from_slice(&key_bytes)
        .map_err(|e| BlueprintError::InternalError {
            message: format!("Invalid HMAC key: {}", e),
        })?;

    mac.update(message.as_bytes());
    let result = mac.finalize();

    Ok(Value::String(Arc::new(hex::encode(result.into_bytes()))))
}
