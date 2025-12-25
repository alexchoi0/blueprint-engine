use std::collections::HashMap;
use std::sync::Arc;

use blueprint_engine_core::{
    validation::{get_string_arg, require_args},
    BlueprintError, NativeFunction, Result, Value,
};
use hmac::{Hmac, Mac};
use md5::Md5;
use sha1::Sha1;
use sha2::{Digest, Sha256, Sha512};

type HmacSha256 = Hmac<Sha256>;
type HmacSha512 = Hmac<Sha512>;

pub fn get_functions() -> Vec<NativeFunction> {
    vec![
        NativeFunction::new("md5", md5_fn),
        NativeFunction::new("sha1", sha1_fn),
        NativeFunction::new("sha256", sha256_fn),
        NativeFunction::new("sha512", sha512_fn),
        NativeFunction::new("hmac_sha256", hmac_sha256_fn),
        NativeFunction::new("hmac_sha512", hmac_sha512_fn),
        NativeFunction::new("constant_time_compare", constant_time_compare_fn),
    ]
}

async fn md5_fn(args: Vec<Value>, _kwargs: HashMap<String, Value>) -> Result<Value> {
    require_args("crypto.md5", &args, 1)?;
    let data = get_string_arg("crypto.md5", &args, 0)?;
    let mut hasher = Md5::new();
    hasher.update(data.as_bytes());
    let result = hasher.finalize();

    Ok(Value::String(Arc::new(hex::encode(result))))
}

async fn sha1_fn(args: Vec<Value>, _kwargs: HashMap<String, Value>) -> Result<Value> {
    require_args("crypto.sha1", &args, 1)?;
    let data = get_string_arg("crypto.sha1", &args, 0)?;
    let mut hasher = Sha1::new();
    hasher.update(data.as_bytes());
    let result = hasher.finalize();

    Ok(Value::String(Arc::new(hex::encode(result))))
}

async fn sha256_fn(args: Vec<Value>, _kwargs: HashMap<String, Value>) -> Result<Value> {
    require_args("crypto.sha256", &args, 1)?;
    let data = get_string_arg("crypto.sha256", &args, 0)?;
    let mut hasher = Sha256::new();
    hasher.update(data.as_bytes());
    let result = hasher.finalize();

    Ok(Value::String(Arc::new(hex::encode(result))))
}

async fn sha512_fn(args: Vec<Value>, _kwargs: HashMap<String, Value>) -> Result<Value> {
    require_args("crypto.sha512", &args, 1)?;
    let data = get_string_arg("crypto.sha512", &args, 0)?;
    let mut hasher = Sha512::new();
    hasher.update(data.as_bytes());
    let result = hasher.finalize();

    Ok(Value::String(Arc::new(hex::encode(result))))
}

async fn hmac_sha256_fn(args: Vec<Value>, kwargs: HashMap<String, Value>) -> Result<Value> {
    require_args("crypto.hmac_sha256", &args, 2)?;
    let key_str = get_string_arg("crypto.hmac_sha256", &args, 0)?;
    let message = get_string_arg("crypto.hmac_sha256", &args, 1)?;

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

    let mut mac =
        HmacSha256::new_from_slice(&key_bytes).map_err(|e| BlueprintError::InternalError {
            message: format!("Invalid HMAC key: {}", e),
        })?;

    mac.update(message.as_bytes());
    let result = mac.finalize();

    Ok(Value::String(Arc::new(hex::encode(result.into_bytes()))))
}

async fn hmac_sha512_fn(args: Vec<Value>, kwargs: HashMap<String, Value>) -> Result<Value> {
    require_args("crypto.hmac_sha512", &args, 2)?;
    let key_str = get_string_arg("crypto.hmac_sha512", &args, 0)?;
    let message = get_string_arg("crypto.hmac_sha512", &args, 1)?;

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

    let mut mac =
        HmacSha512::new_from_slice(&key_bytes).map_err(|e| BlueprintError::InternalError {
            message: format!("Invalid HMAC key: {}", e),
        })?;

    mac.update(message.as_bytes());
    let result = mac.finalize();

    Ok(Value::String(Arc::new(hex::encode(result.into_bytes()))))
}

async fn constant_time_compare_fn(
    args: Vec<Value>,
    _kwargs: HashMap<String, Value>,
) -> Result<Value> {
    require_args("crypto.constant_time_compare", &args, 2)?;
    let a = get_string_arg("crypto.constant_time_compare", &args, 0)?;
    let b = get_string_arg("crypto.constant_time_compare", &args, 1)?;

    use subtle::ConstantTimeEq;
    let result = a.as_bytes().ct_eq(b.as_bytes());

    Ok(Value::Bool(result.into()))
}
