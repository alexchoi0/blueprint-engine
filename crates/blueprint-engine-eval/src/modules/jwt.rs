use std::collections::HashMap;
use std::sync::Arc;

use blueprint_engine_core::{BlueprintError, NativeFunction, Result, Value};
use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};
use serde_json::Value as JsonValue;

pub fn get_functions() -> Vec<NativeFunction> {
    vec![NativeFunction::new("jwt_sign", jwt_sign_fn)]
}

async fn jwt_sign_fn(args: Vec<Value>, kwargs: HashMap<String, Value>) -> Result<Value> {
    let claims = if !args.is_empty() {
        &args[0]
    } else {
        kwargs
            .get("claims")
            .ok_or_else(|| BlueprintError::ArgumentError {
                message: "jwt_sign() requires 'claims' argument".into(),
            })?
    };

    let private_key = kwargs
        .get("private_key")
        .ok_or_else(|| BlueprintError::ArgumentError {
            message: "jwt_sign() requires 'private_key' kwarg".into(),
        })?
        .as_string()?;

    let algorithm = kwargs
        .get("algorithm")
        .map(|v| v.as_string())
        .transpose()?
        .unwrap_or_else(|| "RS256".to_string());

    let alg = match algorithm.as_str() {
        "RS256" => Algorithm::RS256,
        "RS384" => Algorithm::RS384,
        "RS512" => Algorithm::RS512,
        "ES256" => Algorithm::ES256,
        "ES384" => Algorithm::ES384,
        "HS256" => Algorithm::HS256,
        "HS384" => Algorithm::HS384,
        "HS512" => Algorithm::HS512,
        _ => {
            return Err(BlueprintError::ArgumentError {
                message: format!(
                    "Unsupported algorithm '{}'. Supported: RS256, RS384, RS512, ES256, ES384, HS256, HS384, HS512",
                    algorithm
                ),
            });
        }
    };

    let json_claims = value_to_json(claims).await?;

    let header = Header::new(alg);
    let key = if algorithm.starts_with("HS") {
        EncodingKey::from_secret(private_key.as_bytes())
    } else if algorithm.starts_with("RS") {
        EncodingKey::from_rsa_pem(private_key.as_bytes()).map_err(|e| {
            BlueprintError::ArgumentError {
                message: format!("Invalid RSA private key: {}", e),
            }
        })?
    } else {
        EncodingKey::from_ec_pem(private_key.as_bytes()).map_err(|e| {
            BlueprintError::ArgumentError {
                message: format!("Invalid EC private key: {}", e),
            }
        })?
    };

    let token = encode(&header, &json_claims, &key).map_err(|e| BlueprintError::InternalError {
        message: format!("Failed to sign JWT: {}", e),
    })?;

    Ok(Value::String(Arc::new(token)))
}

#[async_recursion::async_recursion]
async fn value_to_json(value: &Value) -> Result<JsonValue> {
    match value {
        Value::None => Ok(JsonValue::Null),
        Value::Bool(b) => Ok(JsonValue::Bool(*b)),
        Value::Int(i) => Ok(JsonValue::Number((*i).into())),
        Value::Float(f) => Ok(JsonValue::Number(
            serde_json::Number::from_f64(*f).unwrap_or_else(|| serde_json::Number::from(0)),
        )),
        Value::String(s) => Ok(JsonValue::String(s.to_string())),
        Value::List(list) => {
            let guard = list.read().await;
            let mut arr = Vec::new();
            for item in guard.iter() {
                arr.push(value_to_json(item).await?);
            }
            Ok(JsonValue::Array(arr))
        }
        Value::Dict(dict) => {
            let guard = dict.read().await;
            let mut map = serde_json::Map::new();
            for (k, v) in guard.iter() {
                map.insert(k.clone(), value_to_json(v).await?);
            }
            Ok(JsonValue::Object(map))
        }
        _ => Err(BlueprintError::TypeError {
            expected: "JSON-serializable value".into(),
            actual: value.type_name().into(),
        }),
    }
}
