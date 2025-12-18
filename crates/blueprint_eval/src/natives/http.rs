use std::collections::HashMap;
use std::sync::Arc;

use blueprint_core::{BlueprintError, HttpResponse, NativeFunction, Result, Value};
use reqwest::Client;

use crate::eval::Evaluator;

pub fn register(evaluator: &mut Evaluator) {
    evaluator.register_native(NativeFunction::new("http_get", http_get));
    evaluator.register_native(NativeFunction::new("http_post", http_post));
    evaluator.register_native(NativeFunction::new("http_put", http_put));
    evaluator.register_native(NativeFunction::new("http_delete", http_delete));
    evaluator.register_native(NativeFunction::new("http_patch", http_patch));
    evaluator.register_native(NativeFunction::new("download", download));
}

async fn http_get(args: Vec<Value>, kwargs: HashMap<String, Value>) -> Result<Value> {
    if args.is_empty() || args.len() > 2 {
        return Err(BlueprintError::ArgumentError {
            message: format!("http_get() takes 1 or 2 arguments ({} given)", args.len()),
        });
    }

    let url = args[0].as_string()?;
    let headers = if args.len() == 2 {
        extract_headers(&args[1]).await?
    } else if let Some(h) = kwargs.get("headers") {
        extract_headers(h).await?
    } else {
        HashMap::new()
    };

    let timeout = kwargs
        .get("timeout")
        .and_then(|v| v.as_float().ok())
        .unwrap_or(30.0);

    make_request("GET", &url, None, headers, timeout).await
}

async fn http_post(args: Vec<Value>, kwargs: HashMap<String, Value>) -> Result<Value> {
    if args.len() < 1 || args.len() > 3 {
        return Err(BlueprintError::ArgumentError {
            message: format!("http_post() takes 1 to 3 arguments ({} given)", args.len()),
        });
    }

    let url = args[0].as_string()?;
    let body = if args.len() >= 2 {
        Some(args[1].as_string()?)
    } else {
        kwargs.get("body").map(|v| v.to_display_string())
    };

    let headers = if args.len() == 3 {
        extract_headers(&args[2]).await?
    } else if let Some(h) = kwargs.get("headers") {
        extract_headers(h).await?
    } else {
        HashMap::new()
    };

    let timeout = kwargs
        .get("timeout")
        .and_then(|v| v.as_float().ok())
        .unwrap_or(30.0);

    make_request("POST", &url, body, headers, timeout).await
}

async fn http_put(args: Vec<Value>, kwargs: HashMap<String, Value>) -> Result<Value> {
    if args.len() < 1 || args.len() > 3 {
        return Err(BlueprintError::ArgumentError {
            message: format!("http_put() takes 1 to 3 arguments ({} given)", args.len()),
        });
    }

    let url = args[0].as_string()?;
    let body = if args.len() >= 2 {
        Some(args[1].as_string()?)
    } else {
        kwargs.get("body").map(|v| v.to_display_string())
    };

    let headers = if args.len() == 3 {
        extract_headers(&args[2]).await?
    } else if let Some(h) = kwargs.get("headers") {
        extract_headers(h).await?
    } else {
        HashMap::new()
    };

    let timeout = kwargs
        .get("timeout")
        .and_then(|v| v.as_float().ok())
        .unwrap_or(30.0);

    make_request("PUT", &url, body, headers, timeout).await
}

async fn http_delete(args: Vec<Value>, kwargs: HashMap<String, Value>) -> Result<Value> {
    if args.is_empty() || args.len() > 2 {
        return Err(BlueprintError::ArgumentError {
            message: format!("http_delete() takes 1 or 2 arguments ({} given)", args.len()),
        });
    }

    let url = args[0].as_string()?;
    let headers = if args.len() == 2 {
        extract_headers(&args[1]).await?
    } else if let Some(h) = kwargs.get("headers") {
        extract_headers(h).await?
    } else {
        HashMap::new()
    };

    let timeout = kwargs
        .get("timeout")
        .and_then(|v| v.as_float().ok())
        .unwrap_or(30.0);

    make_request("DELETE", &url, None, headers, timeout).await
}

async fn http_patch(args: Vec<Value>, kwargs: HashMap<String, Value>) -> Result<Value> {
    if args.len() < 1 || args.len() > 3 {
        return Err(BlueprintError::ArgumentError {
            message: format!("http_patch() takes 1 to 3 arguments ({} given)", args.len()),
        });
    }

    let url = args[0].as_string()?;
    let body = if args.len() >= 2 {
        Some(args[1].as_string()?)
    } else {
        kwargs.get("body").map(|v| v.to_display_string())
    };

    let headers = if args.len() == 3 {
        extract_headers(&args[2]).await?
    } else if let Some(h) = kwargs.get("headers") {
        extract_headers(h).await?
    } else {
        HashMap::new()
    };

    let timeout = kwargs
        .get("timeout")
        .and_then(|v| v.as_float().ok())
        .unwrap_or(30.0);

    make_request("PATCH", &url, body, headers, timeout).await
}

async fn download(args: Vec<Value>, _kwargs: HashMap<String, Value>) -> Result<Value> {
    if args.len() != 2 {
        return Err(BlueprintError::ArgumentError {
            message: format!("download() takes exactly 2 arguments ({} given)", args.len()),
        });
    }

    let url = args[0].as_string()?;
    let path = args[1].as_string()?;

    let response = reqwest::get(&url)
        .await
        .map_err(|e| BlueprintError::HttpError {
            url: url.clone(),
            message: e.to_string(),
        })?;

    if !response.status().is_success() {
        return Err(BlueprintError::HttpError {
            url: url.clone(),
            message: format!("HTTP {}", response.status().as_u16()),
        });
    }

    let bytes = response.bytes().await.map_err(|e| BlueprintError::HttpError {
        url: url.clone(),
        message: e.to_string(),
    })?;

    tokio::fs::write(&path, &bytes)
        .await
        .map_err(|e| BlueprintError::IoError {
            path: path.clone(),
            message: e.to_string(),
        })?;

    Ok(Value::None)
}

async fn extract_headers(value: &Value) -> Result<HashMap<String, String>> {
    match value {
        Value::Dict(d) => {
            let map = d.read().await;
            let mut headers = HashMap::new();
            for (k, v) in map.iter() {
                headers.insert(k.clone(), v.to_display_string());
            }
            Ok(headers)
        }
        _ => Err(BlueprintError::TypeError {
            expected: "dict".into(),
            actual: value.type_name().into(),
        }),
    }
}

async fn make_request(
    method: &str,
    url: &str,
    body: Option<String>,
    headers: HashMap<String, String>,
    timeout_secs: f64,
) -> Result<Value> {
    let client = Client::builder()
        .timeout(std::time::Duration::from_secs_f64(timeout_secs))
        .build()
        .map_err(|e| BlueprintError::HttpError {
            url: url.into(),
            message: e.to_string(),
        })?;

    let mut request = match method {
        "GET" => client.get(url),
        "POST" => client.post(url),
        "PUT" => client.put(url),
        "DELETE" => client.delete(url),
        "PATCH" => client.patch(url),
        _ => {
            return Err(BlueprintError::InternalError {
                message: format!("Unknown HTTP method: {}", method),
            })
        }
    };

    for (key, value) in headers {
        request = request.header(&key, &value);
    }

    if let Some(b) = body {
        request = request.body(b);
    }

    let response = request.send().await.map_err(|e| BlueprintError::HttpError {
        url: url.into(),
        message: e.to_string(),
    })?;

    let status = response.status().as_u16() as i64;

    let resp_headers: HashMap<String, String> = response
        .headers()
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_str().unwrap_or("").to_string()))
        .collect();

    let body_text = response.text().await.map_err(|e| BlueprintError::HttpError {
        url: url.into(),
        message: e.to_string(),
    })?;

    Ok(Value::Response(Arc::new(HttpResponse {
        status,
        body: body_text,
        headers: resp_headers,
    })))
}
