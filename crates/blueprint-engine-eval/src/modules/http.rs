use std::collections::HashMap;
use std::sync::Arc;

use blueprint_engine_core::{
    check_fs_write, check_http,
    validation::{get_string_arg, require_args, require_args_range},
    BlueprintError, HttpResponse, NativeFunction, Result, StreamIterator, Value,
};
use futures_util::StreamExt;
use reqwest::Client;
use tokio::sync::mpsc;

pub fn get_functions() -> Vec<NativeFunction> {
    vec![
        NativeFunction::new("http_request", http_request),
        NativeFunction::new("download", download),
    ]
}

async fn http_request(args: Vec<Value>, kwargs: HashMap<String, Value>) -> Result<Value> {
    require_args_range("http.http_request", &args, 2, 4)?;
    let method = get_string_arg("http.http_request", &args, 0)?.to_uppercase();
    let url = get_string_arg("http.http_request", &args, 1)?;
    check_http(&url).await?;

    let body = if args.len() >= 3 {
        let v = &args[2];
        if matches!(v, Value::None) {
            None
        } else {
            Some(v.as_string()?)
        }
    } else {
        kwargs.get("body").and_then(|v| {
            if matches!(v, Value::None) {
                None
            } else {
                Some(v.to_display_string())
            }
        })
    };

    let headers = if args.len() == 4 {
        extract_headers(&args[3]).await?
    } else if let Some(h) = kwargs.get("headers") {
        extract_headers(h).await?
    } else {
        HashMap::new()
    };

    let timeout = kwargs
        .get("timeout")
        .and_then(|v| v.as_float().ok())
        .unwrap_or(30.0);

    let stream = kwargs.get("stream").map(|v| v.is_truthy()).unwrap_or(false);

    if stream {
        let chunk_size = kwargs
            .get("chunk_size")
            .and_then(|v| v.as_int().ok())
            .map(|n| n as usize);

        let (tx, rx) = mpsc::channel::<Option<String>>(32);
        let iterator = Arc::new(StreamIterator::new(rx));

        let url_clone = url.clone();
        tokio::spawn(async move {
            if let Err(e) =
                stream_request(&method, &url_clone, body, headers, tx.clone(), chunk_size).await
            {
                eprintln!("HTTP stream error: {}", e);
            }
            tx.send(None).await.ok();
        });

        Ok(Value::Iterator(iterator))
    } else {
        make_request(&method, &url, body, headers, timeout).await
    }
}

async fn stream_request(
    method: &str,
    url: &str,
    body: Option<String>,
    headers: HashMap<String, String>,
    tx: mpsc::Sender<Option<String>>,
    chunk_size: Option<usize>,
) -> Result<()> {
    let client = Client::new();

    let mut request = match method {
        "GET" => client.get(url),
        "POST" => client.post(url),
        "PUT" => client.put(url),
        "DELETE" => client.delete(url),
        "PATCH" => client.patch(url),
        "HEAD" => client.head(url),
        "OPTIONS" => client.request(reqwest::Method::OPTIONS, url),
        _ => {
            return Err(BlueprintError::ArgumentError {
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

    let response = request
        .send()
        .await
        .map_err(|e| BlueprintError::HttpError {
            url: url.into(),
            message: e.to_string(),
        })?;

    if !response.status().is_success() {
        return Err(BlueprintError::HttpError {
            url: url.into(),
            message: format!("HTTP {}", response.status().as_u16()),
        });
    }

    let mut stream = response.bytes_stream();
    let target_chunk_size = chunk_size.unwrap_or(8192);
    let mut buffer = Vec::new();

    while let Some(chunk_result) = stream.next().await {
        let chunk = chunk_result.map_err(|e| BlueprintError::HttpError {
            url: url.into(),
            message: e.to_string(),
        })?;

        buffer.extend_from_slice(&chunk);

        while buffer.len() >= target_chunk_size {
            let data: Vec<u8> = buffer.drain(..target_chunk_size).collect();
            if let Ok(s) = String::from_utf8(data.clone()) {
                tx.send(Some(s)).await.ok();
            } else {
                tx.send(Some(String::from_utf8_lossy(&data).to_string()))
                    .await
                    .ok();
            }
        }
    }

    if !buffer.is_empty() {
        if let Ok(s) = String::from_utf8(buffer.clone()) {
            tx.send(Some(s)).await.ok();
        } else {
            tx.send(Some(String::from_utf8_lossy(&buffer).to_string()))
                .await
                .ok();
        }
    }

    Ok(())
}

async fn download(args: Vec<Value>, _kwargs: HashMap<String, Value>) -> Result<Value> {
    require_args("http.download", &args, 2)?;
    let url = get_string_arg("http.download", &args, 0)?;
    let path = get_string_arg("http.download", &args, 1)?;
    check_http(&url).await?;
    check_fs_write(&path).await?;

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

    let bytes = response
        .bytes()
        .await
        .map_err(|e| BlueprintError::HttpError {
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
        "HEAD" => client.head(url),
        "OPTIONS" => client.request(reqwest::Method::OPTIONS, url),
        _ => {
            return Err(BlueprintError::ArgumentError {
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

    let response = request
        .send()
        .await
        .map_err(|e| BlueprintError::HttpError {
            url: url.into(),
            message: e.to_string(),
        })?;

    let status = response.status().as_u16() as i64;

    let resp_headers: HashMap<String, String> = response
        .headers()
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_str().unwrap_or("").to_string()))
        .collect();

    let body_text = response
        .text()
        .await
        .map_err(|e| BlueprintError::HttpError {
            url: url.into(),
            message: e.to_string(),
        })?;

    Ok(Value::Response(Arc::new(HttpResponse {
        status,
        body: body_text,
        headers: resp_headers,
    })))
}
