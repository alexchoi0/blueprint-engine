use std::collections::HashMap;
use std::sync::Arc;

use blueprint_engine_core::{
    validation::{get_int_arg, get_string_arg, require_args},
    BlueprintError, NativeFunction, Result, Value,
};
use indexmap::IndexMap;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream, UdpSocket};
use tokio::sync::RwLock;

pub fn get_functions() -> Vec<NativeFunction> {
    vec![
        NativeFunction::new("tcp_connect", tcp_connect_fn),
        NativeFunction::new("tcp_listen", tcp_listen_fn),
        NativeFunction::new("udp_bind", udp_bind_fn),
        NativeFunction::new("udp_send", udp_send_fn),
        NativeFunction::new("dns_lookup", dns_lookup_fn),
    ]
}

async fn tcp_connect_fn(args: Vec<Value>, kwargs: HashMap<String, Value>) -> Result<Value> {
    require_args("socket.tcp_connect", &args, 2)?;
    let host = get_string_arg("socket.tcp_connect", &args, 0)?;
    let port = get_int_arg("socket.tcp_connect", &args, 1)? as u16;

    let timeout_secs = kwargs
        .get("timeout")
        .and_then(|v| v.as_float().ok())
        .unwrap_or(30.0);

    let addr = format!("{}:{}", host, port);

    let connect_future = TcpStream::connect(&addr);
    let stream = tokio::time::timeout(
        std::time::Duration::from_secs_f64(timeout_secs),
        connect_future,
    )
    .await
    .map_err(|_| BlueprintError::IoError {
        path: addr.clone(),
        message: "TCP connection timed out".to_string(),
    })?
    .map_err(|e| BlueprintError::IoError {
        path: addr.clone(),
        message: format!("Failed to connect: {}", e),
    })?;

    let data = kwargs.get("data").and_then(|v| v.as_string().ok());

    if let Some(send_data) = data {
        let mut stream = stream;
        stream
            .write_all(send_data.as_bytes())
            .await
            .map_err(|e| BlueprintError::IoError {
                path: addr.clone(),
                message: format!("Failed to send data: {}", e),
            })?;

        let read_response = kwargs
            .get("read_response")
            .map(|v| v.is_truthy())
            .unwrap_or(true);

        if read_response {
            let max_bytes = kwargs
                .get("max_bytes")
                .and_then(|v| v.as_int().ok())
                .map(|n| n as usize)
                .unwrap_or(65536);

            let mut buffer = vec![0u8; max_bytes];
            let n = stream
                .read(&mut buffer)
                .await
                .map_err(|e| BlueprintError::IoError {
                    path: addr.clone(),
                    message: format!("Failed to read response: {}", e),
                })?;

            buffer.truncate(n);
            let response = String::from_utf8_lossy(&buffer).into_owned();

            let mut result: IndexMap<String, Value> = IndexMap::new();
            result.insert("connected".to_string(), Value::Bool(true));
            result.insert("response".to_string(), Value::String(Arc::new(response)));
            result.insert("bytes_received".to_string(), Value::Int(n as i64));

            return Ok(Value::Dict(Arc::new(RwLock::new(result))));
        }
    }

    let mut result: IndexMap<String, Value> = IndexMap::new();
    result.insert("connected".to_string(), Value::Bool(true));

    Ok(Value::Dict(Arc::new(RwLock::new(result))))
}

async fn tcp_listen_fn(args: Vec<Value>, kwargs: HashMap<String, Value>) -> Result<Value> {
    require_args("socket.tcp_listen", &args, 1)?;
    let port = get_int_arg("socket.tcp_listen", &args, 0)? as u16;

    let host = kwargs
        .get("host")
        .and_then(|v| v.as_string().ok().map(|s| s.to_string()))
        .unwrap_or_else(|| "0.0.0.0".to_string());

    let addr = format!("{}:{}", host, port);

    let listener = TcpListener::bind(&addr)
        .await
        .map_err(|e| BlueprintError::IoError {
            path: addr.clone(),
            message: format!("Failed to bind: {}", e),
        })?;

    let timeout_secs = kwargs
        .get("timeout")
        .and_then(|v| v.as_float().ok())
        .unwrap_or(60.0);

    let accept_future = listener.accept();
    let (mut stream, peer_addr) = tokio::time::timeout(
        std::time::Duration::from_secs_f64(timeout_secs),
        accept_future,
    )
    .await
    .map_err(|_| BlueprintError::IoError {
        path: addr.clone(),
        message: "Accept timed out waiting for connection".to_string(),
    })?
    .map_err(|e| BlueprintError::IoError {
        path: addr.clone(),
        message: format!("Failed to accept connection: {}", e),
    })?;

    let max_bytes = kwargs
        .get("max_bytes")
        .and_then(|v| v.as_int().ok())
        .map(|n| n as usize)
        .unwrap_or(65536);

    let mut buffer = vec![0u8; max_bytes];
    let n = stream
        .read(&mut buffer)
        .await
        .map_err(|e| BlueprintError::IoError {
            path: addr.clone(),
            message: format!("Failed to read from client: {}", e),
        })?;

    buffer.truncate(n);
    let data = String::from_utf8_lossy(&buffer).into_owned();

    let response = kwargs.get("response").and_then(|v| v.as_string().ok());
    if let Some(resp) = response {
        stream
            .write_all(resp.as_bytes())
            .await
            .map_err(|e| BlueprintError::IoError {
                path: addr.clone(),
                message: format!("Failed to send response: {}", e),
            })?;
    }

    let mut result: IndexMap<String, Value> = IndexMap::new();
    result.insert(
        "peer_addr".to_string(),
        Value::String(Arc::new(peer_addr.to_string())),
    );
    result.insert("data".to_string(), Value::String(Arc::new(data)));
    result.insert("bytes_received".to_string(), Value::Int(n as i64));

    Ok(Value::Dict(Arc::new(RwLock::new(result))))
}

async fn udp_bind_fn(args: Vec<Value>, kwargs: HashMap<String, Value>) -> Result<Value> {
    require_args("socket.udp_bind", &args, 1)?;
    let port = get_int_arg("socket.udp_bind", &args, 0)? as u16;

    let host = kwargs
        .get("host")
        .and_then(|v| v.as_string().ok().map(|s| s.to_string()))
        .unwrap_or_else(|| "0.0.0.0".to_string());

    let addr = format!("{}:{}", host, port);

    let socket = UdpSocket::bind(&addr)
        .await
        .map_err(|e| BlueprintError::IoError {
            path: addr.clone(),
            message: format!("Failed to bind UDP socket: {}", e),
        })?;

    let timeout_secs = kwargs
        .get("timeout")
        .and_then(|v| v.as_float().ok())
        .unwrap_or(30.0);

    let max_bytes = kwargs
        .get("max_bytes")
        .and_then(|v| v.as_int().ok())
        .map(|n| n as usize)
        .unwrap_or(65536);

    let mut buffer = vec![0u8; max_bytes];

    let recv_future = socket.recv_from(&mut buffer);
    let (n, peer_addr) = tokio::time::timeout(
        std::time::Duration::from_secs_f64(timeout_secs),
        recv_future,
    )
    .await
    .map_err(|_| BlueprintError::IoError {
        path: addr.clone(),
        message: "UDP receive timed out".to_string(),
    })?
    .map_err(|e| BlueprintError::IoError {
        path: addr.clone(),
        message: format!("Failed to receive UDP data: {}", e),
    })?;

    buffer.truncate(n);
    let data = String::from_utf8_lossy(&buffer).into_owned();

    let response = kwargs.get("response").and_then(|v| v.as_string().ok());
    if let Some(resp) = response {
        socket
            .send_to(resp.as_bytes(), &peer_addr)
            .await
            .map_err(|e| BlueprintError::IoError {
                path: addr.clone(),
                message: format!("Failed to send UDP response: {}", e),
            })?;
    }

    let mut result: IndexMap<String, Value> = IndexMap::new();
    result.insert(
        "peer_addr".to_string(),
        Value::String(Arc::new(peer_addr.to_string())),
    );
    result.insert("data".to_string(), Value::String(Arc::new(data)));
    result.insert("bytes_received".to_string(), Value::Int(n as i64));

    Ok(Value::Dict(Arc::new(RwLock::new(result))))
}

async fn udp_send_fn(args: Vec<Value>, kwargs: HashMap<String, Value>) -> Result<Value> {
    require_args("socket.udp_send", &args, 3)?;
    let host = get_string_arg("socket.udp_send", &args, 0)?;
    let port = get_int_arg("socket.udp_send", &args, 1)? as u16;
    let data = get_string_arg("socket.udp_send", &args, 2)?;

    let socket = UdpSocket::bind("0.0.0.0:0")
        .await
        .map_err(|e| BlueprintError::IoError {
            path: "0.0.0.0:0".to_string(),
            message: format!("Failed to create UDP socket: {}", e),
        })?;

    let addr = format!("{}:{}", host, port);
    let bytes_sent =
        socket
            .send_to(data.as_bytes(), &addr)
            .await
            .map_err(|e| BlueprintError::IoError {
                path: addr.clone(),
                message: format!("Failed to send UDP data: {}", e),
            })?;

    let wait_response = kwargs
        .get("wait_response")
        .map(|v| v.is_truthy())
        .unwrap_or(false);

    if wait_response {
        let timeout_secs = kwargs
            .get("timeout")
            .and_then(|v| v.as_float().ok())
            .unwrap_or(5.0);

        let max_bytes = kwargs
            .get("max_bytes")
            .and_then(|v| v.as_int().ok())
            .map(|n| n as usize)
            .unwrap_or(65536);

        let mut buffer = vec![0u8; max_bytes];

        let recv_future = socket.recv_from(&mut buffer);
        match tokio::time::timeout(
            std::time::Duration::from_secs_f64(timeout_secs),
            recv_future,
        )
        .await
        {
            Ok(Ok((n, _))) => {
                buffer.truncate(n);
                let response = String::from_utf8_lossy(&buffer).into_owned();

                let mut result: IndexMap<String, Value> = IndexMap::new();
                result.insert("bytes_sent".to_string(), Value::Int(bytes_sent as i64));
                result.insert("response".to_string(), Value::String(Arc::new(response)));
                return Ok(Value::Dict(Arc::new(RwLock::new(result))));
            }
            _ => {
                let mut result: IndexMap<String, Value> = IndexMap::new();
                result.insert("bytes_sent".to_string(), Value::Int(bytes_sent as i64));
                result.insert("response".to_string(), Value::None);
                return Ok(Value::Dict(Arc::new(RwLock::new(result))));
            }
        }
    }

    Ok(Value::Int(bytes_sent as i64))
}

async fn dns_lookup_fn(args: Vec<Value>, _kwargs: HashMap<String, Value>) -> Result<Value> {
    require_args("socket.dns_lookup", &args, 1)?;
    let hostname = get_string_arg("socket.dns_lookup", &args, 0)?;

    let addrs = tokio::net::lookup_host(format!("{}:0", hostname))
        .await
        .map_err(|e| BlueprintError::IoError {
            path: hostname.to_string(),
            message: format!("DNS lookup failed: {}", e),
        })?;

    let addresses: Vec<Value> = addrs
        .map(|addr| Value::String(Arc::new(addr.ip().to_string())))
        .collect();

    Ok(Value::List(Arc::new(RwLock::new(addresses))))
}
