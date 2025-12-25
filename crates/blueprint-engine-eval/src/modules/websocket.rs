use indexmap::IndexMap;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

use axum::{
    extract::ws::{Message as AxumMessage, WebSocket, WebSocketUpgrade},
    extract::State,
    response::IntoResponse,
    routing::get,
    Router,
};
use blueprint_engine_core::{
    check_ws,
    validation::{get_int_arg, get_string_arg, require_args, require_args_min},
    BlueprintError, NativeFunction, Result, StreamIterator, Value,
};
use futures_util::{SinkExt, StreamExt};
use tokio::sync::{mpsc, oneshot, Mutex, RwLock};
use tokio_tungstenite::{connect_async, tungstenite::Message};

use crate::eval::Evaluator;

fn random_id() -> String {
    use rand::Rng;
    let bytes: [u8; 4] = rand::thread_rng().gen();
    hex::encode(bytes)
}
use crate::modules::triggers::{TriggerHandle, TriggerType, TRIGGER_REGISTRY};

pub fn get_functions() -> Vec<NativeFunction> {
    vec![
        NativeFunction::new("ws_connect", ws_connect),
        NativeFunction::new("ws_server", ws_server),
    ]
}

fn create_ws_connection(
    send_tx: mpsc::Sender<String>,
    recv_rx: mpsc::Receiver<Option<String>>,
) -> Value {
    let iterator = Arc::new(StreamIterator::new(recv_rx));
    let iterator_for_recv = iterator.clone();

    let send_tx = Arc::new(Mutex::new(Some(send_tx)));
    let send_tx_for_method = send_tx.clone();

    let send_method = Value::NativeFunction(Arc::new(NativeFunction::new_with_state(
        "send",
        move |args, _kwargs| {
            let tx = send_tx_for_method.clone();
            Box::pin(async move {
                if args.len() != 1 {
                    return Err(BlueprintError::ArgumentError {
                        message: format!("send() takes exactly 1 argument ({} given)", args.len()),
                    });
                }
                let msg = args[0].as_string()?;
                let guard = tx.lock().await;
                if let Some(sender) = guard.as_ref() {
                    sender
                        .send(msg)
                        .await
                        .map_err(|_| BlueprintError::IoError {
                            path: "websocket".into(),
                            message: "WebSocket connection closed".into(),
                        })?;
                } else {
                    return Err(BlueprintError::IoError {
                        path: "websocket".into(),
                        message: "WebSocket connection closed".into(),
                    });
                }
                Ok(Value::None)
            })
        },
    )));

    let recv_method = Value::NativeFunction(Arc::new(NativeFunction::new_with_state(
        "recv",
        move |_args, _kwargs| {
            let iter = iterator_for_recv.clone();
            Box::pin(async move {
                match iter.next().await {
                    Some(v) => Ok(v),
                    None => Ok(Value::None),
                }
            })
        },
    )));

    let send_tx_for_close = send_tx.clone();
    let close_method = Value::NativeFunction(Arc::new(NativeFunction::new_with_state(
        "close",
        move |_args, _kwargs| {
            let tx = send_tx_for_close.clone();
            Box::pin(async move {
                let mut guard = tx.lock().await;
                *guard = None;
                Ok(Value::None)
            })
        },
    )));

    let mut ws_dict: IndexMap<String, Value> = IndexMap::new();
    ws_dict.insert("send".to_string(), send_method);
    ws_dict.insert("recv".to_string(), recv_method);
    ws_dict.insert("close".to_string(), close_method);
    ws_dict.insert("messages".to_string(), Value::Iterator(iterator));

    Value::Dict(Arc::new(RwLock::new(ws_dict)))
}

async fn ws_connect(args: Vec<Value>, kwargs: HashMap<String, Value>) -> Result<Value> {
    require_args("websocket.ws_connect", &args, 1)?;
    let url_str = get_string_arg("websocket.ws_connect", &args, 0)?;
    check_ws(&url_str).await?;

    let headers: HashMap<String, String> = if let Some(h) = kwargs.get("headers") {
        match h {
            Value::Dict(d) => {
                let map = d.read().await;
                map.iter()
                    .map(|(k, v)| (k.clone(), v.to_display_string()))
                    .collect()
            }
            _ => HashMap::new(),
        }
    } else {
        HashMap::new()
    };

    let url = url::Url::parse(&url_str).map_err(|e| BlueprintError::ArgumentError {
        message: format!("Invalid WebSocket URL: {}", e),
    })?;

    let mut request = tokio_tungstenite::tungstenite::http::Request::builder()
        .uri(url_str.clone())
        .header("Host", url.host_str().unwrap_or(""))
        .header("Connection", "Upgrade")
        .header("Upgrade", "websocket")
        .header("Sec-WebSocket-Version", "13")
        .header(
            "Sec-WebSocket-Key",
            tokio_tungstenite::tungstenite::handshake::client::generate_key(),
        );

    for (key, value) in &headers {
        request = request.header(key.as_str(), value.as_str());
    }

    let request = request.body(()).map_err(|e| BlueprintError::IoError {
        path: url_str.clone(),
        message: format!("Failed to build request: {}", e),
    })?;

    let (ws_stream, _) = connect_async(request)
        .await
        .map_err(|e| BlueprintError::IoError {
            path: url_str.clone(),
            message: format!("WebSocket connection failed: {}", e),
        })?;

    let (mut write, mut read) = ws_stream.split();

    let (send_tx, mut send_rx) = mpsc::channel::<String>(32);
    let (recv_tx, recv_rx) = mpsc::channel::<Option<String>>(32);

    let recv_tx_clone = recv_tx.clone();
    tokio::spawn(async move {
        while let Some(msg_result) = read.next().await {
            match msg_result {
                Ok(Message::Text(text)) => {
                    if recv_tx_clone.send(Some(text)).await.is_err() {
                        break;
                    }
                }
                Ok(Message::Binary(data)) => {
                    if let Ok(text) = String::from_utf8(data) {
                        if recv_tx_clone.send(Some(text)).await.is_err() {
                            break;
                        }
                    }
                }
                Ok(Message::Close(_)) => {
                    let _ = recv_tx_clone.send(None).await;
                    break;
                }
                Ok(_) => {}
                Err(_) => {
                    let _ = recv_tx_clone.send(None).await;
                    break;
                }
            }
        }
    });

    tokio::spawn(async move {
        while let Some(msg) = send_rx.recv().await {
            if write.send(Message::Text(msg)).await.is_err() {
                break;
            }
        }
        let _ = write.close().await;
    });

    Ok(create_ws_connection(send_tx, recv_rx))
}

#[derive(Clone)]
struct WsServerState {
    handler: Value,
}

async fn ws_server(args: Vec<Value>, kwargs: HashMap<String, Value>) -> Result<Value> {
    require_args_min("websocket.ws_server", &args, 2)?;
    let port = get_int_arg("websocket.ws_server", &args, 0)? as u16;
    let handler = args[1].clone();

    let host = kwargs
        .get("host")
        .map(|v| v.as_string())
        .transpose()?
        .unwrap_or_else(|| "0.0.0.0".to_string());

    let path = kwargs
        .get("path")
        .map(|v| v.as_string())
        .transpose()?
        .unwrap_or_else(|| "/".to_string());

    let id = format!("ws-{}", random_id());
    let running = Arc::new(RwLock::new(true));

    let state = WsServerState { handler };

    let path_clone = path.clone();
    let router = Router::new()
        .route(&path_clone, get(ws_upgrade_handler))
        .with_state(state);

    let addr: SocketAddr =
        format!("{}:{}", host, port)
            .parse()
            .map_err(|e| BlueprintError::ArgumentError {
                message: format!("Invalid address: {}", e),
            })?;

    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();

    let handle = TriggerHandle {
        id: id.clone(),
        trigger_type: TriggerType::Http {
            port,
            host: host.clone(),
            routes: vec![format!("WS {}", path)],
        },
        running: running.clone(),
    };

    TRIGGER_REGISTRY
        .write()
        .await
        .register(handle.clone(), Some(shutdown_tx));

    let listener =
        tokio::net::TcpListener::bind(addr)
            .await
            .map_err(|e| BlueprintError::IoError {
                path: addr.to_string(),
                message: format!("Failed to bind: {}", e),
            })?;

    tokio::spawn(async move {
        axum::serve(listener, router)
            .with_graceful_shutdown(async {
                let _ = shutdown_rx.await;
            })
            .await
            .ok();
    });

    let mut result = IndexMap::new();
    result.insert("id".to_string(), Value::String(Arc::new(id)));
    result.insert("port".to_string(), Value::Int(port as i64));
    result.insert("host".to_string(), Value::String(Arc::new(host)));
    result.insert("path".to_string(), Value::String(Arc::new(path)));

    Ok(Value::Dict(Arc::new(RwLock::new(result))))
}

async fn ws_upgrade_handler(
    ws: WebSocketUpgrade,
    State(state): State<WsServerState>,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_ws_connection(socket, state.handler))
}

async fn handle_ws_connection(socket: WebSocket, handler: Value) {
    let (mut ws_write, mut ws_read) = socket.split();

    let (send_tx, mut send_rx) = mpsc::channel::<String>(32);
    let (recv_tx, recv_rx) = mpsc::channel::<Option<String>>(32);

    let recv_tx_clone = recv_tx.clone();
    tokio::spawn(async move {
        while let Some(msg_result) = ws_read.next().await {
            match msg_result {
                Ok(AxumMessage::Text(text)) => {
                    if recv_tx_clone.send(Some(text.to_string())).await.is_err() {
                        break;
                    }
                }
                Ok(AxumMessage::Binary(data)) => {
                    if let Ok(text) = String::from_utf8(data.to_vec()) {
                        if recv_tx_clone.send(Some(text)).await.is_err() {
                            break;
                        }
                    }
                }
                Ok(AxumMessage::Close(_)) => {
                    let _ = recv_tx_clone.send(None).await;
                    break;
                }
                Ok(_) => {}
                Err(_) => {
                    let _ = recv_tx_clone.send(None).await;
                    break;
                }
            }
        }
    });

    tokio::spawn(async move {
        while let Some(msg) = send_rx.recv().await {
            if ws_write.send(AxumMessage::Text(msg.into())).await.is_err() {
                break;
            }
        }
        let _ = ws_write.close().await;
    });

    let ws_conn = create_ws_connection(send_tx, recv_rx);

    let evaluator = Evaluator::new();
    let _ = match &handler {
        Value::Lambda(lambda) => {
            evaluator
                .call_lambda_public(lambda, vec![ws_conn], HashMap::new())
                .await
        }
        Value::Function(func) => {
            evaluator
                .call_function_public(func, vec![ws_conn], HashMap::new())
                .await
        }
        Value::NativeFunction(native) => native.call(vec![ws_conn], HashMap::new()).await,
        _ => Ok(Value::None),
    };
}
