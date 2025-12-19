use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

use axum::{
    body::Body,
    http::{Request, StatusCode},
    response::IntoResponse,
    routing::{delete, get, head, patch, post, put},
    Router,
};
use blueprint_core::{BlueprintError, NativeFunction, Result, Value};
use tokio::sync::{oneshot, RwLock};
use tokio_cron_scheduler::{Job, JobScheduler};
use uuid::Uuid;

use crate::eval::Evaluator;

lazy_static::lazy_static! {
    pub static ref TRIGGER_REGISTRY: Arc<RwLock<TriggerRegistry>> = Arc::new(RwLock::new(TriggerRegistry::new()));
}

pub struct TriggerRegistry {
    triggers: HashMap<String, TriggerHandle>,
    shutdown_senders: HashMap<String, oneshot::Sender<()>>,
}

#[derive(Clone)]
pub struct TriggerHandle {
    pub id: String,
    pub trigger_type: TriggerType,
    pub running: Arc<RwLock<bool>>,
}

#[derive(Clone)]
pub enum TriggerType {
    Http {
        port: u16,
        host: String,
        routes: Vec<String>,
    },
    Cron {
        schedule: String,
    },
    Interval {
        seconds: u64,
    },
}

impl TriggerRegistry {
    pub fn new() -> Self {
        Self {
            triggers: HashMap::new(),
            shutdown_senders: HashMap::new(),
        }
    }

    pub fn register(&mut self, handle: TriggerHandle, shutdown_tx: Option<oneshot::Sender<()>>) {
        let id = handle.id.clone();
        self.triggers.insert(id.clone(), handle);
        if let Some(tx) = shutdown_tx {
            self.shutdown_senders.insert(id, tx);
        }
    }

    pub fn stop(&mut self, id: &str) -> bool {
        if let Some(handle) = self.triggers.get(id) {
            let running = handle.running.clone();
            tokio::spawn(async move {
                *running.write().await = false;
            });
        }
        if let Some(tx) = self.shutdown_senders.remove(id) {
            let _ = tx.send(());
            self.triggers.remove(id);
            true
        } else {
            false
        }
    }

    pub fn stop_all(&mut self) {
        let ids: Vec<String> = self.triggers.keys().cloned().collect();
        for id in ids {
            self.stop(&id);
        }
    }

    pub fn is_running(&self, id: &str) -> bool {
        self.triggers.contains_key(id)
    }

    pub fn list(&self) -> Vec<TriggerHandle> {
        self.triggers.values().cloned().collect()
    }

    pub fn is_empty(&self) -> bool {
        self.triggers.is_empty()
    }

    pub fn len(&self) -> usize {
        self.triggers.len()
    }
}

pub fn register(evaluator: &mut Evaluator) {
    evaluator.register_native(NativeFunction::new("http_server", http_server_fn));
    evaluator.register_native(NativeFunction::new("cron", cron_fn));
    evaluator.register_native(NativeFunction::new("interval", interval_fn));
    evaluator.register_native(NativeFunction::new("stop", stop_fn));
    evaluator.register_native(NativeFunction::new("stop_all", stop_all_fn));
    evaluator.register_native(NativeFunction::new("running", running_fn));
    evaluator.register_native(NativeFunction::new("triggers", triggers_fn));
}

pub async fn has_active_triggers() -> bool {
    !TRIGGER_REGISTRY.read().await.is_empty()
}

pub async fn wait_for_shutdown() {
    loop {
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
        if TRIGGER_REGISTRY.read().await.is_empty() {
            break;
        }
    }
}

fn handle_to_value(handle: &TriggerHandle) -> Value {
    let mut map = HashMap::new();
    map.insert("id".to_string(), Value::String(Arc::new(handle.id.clone())));

    match &handle.trigger_type {
        TriggerType::Http { port, host, routes } => {
            map.insert("type".to_string(), Value::String(Arc::new("http".to_string())));
            map.insert("port".to_string(), Value::Int(*port as i64));
            map.insert("host".to_string(), Value::String(Arc::new(host.clone())));
            let route_values: Vec<Value> = routes
                .iter()
                .map(|r| Value::String(Arc::new(r.clone())))
                .collect();
            map.insert(
                "routes".to_string(),
                Value::List(Arc::new(RwLock::new(route_values))),
            );
        }
        TriggerType::Cron { schedule } => {
            map.insert("type".to_string(), Value::String(Arc::new("cron".to_string())));
            map.insert("schedule".to_string(), Value::String(Arc::new(schedule.clone())));
        }
        TriggerType::Interval { seconds } => {
            map.insert("type".to_string(), Value::String(Arc::new("interval".to_string())));
            map.insert("seconds".to_string(), Value::Int(*seconds as i64));
        }
    }

    Value::Dict(Arc::new(RwLock::new(map)))
}

async fn http_server_fn(args: Vec<Value>, kwargs: HashMap<String, Value>) -> Result<Value> {
    if args.len() < 2 {
        return Err(BlueprintError::ArgumentError {
            message: "http_server() requires at least 2 arguments (port, routes)".into(),
        });
    }

    let port = args[0].as_int()? as u16;
    let routes_value = &args[1];

    let host = kwargs
        .get("host")
        .map(|v| v.as_string())
        .transpose()?
        .unwrap_or_else(|| "0.0.0.0".to_string());

    let routes_dict = match routes_value {
        Value::Dict(d) => d.read().await.clone(),
        _ => {
            return Err(BlueprintError::TypeError {
                expected: "dict".into(),
                actual: routes_value.type_name().into(),
            })
        }
    };

    let id = format!("http-{}", Uuid::new_v4().to_string().split('-').next().unwrap());
    let running = Arc::new(RwLock::new(true));

    let mut router = Router::new();
    let mut route_list = Vec::new();

    for (route_key, handler) in routes_dict.iter() {
        let parts: Vec<&str> = route_key.splitn(2, ' ').collect();
        if parts.len() != 2 {
            return Err(BlueprintError::ArgumentError {
                message: format!(
                    "Invalid route format '{}'. Expected 'METHOD /path'",
                    route_key
                ),
            });
        }

        let method = parts[0].to_uppercase();
        let path = parts[1].to_string();
        let handler_clone = handler.clone();

        route_list.push(route_key.clone());

        let handler_fn = move |req: Request<Body>| {
            let handler = handler_clone.clone();
            async move { execute_http_handler(handler, req).await }
        };

        router = match method.as_str() {
            "GET" => router.route(&path, get(handler_fn)),
            "POST" => router.route(&path, post(handler_fn)),
            "PUT" => router.route(&path, put(handler_fn)),
            "DELETE" => router.route(&path, delete(handler_fn)),
            "PATCH" => router.route(&path, patch(handler_fn)),
            "HEAD" => router.route(&path, head(handler_fn)),
            _ => {
                return Err(BlueprintError::ArgumentError {
                    message: format!("Unsupported HTTP method: {}", method),
                })
            }
        };
    }

    let addr: SocketAddr = format!("{}:{}", host, port)
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
            routes: route_list,
        },
        running: running.clone(),
    };

    TRIGGER_REGISTRY
        .write()
        .await
        .register(handle.clone(), Some(shutdown_tx));

    let listener = tokio::net::TcpListener::bind(addr).await.map_err(|e| {
        BlueprintError::IoError {
            path: format!("{}:{}", host, port),
            message: e.to_string(),
        }
    })?;

    let id_clone = id.clone();
    tokio::spawn(async move {
        axum::serve(listener, router)
            .with_graceful_shutdown(async {
                let _ = shutdown_rx.await;
            })
            .await
            .ok();
        TRIGGER_REGISTRY.write().await.triggers.remove(&id_clone);
    });

    Ok(handle_to_value(&handle))
}

async fn execute_http_handler(handler: Value, req: Request<Body>) -> impl IntoResponse {
    let method = req.method().to_string();
    let path = req.uri().path().to_string();
    let query = req.uri().query().unwrap_or("").to_string();

    let mut headers_map = HashMap::new();
    for (name, value) in req.headers().iter() {
        if let Ok(v) = value.to_str() {
            headers_map.insert(
                name.to_string().to_lowercase(),
                Value::String(Arc::new(v.to_string())),
            );
        }
    }

    let body_bytes = axum::body::to_bytes(req.into_body(), usize::MAX)
        .await
        .unwrap_or_default();
    let body_str = String::from_utf8_lossy(&body_bytes).to_string();

    let mut request_dict = HashMap::new();
    request_dict.insert("method".to_string(), Value::String(Arc::new(method)));
    request_dict.insert("path".to_string(), Value::String(Arc::new(path)));
    request_dict.insert("query".to_string(), Value::String(Arc::new(query)));
    request_dict.insert(
        "headers".to_string(),
        Value::Dict(Arc::new(RwLock::new(headers_map))),
    );
    request_dict.insert("body".to_string(), Value::String(Arc::new(body_str)));

    let request_value = Value::Dict(Arc::new(RwLock::new(request_dict)));

    let result = match &handler {
        Value::Lambda(lambda) => {
            let evaluator = Evaluator::new();
            evaluator
                .call_lambda_public(lambda, vec![request_value], HashMap::new())
                .await
        }
        Value::Function(func) => {
            let evaluator = Evaluator::new();
            evaluator
                .call_function_public(func, vec![request_value], HashMap::new())
                .await
        }
        Value::NativeFunction(native) => native.call(vec![request_value], HashMap::new()).await,
        _ => Err(BlueprintError::TypeError {
            expected: "callable".into(),
            actual: handler.type_name().into(),
        }),
    };

    match result {
        Ok(response) => build_http_response(response).await,
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Handler error: {}", e),
        )
            .into_response(),
    }
}

async fn build_http_response(value: Value) -> axum::response::Response {
    match value {
        Value::String(s) => (StatusCode::OK, s.to_string()).into_response(),
        Value::Dict(d) => {
            let dict = d.read().await;

            let has_status = dict.contains_key("status");
            let has_body = dict.contains_key("body");

            if has_status || has_body {
                let status = dict
                    .get("status")
                    .and_then(|v| v.as_int().ok())
                    .unwrap_or(200) as u16;

                let status_code = StatusCode::from_u16(status).unwrap_or(StatusCode::OK);

                let body = match dict.get("body") {
                    Some(Value::String(s)) => s.to_string(),
                    Some(v) => serde_json::to_string(&value_to_json(v).await).unwrap_or_default(),
                    None => String::new(),
                };

                (status_code, body).into_response()
            } else {
                drop(dict);
                let json = serde_json::to_string(&value_to_json(&Value::Dict(d)).await).unwrap_or_default();
                (
                    StatusCode::OK,
                    [("content-type", "application/json")],
                    json,
                )
                    .into_response()
            }
        }
        Value::None => (StatusCode::NO_CONTENT, "").into_response(),
        _ => {
            let json = serde_json::to_string(&value_to_json(&value).await).unwrap_or_default();
            (
                StatusCode::OK,
                [("content-type", "application/json")],
                json,
            )
                .into_response()
        }
    }
}

#[async_recursion::async_recursion]
async fn value_to_json(value: &Value) -> serde_json::Value {
    match value {
        Value::None => serde_json::Value::Null,
        Value::Bool(b) => serde_json::Value::Bool(*b),
        Value::Int(i) => serde_json::Value::Number((*i).into()),
        Value::Float(f) => serde_json::Number::from_f64(*f)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        Value::String(s) => serde_json::Value::String(s.to_string()),
        Value::List(l) => {
            let items = l.read().await;
            let mut arr = Vec::new();
            for item in items.iter() {
                arr.push(value_to_json(item).await);
            }
            serde_json::Value::Array(arr)
        }
        Value::Dict(d) => {
            let items = d.read().await;
            let mut map = serde_json::Map::new();
            for (k, v) in items.iter() {
                map.insert(k.clone(), value_to_json(v).await);
            }
            serde_json::Value::Object(map)
        }
        _ => serde_json::Value::String(value.to_display_string()),
    }
}

async fn cron_fn(args: Vec<Value>, _kwargs: HashMap<String, Value>) -> Result<Value> {
    if args.len() < 2 {
        return Err(BlueprintError::ArgumentError {
            message: "cron() requires 2 arguments (schedule, handler)".into(),
        });
    }

    let schedule = args[0].as_string()?;
    let handler = args[1].clone();

    let id = format!("cron-{}", Uuid::new_v4().to_string().split('-').next().unwrap());
    let running = Arc::new(RwLock::new(true));

    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();

    let handle = TriggerHandle {
        id: id.clone(),
        trigger_type: TriggerType::Cron {
            schedule: schedule.clone(),
        },
        running: running.clone(),
    };

    TRIGGER_REGISTRY
        .write()
        .await
        .register(handle.clone(), Some(shutdown_tx));

    let mut sched = JobScheduler::new().await.map_err(|e| BlueprintError::InternalError {
        message: format!("Failed to create scheduler: {}", e),
    })?;

    let handler_clone = handler.clone();
    let id_clone = id.clone();

    let job = Job::new_async(schedule.as_str(), move |_uuid, _lock| {
        let handler = handler_clone.clone();
        Box::pin(async move {
            let _ = execute_trigger_handler(handler).await;
        })
    })
    .map_err(|e| BlueprintError::ArgumentError {
        message: format!("Invalid cron schedule '{}': {}", schedule, e),
    })?;

    sched.add(job).await.map_err(|e| BlueprintError::InternalError {
        message: format!("Failed to add cron job: {}", e),
    })?;

    sched.start().await.map_err(|e| BlueprintError::InternalError {
        message: format!("Failed to start scheduler: {}", e),
    })?;

    tokio::spawn(async move {
        let _ = shutdown_rx.await;
        sched.shutdown().await.ok();
        TRIGGER_REGISTRY.write().await.triggers.remove(&id_clone);
    });

    Ok(handle_to_value(&handle))
}

async fn interval_fn(args: Vec<Value>, _kwargs: HashMap<String, Value>) -> Result<Value> {
    if args.len() < 2 {
        return Err(BlueprintError::ArgumentError {
            message: "interval() requires 2 arguments (seconds, handler)".into(),
        });
    }

    let seconds = args[0].as_int()? as u64;
    let handler = args[1].clone();

    let id = format!("interval-{}", Uuid::new_v4().to_string().split('-').next().unwrap());
    let running = Arc::new(RwLock::new(true));

    let (shutdown_tx, mut shutdown_rx) = oneshot::channel::<()>();

    let handle = TriggerHandle {
        id: id.clone(),
        trigger_type: TriggerType::Interval { seconds },
        running: running.clone(),
    };

    TRIGGER_REGISTRY
        .write()
        .await
        .register(handle.clone(), Some(shutdown_tx));

    let id_clone = id.clone();

    tokio::spawn(async move {
        let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(seconds));
        interval.tick().await;

        loop {
            tokio::select! {
                _ = interval.tick() => {
                    let _ = execute_trigger_handler(handler.clone()).await;
                }
                _ = &mut shutdown_rx => {
                    break;
                }
            }
        }
        TRIGGER_REGISTRY.write().await.triggers.remove(&id_clone);
    });

    Ok(handle_to_value(&handle))
}

async fn execute_trigger_handler(handler: Value) -> Result<Value> {
    match &handler {
        Value::Lambda(lambda) => {
            let evaluator = Evaluator::new();
            evaluator
                .call_lambda_public(lambda, vec![], HashMap::new())
                .await
        }
        Value::Function(func) => {
            let evaluator = Evaluator::new();
            evaluator
                .call_function_public(func, vec![], HashMap::new())
                .await
        }
        Value::NativeFunction(native) => native.call(vec![], HashMap::new()).await,
        _ => Err(BlueprintError::TypeError {
            expected: "callable".into(),
            actual: handler.type_name().into(),
        }),
    }
}

async fn stop_fn(args: Vec<Value>, _kwargs: HashMap<String, Value>) -> Result<Value> {
    if args.is_empty() {
        return Err(BlueprintError::ArgumentError {
            message: "stop() requires at least 1 argument".into(),
        });
    }

    let mut handles_to_stop = Vec::new();

    for arg in args {
        collect_handles(&arg, &mut handles_to_stop).await?;
    }

    let mut registry = TRIGGER_REGISTRY.write().await;
    for id in handles_to_stop {
        registry.stop(&id);
    }

    Ok(Value::None)
}

async fn collect_handles(value: &Value, handles: &mut Vec<String>) -> Result<()> {
    match value {
        Value::Dict(d) => {
            let dict = d.read().await;
            if let Some(Value::String(id)) = dict.get("id") {
                handles.push(id.to_string());
            }
        }
        Value::List(l) => {
            let list = l.read().await;
            for item in list.iter() {
                Box::pin(collect_handles(item, handles)).await?;
            }
        }
        _ => {
            return Err(BlueprintError::TypeError {
                expected: "handle or list of handles".into(),
                actual: value.type_name().into(),
            })
        }
    }
    Ok(())
}

async fn stop_all_fn(_args: Vec<Value>, _kwargs: HashMap<String, Value>) -> Result<Value> {
    TRIGGER_REGISTRY.write().await.stop_all();
    Ok(Value::None)
}

async fn running_fn(args: Vec<Value>, _kwargs: HashMap<String, Value>) -> Result<Value> {
    if args.len() != 1 {
        return Err(BlueprintError::ArgumentError {
            message: "running() requires exactly 1 argument".into(),
        });
    }

    let handle = &args[0];
    match handle {
        Value::Dict(d) => {
            let dict = d.read().await;
            if let Some(Value::String(id)) = dict.get("id") {
                let is_running = TRIGGER_REGISTRY.read().await.is_running(id);
                Ok(Value::Bool(is_running))
            } else {
                Err(BlueprintError::ArgumentError {
                    message: "Invalid handle: missing 'id' field".into(),
                })
            }
        }
        _ => Err(BlueprintError::TypeError {
            expected: "handle".into(),
            actual: handle.type_name().into(),
        }),
    }
}

async fn triggers_fn(_args: Vec<Value>, _kwargs: HashMap<String, Value>) -> Result<Value> {
    let registry = TRIGGER_REGISTRY.read().await;
    let handles: Vec<Value> = registry.list().iter().map(handle_to_value).collect();
    Ok(Value::List(Arc::new(RwLock::new(handles))))
}
