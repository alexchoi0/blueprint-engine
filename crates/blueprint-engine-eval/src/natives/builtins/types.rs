use std::collections::HashMap;
use std::sync::Arc;

use indexmap::{IndexMap, IndexSet};
use tokio::sync::{mpsc, RwLock};

use blueprint_engine_core::{BlueprintError, Generator, GeneratorMessage, Result, Value};

pub async fn to_str(args: Vec<Value>, _kwargs: HashMap<String, Value>) -> Result<Value> {
    if args.len() != 1 {
        return Err(BlueprintError::ArgumentError {
            message: format!("str() takes exactly 1 argument ({} given)", args.len()),
        });
    }

    Ok(Value::String(Arc::new(args[0].to_display_string())))
}

pub async fn to_int(args: Vec<Value>, _kwargs: HashMap<String, Value>) -> Result<Value> {
    if args.is_empty() || args.len() > 2 {
        return Err(BlueprintError::ArgumentError {
            message: format!("int() takes 1 or 2 arguments ({} given)", args.len()),
        });
    }

    let base = if args.len() == 2 {
        args[1].as_int()? as u32
    } else {
        10
    };

    match &args[0] {
        Value::Int(i) => Ok(Value::Int(*i)),
        Value::Float(f) => Ok(Value::Int(*f as i64)),
        Value::String(s) => {
            let s = s.trim();
            let result = if base == 10 {
                s.parse::<i64>()
            } else {
                i64::from_str_radix(s.trim_start_matches("0x").trim_start_matches("0X"), base)
            };
            result.map(Value::Int).map_err(|_| BlueprintError::ValueError {
                message: format!("invalid literal for int() with base {}: '{}'", base, s),
            })
        }
        Value::Bool(b) => Ok(Value::Int(if *b { 1 } else { 0 })),
        other => Err(BlueprintError::TypeError {
            expected: "int, float, string, or bool".into(),
            actual: other.type_name().into(),
        }),
    }
}

pub async fn to_float(args: Vec<Value>, _kwargs: HashMap<String, Value>) -> Result<Value> {
    if args.len() != 1 {
        return Err(BlueprintError::ArgumentError {
            message: format!("float() takes exactly 1 argument ({} given)", args.len()),
        });
    }

    match &args[0] {
        Value::Int(i) => Ok(Value::Float(*i as f64)),
        Value::Float(f) => Ok(Value::Float(*f)),
        Value::String(s) => s
            .trim()
            .parse::<f64>()
            .map(Value::Float)
            .map_err(|_| BlueprintError::ValueError {
                message: format!("could not convert string to float: '{}'", s),
            }),
        other => Err(BlueprintError::TypeError {
            expected: "int, float, or string".into(),
            actual: other.type_name().into(),
        }),
    }
}

pub async fn to_bool(args: Vec<Value>, _kwargs: HashMap<String, Value>) -> Result<Value> {
    if args.len() != 1 {
        return Err(BlueprintError::ArgumentError {
            message: format!("bool() takes exactly 1 argument ({} given)", args.len()),
        });
    }

    Ok(Value::Bool(args[0].is_truthy()))
}

pub async fn to_list(args: Vec<Value>, _kwargs: HashMap<String, Value>) -> Result<Value> {
    if args.is_empty() {
        return Ok(Value::List(Arc::new(RwLock::new(vec![]))));
    }

    if args.len() != 1 {
        return Err(BlueprintError::ArgumentError {
            message: format!("list() takes at most 1 argument ({} given)", args.len()),
        });
    }

    let items = match &args[0] {
        Value::List(l) => l.read().await.clone(),
        Value::Tuple(t) => t.as_ref().clone(),
        Value::String(s) => s.chars().map(|c| Value::String(Arc::new(c.to_string()))).collect(),
        Value::Dict(d) => d.read().await.keys().map(|k| Value::String(Arc::new(k.clone()))).collect(),
        Value::Set(s) => s.read().await.iter().cloned().collect(),
        Value::Generator(gen) => {
            let mut items = Vec::new();
            while let Some(item) = gen.next().await {
                items.push(item);
            }
            items
        }
        Value::Iterator(iter) => {
            let mut items = Vec::new();
            while let Some(item) = iter.next().await {
                items.push(item);
            }
            items
        }
        other => {
            return Err(BlueprintError::TypeError {
                expected: "iterable".into(),
                actual: other.type_name().into(),
            })
        }
    };

    Ok(Value::List(Arc::new(RwLock::new(items))))
}

pub async fn to_dict(args: Vec<Value>, _kwargs: HashMap<String, Value>) -> Result<Value> {
    if args.is_empty() {
        return Ok(Value::Dict(Arc::new(RwLock::new(IndexMap::new()))));
    }

    if args.len() != 1 {
        return Err(BlueprintError::ArgumentError {
            message: format!("dict() takes at most 1 argument ({} given)", args.len()),
        });
    }

    match &args[0] {
        Value::Dict(d) => Ok(Value::Dict(Arc::new(RwLock::new(d.read().await.clone())))),
        Value::List(l) => {
            let items = l.read().await;
            let mut map = IndexMap::new();
            for item in items.iter() {
                match item {
                    Value::List(pair_list) => {
                        let pair = pair_list.read().await;
                        if pair.len() != 2 {
                            return Err(BlueprintError::ValueError {
                                message: "dict() argument must be iterable of key-value pairs".into(),
                            });
                        }
                        let key = match &pair[0] {
                            Value::String(s) => s.as_ref().clone(),
                            other => other.to_display_string(),
                        };
                        map.insert(key, pair[1].clone());
                    }
                    Value::Tuple(pair) => {
                        if pair.len() != 2 {
                            return Err(BlueprintError::ValueError {
                                message: "dict() argument must be iterable of key-value pairs".into(),
                            });
                        }
                        let key = match &pair[0] {
                            Value::String(s) => s.as_ref().clone(),
                            other => other.to_display_string(),
                        };
                        map.insert(key, pair[1].clone());
                    }
                    _ => {
                        return Err(BlueprintError::ValueError {
                            message: "dict() argument must be iterable of key-value pairs".into(),
                        })
                    }
                }
            }
            Ok(Value::Dict(Arc::new(RwLock::new(map))))
        }
        other => Err(BlueprintError::TypeError {
            expected: "dict or iterable of pairs".into(),
            actual: other.type_name().into(),
        }),
    }
}

pub async fn to_tuple(args: Vec<Value>, _kwargs: HashMap<String, Value>) -> Result<Value> {
    if args.is_empty() {
        return Ok(Value::Tuple(Arc::new(vec![])));
    }

    if args.len() != 1 {
        return Err(BlueprintError::ArgumentError {
            message: format!("tuple() takes at most 1 argument ({} given)", args.len()),
        });
    }

    let items = match &args[0] {
        Value::Tuple(t) => t.as_ref().clone(),
        Value::List(l) => l.read().await.clone(),
        Value::String(s) => s.chars().map(|c| Value::String(Arc::new(c.to_string()))).collect(),
        other => {
            return Err(BlueprintError::TypeError {
                expected: "iterable".into(),
                actual: other.type_name().into(),
            })
        }
    };

    Ok(Value::Tuple(Arc::new(items)))
}

pub async fn to_set(args: Vec<Value>, _kwargs: HashMap<String, Value>) -> Result<Value> {
    if args.is_empty() {
        return Ok(Value::Set(Arc::new(RwLock::new(IndexSet::new()))));
    }

    if args.len() != 1 {
        return Err(BlueprintError::ArgumentError {
            message: format!("set() takes at most 1 argument ({} given)", args.len()),
        });
    }

    let items: IndexSet<Value> = match &args[0] {
        Value::Set(s) => s.read().await.clone(),
        Value::List(l) => l.read().await.iter().cloned().collect(),
        Value::Tuple(t) => t.iter().cloned().collect(),
        Value::String(s) => s.chars().map(|c| Value::String(Arc::new(c.to_string()))).collect(),
        Value::Dict(d) => d.read().await.keys().map(|k| Value::String(Arc::new(k.clone()))).collect(),
        Value::Generator(gen) => {
            let mut items = IndexSet::new();
            while let Some(item) = gen.next().await {
                items.insert(item);
            }
            items
        }
        Value::Iterator(iter) => {
            let mut items = IndexSet::new();
            while let Some(item) = iter.next().await {
                items.insert(item);
            }
            items
        }
        other => {
            return Err(BlueprintError::TypeError {
                expected: "iterable".into(),
                actual: other.type_name().into(),
            })
        }
    };

    Ok(Value::Set(Arc::new(RwLock::new(items))))
}

pub async fn to_iter(args: Vec<Value>, _kwargs: HashMap<String, Value>) -> Result<Value> {
    if args.len() != 1 {
        return Err(BlueprintError::ArgumentError {
            message: format!("iter() takes exactly 1 argument ({} given)", args.len()),
        });
    }

    let iterable = args[0].clone();

    match &iterable {
        Value::Generator(_) | Value::Iterator(_) => Ok(iterable),
        Value::List(_) | Value::Tuple(_) | Value::String(_) | Value::Dict(_) => {
            let (tx, rx) = mpsc::channel::<GeneratorMessage>(1);

            tokio::spawn(async move {
                let _ = iter_generator_task(iterable, tx.clone()).await;
            });

            Ok(Value::Generator(Arc::new(Generator::new(rx, "iter".to_string()))))
        }
        other => Err(BlueprintError::TypeError {
            expected: "iterable".into(),
            actual: other.type_name().into(),
        }),
    }
}

async fn iter_generator_task(
    iterable: Value,
    tx: mpsc::Sender<GeneratorMessage>,
) -> Result<()> {
    match iterable {
        Value::List(l) => {
            let items = l.read().await.clone();
            for item in items {
                let (resume_tx, resume_rx) = tokio::sync::oneshot::channel();
                if tx.send(GeneratorMessage::Yielded(item, resume_tx)).await.is_err() {
                    break;
                }
                let _ = resume_rx.await;
            }
        }
        Value::Tuple(t) => {
            for item in t.iter().cloned() {
                let (resume_tx, resume_rx) = tokio::sync::oneshot::channel();
                if tx.send(GeneratorMessage::Yielded(item, resume_tx)).await.is_err() {
                    break;
                }
                let _ = resume_rx.await;
            }
        }
        Value::String(s) => {
            for c in s.chars() {
                let item = Value::String(Arc::new(c.to_string()));
                let (resume_tx, resume_rx) = tokio::sync::oneshot::channel();
                if tx.send(GeneratorMessage::Yielded(item, resume_tx)).await.is_err() {
                    break;
                }
                let _ = resume_rx.await;
            }
        }
        Value::Dict(d) => {
            let keys: Vec<String> = d.read().await.keys().cloned().collect();
            for key in keys {
                let item = Value::String(Arc::new(key));
                let (resume_tx, resume_rx) = tokio::sync::oneshot::channel();
                if tx.send(GeneratorMessage::Yielded(item, resume_tx)).await.is_err() {
                    break;
                }
                let _ = resume_rx.await;
            }
        }
        _ => {}
    }
    let _ = tx.send(GeneratorMessage::Complete).await;
    Ok(())
}
