use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{mpsc, RwLock};

use blueprint_core::{BlueprintError, Generator, GeneratorMessage, NativeFunction, Result, Value};

use crate::eval::Evaluator;
use crate::scope::{Scope, ScopeKind};

pub fn register(evaluator: &mut Evaluator) {
    evaluator.register_native(NativeFunction::new("len", len));
    evaluator.register_native(NativeFunction::new("str", to_str));
    evaluator.register_native(NativeFunction::new("int", to_int));
    evaluator.register_native(NativeFunction::new("float", to_float));
    evaluator.register_native(NativeFunction::new("bool", to_bool));
    evaluator.register_native(NativeFunction::new("list", to_list));
    evaluator.register_native(NativeFunction::new("dict", to_dict));
    evaluator.register_native(NativeFunction::new("tuple", to_tuple));
    evaluator.register_native(NativeFunction::new("iter", to_iter));
    evaluator.register_native(NativeFunction::new("range", range));
    evaluator.register_native(NativeFunction::new("map", map_fn));
    evaluator.register_native(NativeFunction::new("filter", filter_fn));
    evaluator.register_native(NativeFunction::new("enumerate", enumerate));
    evaluator.register_native(NativeFunction::new("zip", zip));
    evaluator.register_native(NativeFunction::new("sorted", sorted));
    evaluator.register_native(NativeFunction::new("reversed", reversed));
    evaluator.register_native(NativeFunction::new("min", min));
    evaluator.register_native(NativeFunction::new("max", max));
    evaluator.register_native(NativeFunction::new("sum", sum));
    evaluator.register_native(NativeFunction::new("abs", abs));
    evaluator.register_native(NativeFunction::new("all", all));
    evaluator.register_native(NativeFunction::new("any", any));
    evaluator.register_native(NativeFunction::new("type", type_of));
    evaluator.register_native(NativeFunction::new("hasattr", hasattr));
    evaluator.register_native(NativeFunction::new("getattr", getattr));
    evaluator.register_native(NativeFunction::new("repr", repr));
    evaluator.register_native(NativeFunction::new("fail", fail));
    evaluator.register_native(NativeFunction::new("assert_", assert_));
    evaluator.register_native(NativeFunction::new("assert_eq", assert_eq));
    evaluator.register_native(NativeFunction::new("assert_contains", assert_contains));
}

async fn len(args: Vec<Value>, _kwargs: HashMap<String, Value>) -> Result<Value> {
    if args.len() != 1 {
        return Err(BlueprintError::ArgumentError {
            message: format!("len() takes exactly 1 argument ({} given)", args.len()),
        });
    }

    let length = match &args[0] {
        Value::String(s) => s.chars().count() as i64,
        Value::List(l) => l.read().await.len() as i64,
        Value::Dict(d) => d.read().await.len() as i64,
        Value::Tuple(t) => t.len() as i64,
        other => {
            return Err(BlueprintError::TypeError {
                expected: "string, list, dict, or tuple".into(),
                actual: other.type_name().into(),
            })
        }
    };

    Ok(Value::Int(length))
}

async fn to_str(args: Vec<Value>, _kwargs: HashMap<String, Value>) -> Result<Value> {
    if args.len() != 1 {
        return Err(BlueprintError::ArgumentError {
            message: format!("str() takes exactly 1 argument ({} given)", args.len()),
        });
    }

    Ok(Value::String(Arc::new(args[0].to_display_string())))
}

async fn to_int(args: Vec<Value>, _kwargs: HashMap<String, Value>) -> Result<Value> {
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

async fn to_float(args: Vec<Value>, _kwargs: HashMap<String, Value>) -> Result<Value> {
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

async fn to_bool(args: Vec<Value>, _kwargs: HashMap<String, Value>) -> Result<Value> {
    if args.len() != 1 {
        return Err(BlueprintError::ArgumentError {
            message: format!("bool() takes exactly 1 argument ({} given)", args.len()),
        });
    }

    Ok(Value::Bool(args[0].is_truthy()))
}

async fn to_list(args: Vec<Value>, _kwargs: HashMap<String, Value>) -> Result<Value> {
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

async fn to_dict(args: Vec<Value>, _kwargs: HashMap<String, Value>) -> Result<Value> {
    if args.is_empty() {
        return Ok(Value::Dict(Arc::new(RwLock::new(HashMap::new()))));
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
            let mut map = HashMap::new();
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

async fn to_tuple(args: Vec<Value>, _kwargs: HashMap<String, Value>) -> Result<Value> {
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

async fn to_iter(args: Vec<Value>, _kwargs: HashMap<String, Value>) -> Result<Value> {
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

async fn range(args: Vec<Value>, _kwargs: HashMap<String, Value>) -> Result<Value> {
    let (start, end, step) = match args.len() {
        1 => (0, args[0].as_int()?, 1),
        2 => (args[0].as_int()?, args[1].as_int()?, 1),
        3 => (args[0].as_int()?, args[1].as_int()?, args[2].as_int()?),
        n => {
            return Err(BlueprintError::ArgumentError {
                message: format!("range() takes 1 to 3 arguments ({} given)", n),
            })
        }
    };

    if step == 0 {
        return Err(BlueprintError::ValueError {
            message: "range() step argument must not be zero".into(),
        });
    }

    let mut result = Vec::new();
    let mut i = start;

    if step > 0 {
        while i < end {
            result.push(Value::Int(i));
            i += step;
        }
    } else {
        while i > end {
            result.push(Value::Int(i));
            i += step;
        }
    }

    Ok(Value::List(Arc::new(RwLock::new(result))))
}

async fn map_fn(args: Vec<Value>, _kwargs: HashMap<String, Value>) -> Result<Value> {
    if args.len() != 2 {
        return Err(BlueprintError::ArgumentError {
            message: format!("map() takes exactly 2 arguments ({} given)", args.len()),
        });
    }

    let func = args[0].clone();
    let iterable = args[1].clone();

    let (tx, rx) = mpsc::channel::<GeneratorMessage>(1);

    tokio::spawn(async move {
        let result = map_generator_task(func, iterable, tx.clone()).await;
        if result.is_err() {
            let _ = tx.send(GeneratorMessage::Complete).await;
        }
    });

    Ok(Value::Generator(Arc::new(Generator::new(rx, "map".to_string()))))
}

async fn map_generator_task(
    func: Value,
    iterable: Value,
    tx: mpsc::Sender<GeneratorMessage>,
) -> Result<()> {
    match iterable {
        Value::Generator(gen) => {
            while let Some(item) = gen.next().await {
                let result = call_func(&func, vec![item]).await?;
                let (resume_tx, resume_rx) = tokio::sync::oneshot::channel();
                if tx.send(GeneratorMessage::Yielded(result, resume_tx)).await.is_err() {
                    break;
                }
                let _ = resume_rx.await;
            }
        }
        Value::Iterator(iter) => {
            while let Some(item) = iter.next().await {
                let result = call_func(&func, vec![item]).await?;
                let (resume_tx, resume_rx) = tokio::sync::oneshot::channel();
                if tx.send(GeneratorMessage::Yielded(result, resume_tx)).await.is_err() {
                    break;
                }
                let _ = resume_rx.await;
            }
        }
        Value::List(l) => {
            let items = l.read().await.clone();
            for item in items {
                let result = call_func(&func, vec![item]).await?;
                let (resume_tx, resume_rx) = tokio::sync::oneshot::channel();
                if tx.send(GeneratorMessage::Yielded(result, resume_tx)).await.is_err() {
                    break;
                }
                let _ = resume_rx.await;
            }
        }
        Value::Tuple(t) => {
            for item in t.iter().cloned() {
                let result = call_func(&func, vec![item]).await?;
                let (resume_tx, resume_rx) = tokio::sync::oneshot::channel();
                if tx.send(GeneratorMessage::Yielded(result, resume_tx)).await.is_err() {
                    break;
                }
                let _ = resume_rx.await;
            }
        }
        Value::String(s) => {
            for c in s.chars() {
                let item = Value::String(Arc::new(c.to_string()));
                let result = call_func(&func, vec![item]).await?;
                let (resume_tx, resume_rx) = tokio::sync::oneshot::channel();
                if tx.send(GeneratorMessage::Yielded(result, resume_tx)).await.is_err() {
                    break;
                }
                let _ = resume_rx.await;
            }
        }
        other => {
            return Err(BlueprintError::TypeError {
                expected: "iterable".into(),
                actual: other.type_name().into(),
            });
        }
    }
    let _ = tx.send(GeneratorMessage::Complete).await;
    Ok(())
}

async fn filter_fn(args: Vec<Value>, _kwargs: HashMap<String, Value>) -> Result<Value> {
    if args.len() != 2 {
        return Err(BlueprintError::ArgumentError {
            message: format!("filter() takes exactly 2 arguments ({} given)", args.len()),
        });
    }

    let func = args[0].clone();
    let iterable = args[1].clone();

    let (tx, rx) = mpsc::channel::<GeneratorMessage>(1);

    tokio::spawn(async move {
        let result = filter_generator_task(func, iterable, tx.clone()).await;
        if result.is_err() {
            let _ = tx.send(GeneratorMessage::Complete).await;
        }
    });

    Ok(Value::Generator(Arc::new(Generator::new(rx, "filter".to_string()))))
}

async fn filter_generator_task(
    func: Value,
    iterable: Value,
    tx: mpsc::Sender<GeneratorMessage>,
) -> Result<()> {
    match iterable {
        Value::Generator(gen) => {
            while let Some(item) = gen.next().await {
                let predicate = if func.is_none() {
                    item.is_truthy_async().await
                } else {
                    call_func(&func, vec![item.clone()]).await?.is_truthy_async().await
                };
                if predicate {
                    let (resume_tx, resume_rx) = tokio::sync::oneshot::channel();
                    if tx.send(GeneratorMessage::Yielded(item, resume_tx)).await.is_err() {
                        break;
                    }
                    let _ = resume_rx.await;
                }
            }
        }
        Value::Iterator(iter) => {
            while let Some(item) = iter.next().await {
                let predicate = if func.is_none() {
                    item.is_truthy_async().await
                } else {
                    call_func(&func, vec![item.clone()]).await?.is_truthy_async().await
                };
                if predicate {
                    let (resume_tx, resume_rx) = tokio::sync::oneshot::channel();
                    if tx.send(GeneratorMessage::Yielded(item, resume_tx)).await.is_err() {
                        break;
                    }
                    let _ = resume_rx.await;
                }
            }
        }
        Value::List(l) => {
            let items = l.read().await.clone();
            for item in items {
                let predicate = if func.is_none() {
                    item.is_truthy_async().await
                } else {
                    call_func(&func, vec![item.clone()]).await?.is_truthy_async().await
                };
                if predicate {
                    let (resume_tx, resume_rx) = tokio::sync::oneshot::channel();
                    if tx.send(GeneratorMessage::Yielded(item, resume_tx)).await.is_err() {
                        break;
                    }
                    let _ = resume_rx.await;
                }
            }
        }
        Value::Tuple(t) => {
            for item in t.iter().cloned() {
                let predicate = if func.is_none() {
                    item.is_truthy_async().await
                } else {
                    call_func(&func, vec![item.clone()]).await?.is_truthy_async().await
                };
                if predicate {
                    let (resume_tx, resume_rx) = tokio::sync::oneshot::channel();
                    if tx.send(GeneratorMessage::Yielded(item, resume_tx)).await.is_err() {
                        break;
                    }
                    let _ = resume_rx.await;
                }
            }
        }
        other => {
            return Err(BlueprintError::TypeError {
                expected: "iterable".into(),
                actual: other.type_name().into(),
            });
        }
    }
    let _ = tx.send(GeneratorMessage::Complete).await;
    Ok(())
}

async fn call_func(func: &Value, args: Vec<Value>) -> Result<Value> {
    match func {
        Value::Lambda(lambda) => {
            let body = lambda
                .body
                .downcast_ref::<blueprint_parser::AstExpr>()
                .ok_or_else(|| BlueprintError::InternalError {
                    message: "Invalid lambda body".into(),
                })?;

            let closure_scope = lambda
                .closure
                .as_ref()
                .and_then(|c| c.downcast_ref::<Arc<Scope>>().cloned());

            let base_scope = closure_scope.unwrap_or_else(Scope::new_global);
            let call_scope = Scope::new_child(base_scope, ScopeKind::Function);

            for (i, param) in lambda.params.iter().enumerate() {
                let value = args.get(i).cloned().or_else(|| param.default.clone());
                if let Some(v) = value {
                    call_scope.define(&param.name, v).await;
                }
            }

            let evaluator = Evaluator::new();
            evaluator.eval_expr(body, call_scope).await
        }
        Value::Function(func) => {
            let body = func
                .body
                .downcast_ref::<blueprint_parser::AstStmt>()
                .ok_or_else(|| BlueprintError::InternalError {
                    message: "Invalid function body".into(),
                })?;

            let closure_scope = func
                .closure
                .as_ref()
                .and_then(|c| c.downcast_ref::<Arc<Scope>>().cloned());

            let base_scope = closure_scope.unwrap_or_else(Scope::new_global);
            let call_scope = Scope::new_child(base_scope, ScopeKind::Function);

            for (i, param) in func.params.iter().enumerate() {
                let value = args.get(i).cloned().or_else(|| param.default.clone());
                if let Some(v) = value {
                    call_scope.define(&param.name, v).await;
                }
            }

            let evaluator = Evaluator::new();
            match evaluator.eval_stmt(body, call_scope).await {
                Ok(_) => Ok(Value::None),
                Err(BlueprintError::Return { value }) => Ok((*value).clone()),
                Err(e) => Err(e),
            }
        }
        Value::NativeFunction(native) => native.call(args, HashMap::new()).await,
        _ => Err(BlueprintError::NotCallable {
            type_name: func.type_name().into(),
        }),
    }
}

async fn enumerate(args: Vec<Value>, _kwargs: HashMap<String, Value>) -> Result<Value> {
    if args.is_empty() || args.len() > 2 {
        return Err(BlueprintError::ArgumentError {
            message: format!("enumerate() takes 1 or 2 arguments ({} given)", args.len()),
        });
    }

    let start = if args.len() == 2 { args[1].as_int()? } else { 0 };
    let iterable = args[0].clone();

    match &iterable {
        Value::Generator(_) | Value::Iterator(_) => {
            let (tx, rx) = mpsc::channel::<GeneratorMessage>(1);

            tokio::spawn(async move {
                let _ = enumerate_generator_task(iterable, start, tx.clone()).await;
            });

            Ok(Value::Generator(Arc::new(Generator::new(rx, "enumerate".to_string()))))
        }
        _ => {
            let items = match &iterable {
                Value::List(l) => l.read().await.clone(),
                Value::Tuple(t) => t.as_ref().clone(),
                Value::String(s) => s.chars().map(|c| Value::String(Arc::new(c.to_string()))).collect(),
                other => {
                    return Err(BlueprintError::TypeError {
                        expected: "iterable".into(),
                        actual: other.type_name().into(),
                    })
                }
            };

            let enumerated: Vec<Value> = items
                .into_iter()
                .enumerate()
                .map(|(i, v)| Value::Tuple(Arc::new(vec![Value::Int(start + i as i64), v])))
                .collect();

            Ok(Value::List(Arc::new(RwLock::new(enumerated))))
        }
    }
}

async fn enumerate_generator_task(
    iterable: Value,
    start: i64,
    tx: mpsc::Sender<GeneratorMessage>,
) -> Result<()> {
    let mut idx = start;
    match iterable {
        Value::Generator(gen) => {
            while let Some(item) = gen.next().await {
                let tuple = Value::Tuple(Arc::new(vec![Value::Int(idx), item]));
                let (resume_tx, resume_rx) = tokio::sync::oneshot::channel();
                if tx.send(GeneratorMessage::Yielded(tuple, resume_tx)).await.is_err() {
                    break;
                }
                let _ = resume_rx.await;
                idx += 1;
            }
        }
        Value::Iterator(iter) => {
            while let Some(item) = iter.next().await {
                let tuple = Value::Tuple(Arc::new(vec![Value::Int(idx), item]));
                let (resume_tx, resume_rx) = tokio::sync::oneshot::channel();
                if tx.send(GeneratorMessage::Yielded(tuple, resume_tx)).await.is_err() {
                    break;
                }
                let _ = resume_rx.await;
                idx += 1;
            }
        }
        _ => {}
    }
    let _ = tx.send(GeneratorMessage::Complete).await;
    Ok(())
}

async fn zip(args: Vec<Value>, _kwargs: HashMap<String, Value>) -> Result<Value> {
    if args.is_empty() {
        return Ok(Value::List(Arc::new(RwLock::new(vec![]))));
    }

    let mut iterables: Vec<Vec<Value>> = Vec::new();
    for arg in &args {
        let items = match arg {
            Value::List(l) => l.read().await.clone(),
            Value::Tuple(t) => t.as_ref().clone(),
            Value::String(s) => s.chars().map(|c| Value::String(Arc::new(c.to_string()))).collect(),
            other => {
                return Err(BlueprintError::TypeError {
                    expected: "iterable".into(),
                    actual: other.type_name().into(),
                })
            }
        };
        iterables.push(items);
    }

    let min_len = iterables.iter().map(|i| i.len()).min().unwrap_or(0);

    let zipped: Vec<Value> = (0..min_len)
        .map(|i| {
            Value::Tuple(Arc::new(
                iterables.iter().map(|iter| iter[i].clone()).collect(),
            ))
        })
        .collect();

    Ok(Value::List(Arc::new(RwLock::new(zipped))))
}

async fn sorted(args: Vec<Value>, kwargs: HashMap<String, Value>) -> Result<Value> {
    if args.len() != 1 {
        return Err(BlueprintError::ArgumentError {
            message: format!("sorted() takes exactly 1 argument ({} given)", args.len()),
        });
    }

    let reverse = kwargs
        .get("reverse")
        .map(|v| v.is_truthy())
        .unwrap_or(false);

    let mut items = match &args[0] {
        Value::List(l) => l.read().await.clone(),
        Value::Tuple(t) => t.as_ref().clone(),
        Value::String(s) => s.chars().map(|c| Value::String(Arc::new(c.to_string()))).collect(),
        other => {
            return Err(BlueprintError::TypeError {
                expected: "iterable".into(),
                actual: other.type_name().into(),
            })
        }
    };

    items.sort_by(|a, b| {
        match (a, b) {
            (Value::Int(x), Value::Int(y)) => x.cmp(y),
            (Value::Float(x), Value::Float(y)) => x.partial_cmp(y).unwrap_or(std::cmp::Ordering::Equal),
            (Value::String(x), Value::String(y)) => x.cmp(y),
            _ => std::cmp::Ordering::Equal,
        }
    });

    if reverse {
        items.reverse();
    }

    Ok(Value::List(Arc::new(RwLock::new(items))))
}

async fn reversed(args: Vec<Value>, _kwargs: HashMap<String, Value>) -> Result<Value> {
    if args.len() != 1 {
        return Err(BlueprintError::ArgumentError {
            message: format!("reversed() takes exactly 1 argument ({} given)", args.len()),
        });
    }

    let mut items = match &args[0] {
        Value::List(l) => l.read().await.clone(),
        Value::Tuple(t) => t.as_ref().clone(),
        Value::String(s) => s.chars().map(|c| Value::String(Arc::new(c.to_string()))).collect(),
        other => {
            return Err(BlueprintError::TypeError {
                expected: "iterable".into(),
                actual: other.type_name().into(),
            })
        }
    };

    items.reverse();
    Ok(Value::List(Arc::new(RwLock::new(items))))
}

async fn min(args: Vec<Value>, _kwargs: HashMap<String, Value>) -> Result<Value> {
    if args.is_empty() {
        return Err(BlueprintError::ArgumentError {
            message: "min() requires at least one argument".into(),
        });
    }

    let items = if args.len() == 1 {
        match &args[0] {
            Value::List(l) => l.read().await.clone(),
            Value::Tuple(t) => t.as_ref().clone(),
            _ => args.clone(),
        }
    } else {
        args.clone()
    };

    if items.is_empty() {
        return Err(BlueprintError::ValueError {
            message: "min() argument is an empty sequence".into(),
        });
    }

    let mut min_val = items[0].clone();
    for item in items.iter().skip(1) {
        let is_less = match (&min_val, item) {
            (Value::Int(a), Value::Int(b)) => b < a,
            (Value::Float(a), Value::Float(b)) => b < a,
            (Value::String(a), Value::String(b)) => b < a,
            _ => false,
        };
        if is_less {
            min_val = item.clone();
        }
    }

    Ok(min_val)
}

async fn max(args: Vec<Value>, _kwargs: HashMap<String, Value>) -> Result<Value> {
    if args.is_empty() {
        return Err(BlueprintError::ArgumentError {
            message: "max() requires at least one argument".into(),
        });
    }

    let items = if args.len() == 1 {
        match &args[0] {
            Value::List(l) => l.read().await.clone(),
            Value::Tuple(t) => t.as_ref().clone(),
            _ => args.clone(),
        }
    } else {
        args.clone()
    };

    if items.is_empty() {
        return Err(BlueprintError::ValueError {
            message: "max() argument is an empty sequence".into(),
        });
    }

    let mut max_val = items[0].clone();
    for item in items.iter().skip(1) {
        let is_greater = match (&max_val, item) {
            (Value::Int(a), Value::Int(b)) => b > a,
            (Value::Float(a), Value::Float(b)) => b > a,
            (Value::String(a), Value::String(b)) => b > a,
            _ => false,
        };
        if is_greater {
            max_val = item.clone();
        }
    }

    Ok(max_val)
}

async fn sum(args: Vec<Value>, _kwargs: HashMap<String, Value>) -> Result<Value> {
    if args.is_empty() || args.len() > 2 {
        return Err(BlueprintError::ArgumentError {
            message: format!("sum() takes 1 or 2 arguments ({} given)", args.len()),
        });
    }

    let start = if args.len() == 2 {
        args[1].clone()
    } else {
        Value::Int(0)
    };

    let items = match &args[0] {
        Value::List(l) => l.read().await.clone(),
        Value::Tuple(t) => t.as_ref().clone(),
        other => {
            return Err(BlueprintError::TypeError {
                expected: "iterable".into(),
                actual: other.type_name().into(),
            })
        }
    };

    let mut total = start;
    for item in items {
        total = match (&total, &item) {
            (Value::Int(a), Value::Int(b)) => Value::Int(a + b),
            (Value::Float(a), Value::Float(b)) => Value::Float(a + b),
            (Value::Int(a), Value::Float(b)) => Value::Float(*a as f64 + b),
            (Value::Float(a), Value::Int(b)) => Value::Float(a + *b as f64),
            _ => {
                return Err(BlueprintError::TypeError {
                    expected: "numbers".into(),
                    actual: format!("{} and {}", total.type_name(), item.type_name()),
                })
            }
        };
    }

    Ok(total)
}

async fn abs(args: Vec<Value>, _kwargs: HashMap<String, Value>) -> Result<Value> {
    if args.len() != 1 {
        return Err(BlueprintError::ArgumentError {
            message: format!("abs() takes exactly 1 argument ({} given)", args.len()),
        });
    }

    match &args[0] {
        Value::Int(i) => Ok(Value::Int(i.abs())),
        Value::Float(f) => Ok(Value::Float(f.abs())),
        other => Err(BlueprintError::TypeError {
            expected: "number".into(),
            actual: other.type_name().into(),
        }),
    }
}

async fn all(args: Vec<Value>, _kwargs: HashMap<String, Value>) -> Result<Value> {
    if args.len() != 1 {
        return Err(BlueprintError::ArgumentError {
            message: format!("all() takes exactly 1 argument ({} given)", args.len()),
        });
    }

    let items = match &args[0] {
        Value::List(l) => l.read().await.clone(),
        Value::Tuple(t) => t.as_ref().clone(),
        other => {
            return Err(BlueprintError::TypeError {
                expected: "iterable".into(),
                actual: other.type_name().into(),
            })
        }
    };

    Ok(Value::Bool(items.iter().all(|v| v.is_truthy())))
}

async fn any(args: Vec<Value>, _kwargs: HashMap<String, Value>) -> Result<Value> {
    if args.len() != 1 {
        return Err(BlueprintError::ArgumentError {
            message: format!("any() takes exactly 1 argument ({} given)", args.len()),
        });
    }

    let items = match &args[0] {
        Value::List(l) => l.read().await.clone(),
        Value::Tuple(t) => t.as_ref().clone(),
        other => {
            return Err(BlueprintError::TypeError {
                expected: "iterable".into(),
                actual: other.type_name().into(),
            })
        }
    };

    Ok(Value::Bool(items.iter().any(|v| v.is_truthy())))
}

async fn type_of(args: Vec<Value>, _kwargs: HashMap<String, Value>) -> Result<Value> {
    if args.len() != 1 {
        return Err(BlueprintError::ArgumentError {
            message: format!("type() takes exactly 1 argument ({} given)", args.len()),
        });
    }

    Ok(Value::String(Arc::new(args[0].type_name().to_string())))
}

async fn hasattr(args: Vec<Value>, _kwargs: HashMap<String, Value>) -> Result<Value> {
    if args.len() != 2 {
        return Err(BlueprintError::ArgumentError {
            message: format!("hasattr() takes exactly 2 arguments ({} given)", args.len()),
        });
    }

    let attr_name = args[1].as_string()?;
    Ok(Value::Bool(args[0].has_attr(&attr_name)))
}

async fn getattr(args: Vec<Value>, _kwargs: HashMap<String, Value>) -> Result<Value> {
    if args.len() < 2 || args.len() > 3 {
        return Err(BlueprintError::ArgumentError {
            message: format!("getattr() takes 2 or 3 arguments ({} given)", args.len()),
        });
    }

    let attr_name = args[1].as_string()?;
    match args[0].get_attr(&attr_name) {
        Some(v) => Ok(v),
        None if args.len() == 3 => Ok(args[2].clone()),
        None => Err(BlueprintError::AttributeError {
            type_name: args[0].type_name().to_string(),
            attr: attr_name,
        }),
    }
}

async fn repr(args: Vec<Value>, _kwargs: HashMap<String, Value>) -> Result<Value> {
    if args.len() != 1 {
        return Err(BlueprintError::ArgumentError {
            message: format!("repr() takes exactly 1 argument ({} given)", args.len()),
        });
    }

    Ok(Value::String(Arc::new(args[0].repr())))
}

async fn fail(args: Vec<Value>, _kwargs: HashMap<String, Value>) -> Result<Value> {
    let message = if args.is_empty() {
        "fail".to_string()
    } else {
        args[0].to_display_string()
    };

    Err(BlueprintError::UserError { message })
}

async fn assert_(args: Vec<Value>, _kwargs: HashMap<String, Value>) -> Result<Value> {
    if args.is_empty() || args.len() > 2 {
        return Err(BlueprintError::ArgumentError {
            message: format!("assert_() takes 1 or 2 arguments ({} given)", args.len()),
        });
    }

    if !args[0].is_truthy() {
        let message = if args.len() == 2 {
            args[1].to_display_string()
        } else {
            "assertion failed".to_string()
        };
        return Err(BlueprintError::AssertionError { message });
    }

    Ok(Value::None)
}

async fn assert_eq(args: Vec<Value>, _kwargs: HashMap<String, Value>) -> Result<Value> {
    if args.len() < 2 || args.len() > 3 {
        return Err(BlueprintError::ArgumentError {
            message: format!("assert_eq() takes 2 or 3 arguments ({} given)", args.len()),
        });
    }

    let expected = &args[0];
    let actual = &args[1];

    if !values_equal(expected, actual).await {
        let user_msg = if args.len() == 3 {
            format!("{}\n", args[2].to_display_string())
        } else {
            String::new()
        };
        let diff = generate_diff(expected, actual).await;
        let message = format!(
            "{user_msg}\n  expected: {}\n  actual:   {}\n  diff:\n{diff}",
            expected.repr(),
            actual.repr(),
        );
        return Err(BlueprintError::AssertionError { message });
    }

    Ok(Value::None)
}

async fn assert_contains(args: Vec<Value>, _kwargs: HashMap<String, Value>) -> Result<Value> {
    if args.len() < 2 || args.len() > 3 {
        return Err(BlueprintError::ArgumentError {
            message: format!("assert_contains() takes 2 or 3 arguments ({} given)", args.len()),
        });
    }

    let expected = &args[0];
    let actual = &args[1];

    if !value_contains(expected, actual).await {
        let user_msg = if args.len() == 3 {
            format!("{}\n", args[2].to_display_string())
        } else {
            String::new()
        };
        let diff = generate_contains_diff(expected, actual).await;
        let message = format!(
            "{user_msg}\n  expected: {}\n  actual:   {}\n  diff:\n{diff}",
            expected.repr(),
            actual.repr(),
        );
        return Err(BlueprintError::AssertionError { message });
    }

    Ok(Value::None)
}

async fn values_equal(left: &Value, right: &Value) -> bool {
    match (left, right) {
        (Value::Dict(a), Value::Dict(b)) => {
            let a_map = a.read().await;
            let b_map = b.read().await;
            if a_map.len() != b_map.len() {
                return false;
            }
            for (k, v) in a_map.iter() {
                match b_map.get(k) {
                    Some(bv) => {
                        if !Box::pin(values_equal(v, bv)).await {
                            return false;
                        }
                    }
                    None => return false,
                }
            }
            true
        }
        (Value::List(a), Value::List(b)) => {
            let a_list = a.read().await;
            let b_list = b.read().await;
            if a_list.len() != b_list.len() {
                return false;
            }
            for (av, bv) in a_list.iter().zip(b_list.iter()) {
                if !Box::pin(values_equal(av, bv)).await {
                    return false;
                }
            }
            true
        }
        _ => left == right,
    }
}

async fn value_contains(expected: &Value, actual: &Value) -> bool {
    match (expected, actual) {
        (Value::Dict(exp), Value::Dict(act)) => {
            let exp_map = exp.read().await;
            let act_map = act.read().await;
            for (k, v) in exp_map.iter() {
                match act_map.get(k) {
                    Some(av) => {
                        if !Box::pin(value_contains(v, av)).await {
                            return false;
                        }
                    }
                    None => return false,
                }
            }
            true
        }
        (Value::List(exp), Value::List(act)) => {
            let exp_list = exp.read().await;
            let act_list = act.read().await;
            if exp_list.len() > act_list.len() {
                return false;
            }
            for (ev, av) in exp_list.iter().zip(act_list.iter()) {
                if !Box::pin(value_contains(ev, av)).await {
                    return false;
                }
            }
            true
        }
        _ => expected == actual,
    }
}

async fn generate_diff(expected: &Value, actual: &Value) -> String {
    let mut lines = Vec::new();
    generate_diff_inner(expected, actual, &mut lines, "").await;
    lines.join("\n")
}

#[async_recursion::async_recursion]
async fn generate_diff_inner(expected: &Value, actual: &Value, lines: &mut Vec<String>, path: &str) {
    match (expected, actual) {
        (Value::Dict(exp), Value::Dict(act)) => {
            let exp_map = exp.read().await;
            let act_map = act.read().await;

            for (k, v) in exp_map.iter() {
                let key_path = if path.is_empty() { k.clone() } else { format!("{}.{}", path, k) };
                match act_map.get(k) {
                    Some(av) => {
                        if !Box::pin(values_equal(v, av)).await {
                            generate_diff_inner(v, av, lines, &key_path).await;
                        }
                    }
                    None => {
                        lines.push(format!("    - {}: {}", key_path, v.repr()));
                    }
                }
            }
            for (k, v) in act_map.iter() {
                if !exp_map.contains_key(k) {
                    let key_path = if path.is_empty() { k.clone() } else { format!("{}.{}", path, k) };
                    lines.push(format!("    + {}: {}", key_path, v.repr()));
                }
            }
        }
        (Value::List(exp), Value::List(act)) => {
            let exp_list = exp.read().await;
            let act_list = act.read().await;
            let max_len = exp_list.len().max(act_list.len());

            for i in 0..max_len {
                let idx_path = if path.is_empty() { format!("[{}]", i) } else { format!("{}[{}]", path, i) };
                match (exp_list.get(i), act_list.get(i)) {
                    (Some(ev), Some(av)) => {
                        if !Box::pin(values_equal(ev, av)).await {
                            generate_diff_inner(ev, av, lines, &idx_path).await;
                        }
                    }
                    (Some(ev), None) => {
                        lines.push(format!("    - {}: {}", idx_path, ev.repr()));
                    }
                    (None, Some(av)) => {
                        lines.push(format!("    + {}: {}", idx_path, av.repr()));
                    }
                    (None, None) => {}
                }
            }
        }
        _ => {
            if path.is_empty() {
                lines.push(format!("    - {}", expected.repr()));
                lines.push(format!("    + {}", actual.repr()));
            } else {
                lines.push(format!("    - {}: {}", path, expected.repr()));
                lines.push(format!("    + {}: {}", path, actual.repr()));
            }
        }
    }
}

async fn generate_contains_diff(expected: &Value, actual: &Value) -> String {
    let mut lines = Vec::new();
    generate_contains_diff_inner(expected, actual, &mut lines, "").await;
    lines.join("\n")
}

#[async_recursion::async_recursion]
async fn generate_contains_diff_inner(expected: &Value, actual: &Value, lines: &mut Vec<String>, path: &str) {
    match (expected, actual) {
        (Value::Dict(exp), Value::Dict(act)) => {
            let exp_map = exp.read().await;
            let act_map = act.read().await;

            for (k, v) in exp_map.iter() {
                let key_path = if path.is_empty() { k.clone() } else { format!("{}.{}", path, k) };
                match act_map.get(k) {
                    Some(av) => {
                        if !Box::pin(value_contains(v, av)).await {
                            generate_contains_diff_inner(v, av, lines, &key_path).await;
                        }
                    }
                    None => {
                        lines.push(format!("    missing key '{}': expected {}", key_path, v.repr()));
                    }
                }
            }
        }
        (Value::List(exp), Value::List(act)) => {
            let exp_list = exp.read().await;
            let act_list = act.read().await;

            for (i, ev) in exp_list.iter().enumerate() {
                let idx_path = if path.is_empty() { format!("[{}]", i) } else { format!("{}[{}]", path, i) };
                match act_list.get(i) {
                    Some(av) => {
                        if !Box::pin(value_contains(ev, av)).await {
                            generate_contains_diff_inner(ev, av, lines, &idx_path).await;
                        }
                    }
                    None => {
                        lines.push(format!("    missing {}: expected {}", idx_path, ev.repr()));
                    }
                }
            }
        }
        _ => {
            if path.is_empty() {
                lines.push(format!("    expected: {}", expected.repr()));
                lines.push(format!("    actual:   {}", actual.repr()));
            } else {
                lines.push(format!("    {}: expected {}, got {}", path, expected.repr(), actual.repr()));
            }
        }
    }
}
