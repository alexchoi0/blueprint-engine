use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::{mpsc, RwLock};

use blueprint_engine_core::{BlueprintError, Generator, GeneratorMessage, Result, Value};

use super::call_func;

pub async fn range(args: Vec<Value>, _kwargs: HashMap<String, Value>) -> Result<Value> {
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

pub async fn map_fn(args: Vec<Value>, _kwargs: HashMap<String, Value>) -> Result<Value> {
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

    Ok(Value::Generator(Arc::new(Generator::new(
        rx,
        "map".to_string(),
    ))))
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
                if tx
                    .send(GeneratorMessage::Yielded(result, resume_tx))
                    .await
                    .is_err()
                {
                    break;
                }
                let _ = resume_rx.await;
            }
        }
        Value::Iterator(iter) => {
            while let Some(item) = iter.next().await {
                let result = call_func(&func, vec![item]).await?;
                let (resume_tx, resume_rx) = tokio::sync::oneshot::channel();
                if tx
                    .send(GeneratorMessage::Yielded(result, resume_tx))
                    .await
                    .is_err()
                {
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
                if tx
                    .send(GeneratorMessage::Yielded(result, resume_tx))
                    .await
                    .is_err()
                {
                    break;
                }
                let _ = resume_rx.await;
            }
        }
        Value::Tuple(t) => {
            for item in t.iter().cloned() {
                let result = call_func(&func, vec![item]).await?;
                let (resume_tx, resume_rx) = tokio::sync::oneshot::channel();
                if tx
                    .send(GeneratorMessage::Yielded(result, resume_tx))
                    .await
                    .is_err()
                {
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
                if tx
                    .send(GeneratorMessage::Yielded(result, resume_tx))
                    .await
                    .is_err()
                {
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

pub async fn filter_fn(args: Vec<Value>, _kwargs: HashMap<String, Value>) -> Result<Value> {
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

    Ok(Value::Generator(Arc::new(Generator::new(
        rx,
        "filter".to_string(),
    ))))
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
                    call_func(&func, vec![item.clone()])
                        .await?
                        .is_truthy_async()
                        .await
                };
                if predicate {
                    let (resume_tx, resume_rx) = tokio::sync::oneshot::channel();
                    if tx
                        .send(GeneratorMessage::Yielded(item, resume_tx))
                        .await
                        .is_err()
                    {
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
                    call_func(&func, vec![item.clone()])
                        .await?
                        .is_truthy_async()
                        .await
                };
                if predicate {
                    let (resume_tx, resume_rx) = tokio::sync::oneshot::channel();
                    if tx
                        .send(GeneratorMessage::Yielded(item, resume_tx))
                        .await
                        .is_err()
                    {
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
                    call_func(&func, vec![item.clone()])
                        .await?
                        .is_truthy_async()
                        .await
                };
                if predicate {
                    let (resume_tx, resume_rx) = tokio::sync::oneshot::channel();
                    if tx
                        .send(GeneratorMessage::Yielded(item, resume_tx))
                        .await
                        .is_err()
                    {
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
                    call_func(&func, vec![item.clone()])
                        .await?
                        .is_truthy_async()
                        .await
                };
                if predicate {
                    let (resume_tx, resume_rx) = tokio::sync::oneshot::channel();
                    if tx
                        .send(GeneratorMessage::Yielded(item, resume_tx))
                        .await
                        .is_err()
                    {
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

pub async fn enumerate(args: Vec<Value>, _kwargs: HashMap<String, Value>) -> Result<Value> {
    if args.is_empty() || args.len() > 2 {
        return Err(BlueprintError::ArgumentError {
            message: format!("enumerate() takes 1 or 2 arguments ({} given)", args.len()),
        });
    }

    let start = if args.len() == 2 {
        args[1].as_int()?
    } else {
        0
    };
    let iterable = args[0].clone();

    match &iterable {
        Value::Generator(_) | Value::Iterator(_) => {
            let (tx, rx) = mpsc::channel::<GeneratorMessage>(1);

            tokio::spawn(async move {
                let _ = enumerate_generator_task(iterable, start, tx.clone()).await;
            });

            Ok(Value::Generator(Arc::new(Generator::new(
                rx,
                "enumerate".to_string(),
            ))))
        }
        _ => {
            let items = match &iterable {
                Value::List(l) => l.read().await.clone(),
                Value::Tuple(t) => t.as_ref().clone(),
                Value::String(s) => s
                    .chars()
                    .map(|c| Value::String(Arc::new(c.to_string())))
                    .collect(),
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
                if tx
                    .send(GeneratorMessage::Yielded(tuple, resume_tx))
                    .await
                    .is_err()
                {
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
                if tx
                    .send(GeneratorMessage::Yielded(tuple, resume_tx))
                    .await
                    .is_err()
                {
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

pub async fn zip(args: Vec<Value>, _kwargs: HashMap<String, Value>) -> Result<Value> {
    if args.is_empty() {
        return Ok(Value::List(Arc::new(RwLock::new(vec![]))));
    }

    let mut iterables: Vec<Vec<Value>> = Vec::new();
    for arg in &args {
        let items = match arg {
            Value::List(l) => l.read().await.clone(),
            Value::Tuple(t) => t.as_ref().clone(),
            Value::String(s) => s
                .chars()
                .map(|c| Value::String(Arc::new(c.to_string())))
                .collect(),
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

pub async fn sorted(args: Vec<Value>, kwargs: HashMap<String, Value>) -> Result<Value> {
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
        Value::String(s) => s
            .chars()
            .map(|c| Value::String(Arc::new(c.to_string())))
            .collect(),
        other => {
            return Err(BlueprintError::TypeError {
                expected: "iterable".into(),
                actual: other.type_name().into(),
            })
        }
    };

    items.sort_by(|a, b| match (a, b) {
        (Value::Int(x), Value::Int(y)) => x.cmp(y),
        (Value::Float(x), Value::Float(y)) => x.partial_cmp(y).unwrap_or(std::cmp::Ordering::Equal),
        (Value::String(x), Value::String(y)) => x.cmp(y),
        _ => std::cmp::Ordering::Equal,
    });

    if reverse {
        items.reverse();
    }

    Ok(Value::List(Arc::new(RwLock::new(items))))
}

pub async fn reversed(args: Vec<Value>, _kwargs: HashMap<String, Value>) -> Result<Value> {
    if args.len() != 1 {
        return Err(BlueprintError::ArgumentError {
            message: format!("reversed() takes exactly 1 argument ({} given)", args.len()),
        });
    }

    let mut items = match &args[0] {
        Value::List(l) => l.read().await.clone(),
        Value::Tuple(t) => t.as_ref().clone(),
        Value::String(s) => s
            .chars()
            .map(|c| Value::String(Arc::new(c.to_string())))
            .collect(),
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
