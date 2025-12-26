use std::sync::Arc;

use tokio::sync::RwLock;

use crate::error::BlueprintError;
use crate::value::{NativeFunction, Value};

pub fn get_list_method(l: Arc<RwLock<Vec<Value>>>, name: &str) -> Option<Value> {
    match name {
        "append" => {
            let l_clone = l.clone();
            Some(Value::NativeFunction(Arc::new(
                NativeFunction::new_with_state("append", move |args, _kwargs| {
                    let l = l_clone.clone();
                    Box::pin(async move {
                        if args.len() != 1 {
                            return Err(BlueprintError::ArgumentError {
                                message: format!(
                                    "append() takes exactly 1 argument ({} given)",
                                    args.len()
                                ),
                            });
                        }
                        let mut list = l.write().await;
                        list.push(args[0].clone());
                        Ok(Value::None)
                    })
                }),
            )))
        }
        "extend" => {
            let l_clone = l.clone();
            Some(Value::NativeFunction(Arc::new(
                NativeFunction::new_with_state("extend", move |args, _kwargs| {
                    let l = l_clone.clone();
                    Box::pin(async move {
                        if args.len() != 1 {
                            return Err(BlueprintError::ArgumentError {
                                message: format!(
                                    "extend() takes exactly 1 argument ({} given)",
                                    args.len()
                                ),
                            });
                        }
                        let items = match &args[0] {
                            Value::List(other) => other.read().await.clone(),
                            Value::Tuple(t) => t.as_ref().clone(),
                            _ => {
                                return Err(BlueprintError::TypeError {
                                    expected: "list or tuple".into(),
                                    actual: args[0].type_name().into(),
                                })
                            }
                        };
                        let mut list = l.write().await;
                        list.extend(items);
                        Ok(Value::None)
                    })
                }),
            )))
        }
        "insert" => {
            let l_clone = l.clone();
            Some(Value::NativeFunction(Arc::new(
                NativeFunction::new_with_state("insert", move |args, _kwargs| {
                    let l = l_clone.clone();
                    Box::pin(async move {
                        if args.len() != 2 {
                            return Err(BlueprintError::ArgumentError {
                                message: format!(
                                    "insert() takes exactly 2 arguments ({} given)",
                                    args.len()
                                ),
                            });
                        }
                        let index = args[0].as_int()? as usize;
                        let mut list = l.write().await;
                        let len = list.len();
                        let index = index.min(len);
                        list.insert(index, args[1].clone());
                        Ok(Value::None)
                    })
                }),
            )))
        }
        "pop" => {
            let l_clone = l.clone();
            Some(Value::NativeFunction(Arc::new(
                NativeFunction::new_with_state("pop", move |args, _kwargs| {
                    let l = l_clone.clone();
                    Box::pin(async move {
                        if args.len() > 1 {
                            return Err(BlueprintError::ArgumentError {
                                message: format!(
                                    "pop() takes at most 1 argument ({} given)",
                                    args.len()
                                ),
                            });
                        }
                        let mut list = l.write().await;
                        if list.is_empty() {
                            return Err(BlueprintError::IndexError {
                                message: "pop from empty list".into(),
                            });
                        }
                        let index = if args.is_empty() {
                            list.len() - 1
                        } else {
                            let i = args[0].as_int()?;
                            if i < 0 {
                                (list.len() as i64 + i) as usize
                            } else {
                                i as usize
                            }
                        };
                        if index >= list.len() {
                            return Err(BlueprintError::IndexError {
                                message: format!("pop index {} out of range", index),
                            });
                        }
                        Ok(list.remove(index))
                    })
                }),
            )))
        }
        "remove" => {
            let l_clone = l.clone();
            Some(Value::NativeFunction(Arc::new(
                NativeFunction::new_with_state("remove", move |args, _kwargs| {
                    let l = l_clone.clone();
                    Box::pin(async move {
                        if args.len() != 1 {
                            return Err(BlueprintError::ArgumentError {
                                message: format!(
                                    "remove() takes exactly 1 argument ({} given)",
                                    args.len()
                                ),
                            });
                        }
                        let mut list = l.write().await;
                        let pos = list.iter().position(|x| x == &args[0]);
                        match pos {
                            Some(i) => {
                                list.remove(i);
                                Ok(Value::None)
                            }
                            None => Err(BlueprintError::ValueError {
                                message: "value not in list".into(),
                            }),
                        }
                    })
                }),
            )))
        }
        "clear" => {
            let l_clone = l.clone();
            Some(Value::NativeFunction(Arc::new(
                NativeFunction::new_with_state("clear", move |_args, _kwargs| {
                    let l = l_clone.clone();
                    Box::pin(async move {
                        let mut list = l.write().await;
                        list.clear();
                        Ok(Value::None)
                    })
                }),
            )))
        }
        "index" => {
            let l_clone = l.clone();
            Some(Value::NativeFunction(Arc::new(
                NativeFunction::new_with_state("index", move |args, _kwargs| {
                    let l = l_clone.clone();
                    Box::pin(async move {
                        if args.is_empty() || args.len() > 3 {
                            return Err(BlueprintError::ArgumentError {
                                message: format!(
                                    "index() takes 1 to 3 arguments ({} given)",
                                    args.len()
                                ),
                            });
                        }
                        let list = l.read().await;
                        let start = if args.len() > 1 {
                            args[1].as_int()? as usize
                        } else {
                            0
                        };
                        let end = if args.len() > 2 {
                            args[2].as_int()? as usize
                        } else {
                            list.len()
                        };
                        for (i, item) in list.iter().enumerate().skip(start).take(end - start) {
                            if item == &args[0] {
                                return Ok(Value::Int(i as i64));
                            }
                        }
                        Err(BlueprintError::ValueError {
                            message: "value not in list".into(),
                        })
                    })
                }),
            )))
        }
        "count" => {
            let l_clone = l.clone();
            Some(Value::NativeFunction(Arc::new(
                NativeFunction::new_with_state("count", move |args, _kwargs| {
                    let l = l_clone.clone();
                    Box::pin(async move {
                        if args.len() != 1 {
                            return Err(BlueprintError::ArgumentError {
                                message: format!(
                                    "count() takes exactly 1 argument ({} given)",
                                    args.len()
                                ),
                            });
                        }
                        let list = l.read().await;
                        let count = list.iter().filter(|x| *x == &args[0]).count();
                        Ok(Value::Int(count as i64))
                    })
                }),
            )))
        }
        "reverse" => {
            let l_clone = l.clone();
            Some(Value::NativeFunction(Arc::new(
                NativeFunction::new_with_state("reverse", move |_args, _kwargs| {
                    let l = l_clone.clone();
                    Box::pin(async move {
                        let mut list = l.write().await;
                        list.reverse();
                        Ok(Value::None)
                    })
                }),
            )))
        }
        "copy" => {
            let l_clone = l.clone();
            Some(Value::NativeFunction(Arc::new(
                NativeFunction::new_with_state("copy", move |_args, _kwargs| {
                    let l = l_clone.clone();
                    Box::pin(async move {
                        let list = l.read().await;
                        Ok(Value::List(Arc::new(RwLock::new(list.clone()))))
                    })
                }),
            )))
        }
        _ => None,
    }
}
