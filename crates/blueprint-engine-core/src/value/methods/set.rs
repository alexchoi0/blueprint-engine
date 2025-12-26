use std::sync::Arc;

use indexmap::IndexSet;
use tokio::sync::RwLock;

use crate::error::BlueprintError;
use crate::value::{NativeFunction, Value};

pub fn get_set_method(s: Arc<RwLock<IndexSet<Value>>>, name: &str) -> Option<Value> {
    match name {
        "add" => {
            let s_clone = s.clone();
            Some(Value::NativeFunction(Arc::new(
                NativeFunction::new_with_state("add", move |args, _kwargs| {
                    let s = s_clone.clone();
                    Box::pin(async move {
                        if args.len() != 1 {
                            return Err(BlueprintError::ArgumentError {
                                message: format!(
                                    "add() takes exactly 1 argument ({} given)",
                                    args.len()
                                ),
                            });
                        }
                        let mut set = s.write().await;
                        set.insert(args[0].clone());
                        Ok(Value::None)
                    })
                }),
            )))
        }
        "remove" => {
            let s_clone = s.clone();
            Some(Value::NativeFunction(Arc::new(
                NativeFunction::new_with_state("remove", move |args, _kwargs| {
                    let s = s_clone.clone();
                    Box::pin(async move {
                        if args.len() != 1 {
                            return Err(BlueprintError::ArgumentError {
                                message: format!(
                                    "remove() takes exactly 1 argument ({} given)",
                                    args.len()
                                ),
                            });
                        }
                        let mut set = s.write().await;
                        if !set.shift_remove(&args[0]) {
                            return Err(BlueprintError::KeyError {
                                key: args[0].to_display_string(),
                            });
                        }
                        Ok(Value::None)
                    })
                }),
            )))
        }
        "discard" => {
            let s_clone = s.clone();
            Some(Value::NativeFunction(Arc::new(
                NativeFunction::new_with_state("discard", move |args, _kwargs| {
                    let s = s_clone.clone();
                    Box::pin(async move {
                        if args.len() != 1 {
                            return Err(BlueprintError::ArgumentError {
                                message: format!(
                                    "discard() takes exactly 1 argument ({} given)",
                                    args.len()
                                ),
                            });
                        }
                        let mut set = s.write().await;
                        set.shift_remove(&args[0]);
                        Ok(Value::None)
                    })
                }),
            )))
        }
        "pop" => {
            let s_clone = s.clone();
            Some(Value::NativeFunction(Arc::new(
                NativeFunction::new_with_state("pop", move |_args, _kwargs| {
                    let s = s_clone.clone();
                    Box::pin(async move {
                        let mut set = s.write().await;
                        if set.is_empty() {
                            return Err(BlueprintError::KeyError {
                                key: "pop from an empty set".into(),
                            });
                        }
                        let item = set.iter().next().cloned().unwrap();
                        set.shift_remove(&item);
                        Ok(item)
                    })
                }),
            )))
        }
        "clear" => {
            let s_clone = s.clone();
            Some(Value::NativeFunction(Arc::new(
                NativeFunction::new_with_state("clear", move |_args, _kwargs| {
                    let s = s_clone.clone();
                    Box::pin(async move {
                        let mut set = s.write().await;
                        set.clear();
                        Ok(Value::None)
                    })
                }),
            )))
        }
        "copy" => {
            let s_clone = s.clone();
            Some(Value::NativeFunction(Arc::new(
                NativeFunction::new_with_state("copy", move |_args, _kwargs| {
                    let s = s_clone.clone();
                    Box::pin(async move {
                        let set = s.read().await;
                        Ok(Value::Set(Arc::new(RwLock::new(set.clone()))))
                    })
                }),
            )))
        }
        "union" => {
            let s_clone = s.clone();
            Some(Value::NativeFunction(Arc::new(
                NativeFunction::new_with_state("union", move |args, _kwargs| {
                    let s = s_clone.clone();
                    Box::pin(async move {
                        if args.len() != 1 {
                            return Err(BlueprintError::ArgumentError {
                                message: format!(
                                    "union() takes exactly 1 argument ({} given)",
                                    args.len()
                                ),
                            });
                        }
                        let set = s.read().await;
                        let other = match &args[0] {
                            Value::Set(other) => other.read().await.clone(),
                            Value::List(l) => l.read().await.iter().cloned().collect(),
                            Value::Tuple(t) => t.iter().cloned().collect(),
                            _ => {
                                return Err(BlueprintError::TypeError {
                                    expected: "set, list, or tuple".into(),
                                    actual: args[0].type_name().into(),
                                })
                            }
                        };
                        let result: IndexSet<Value> = set.union(&other).cloned().collect();
                        Ok(Value::Set(Arc::new(RwLock::new(result))))
                    })
                }),
            )))
        }
        "intersection" => {
            let s_clone = s.clone();
            Some(Value::NativeFunction(Arc::new(
                NativeFunction::new_with_state("intersection", move |args, _kwargs| {
                    let s = s_clone.clone();
                    Box::pin(async move {
                        if args.len() != 1 {
                            return Err(BlueprintError::ArgumentError {
                                message: format!(
                                    "intersection() takes exactly 1 argument ({} given)",
                                    args.len()
                                ),
                            });
                        }
                        let set = s.read().await;
                        let other = match &args[0] {
                            Value::Set(other) => other.read().await.clone(),
                            Value::List(l) => l.read().await.iter().cloned().collect(),
                            Value::Tuple(t) => t.iter().cloned().collect(),
                            _ => {
                                return Err(BlueprintError::TypeError {
                                    expected: "set, list, or tuple".into(),
                                    actual: args[0].type_name().into(),
                                })
                            }
                        };
                        let result: IndexSet<Value> = set.intersection(&other).cloned().collect();
                        Ok(Value::Set(Arc::new(RwLock::new(result))))
                    })
                }),
            )))
        }
        "difference" => {
            let s_clone = s.clone();
            Some(Value::NativeFunction(Arc::new(
                NativeFunction::new_with_state("difference", move |args, _kwargs| {
                    let s = s_clone.clone();
                    Box::pin(async move {
                        if args.len() != 1 {
                            return Err(BlueprintError::ArgumentError {
                                message: format!(
                                    "difference() takes exactly 1 argument ({} given)",
                                    args.len()
                                ),
                            });
                        }
                        let set = s.read().await;
                        let other = match &args[0] {
                            Value::Set(other) => other.read().await.clone(),
                            Value::List(l) => l.read().await.iter().cloned().collect(),
                            Value::Tuple(t) => t.iter().cloned().collect(),
                            _ => {
                                return Err(BlueprintError::TypeError {
                                    expected: "set, list, or tuple".into(),
                                    actual: args[0].type_name().into(),
                                })
                            }
                        };
                        let result: IndexSet<Value> = set.difference(&other).cloned().collect();
                        Ok(Value::Set(Arc::new(RwLock::new(result))))
                    })
                }),
            )))
        }
        "symmetric_difference" => {
            let s_clone = s.clone();
            Some(Value::NativeFunction(Arc::new(
                NativeFunction::new_with_state("symmetric_difference", move |args, _kwargs| {
                    let s = s_clone.clone();
                    Box::pin(async move {
                        if args.len() != 1 {
                            return Err(BlueprintError::ArgumentError {
                                message: format!(
                                    "symmetric_difference() takes exactly 1 argument ({} given)",
                                    args.len()
                                ),
                            });
                        }
                        let set = s.read().await;
                        let other = match &args[0] {
                            Value::Set(other) => other.read().await.clone(),
                            Value::List(l) => l.read().await.iter().cloned().collect(),
                            Value::Tuple(t) => t.iter().cloned().collect(),
                            _ => {
                                return Err(BlueprintError::TypeError {
                                    expected: "set, list, or tuple".into(),
                                    actual: args[0].type_name().into(),
                                })
                            }
                        };
                        let result: IndexSet<Value> =
                            set.symmetric_difference(&other).cloned().collect();
                        Ok(Value::Set(Arc::new(RwLock::new(result))))
                    })
                }),
            )))
        }
        "issubset" => {
            let s_clone = s.clone();
            Some(Value::NativeFunction(Arc::new(
                NativeFunction::new_with_state("issubset", move |args, _kwargs| {
                    let s = s_clone.clone();
                    Box::pin(async move {
                        if args.len() != 1 {
                            return Err(BlueprintError::ArgumentError {
                                message: format!(
                                    "issubset() takes exactly 1 argument ({} given)",
                                    args.len()
                                ),
                            });
                        }
                        let set = s.read().await;
                        let other = match &args[0] {
                            Value::Set(other) => other.read().await.clone(),
                            Value::List(l) => l.read().await.iter().cloned().collect(),
                            Value::Tuple(t) => t.iter().cloned().collect(),
                            _ => {
                                return Err(BlueprintError::TypeError {
                                    expected: "set, list, or tuple".into(),
                                    actual: args[0].type_name().into(),
                                })
                            }
                        };
                        Ok(Value::Bool(set.is_subset(&other)))
                    })
                }),
            )))
        }
        "issuperset" => {
            let s_clone = s.clone();
            Some(Value::NativeFunction(Arc::new(
                NativeFunction::new_with_state("issuperset", move |args, _kwargs| {
                    let s = s_clone.clone();
                    Box::pin(async move {
                        if args.len() != 1 {
                            return Err(BlueprintError::ArgumentError {
                                message: format!(
                                    "issuperset() takes exactly 1 argument ({} given)",
                                    args.len()
                                ),
                            });
                        }
                        let set = s.read().await;
                        let other = match &args[0] {
                            Value::Set(other) => other.read().await.clone(),
                            Value::List(l) => l.read().await.iter().cloned().collect(),
                            Value::Tuple(t) => t.iter().cloned().collect(),
                            _ => {
                                return Err(BlueprintError::TypeError {
                                    expected: "set, list, or tuple".into(),
                                    actual: args[0].type_name().into(),
                                })
                            }
                        };
                        Ok(Value::Bool(set.is_superset(&other)))
                    })
                }),
            )))
        }
        "isdisjoint" => {
            let s_clone = s.clone();
            Some(Value::NativeFunction(Arc::new(
                NativeFunction::new_with_state("isdisjoint", move |args, _kwargs| {
                    let s = s_clone.clone();
                    Box::pin(async move {
                        if args.len() != 1 {
                            return Err(BlueprintError::ArgumentError {
                                message: format!(
                                    "isdisjoint() takes exactly 1 argument ({} given)",
                                    args.len()
                                ),
                            });
                        }
                        let set = s.read().await;
                        let other = match &args[0] {
                            Value::Set(other) => other.read().await.clone(),
                            Value::List(l) => l.read().await.iter().cloned().collect(),
                            Value::Tuple(t) => t.iter().cloned().collect(),
                            _ => {
                                return Err(BlueprintError::TypeError {
                                    expected: "set, list, or tuple".into(),
                                    actual: args[0].type_name().into(),
                                })
                            }
                        };
                        Ok(Value::Bool(set.is_disjoint(&other)))
                    })
                }),
            )))
        }
        "update" => {
            let s_clone = s.clone();
            Some(Value::NativeFunction(Arc::new(
                NativeFunction::new_with_state("update", move |args, _kwargs| {
                    let s = s_clone.clone();
                    Box::pin(async move {
                        if args.len() != 1 {
                            return Err(BlueprintError::ArgumentError {
                                message: format!(
                                    "update() takes exactly 1 argument ({} given)",
                                    args.len()
                                ),
                            });
                        }
                        let other = match &args[0] {
                            Value::Set(other) => other.read().await.clone(),
                            Value::List(l) => l.read().await.iter().cloned().collect(),
                            Value::Tuple(t) => t.iter().cloned().collect(),
                            _ => {
                                return Err(BlueprintError::TypeError {
                                    expected: "set, list, or tuple".into(),
                                    actual: args[0].type_name().into(),
                                })
                            }
                        };
                        let mut set = s.write().await;
                        set.extend(other);
                        Ok(Value::None)
                    })
                }),
            )))
        }
        _ => None,
    }
}
