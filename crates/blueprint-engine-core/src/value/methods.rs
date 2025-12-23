use std::sync::Arc;

use indexmap::{IndexMap, IndexSet};
use tokio::sync::RwLock;

use crate::error::BlueprintError;
use super::{NativeFunction, Value};

pub fn get_string_method(s: Arc<String>, name: &str) -> Option<Value> {
    let s_clone = s.clone();
    match name {
        "upper" => Some(Value::NativeFunction(Arc::new(NativeFunction::new_with_state(
            "upper",
            move |_args, _kwargs| {
                let result = s_clone.to_uppercase();
                Box::pin(async move { Ok(Value::String(Arc::new(result))) })
            },
        )))),
        "lower" => {
            let s = s.clone();
            Some(Value::NativeFunction(Arc::new(NativeFunction::new_with_state(
                "lower",
                move |_args, _kwargs| {
                    let result = s.to_lowercase();
                    Box::pin(async move { Ok(Value::String(Arc::new(result))) })
                },
            ))))
        }
        "strip" => {
            let s = s.clone();
            Some(Value::NativeFunction(Arc::new(NativeFunction::new_with_state(
                "strip",
                move |_args, _kwargs| {
                    let result = s.trim().to_string();
                    Box::pin(async move { Ok(Value::String(Arc::new(result))) })
                },
            ))))
        }
        "split" => {
            let s = s.clone();
            Some(Value::NativeFunction(Arc::new(NativeFunction::new_with_state(
                "split",
                move |args, _kwargs| {
                    let sep = if args.is_empty() {
                        None
                    } else {
                        Some(args[0].to_display_string())
                    };
                    let parts: Vec<Value> = match sep {
                        Some(ref sep) => s.split(sep.as_str()).map(|p| Value::String(Arc::new(p.to_string()))).collect(),
                        None => s.split_whitespace().map(|p| Value::String(Arc::new(p.to_string()))).collect(),
                    };
                    Box::pin(async move { Ok(Value::List(Arc::new(tokio::sync::RwLock::new(parts)))) })
                },
            ))))
        }
        "join" => {
            let s = s.clone();
            Some(Value::NativeFunction(Arc::new(NativeFunction::new_with_state(
                "join",
                move |args, _kwargs| {
                    let s = s.clone();
                    Box::pin(async move {
                        if args.is_empty() {
                            return Err(BlueprintError::ArgumentError {
                                message: "join() requires 1 argument".into(),
                            });
                        }
                        let items = match &args[0] {
                            Value::List(l) => l.read().await.clone(),
                            Value::Tuple(t) => t.as_ref().clone(),
                            _ => return Err(BlueprintError::TypeError {
                                expected: "list or tuple".into(),
                                actual: args[0].type_name().into(),
                            }),
                        };
                        let strings: Vec<String> = items.iter().map(|v| v.to_display_string()).collect();
                        Ok(Value::String(Arc::new(strings.join(s.as_str()))))
                    })
                },
            ))))
        }
        "replace" => {
            let s = s.clone();
            Some(Value::NativeFunction(Arc::new(NativeFunction::new_with_state(
                "replace",
                move |args, _kwargs| {
                    let s = s.clone();
                    Box::pin(async move {
                        if args.len() < 2 {
                            return Err(BlueprintError::ArgumentError {
                                message: "replace() requires 2 arguments".into(),
                            });
                        }
                        let old = args[0].to_display_string();
                        let new = args[1].to_display_string();
                        let result = s.replace(&old, &new);
                        Ok(Value::String(Arc::new(result)))
                    })
                },
            ))))
        }
        "startswith" => {
            let s = s.clone();
            Some(Value::NativeFunction(Arc::new(NativeFunction::new_with_state(
                "startswith",
                move |args, _kwargs| {
                    let s = s.clone();
                    Box::pin(async move {
                        if args.is_empty() {
                            return Err(BlueprintError::ArgumentError {
                                message: "startswith() requires 1 argument".into(),
                            });
                        }
                        let prefix = args[0].to_display_string();
                        Ok(Value::Bool(s.starts_with(&prefix)))
                    })
                },
            ))))
        }
        "endswith" => {
            let s = s.clone();
            Some(Value::NativeFunction(Arc::new(NativeFunction::new_with_state(
                "endswith",
                move |args, _kwargs| {
                    let s = s.clone();
                    Box::pin(async move {
                        if args.is_empty() {
                            return Err(BlueprintError::ArgumentError {
                                message: "endswith() requires 1 argument".into(),
                            });
                        }
                        let suffix = args[0].to_display_string();
                        Ok(Value::Bool(s.ends_with(&suffix)))
                    })
                },
            ))))
        }
        "find" => {
            let s = s.clone();
            Some(Value::NativeFunction(Arc::new(NativeFunction::new_with_state(
                "find",
                move |args, _kwargs| {
                    let s = s.clone();
                    Box::pin(async move {
                        if args.is_empty() {
                            return Err(BlueprintError::ArgumentError {
                                message: "find() requires 1 argument".into(),
                            });
                        }
                        let needle = args[0].to_display_string();
                        let result = s.find(&needle).map(|i| i as i64).unwrap_or(-1);
                        Ok(Value::Int(result))
                    })
                },
            ))))
        }
        "format" => {
            let s = s.clone();
            Some(Value::NativeFunction(Arc::new(NativeFunction::new_with_state(
                "format",
                move |args, _kwargs| {
                    let s = s.clone();
                    Box::pin(async move {
                        let mut result = s.as_str().to_string();
                        for arg in args {
                            if let Some(pos) = result.find("{}") {
                                result = format!("{}{}{}", &result[..pos], arg.to_display_string(), &result[pos+2..]);
                            }
                        }
                        Ok(Value::String(Arc::new(result)))
                    })
                },
            ))))
        }
        _ => None,
    }
}

pub fn get_list_method(l: Arc<RwLock<Vec<Value>>>, name: &str) -> Option<Value> {
    match name {
        "append" => {
            let l_clone = l.clone();
            Some(Value::NativeFunction(Arc::new(NativeFunction::new_with_state(
                "append",
                move |args, _kwargs| {
                    let l = l_clone.clone();
                    Box::pin(async move {
                        if args.len() != 1 {
                            return Err(BlueprintError::ArgumentError {
                                message: format!("append() takes exactly 1 argument ({} given)", args.len()),
                            });
                        }
                        let mut list = l.write().await;
                        list.push(args[0].clone());
                        Ok(Value::None)
                    })
                },
            ))))
        }
        "extend" => {
            let l_clone = l.clone();
            Some(Value::NativeFunction(Arc::new(NativeFunction::new_with_state(
                "extend",
                move |args, _kwargs| {
                    let l = l_clone.clone();
                    Box::pin(async move {
                        if args.len() != 1 {
                            return Err(BlueprintError::ArgumentError {
                                message: format!("extend() takes exactly 1 argument ({} given)", args.len()),
                            });
                        }
                        let items = match &args[0] {
                            Value::List(other) => other.read().await.clone(),
                            Value::Tuple(t) => t.as_ref().clone(),
                            _ => return Err(BlueprintError::TypeError {
                                expected: "list or tuple".into(),
                                actual: args[0].type_name().into(),
                            }),
                        };
                        let mut list = l.write().await;
                        list.extend(items);
                        Ok(Value::None)
                    })
                },
            ))))
        }
        "insert" => {
            let l_clone = l.clone();
            Some(Value::NativeFunction(Arc::new(NativeFunction::new_with_state(
                "insert",
                move |args, _kwargs| {
                    let l = l_clone.clone();
                    Box::pin(async move {
                        if args.len() != 2 {
                            return Err(BlueprintError::ArgumentError {
                                message: format!("insert() takes exactly 2 arguments ({} given)", args.len()),
                            });
                        }
                        let index = args[0].as_int()? as usize;
                        let mut list = l.write().await;
                        let len = list.len();
                        let index = index.min(len);
                        list.insert(index, args[1].clone());
                        Ok(Value::None)
                    })
                },
            ))))
        }
        "pop" => {
            let l_clone = l.clone();
            Some(Value::NativeFunction(Arc::new(NativeFunction::new_with_state(
                "pop",
                move |args, _kwargs| {
                    let l = l_clone.clone();
                    Box::pin(async move {
                        if args.len() > 1 {
                            return Err(BlueprintError::ArgumentError {
                                message: format!("pop() takes at most 1 argument ({} given)", args.len()),
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
                },
            ))))
        }
        "remove" => {
            let l_clone = l.clone();
            Some(Value::NativeFunction(Arc::new(NativeFunction::new_with_state(
                "remove",
                move |args, _kwargs| {
                    let l = l_clone.clone();
                    Box::pin(async move {
                        if args.len() != 1 {
                            return Err(BlueprintError::ArgumentError {
                                message: format!("remove() takes exactly 1 argument ({} given)", args.len()),
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
                },
            ))))
        }
        "clear" => {
            let l_clone = l.clone();
            Some(Value::NativeFunction(Arc::new(NativeFunction::new_with_state(
                "clear",
                move |_args, _kwargs| {
                    let l = l_clone.clone();
                    Box::pin(async move {
                        let mut list = l.write().await;
                        list.clear();
                        Ok(Value::None)
                    })
                },
            ))))
        }
        "index" => {
            let l_clone = l.clone();
            Some(Value::NativeFunction(Arc::new(NativeFunction::new_with_state(
                "index",
                move |args, _kwargs| {
                    let l = l_clone.clone();
                    Box::pin(async move {
                        if args.is_empty() || args.len() > 3 {
                            return Err(BlueprintError::ArgumentError {
                                message: format!("index() takes 1 to 3 arguments ({} given)", args.len()),
                            });
                        }
                        let list = l.read().await;
                        let start = if args.len() > 1 { args[1].as_int()? as usize } else { 0 };
                        let end = if args.len() > 2 { args[2].as_int()? as usize } else { list.len() };
                        for (i, item) in list.iter().enumerate().skip(start).take(end - start) {
                            if item == &args[0] {
                                return Ok(Value::Int(i as i64));
                            }
                        }
                        Err(BlueprintError::ValueError {
                            message: "value not in list".into(),
                        })
                    })
                },
            ))))
        }
        "count" => {
            let l_clone = l.clone();
            Some(Value::NativeFunction(Arc::new(NativeFunction::new_with_state(
                "count",
                move |args, _kwargs| {
                    let l = l_clone.clone();
                    Box::pin(async move {
                        if args.len() != 1 {
                            return Err(BlueprintError::ArgumentError {
                                message: format!("count() takes exactly 1 argument ({} given)", args.len()),
                            });
                        }
                        let list = l.read().await;
                        let count = list.iter().filter(|x| *x == &args[0]).count();
                        Ok(Value::Int(count as i64))
                    })
                },
            ))))
        }
        "reverse" => {
            let l_clone = l.clone();
            Some(Value::NativeFunction(Arc::new(NativeFunction::new_with_state(
                "reverse",
                move |_args, _kwargs| {
                    let l = l_clone.clone();
                    Box::pin(async move {
                        let mut list = l.write().await;
                        list.reverse();
                        Ok(Value::None)
                    })
                },
            ))))
        }
        "copy" => {
            let l_clone = l.clone();
            Some(Value::NativeFunction(Arc::new(NativeFunction::new_with_state(
                "copy",
                move |_args, _kwargs| {
                    let l = l_clone.clone();
                    Box::pin(async move {
                        let list = l.read().await;
                        Ok(Value::List(Arc::new(RwLock::new(list.clone()))))
                    })
                },
            ))))
        }
        _ => None,
    }
}

pub fn get_dict_method(d: Arc<RwLock<IndexMap<String, Value>>>, name: &str) -> Option<Value> {
    match name {
        "get" => {
            let d_clone = d.clone();
            Some(Value::NativeFunction(Arc::new(NativeFunction::new_with_state(
                "get",
                move |args, _kwargs| {
                    let d = d_clone.clone();
                    Box::pin(async move {
                        if args.is_empty() || args.len() > 2 {
                            return Err(BlueprintError::ArgumentError {
                                message: format!("get() takes 1 or 2 arguments ({} given)", args.len()),
                            });
                        }
                        let key = match &args[0] {
                            Value::String(s) => s.as_ref().clone(),
                            v => return Err(BlueprintError::TypeError {
                                expected: "string".into(),
                                actual: v.type_name().into(),
                            }),
                        };
                        let default = if args.len() == 2 {
                            args[1].clone()
                        } else {
                            Value::None
                        };
                        let map = d.read().await;
                        Ok(map.get(&key).cloned().unwrap_or(default))
                    })
                },
            ))))
        }
        "keys" => {
            let d_clone = d.clone();
            Some(Value::NativeFunction(Arc::new(NativeFunction::new_with_state(
                "keys",
                move |_args, _kwargs| {
                    let d = d_clone.clone();
                    Box::pin(async move {
                        let map = d.read().await;
                        let keys: Vec<Value> = map.keys().map(|k| Value::String(Arc::new(k.clone()))).collect();
                        Ok(Value::List(Arc::new(RwLock::new(keys))))
                    })
                },
            ))))
        }
        "values" => {
            let d_clone = d.clone();
            Some(Value::NativeFunction(Arc::new(NativeFunction::new_with_state(
                "values",
                move |_args, _kwargs| {
                    let d = d_clone.clone();
                    Box::pin(async move {
                        let map = d.read().await;
                        let values: Vec<Value> = map.values().cloned().collect();
                        Ok(Value::List(Arc::new(RwLock::new(values))))
                    })
                },
            ))))
        }
        "items" => {
            let d_clone = d.clone();
            Some(Value::NativeFunction(Arc::new(NativeFunction::new_with_state(
                "items",
                move |_args, _kwargs| {
                    let d = d_clone.clone();
                    Box::pin(async move {
                        let map = d.read().await;
                        let items: Vec<Value> = map.iter()
                            .map(|(k, v)| Value::Tuple(Arc::new(vec![Value::String(Arc::new(k.clone())), v.clone()])))
                            .collect();
                        Ok(Value::List(Arc::new(RwLock::new(items))))
                    })
                },
            ))))
        }
        _ => None,
    }
}

pub fn get_set_method(s: Arc<RwLock<IndexSet<Value>>>, name: &str) -> Option<Value> {
    match name {
        "add" => {
            let s_clone = s.clone();
            Some(Value::NativeFunction(Arc::new(NativeFunction::new_with_state(
                "add",
                move |args, _kwargs| {
                    let s = s_clone.clone();
                    Box::pin(async move {
                        if args.len() != 1 {
                            return Err(BlueprintError::ArgumentError {
                                message: format!("add() takes exactly 1 argument ({} given)", args.len()),
                            });
                        }
                        let mut set = s.write().await;
                        set.insert(args[0].clone());
                        Ok(Value::None)
                    })
                },
            ))))
        }
        "remove" => {
            let s_clone = s.clone();
            Some(Value::NativeFunction(Arc::new(NativeFunction::new_with_state(
                "remove",
                move |args, _kwargs| {
                    let s = s_clone.clone();
                    Box::pin(async move {
                        if args.len() != 1 {
                            return Err(BlueprintError::ArgumentError {
                                message: format!("remove() takes exactly 1 argument ({} given)", args.len()),
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
                },
            ))))
        }
        "discard" => {
            let s_clone = s.clone();
            Some(Value::NativeFunction(Arc::new(NativeFunction::new_with_state(
                "discard",
                move |args, _kwargs| {
                    let s = s_clone.clone();
                    Box::pin(async move {
                        if args.len() != 1 {
                            return Err(BlueprintError::ArgumentError {
                                message: format!("discard() takes exactly 1 argument ({} given)", args.len()),
                            });
                        }
                        let mut set = s.write().await;
                        set.shift_remove(&args[0]);
                        Ok(Value::None)
                    })
                },
            ))))
        }
        "pop" => {
            let s_clone = s.clone();
            Some(Value::NativeFunction(Arc::new(NativeFunction::new_with_state(
                "pop",
                move |_args, _kwargs| {
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
                },
            ))))
        }
        "clear" => {
            let s_clone = s.clone();
            Some(Value::NativeFunction(Arc::new(NativeFunction::new_with_state(
                "clear",
                move |_args, _kwargs| {
                    let s = s_clone.clone();
                    Box::pin(async move {
                        let mut set = s.write().await;
                        set.clear();
                        Ok(Value::None)
                    })
                },
            ))))
        }
        "copy" => {
            let s_clone = s.clone();
            Some(Value::NativeFunction(Arc::new(NativeFunction::new_with_state(
                "copy",
                move |_args, _kwargs| {
                    let s = s_clone.clone();
                    Box::pin(async move {
                        let set = s.read().await;
                        Ok(Value::Set(Arc::new(RwLock::new(set.clone()))))
                    })
                },
            ))))
        }
        "union" => {
            let s_clone = s.clone();
            Some(Value::NativeFunction(Arc::new(NativeFunction::new_with_state(
                "union",
                move |args, _kwargs| {
                    let s = s_clone.clone();
                    Box::pin(async move {
                        if args.len() != 1 {
                            return Err(BlueprintError::ArgumentError {
                                message: format!("union() takes exactly 1 argument ({} given)", args.len()),
                            });
                        }
                        let set = s.read().await;
                        let other = match &args[0] {
                            Value::Set(other) => other.read().await.clone(),
                            Value::List(l) => l.read().await.iter().cloned().collect(),
                            Value::Tuple(t) => t.iter().cloned().collect(),
                            _ => return Err(BlueprintError::TypeError {
                                expected: "set, list, or tuple".into(),
                                actual: args[0].type_name().into(),
                            }),
                        };
                        let result: IndexSet<Value> = set.union(&other).cloned().collect();
                        Ok(Value::Set(Arc::new(RwLock::new(result))))
                    })
                },
            ))))
        }
        "intersection" => {
            let s_clone = s.clone();
            Some(Value::NativeFunction(Arc::new(NativeFunction::new_with_state(
                "intersection",
                move |args, _kwargs| {
                    let s = s_clone.clone();
                    Box::pin(async move {
                        if args.len() != 1 {
                            return Err(BlueprintError::ArgumentError {
                                message: format!("intersection() takes exactly 1 argument ({} given)", args.len()),
                            });
                        }
                        let set = s.read().await;
                        let other = match &args[0] {
                            Value::Set(other) => other.read().await.clone(),
                            Value::List(l) => l.read().await.iter().cloned().collect(),
                            Value::Tuple(t) => t.iter().cloned().collect(),
                            _ => return Err(BlueprintError::TypeError {
                                expected: "set, list, or tuple".into(),
                                actual: args[0].type_name().into(),
                            }),
                        };
                        let result: IndexSet<Value> = set.intersection(&other).cloned().collect();
                        Ok(Value::Set(Arc::new(RwLock::new(result))))
                    })
                },
            ))))
        }
        "difference" => {
            let s_clone = s.clone();
            Some(Value::NativeFunction(Arc::new(NativeFunction::new_with_state(
                "difference",
                move |args, _kwargs| {
                    let s = s_clone.clone();
                    Box::pin(async move {
                        if args.len() != 1 {
                            return Err(BlueprintError::ArgumentError {
                                message: format!("difference() takes exactly 1 argument ({} given)", args.len()),
                            });
                        }
                        let set = s.read().await;
                        let other = match &args[0] {
                            Value::Set(other) => other.read().await.clone(),
                            Value::List(l) => l.read().await.iter().cloned().collect(),
                            Value::Tuple(t) => t.iter().cloned().collect(),
                            _ => return Err(BlueprintError::TypeError {
                                expected: "set, list, or tuple".into(),
                                actual: args[0].type_name().into(),
                            }),
                        };
                        let result: IndexSet<Value> = set.difference(&other).cloned().collect();
                        Ok(Value::Set(Arc::new(RwLock::new(result))))
                    })
                },
            ))))
        }
        "symmetric_difference" => {
            let s_clone = s.clone();
            Some(Value::NativeFunction(Arc::new(NativeFunction::new_with_state(
                "symmetric_difference",
                move |args, _kwargs| {
                    let s = s_clone.clone();
                    Box::pin(async move {
                        if args.len() != 1 {
                            return Err(BlueprintError::ArgumentError {
                                message: format!("symmetric_difference() takes exactly 1 argument ({} given)", args.len()),
                            });
                        }
                        let set = s.read().await;
                        let other = match &args[0] {
                            Value::Set(other) => other.read().await.clone(),
                            Value::List(l) => l.read().await.iter().cloned().collect(),
                            Value::Tuple(t) => t.iter().cloned().collect(),
                            _ => return Err(BlueprintError::TypeError {
                                expected: "set, list, or tuple".into(),
                                actual: args[0].type_name().into(),
                            }),
                        };
                        let result: IndexSet<Value> = set.symmetric_difference(&other).cloned().collect();
                        Ok(Value::Set(Arc::new(RwLock::new(result))))
                    })
                },
            ))))
        }
        "issubset" => {
            let s_clone = s.clone();
            Some(Value::NativeFunction(Arc::new(NativeFunction::new_with_state(
                "issubset",
                move |args, _kwargs| {
                    let s = s_clone.clone();
                    Box::pin(async move {
                        if args.len() != 1 {
                            return Err(BlueprintError::ArgumentError {
                                message: format!("issubset() takes exactly 1 argument ({} given)", args.len()),
                            });
                        }
                        let set = s.read().await;
                        let other = match &args[0] {
                            Value::Set(other) => other.read().await.clone(),
                            Value::List(l) => l.read().await.iter().cloned().collect(),
                            Value::Tuple(t) => t.iter().cloned().collect(),
                            _ => return Err(BlueprintError::TypeError {
                                expected: "set, list, or tuple".into(),
                                actual: args[0].type_name().into(),
                            }),
                        };
                        Ok(Value::Bool(set.is_subset(&other)))
                    })
                },
            ))))
        }
        "issuperset" => {
            let s_clone = s.clone();
            Some(Value::NativeFunction(Arc::new(NativeFunction::new_with_state(
                "issuperset",
                move |args, _kwargs| {
                    let s = s_clone.clone();
                    Box::pin(async move {
                        if args.len() != 1 {
                            return Err(BlueprintError::ArgumentError {
                                message: format!("issuperset() takes exactly 1 argument ({} given)", args.len()),
                            });
                        }
                        let set = s.read().await;
                        let other = match &args[0] {
                            Value::Set(other) => other.read().await.clone(),
                            Value::List(l) => l.read().await.iter().cloned().collect(),
                            Value::Tuple(t) => t.iter().cloned().collect(),
                            _ => return Err(BlueprintError::TypeError {
                                expected: "set, list, or tuple".into(),
                                actual: args[0].type_name().into(),
                            }),
                        };
                        Ok(Value::Bool(set.is_superset(&other)))
                    })
                },
            ))))
        }
        "isdisjoint" => {
            let s_clone = s.clone();
            Some(Value::NativeFunction(Arc::new(NativeFunction::new_with_state(
                "isdisjoint",
                move |args, _kwargs| {
                    let s = s_clone.clone();
                    Box::pin(async move {
                        if args.len() != 1 {
                            return Err(BlueprintError::ArgumentError {
                                message: format!("isdisjoint() takes exactly 1 argument ({} given)", args.len()),
                            });
                        }
                        let set = s.read().await;
                        let other = match &args[0] {
                            Value::Set(other) => other.read().await.clone(),
                            Value::List(l) => l.read().await.iter().cloned().collect(),
                            Value::Tuple(t) => t.iter().cloned().collect(),
                            _ => return Err(BlueprintError::TypeError {
                                expected: "set, list, or tuple".into(),
                                actual: args[0].type_name().into(),
                            }),
                        };
                        Ok(Value::Bool(set.is_disjoint(&other)))
                    })
                },
            ))))
        }
        "update" => {
            let s_clone = s.clone();
            Some(Value::NativeFunction(Arc::new(NativeFunction::new_with_state(
                "update",
                move |args, _kwargs| {
                    let s = s_clone.clone();
                    Box::pin(async move {
                        if args.len() != 1 {
                            return Err(BlueprintError::ArgumentError {
                                message: format!("update() takes exactly 1 argument ({} given)", args.len()),
                            });
                        }
                        let other = match &args[0] {
                            Value::Set(other) => other.read().await.clone(),
                            Value::List(l) => l.read().await.iter().cloned().collect(),
                            Value::Tuple(t) => t.iter().cloned().collect(),
                            _ => return Err(BlueprintError::TypeError {
                                expected: "set, list, or tuple".into(),
                                actual: args[0].type_name().into(),
                            }),
                        };
                        let mut set = s.write().await;
                        set.extend(other);
                        Ok(Value::None)
                    })
                },
            ))))
        }
        _ => None,
    }
}
