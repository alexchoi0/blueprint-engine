use std::sync::Arc;

use crate::error::BlueprintError;
use crate::value::{NativeFunction, Value};

pub fn get_string_method(s: Arc<String>, name: &str) -> Option<Value> {
    let s_clone = s.clone();
    match name {
        "upper" => Some(Value::NativeFunction(Arc::new(
            NativeFunction::new_with_state("upper", move |_args, _kwargs| {
                let result = s_clone.to_uppercase();
                Box::pin(async move { Ok(Value::String(Arc::new(result))) })
            }),
        ))),
        "lower" => {
            let s = s.clone();
            Some(Value::NativeFunction(Arc::new(
                NativeFunction::new_with_state("lower", move |_args, _kwargs| {
                    let result = s.to_lowercase();
                    Box::pin(async move { Ok(Value::String(Arc::new(result))) })
                }),
            )))
        }
        "strip" => {
            let s = s.clone();
            Some(Value::NativeFunction(Arc::new(
                NativeFunction::new_with_state("strip", move |_args, _kwargs| {
                    let result = s.trim().to_string();
                    Box::pin(async move { Ok(Value::String(Arc::new(result))) })
                }),
            )))
        }
        "split" => {
            let s = s.clone();
            Some(Value::NativeFunction(Arc::new(
                NativeFunction::new_with_state("split", move |args, _kwargs| {
                    let sep = if args.is_empty() {
                        None
                    } else {
                        Some(args[0].to_display_string())
                    };
                    let parts: Vec<Value> = match sep {
                        Some(ref sep) => s
                            .split(sep.as_str())
                            .map(|p| Value::String(Arc::new(p.to_string())))
                            .collect(),
                        None => s
                            .split_whitespace()
                            .map(|p| Value::String(Arc::new(p.to_string())))
                            .collect(),
                    };
                    Box::pin(
                        async move { Ok(Value::List(Arc::new(tokio::sync::RwLock::new(parts)))) },
                    )
                }),
            )))
        }
        "join" => {
            let s = s.clone();
            Some(Value::NativeFunction(Arc::new(
                NativeFunction::new_with_state("join", move |args, _kwargs| {
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
                            _ => {
                                return Err(BlueprintError::TypeError {
                                    expected: "list or tuple".into(),
                                    actual: args[0].type_name().into(),
                                })
                            }
                        };
                        let strings: Vec<String> =
                            items.iter().map(|v| v.to_display_string()).collect();
                        Ok(Value::String(Arc::new(strings.join(s.as_str()))))
                    })
                }),
            )))
        }
        "replace" => {
            let s = s.clone();
            Some(Value::NativeFunction(Arc::new(
                NativeFunction::new_with_state("replace", move |args, _kwargs| {
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
                }),
            )))
        }
        "startswith" => {
            let s = s.clone();
            Some(Value::NativeFunction(Arc::new(
                NativeFunction::new_with_state("startswith", move |args, _kwargs| {
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
                }),
            )))
        }
        "endswith" => {
            let s = s.clone();
            Some(Value::NativeFunction(Arc::new(
                NativeFunction::new_with_state("endswith", move |args, _kwargs| {
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
                }),
            )))
        }
        "find" => {
            let s = s.clone();
            Some(Value::NativeFunction(Arc::new(
                NativeFunction::new_with_state("find", move |args, _kwargs| {
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
                }),
            )))
        }
        "format" => {
            let s = s.clone();
            Some(Value::NativeFunction(Arc::new(
                NativeFunction::new_with_state("format", move |args, _kwargs| {
                    let s = s.clone();
                    Box::pin(async move {
                        let mut result = s.as_str().to_string();
                        for arg in args {
                            if let Some(pos) = result.find("{}") {
                                result = format!(
                                    "{}{}{}",
                                    &result[..pos],
                                    arg.to_display_string(),
                                    &result[pos + 2..]
                                );
                            }
                        }
                        Ok(Value::String(Arc::new(result)))
                    })
                }),
            )))
        }
        _ => None,
    }
}
