use std::sync::Arc;

use indexmap::IndexMap;
use tokio::sync::RwLock;

use crate::error::BlueprintError;
use crate::value::{NativeFunction, Value};

pub fn get_dict_method(d: Arc<RwLock<IndexMap<String, Value>>>, name: &str) -> Option<Value> {
    match name {
        "get" => {
            let d_clone = d.clone();
            Some(Value::NativeFunction(Arc::new(
                NativeFunction::new_with_state("get", move |args, _kwargs| {
                    let d = d_clone.clone();
                    Box::pin(async move {
                        if args.is_empty() || args.len() > 2 {
                            return Err(BlueprintError::ArgumentError {
                                message: format!(
                                    "get() takes 1 or 2 arguments ({} given)",
                                    args.len()
                                ),
                            });
                        }
                        let key = match &args[0] {
                            Value::String(s) => s.as_ref().clone(),
                            v => {
                                return Err(BlueprintError::TypeError {
                                    expected: "string".into(),
                                    actual: v.type_name().into(),
                                })
                            }
                        };
                        let default = if args.len() == 2 {
                            args[1].clone()
                        } else {
                            Value::None
                        };
                        let map = d.read().await;
                        Ok(map.get(&key).cloned().unwrap_or(default))
                    })
                }),
            )))
        }
        "keys" => {
            let d_clone = d.clone();
            Some(Value::NativeFunction(Arc::new(
                NativeFunction::new_with_state("keys", move |_args, _kwargs| {
                    let d = d_clone.clone();
                    Box::pin(async move {
                        let map = d.read().await;
                        let keys: Vec<Value> = map
                            .keys()
                            .map(|k| Value::String(Arc::new(k.clone())))
                            .collect();
                        Ok(Value::List(Arc::new(RwLock::new(keys))))
                    })
                }),
            )))
        }
        "values" => {
            let d_clone = d.clone();
            Some(Value::NativeFunction(Arc::new(
                NativeFunction::new_with_state("values", move |_args, _kwargs| {
                    let d = d_clone.clone();
                    Box::pin(async move {
                        let map = d.read().await;
                        let values: Vec<Value> = map.values().cloned().collect();
                        Ok(Value::List(Arc::new(RwLock::new(values))))
                    })
                }),
            )))
        }
        "items" => {
            let d_clone = d.clone();
            Some(Value::NativeFunction(Arc::new(
                NativeFunction::new_with_state("items", move |_args, _kwargs| {
                    let d = d_clone.clone();
                    Box::pin(async move {
                        let map = d.read().await;
                        let items: Vec<Value> = map
                            .iter()
                            .map(|(k, v)| {
                                Value::Tuple(Arc::new(vec![
                                    Value::String(Arc::new(k.clone())),
                                    v.clone(),
                                ]))
                            })
                            .collect();
                        Ok(Value::List(Arc::new(RwLock::new(items))))
                    })
                }),
            )))
        }
        _ => None,
    }
}
