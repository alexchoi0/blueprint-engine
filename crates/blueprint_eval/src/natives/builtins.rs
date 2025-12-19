use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

use blueprint_core::{BlueprintError, NativeFunction, Result, Value};

use crate::eval::Evaluator;

pub fn register(evaluator: &mut Evaluator) {
    evaluator.register_native(NativeFunction::new("len", len));
    evaluator.register_native(NativeFunction::new("str", to_str));
    evaluator.register_native(NativeFunction::new("int", to_int));
    evaluator.register_native(NativeFunction::new("float", to_float));
    evaluator.register_native(NativeFunction::new("bool", to_bool));
    evaluator.register_native(NativeFunction::new("list", to_list));
    evaluator.register_native(NativeFunction::new("dict", to_dict));
    evaluator.register_native(NativeFunction::new("tuple", to_tuple));
    evaluator.register_native(NativeFunction::new("range", range));
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

async fn enumerate(args: Vec<Value>, _kwargs: HashMap<String, Value>) -> Result<Value> {
    if args.is_empty() || args.len() > 2 {
        return Err(BlueprintError::ArgumentError {
            message: format!("enumerate() takes 1 or 2 arguments ({} given)", args.len()),
        });
    }

    let start = if args.len() == 2 { args[1].as_int()? } else { 0 };

    let items = match &args[0] {
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
