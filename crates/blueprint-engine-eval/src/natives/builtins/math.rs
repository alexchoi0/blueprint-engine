use std::collections::HashMap;

use blueprint_engine_core::{BlueprintError, Result, Value};

pub async fn min(args: Vec<Value>, _kwargs: HashMap<String, Value>) -> Result<Value> {
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

pub async fn max(args: Vec<Value>, _kwargs: HashMap<String, Value>) -> Result<Value> {
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

pub async fn sum(args: Vec<Value>, _kwargs: HashMap<String, Value>) -> Result<Value> {
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

pub async fn abs(args: Vec<Value>, _kwargs: HashMap<String, Value>) -> Result<Value> {
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

pub async fn all(args: Vec<Value>, _kwargs: HashMap<String, Value>) -> Result<Value> {
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

pub async fn any(args: Vec<Value>, _kwargs: HashMap<String, Value>) -> Result<Value> {
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
