use std::sync::Arc;

use blueprint_engine_core::{BlueprintError, Result, Value};
use blueprint_starlark_syntax::syntax::ast::BinOp;
use blueprint_engine_parser::AssignOp;

pub fn eval_unary_minus(value: Value) -> Result<Value> {
    match value {
        Value::Int(i) => Ok(Value::Int(-i)),
        Value::Float(f) => Ok(Value::Float(-f)),
        _ => Err(BlueprintError::TypeError {
            expected: "number".into(),
            actual: value.type_name().into(),
        }),
    }
}

pub async fn eval_binary_op(left: Value, op: BinOp, right: Value) -> Result<Value> {
    match op {
        BinOp::Add => eval_add(left, right).await,
        BinOp::Subtract => eval_sub(left, right),
        BinOp::Multiply => eval_mul(left, right),
        BinOp::Divide => eval_div(left, right),
        BinOp::FloorDivide => eval_floor_div(left, right),
        BinOp::Percent => eval_mod(left, right),
        BinOp::Equal => Ok(Value::Bool(left == right)),
        BinOp::NotEqual => Ok(Value::Bool(left != right)),
        BinOp::Less => eval_compare(left, right, |o| o.is_lt()),
        BinOp::LessOrEqual => eval_compare(left, right, |o| o.is_le()),
        BinOp::Greater => eval_compare(left, right, |o| o.is_gt()),
        BinOp::GreaterOrEqual => eval_compare(left, right, |o| o.is_ge()),
        BinOp::In | BinOp::NotIn => unreachable!("handled in eval_expr"),
        BinOp::BitAnd => eval_bit_and(left, right),
        BinOp::BitOr => eval_bit_or(left, right),
        BinOp::BitXor => eval_bit_xor(left, right),
        BinOp::LeftShift => eval_left_shift(left, right),
        BinOp::RightShift => eval_right_shift(left, right),
        BinOp::And | BinOp::Or => unreachable!("Short-circuit handled above"),
    }
}

pub async fn eval_add(left: Value, right: Value) -> Result<Value> {
    match (&left, &right) {
        (Value::Int(a), Value::Int(b)) => Ok(Value::Int(a + b)),
        (Value::Float(a), Value::Float(b)) => Ok(Value::Float(a + b)),
        (Value::Int(a), Value::Float(b)) => Ok(Value::Float(*a as f64 + b)),
        (Value::Float(a), Value::Int(b)) => Ok(Value::Float(a + *b as f64)),
        (Value::String(a), Value::String(b)) => {
            Ok(Value::String(Arc::new(format!("{}{}", a, b))))
        }
        (Value::List(a), Value::List(b)) => {
            let mut result = a.read().await.clone();
            result.extend(b.read().await.iter().cloned());
            Ok(Value::List(Arc::new(tokio::sync::RwLock::new(result))))
        }
        _ => Err(BlueprintError::TypeError {
            expected: format!("compatible types for +"),
            actual: format!("{} and {}", left.type_name(), right.type_name()),
        }),
    }
}

pub fn eval_sub(left: Value, right: Value) -> Result<Value> {
    match (&left, &right) {
        (Value::Int(a), Value::Int(b)) => Ok(Value::Int(a - b)),
        (Value::Float(a), Value::Float(b)) => Ok(Value::Float(a - b)),
        (Value::Int(a), Value::Float(b)) => Ok(Value::Float(*a as f64 - b)),
        (Value::Float(a), Value::Int(b)) => Ok(Value::Float(a - *b as f64)),
        _ => Err(BlueprintError::TypeError {
            expected: "numbers".into(),
            actual: format!("{} and {}", left.type_name(), right.type_name()),
        }),
    }
}

pub fn eval_mul(left: Value, right: Value) -> Result<Value> {
    match (&left, &right) {
        (Value::Int(a), Value::Int(b)) => Ok(Value::Int(a * b)),
        (Value::Float(a), Value::Float(b)) => Ok(Value::Float(a * b)),
        (Value::Int(a), Value::Float(b)) => Ok(Value::Float(*a as f64 * b)),
        (Value::Float(a), Value::Int(b)) => Ok(Value::Float(a * *b as f64)),
        (Value::String(s), Value::Int(n)) | (Value::Int(n), Value::String(s)) => {
            if *n <= 0 {
                Ok(Value::String(Arc::new(String::new())))
            } else {
                Ok(Value::String(Arc::new(s.repeat(*n as usize))))
            }
        }
        (Value::List(l), Value::Int(n)) | (Value::Int(n), Value::List(l)) => {
            if *n <= 0 {
                Ok(Value::List(Arc::new(tokio::sync::RwLock::new(vec![]))))
            } else {
                let items = l.blocking_read();
                let mut result = Vec::with_capacity(items.len() * (*n as usize));
                for _ in 0..*n {
                    result.extend(items.iter().cloned());
                }
                Ok(Value::List(Arc::new(tokio::sync::RwLock::new(result))))
            }
        }
        _ => Err(BlueprintError::TypeError {
            expected: "compatible types for *".into(),
            actual: format!("{} and {}", left.type_name(), right.type_name()),
        }),
    }
}

pub fn eval_div(left: Value, right: Value) -> Result<Value> {
    match (&left, &right) {
        (Value::Int(a), Value::Int(b)) => {
            if *b == 0 {
                Err(BlueprintError::DivisionByZero)
            } else {
                Ok(Value::Float(*a as f64 / *b as f64))
            }
        }
        (Value::Float(a), Value::Float(b)) => {
            if *b == 0.0 {
                Err(BlueprintError::DivisionByZero)
            } else {
                Ok(Value::Float(a / b))
            }
        }
        (Value::Int(a), Value::Float(b)) => {
            if *b == 0.0 {
                Err(BlueprintError::DivisionByZero)
            } else {
                Ok(Value::Float(*a as f64 / b))
            }
        }
        (Value::Float(a), Value::Int(b)) => {
            if *b == 0 {
                Err(BlueprintError::DivisionByZero)
            } else {
                Ok(Value::Float(a / *b as f64))
            }
        }
        _ => Err(BlueprintError::TypeError {
            expected: "numbers".into(),
            actual: format!("{} and {}", left.type_name(), right.type_name()),
        }),
    }
}

pub fn eval_floor_div(left: Value, right: Value) -> Result<Value> {
    match (&left, &right) {
        (Value::Int(a), Value::Int(b)) => {
            if *b == 0 {
                Err(BlueprintError::DivisionByZero)
            } else {
                Ok(Value::Int(a.div_euclid(*b)))
            }
        }
        (Value::Float(a), Value::Float(b)) => {
            if *b == 0.0 {
                Err(BlueprintError::DivisionByZero)
            } else {
                Ok(Value::Float((a / b).floor()))
            }
        }
        (Value::Int(a), Value::Float(b)) => {
            if *b == 0.0 {
                Err(BlueprintError::DivisionByZero)
            } else {
                Ok(Value::Float((*a as f64 / b).floor()))
            }
        }
        (Value::Float(a), Value::Int(b)) => {
            if *b == 0 {
                Err(BlueprintError::DivisionByZero)
            } else {
                Ok(Value::Float((a / *b as f64).floor()))
            }
        }
        _ => Err(BlueprintError::TypeError {
            expected: "numbers".into(),
            actual: format!("{} and {}", left.type_name(), right.type_name()),
        }),
    }
}

pub fn eval_mod(left: Value, right: Value) -> Result<Value> {
    match (&left, &right) {
        (Value::Int(a), Value::Int(b)) => {
            if *b == 0 {
                Err(BlueprintError::DivisionByZero)
            } else {
                Ok(Value::Int(a.rem_euclid(*b)))
            }
        }
        (Value::Float(a), Value::Float(b)) => {
            if *b == 0.0 {
                Err(BlueprintError::DivisionByZero)
            } else {
                Ok(Value::Float(a.rem_euclid(*b)))
            }
        }
        (Value::String(fmt), _) => format_string(fmt, &right),
        _ => Err(BlueprintError::TypeError {
            expected: "numbers or string formatting".into(),
            actual: format!("{} and {}", left.type_name(), right.type_name()),
        }),
    }
}

pub fn format_string(fmt: &str, args: &Value) -> Result<Value> {
    let arg_list = match args {
        Value::Tuple(t) => t.as_ref().clone(),
        other => vec![other.clone()],
    };

    let mut result = String::new();
    let mut arg_idx = 0;
    let mut chars = fmt.chars().peekable();

    while let Some(c) = chars.next() {
        if c == '%' {
            if chars.peek() == Some(&'%') {
                chars.next();
                result.push('%');
            } else {
                while chars.peek().map(|c| c.is_ascii_digit() || *c == '-' || *c == '+' || *c == ' ' || *c == '.').unwrap_or(false) {
                    chars.next();
                }

                let spec = chars.next().ok_or_else(|| BlueprintError::ValueError {
                    message: "incomplete format".into(),
                })?;

                if arg_idx >= arg_list.len() {
                    return Err(BlueprintError::ValueError {
                        message: "not enough arguments for format string".into(),
                    });
                }

                let arg = &arg_list[arg_idx];
                arg_idx += 1;

                match spec {
                    's' => result.push_str(&arg.to_display_string()),
                    'd' | 'i' => {
                        let i = arg.as_int()?;
                        result.push_str(&i.to_string());
                    }
                    'f' => {
                        let f = arg.as_float()?;
                        result.push_str(&f.to_string());
                    }
                    'r' => result.push_str(&arg.repr()),
                    _ => {
                        return Err(BlueprintError::ValueError {
                            message: format!("unsupported format character: {}", spec),
                        })
                    }
                }
            }
        } else {
            result.push(c);
        }
    }

    Ok(Value::String(Arc::new(result)))
}

pub fn eval_compare<F>(left: Value, right: Value, cmp: F) -> Result<Value>
where
    F: Fn(std::cmp::Ordering) -> bool,
{
    let ordering = match (&left, &right) {
        (Value::Int(a), Value::Int(b)) => a.cmp(b),
        (Value::Float(a), Value::Float(b)) => a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal),
        (Value::Int(a), Value::Float(b)) => (*a as f64).partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal),
        (Value::Float(a), Value::Int(b)) => a.partial_cmp(&(*b as f64)).unwrap_or(std::cmp::Ordering::Equal),
        (Value::String(a), Value::String(b)) => a.cmp(b),
        _ => {
            return Err(BlueprintError::TypeError {
                expected: "comparable types".into(),
                actual: format!("{} and {}", left.type_name(), right.type_name()),
            })
        }
    };
    Ok(Value::Bool(cmp(ordering)))
}

pub async fn eval_in(left: Value, right: Value, value_to_dict_key: impl Fn(&Value) -> Result<String>) -> Result<Value> {
    match &right {
        Value::List(l) => {
            let items = l.read().await;
            Ok(Value::Bool(items.iter().any(|item| *item == left)))
        }
        Value::Dict(d) => {
            let key = value_to_dict_key(&left)?;
            let map = d.read().await;
            Ok(Value::Bool(map.contains_key(&key)))
        }
        Value::String(s) => {
            let needle = left.as_string()?;
            Ok(Value::Bool(s.contains(&needle)))
        }
        Value::Tuple(t) => Ok(Value::Bool(t.iter().any(|item| *item == left))),
        Value::Set(s) => {
            let set = s.read().await;
            Ok(Value::Bool(set.contains(&left)))
        }
        _ => Err(BlueprintError::TypeError {
            expected: "iterable".into(),
            actual: right.type_name().into(),
        }),
    }
}

pub fn eval_bit_and(left: Value, right: Value) -> Result<Value> {
    match (&left, &right) {
        (Value::Int(a), Value::Int(b)) => Ok(Value::Int(a & b)),
        _ => Err(BlueprintError::TypeError {
            expected: "integers".into(),
            actual: format!("{} and {}", left.type_name(), right.type_name()),
        }),
    }
}

pub fn eval_bit_or(left: Value, right: Value) -> Result<Value> {
    match (&left, &right) {
        (Value::Int(a), Value::Int(b)) => Ok(Value::Int(a | b)),
        _ => Err(BlueprintError::TypeError {
            expected: "integers".into(),
            actual: format!("{} and {}", left.type_name(), right.type_name()),
        }),
    }
}

pub fn eval_bit_xor(left: Value, right: Value) -> Result<Value> {
    match (&left, &right) {
        (Value::Int(a), Value::Int(b)) => Ok(Value::Int(a ^ b)),
        _ => Err(BlueprintError::TypeError {
            expected: "integers".into(),
            actual: format!("{} and {}", left.type_name(), right.type_name()),
        }),
    }
}

pub fn eval_left_shift(left: Value, right: Value) -> Result<Value> {
    match (&left, &right) {
        (Value::Int(a), Value::Int(b)) => {
            if *b < 0 {
                Err(BlueprintError::ValueError {
                    message: "negative shift count".into(),
                })
            } else {
                Ok(Value::Int(a << b))
            }
        }
        _ => Err(BlueprintError::TypeError {
            expected: "integers".into(),
            actual: format!("{} and {}", left.type_name(), right.type_name()),
        }),
    }
}

pub fn eval_right_shift(left: Value, right: Value) -> Result<Value> {
    match (&left, &right) {
        (Value::Int(a), Value::Int(b)) => {
            if *b < 0 {
                Err(BlueprintError::ValueError {
                    message: "negative shift count".into(),
                })
            } else {
                Ok(Value::Int(a >> b))
            }
        }
        _ => Err(BlueprintError::TypeError {
            expected: "integers".into(),
            actual: format!("{} and {}", left.type_name(), right.type_name()),
        }),
    }
}

pub async fn apply_assign_op(op: AssignOp, left: Value, right: Value) -> Result<Value> {
    match op {
        AssignOp::Add => eval_add(left, right).await,
        AssignOp::Subtract => eval_sub(left, right),
        AssignOp::Multiply => eval_mul(left, right),
        AssignOp::Divide => eval_div(left, right),
        AssignOp::FloorDivide => eval_floor_div(left, right),
        AssignOp::Percent => eval_mod(left, right),
        AssignOp::BitAnd => eval_bit_and(left, right),
        AssignOp::BitOr => eval_bit_or(left, right),
        AssignOp::BitXor => eval_bit_xor(left, right),
        AssignOp::LeftShift => eval_left_shift(left, right),
        AssignOp::RightShift => eval_right_shift(left, right),
    }
}
