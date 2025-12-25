use std::collections::HashMap;

use blueprint_engine_core::{BlueprintError, Result, Value};

pub async fn fail(args: Vec<Value>, _kwargs: HashMap<String, Value>) -> Result<Value> {
    let message = if args.is_empty() {
        "fail".to_string()
    } else {
        args[0].to_display_string()
    };

    Err(BlueprintError::UserError { message })
}

pub async fn exit(args: Vec<Value>, _kwargs: HashMap<String, Value>) -> Result<Value> {
    let code = if args.is_empty() {
        0
    } else if args.len() == 1 {
        match &args[0] {
            Value::Int(n) => *n as i32,
            other => {
                return Err(BlueprintError::TypeError {
                    expected: "int".into(),
                    actual: other.type_name().into(),
                });
            }
        }
    } else {
        return Err(BlueprintError::ArgumentError {
            message: format!("exit() takes 0 or 1 argument ({} given)", args.len()),
        });
    };

    Err(BlueprintError::Exit { code })
}

pub async fn assert_fn(args: Vec<Value>, _kwargs: HashMap<String, Value>) -> Result<Value> {
    if args.is_empty() || args.len() > 2 {
        return Err(BlueprintError::ArgumentError {
            message: format!("assert() takes 1 or 2 arguments ({} given)", args.len()),
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
