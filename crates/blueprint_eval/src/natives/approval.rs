use std::collections::HashMap;
use indexmap::IndexMap;
use std::io::{self, BufRead, Write};
use std::sync::Arc;
use std::time::Duration;

use blueprint_core::{BlueprintError, NativeFunction, Result, Value};
use tokio::sync::RwLock;
use tokio::time::timeout;

use crate::eval::Evaluator;

pub fn register(evaluator: &mut Evaluator) {
    evaluator.register_native(NativeFunction::new("ask_for_approval", ask_for_approval));
}

async fn ask_for_approval(args: Vec<Value>, kwargs: HashMap<String, Value>) -> Result<Value> {
    if args.is_empty() || args.len() > 1 {
        return Err(BlueprintError::ArgumentError {
            message: format!("ask_for_approval() takes 1 argument ({} given)", args.len()),
        });
    }

    let prompt = args[0].as_string()?;

    let method = kwargs
        .get("method")
        .map(|v| v.as_string())
        .transpose()?
        .unwrap_or_else(|| "terminal".to_string());

    let timeout_secs = kwargs
        .get("timeout")
        .map(|v| v.as_float())
        .transpose()?;

    match method.as_str() {
        "terminal" => ask_terminal(&prompt, timeout_secs).await,
        _ => Err(BlueprintError::ArgumentError {
            message: format!(
                "Unknown approval method '{}'. Supported: terminal",
                method
            ),
        }),
    }
}

async fn ask_terminal(prompt: &str, timeout_secs: Option<f64>) -> Result<Value> {
    print!("\n🔐 APPROVAL REQUIRED: {}\n", prompt);
    print!("   Continue? [y/N]: ");
    io::stdout().flush().map_err(|e| BlueprintError::IoError {
        path: "stdout".into(),
        message: e.to_string(),
    })?;

    let read_input = tokio::task::spawn_blocking(|| {
        let stdin = io::stdin();
        let mut line = String::new();
        stdin.lock().read_line(&mut line).ok();
        line.trim().to_lowercase()
    });

    let response = if let Some(secs) = timeout_secs {
        match timeout(Duration::from_secs_f64(secs), read_input).await {
            Ok(Ok(resp)) => resp,
            Ok(Err(_)) => {
                println!("\n   ❌ Input error");
                return Ok(build_response(false, "input_error", None));
            }
            Err(_) => {
                println!("\n   ⏰ Approval timed out after {}s", secs);
                return Ok(build_response(false, "timeout", Some(secs)));
            }
        }
    } else {
        read_input.await.map_err(|e| BlueprintError::InternalError {
            message: e.to_string(),
        })?
    };

    let approved = matches!(response.as_str(), "y" | "yes");

    if approved {
        println!("   ✅ Approved");
    } else {
        println!("   ❌ Denied");
    }

    Ok(build_response(approved, "terminal", None))
}

fn build_response(approved: bool, method: &str, timeout_secs: Option<f64>) -> Value {
    let mut result = IndexMap::new();
    result.insert("approved".to_string(), Value::Bool(approved));
    result.insert("method".to_string(), Value::String(Arc::new(method.to_string())));

    if let Some(t) = timeout_secs {
        result.insert("timeout".to_string(), Value::Float(t));
    }

    Value::Dict(Arc::new(RwLock::new(result)))
}
