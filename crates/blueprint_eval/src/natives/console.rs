use std::collections::HashMap;
use std::sync::Arc;

use blueprint_core::{BlueprintError, NativeFunction, Result, Value};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

use crate::eval::Evaluator;

pub fn register(evaluator: &mut Evaluator) {
    evaluator.register_native(NativeFunction::new("print", print));
    evaluator.register_native(NativeFunction::new("eprint", eprint));
    evaluator.register_native(NativeFunction::new("input", input));
}

async fn print(args: Vec<Value>, kwargs: HashMap<String, Value>) -> Result<Value> {
    let sep = kwargs
        .get("sep")
        .map(|v| v.to_display_string())
        .unwrap_or_else(|| " ".to_string());

    let end = kwargs
        .get("end")
        .map(|v| v.to_display_string())
        .unwrap_or_else(|| "\n".to_string());

    let output: String = args
        .iter()
        .map(|v| v.to_display_string())
        .collect::<Vec<_>>()
        .join(&sep);

    let mut stdout = tokio::io::stdout();
    stdout.write_all(output.as_bytes()).await.ok();
    stdout.write_all(end.as_bytes()).await.ok();
    stdout.flush().await.ok();

    Ok(Value::None)
}

async fn eprint(args: Vec<Value>, kwargs: HashMap<String, Value>) -> Result<Value> {
    let sep = kwargs
        .get("sep")
        .map(|v| v.to_display_string())
        .unwrap_or_else(|| " ".to_string());

    let end = kwargs
        .get("end")
        .map(|v| v.to_display_string())
        .unwrap_or_else(|| "\n".to_string());

    let output: String = args
        .iter()
        .map(|v| v.to_display_string())
        .collect::<Vec<_>>()
        .join(&sep);

    let mut stderr = tokio::io::stderr();
    stderr.write_all(output.as_bytes()).await.ok();
    stderr.write_all(end.as_bytes()).await.ok();
    stderr.flush().await.ok();

    Ok(Value::None)
}

async fn input(args: Vec<Value>, _kwargs: HashMap<String, Value>) -> Result<Value> {
    if args.len() > 1 {
        return Err(BlueprintError::ArgumentError {
            message: format!("input() takes at most 1 argument ({} given)", args.len()),
        });
    }

    if !args.is_empty() {
        let prompt = args[0].to_display_string();
        let mut stdout = tokio::io::stdout();
        stdout.write_all(prompt.as_bytes()).await.ok();
        stdout.flush().await.ok();
    }

    let stdin = tokio::io::stdin();
    let mut reader = BufReader::new(stdin);
    let mut line = String::new();

    reader
        .read_line(&mut line)
        .await
        .map_err(|e| BlueprintError::IoError {
            path: "stdin".into(),
            message: e.to_string(),
        })?;

    if line.ends_with('\n') {
        line.pop();
        if line.ends_with('\r') {
            line.pop();
        }
    }

    Ok(Value::String(Arc::new(line)))
}
