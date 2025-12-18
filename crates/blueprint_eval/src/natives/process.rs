use std::collections::HashMap;
use std::sync::Arc;

use blueprint_core::{BlueprintError, NativeFunction, ProcessResult, Result, Value};
use tokio::process::Command;

use crate::eval::Evaluator;

pub fn register(evaluator: &mut Evaluator) {
    evaluator.register_native(NativeFunction::new("run", run));
    evaluator.register_native(NativeFunction::new("shell", shell));
    evaluator.register_native(NativeFunction::new("env", env_var));
    evaluator.register_native(NativeFunction::new("set_env", set_env));
    evaluator.register_native(NativeFunction::new("getenv", env_var));
    evaluator.register_native(NativeFunction::new("setenv", set_env));
}

async fn run(args: Vec<Value>, kwargs: HashMap<String, Value>) -> Result<Value> {
    if args.len() != 1 {
        return Err(BlueprintError::ArgumentError {
            message: format!("run() takes exactly 1 argument ({} given)", args.len()),
        });
    }

    let cmd_args = match &args[0] {
        Value::List(l) => {
            let items = l.read().await;
            let mut strs = Vec::new();
            for item in items.iter() {
                strs.push(item.to_display_string());
            }
            strs
        }
        Value::String(s) => {
            return shell_impl(s.as_ref(), &kwargs).await;
        }
        other => {
            return Err(BlueprintError::TypeError {
                expected: "list or string".into(),
                actual: other.type_name().into(),
            })
        }
    };

    if cmd_args.is_empty() {
        return Err(BlueprintError::ArgumentError {
            message: "run() requires at least one command argument".into(),
        });
    }

    let program = &cmd_args[0];
    let args_slice = &cmd_args[1..];

    let cwd = kwargs.get("cwd").map(|v| v.to_display_string());
    let env_vars = extract_env(&kwargs).await?;

    let mut command = Command::new(program);
    command.args(args_slice);

    if let Some(dir) = cwd {
        command.current_dir(dir);
    }

    for (key, value) in env_vars {
        command.env(key, value);
    }

    let output = command
        .output()
        .await
        .map_err(|e| BlueprintError::ProcessError {
            command: program.clone(),
            message: e.to_string(),
        })?;

    Ok(Value::ProcessResult(Arc::new(ProcessResult {
        code: output.status.code().unwrap_or(-1) as i64,
        stdout: String::from_utf8_lossy(&output.stdout).to_string(),
        stderr: String::from_utf8_lossy(&output.stderr).to_string(),
    })))
}

async fn shell(args: Vec<Value>, kwargs: HashMap<String, Value>) -> Result<Value> {
    if args.len() != 1 {
        return Err(BlueprintError::ArgumentError {
            message: format!("shell() takes exactly 1 argument ({} given)", args.len()),
        });
    }

    let cmd = args[0].as_string()?;
    shell_impl(&cmd, &kwargs).await
}

async fn shell_impl(cmd: &str, kwargs: &HashMap<String, Value>) -> Result<Value> {
    let shell = if cfg!(windows) { "cmd" } else { "sh" };
    let shell_arg = if cfg!(windows) { "/C" } else { "-c" };

    let cwd = kwargs.get("cwd").map(|v| v.to_display_string());
    let env_vars = extract_env(kwargs).await?;

    let mut command = Command::new(shell);
    command.arg(shell_arg).arg(cmd);

    if let Some(dir) = cwd {
        command.current_dir(dir);
    }

    for (key, value) in env_vars {
        command.env(key, value);
    }

    let output = command
        .output()
        .await
        .map_err(|e| BlueprintError::ProcessError {
            command: cmd.into(),
            message: e.to_string(),
        })?;

    Ok(Value::ProcessResult(Arc::new(ProcessResult {
        code: output.status.code().unwrap_or(-1) as i64,
        stdout: String::from_utf8_lossy(&output.stdout).to_string(),
        stderr: String::from_utf8_lossy(&output.stderr).to_string(),
    })))
}

async fn env_var(args: Vec<Value>, _kwargs: HashMap<String, Value>) -> Result<Value> {
    if args.is_empty() || args.len() > 2 {
        return Err(BlueprintError::ArgumentError {
            message: format!("env() takes 1 or 2 arguments ({} given)", args.len()),
        });
    }

    let name = args[0].as_string()?;
    let default = if args.len() == 2 {
        args[1].to_display_string()
    } else {
        String::new()
    };

    let value = std::env::var(&name).unwrap_or(default);
    Ok(Value::String(Arc::new(value)))
}

async fn set_env(args: Vec<Value>, _kwargs: HashMap<String, Value>) -> Result<Value> {
    if args.len() != 2 {
        return Err(BlueprintError::ArgumentError {
            message: format!("set_env() takes exactly 2 arguments ({} given)", args.len()),
        });
    }

    let name = args[0].as_string()?;
    let value = args[1].as_string()?;

    std::env::set_var(&name, &value);
    Ok(Value::None)
}

async fn extract_env(kwargs: &HashMap<String, Value>) -> Result<HashMap<String, String>> {
    let mut env_vars = HashMap::new();

    if let Some(env) = kwargs.get("env") {
        match env {
            Value::Dict(d) => {
                let map = d.read().await;
                for (k, v) in map.iter() {
                    env_vars.insert(k.clone(), v.to_display_string());
                }
            }
            _ => {
                return Err(BlueprintError::TypeError {
                    expected: "dict".into(),
                    actual: env.type_name().into(),
                })
            }
        }
    }

    Ok(env_vars)
}
