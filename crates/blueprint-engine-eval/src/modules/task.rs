use indexmap::IndexMap;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use blueprint_engine_core::{
    validation::require_args,
    BlueprintError, NativeFunction, Result, Value,
};
use tokio::sync::RwLock;
use tokio::time::timeout;

use crate::eval::Evaluator;

pub fn get_functions() -> Vec<NativeFunction> {
    vec![NativeFunction::new("task", task_fn)]
}

async fn task_fn(args: Vec<Value>, kwargs: HashMap<String, Value>) -> Result<Value> {
    require_args("task.task", &args, 1)?;

    let max_wait = kwargs.get("max_wait").map(|v| v.as_float()).transpose()?;
    let wait_until = kwargs.get("wait_until").map(|v| v.as_float()).transpose()?;

    if max_wait.is_some() && wait_until.is_some() {
        return Err(BlueprintError::ArgumentError {
            message: "task() cannot have both max_wait and wait_until".into(),
        });
    }

    let timeout_duration = if let Some(secs) = max_wait {
        if secs < 0.0 {
            return Err(BlueprintError::ValueError {
                message: "max_wait must not be negative".into(),
            });
        }
        Some(Duration::from_secs_f64(secs))
    } else if let Some(timestamp) = wait_until {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs_f64();

        let remaining = timestamp - now;
        if remaining <= 0.0 {
            return Ok(build_result(Value::None, false, Some("deadline_passed")));
        }
        Some(Duration::from_secs_f64(remaining))
    } else {
        None
    };

    let func_value = args[0].clone();
    let start_time = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs_f64();

    let execution = execute_callable(func_value);

    let (result_value, timed_out) = if let Some(duration) = timeout_duration {
        match timeout(duration, execution).await {
            Ok(Ok(value)) => (value, false),
            Ok(Err(e)) => return Err(e),
            Err(_) => (Value::None, true),
        }
    } else {
        (execution.await?, false)
    };

    let end_time = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs_f64();

    let elapsed = end_time - start_time;

    Ok(build_result_with_timing(
        result_value,
        !timed_out,
        if timed_out { Some("timeout") } else { None },
        elapsed,
    ))
}

async fn execute_callable(func_value: Value) -> Result<Value> {
    match func_value {
        Value::NativeFunction(native) => native.call(vec![], HashMap::new()).await,
        Value::Lambda(lambda) => {
            let body = lambda
                .body
                .downcast_ref::<blueprint_engine_parser::AstExpr>()
                .ok_or_else(|| BlueprintError::InternalError {
                    message: "Invalid lambda body in task()".into(),
                })?;

            let closure_scope = lambda
                .closure
                .as_ref()
                .and_then(|c| c.downcast_ref::<Arc<crate::scope::Scope>>().cloned());

            let base_scope = closure_scope.unwrap_or_else(crate::scope::Scope::new_global);
            let call_scope =
                crate::scope::Scope::new_child(base_scope, crate::scope::ScopeKind::Function);

            for param in &lambda.params {
                if let Some(ref default) = param.default {
                    call_scope.define(&param.name, default.clone()).await;
                }
            }

            let evaluator = Evaluator::new();
            evaluator.eval_expr(body, call_scope).await
        }
        Value::Function(func) => {
            let body = func
                .body
                .downcast_ref::<blueprint_engine_parser::AstStmt>()
                .ok_or_else(|| BlueprintError::InternalError {
                    message: "Invalid function body in task()".into(),
                })?;

            let closure_scope = func
                .closure
                .as_ref()
                .and_then(|c| c.downcast_ref::<Arc<crate::scope::Scope>>().cloned());

            let base_scope = closure_scope.unwrap_or_else(crate::scope::Scope::new_global);
            let call_scope =
                crate::scope::Scope::new_child(base_scope, crate::scope::ScopeKind::Function);

            for param in &func.params {
                if let Some(ref default) = param.default {
                    call_scope.define(&param.name, default.clone()).await;
                }
            }

            let evaluator = Evaluator::new();
            match evaluator.eval_stmt(body, call_scope).await {
                Ok(_) => Ok(Value::None),
                Err(BlueprintError::Return { value }) => Ok((*value).clone()),
                Err(e) => Err(e),
            }
        }
        other => Err(BlueprintError::TypeError {
            expected: "callable (function or lambda)".into(),
            actual: other.type_name().into(),
        }),
    }
}

fn build_result(value: Value, success: bool, reason: Option<&str>) -> Value {
    let mut result = IndexMap::new();
    result.insert("value".to_string(), value);
    result.insert("success".to_string(), Value::Bool(success));

    if let Some(r) = reason {
        result.insert("reason".to_string(), Value::String(Arc::new(r.to_string())));
    }

    Value::Dict(Arc::new(RwLock::new(result)))
}

fn build_result_with_timing(
    value: Value,
    success: bool,
    reason: Option<&str>,
    elapsed: f64,
) -> Value {
    let mut result = IndexMap::new();
    result.insert("value".to_string(), value);
    result.insert("success".to_string(), Value::Bool(success));
    result.insert("elapsed".to_string(), Value::Float(elapsed));

    if let Some(r) = reason {
        result.insert("reason".to_string(), Value::String(Arc::new(r.to_string())));
    }

    Value::Dict(Arc::new(RwLock::new(result)))
}
