use std::collections::HashMap;
use std::sync::Arc;

use blueprint_core::{BlueprintError, NativeFunction, Result, Value};
use tokio::sync::RwLock;
use tokio::task::JoinSet;

use crate::eval::Evaluator;

pub fn register(evaluator: &mut Evaluator) {
    evaluator.register_native(NativeFunction::new("parallel", parallel));
}

async fn parallel(args: Vec<Value>, _kwargs: HashMap<String, Value>) -> Result<Value> {
    if args.len() != 1 {
        return Err(BlueprintError::ArgumentError {
            message: format!("parallel() takes exactly 1 argument ({} given)", args.len()),
        });
    }

    let functions = match &args[0] {
        Value::List(l) => l.read().await.clone(),
        Value::Tuple(t) => t.as_ref().clone(),
        other => {
            return Err(BlueprintError::TypeError {
                expected: "list or tuple of functions".into(),
                actual: other.type_name().into(),
            })
        }
    };

    if functions.is_empty() {
        return Ok(Value::List(Arc::new(RwLock::new(vec![]))));
    }

    let mut join_set: JoinSet<std::result::Result<(usize, Value), (usize, BlueprintError)>> =
        JoinSet::new();

    for (idx, func_value) in functions.into_iter().enumerate() {
        match func_value {
            Value::NativeFunction(native) => {
                let native = native.clone();
                join_set.spawn(async move {
                    match native.call(vec![], HashMap::new()).await {
                        Ok(v) => Ok((idx, v)),
                        Err(e) => Err((idx, e)),
                    }
                });
            }
            Value::Lambda(lambda) => {
                let lambda = lambda.clone();
                join_set.spawn(async move {
                    let body = lambda
                        .body
                        .downcast_ref::<blueprint_parser::AstExpr>()
                        .ok_or_else(|| {
                            (
                                idx,
                                BlueprintError::InternalError {
                                    message: "Invalid lambda body in parallel()".into(),
                                },
                            )
                        })?;

                    let closure_scope = lambda.closure.as_ref().and_then(|c| {
                        c.downcast_ref::<Arc<crate::scope::Scope>>().cloned()
                    });

                    let base_scope =
                        closure_scope.unwrap_or_else(crate::scope::Scope::new_global);
                    let call_scope =
                        crate::scope::Scope::new_child(base_scope, crate::scope::ScopeKind::Function);

                    for param in &lambda.params {
                        if let Some(ref default) = param.default {
                            call_scope.define(&param.name, default.clone()).await;
                        }
                    }

                    let evaluator = Evaluator::new();
                    match evaluator.eval_expr(body, call_scope).await {
                        Ok(v) => Ok((idx, v)),
                        Err(e) => Err((idx, e)),
                    }
                });
            }
            Value::Function(func) => {
                let func = func.clone();
                join_set.spawn(async move {
                    let body = func
                        .body
                        .downcast_ref::<blueprint_parser::AstStmt>()
                        .ok_or_else(|| {
                            (
                                idx,
                                BlueprintError::InternalError {
                                    message: "Invalid function body in parallel()".into(),
                                },
                            )
                        })?;

                    let closure_scope = func.closure.as_ref().and_then(|c| {
                        c.downcast_ref::<Arc<crate::scope::Scope>>().cloned()
                    });

                    let base_scope =
                        closure_scope.unwrap_or_else(crate::scope::Scope::new_global);
                    let call_scope =
                        crate::scope::Scope::new_child(base_scope, crate::scope::ScopeKind::Function);

                    for param in &func.params {
                        if let Some(ref default) = param.default {
                            call_scope.define(&param.name, default.clone()).await;
                        }
                    }

                    let evaluator = Evaluator::new();
                    match evaluator.eval_stmt(body, call_scope).await {
                        Ok(_) => Ok((idx, Value::None)),
                        Err(BlueprintError::Return { value }) => Ok((idx, (*value).clone())),
                        Err(e) => Err((idx, e)),
                    }
                });
            }
            other => {
                return Err(BlueprintError::TypeError {
                    expected: "callable (function or lambda)".into(),
                    actual: other.type_name().into(),
                })
            }
        }
    }

    let total = join_set.len();
    let mut results: Vec<Option<Value>> = vec![None; total];
    let mut first_error: Option<BlueprintError> = None;

    while let Some(result) = join_set.join_next().await {
        match result {
            Ok(Ok((idx, value))) => {
                results[idx] = Some(value);
            }
            Ok(Err((idx, error))) => {
                if first_error.is_none() {
                    first_error = Some(error);
                }
                results[idx] = Some(Value::None);
            }
            Err(join_error) => {
                if first_error.is_none() {
                    first_error = Some(BlueprintError::InternalError {
                        message: format!("parallel task panicked: {}", join_error),
                    });
                }
            }
        }
    }

    if let Some(error) = first_error {
        return Err(error);
    }

    let final_results: Vec<Value> = results
        .into_iter()
        .map(|opt| opt.unwrap_or(Value::None))
        .collect();

    Ok(Value::List(Arc::new(RwLock::new(final_results))))
}
