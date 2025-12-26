use std::collections::HashMap;
use std::sync::Arc;

use indexmap::IndexMap;
use tokio::sync::mpsc;

use blueprint_engine_core::{
    BlueprintError, Generator, GeneratorMessage, Result, StackFrame, Value,
};
use blueprint_engine_parser::{AstExpr, AstStmt};

use super::Evaluator;
use crate::scope::{Scope, ScopeKind};

impl Evaluator {
    pub async fn handle_yield(&self, expr: Option<&AstExpr>, scope: Arc<Scope>) -> Result<Value> {
        let yield_tx = scope
            .get_yield_tx()
            .ok_or_else(|| BlueprintError::ArgumentError {
                message: "yield used outside of a generator function".into(),
            })?;

        let value = match expr {
            Some(e) => self.eval_expr(e, scope).await?,
            None => Value::None,
        };

        let (resume_tx, resume_rx) = tokio::sync::oneshot::channel();

        yield_tx
            .send(GeneratorMessage::Yielded(value, resume_tx))
            .await
            .map_err(|_| BlueprintError::InternalError {
                message: "Generator receiver dropped".into(),
            })?;

        resume_rx.await.map_err(|_| BlueprintError::InternalError {
            message: "Generator consumer stopped".into(),
        })?;

        Ok(Value::None)
    }

    pub async fn call_function(
        &self,
        func: Value,
        args: Vec<Value>,
        kwargs: HashMap<String, Value>,
        scope: Arc<Scope>,
    ) -> Result<Value> {
        match func {
            Value::NativeFunction(f) => f.call(args, kwargs).await,
            Value::Function(f) => self.call_user_function(&f, args, kwargs, scope).await,
            Value::Lambda(f) => self.call_lambda(&f, args, kwargs, scope).await,
            Value::StructType(s) => {
                let instance = s.instantiate(args, kwargs)?;
                Ok(Value::StructInstance(Arc::new(instance)))
            }
            _ => Err(BlueprintError::NotCallable {
                type_name: func.type_name().into(),
            }),
        }
    }

    pub async fn call_user_function(
        &self,
        func: &blueprint_engine_core::UserFunction,
        args: Vec<Value>,
        kwargs: HashMap<String, Value>,
        _parent_scope: Arc<Scope>,
    ) -> Result<Value> {
        let body =
            func.body
                .downcast_ref::<AstStmt>()
                .ok_or_else(|| BlueprintError::InternalError {
                    message: "Invalid function body".into(),
                })?;

        if Self::contains_yield(body) {
            return self.create_generator(func, args, kwargs).await;
        }

        let closure_scope = func
            .closure
            .as_ref()
            .and_then(|c| c.downcast_ref::<Arc<Scope>>().cloned());
        let base_scope = closure_scope.unwrap_or_else(Scope::new_global);
        let call_scope = Scope::new_child(base_scope, ScopeKind::Function);

        self.bind_parameters(&func.params, args, kwargs, &call_scope)
            .await?;

        let func_name = func.name.clone();
        let file = self.current_file.as_ref().map(|p| p.display().to_string());
        let (line, column) = self.get_span_location(&body.span);

        match self.eval_stmt(body, call_scope).await {
            Ok(_) => Ok(Value::None),
            Err(BlueprintError::Return { value }) => Ok((*value).clone()),
            Err(e) => Err(e.with_stack_frame(StackFrame {
                function_name: func_name,
                file,
                line,
                column,
            })),
        }
    }

    pub async fn create_generator(
        &self,
        func: &blueprint_engine_core::UserFunction,
        args: Vec<Value>,
        kwargs: HashMap<String, Value>,
    ) -> Result<Value> {
        let (tx, rx) = mpsc::channel::<GeneratorMessage>(1);

        let closure_scope = func
            .closure
            .as_ref()
            .and_then(|c| c.downcast_ref::<Arc<Scope>>().cloned());
        let base_scope = closure_scope.unwrap_or_else(Scope::new_global);
        let gen_scope = Scope::new_generator(base_scope, tx.clone());

        self.bind_parameters(&func.params, args, kwargs, &gen_scope)
            .await?;

        let body = func
            .body
            .downcast_ref::<AstStmt>()
            .ok_or_else(|| BlueprintError::InternalError {
                message: "Invalid function body".into(),
            })?
            .clone();

        let func_name = func.name.clone();

        let evaluator = Evaluator::new();

        tokio::spawn(async move {
            let result = evaluator.eval_stmt(&body, gen_scope).await;

            match result {
                Ok(_) | Err(BlueprintError::Return { .. }) => {
                    let _ = tx.send(GeneratorMessage::Complete).await;
                }
                Err(_) => {
                    let _ = tx.send(GeneratorMessage::Complete).await;
                }
            }
        });

        Ok(Value::Generator(Arc::new(Generator::new(rx, func_name))))
    }

    pub async fn call_lambda(
        &self,
        func: &blueprint_engine_core::LambdaFunction,
        args: Vec<Value>,
        kwargs: HashMap<String, Value>,
        _parent_scope: Arc<Scope>,
    ) -> Result<Value> {
        let closure_scope = func
            .closure
            .as_ref()
            .and_then(|c| c.downcast_ref::<Arc<Scope>>().cloned());
        let base_scope = closure_scope.unwrap_or_else(Scope::new_global);
        let call_scope = Scope::new_child(base_scope, ScopeKind::Function);

        self.bind_parameters(&func.params, args, kwargs, &call_scope)
            .await?;

        let body =
            func.body
                .downcast_ref::<AstExpr>()
                .ok_or_else(|| BlueprintError::InternalError {
                    message: "Invalid lambda body".into(),
                })?;

        let file = self.current_file.as_ref().map(|p| p.display().to_string());
        let (line, column) = self.get_span_location(&body.span);

        self.eval_expr(body, call_scope.clone()).await.map_err(|e| {
            e.with_stack_frame(StackFrame {
                function_name: "<lambda>".to_string(),
                file,
                line,
                column,
            })
        })
    }

    pub async fn call_lambda_public(
        &self,
        func: &blueprint_engine_core::LambdaFunction,
        args: Vec<Value>,
        kwargs: HashMap<String, Value>,
    ) -> Result<Value> {
        let scope = Scope::new_global();
        self.call_lambda(func, args, kwargs, scope).await
    }

    pub async fn call_function_public(
        &self,
        func: &blueprint_engine_core::UserFunction,
        args: Vec<Value>,
        kwargs: HashMap<String, Value>,
    ) -> Result<Value> {
        let scope = Scope::new_global();
        self.call_user_function(func, args, kwargs, scope).await
    }

    pub async fn bind_parameters(
        &self,
        params: &[blueprint_engine_core::Parameter],
        args: Vec<Value>,
        mut kwargs: HashMap<String, Value>,
        scope: &Arc<Scope>,
    ) -> Result<()> {
        let mut arg_idx = 0;

        for param in params {
            match param.kind {
                blueprint_engine_core::ParameterKind::Positional => {
                    let value = if arg_idx < args.len() {
                        let v = args[arg_idx].clone();
                        arg_idx += 1;
                        v
                    } else if let Some(v) = kwargs.remove(&param.name) {
                        v
                    } else if let Some(ref default) = param.default {
                        default.clone()
                    } else {
                        return Err(BlueprintError::ArgumentError {
                            message: format!("missing required argument: {}", param.name),
                        });
                    };
                    scope.define(&param.name, value).await;
                }
                blueprint_engine_core::ParameterKind::Args => {
                    let remaining: Vec<Value> = args[arg_idx..].to_vec();
                    scope
                        .define(
                            &param.name,
                            Value::List(Arc::new(tokio::sync::RwLock::new(remaining))),
                        )
                        .await;
                    arg_idx = args.len();
                }
                blueprint_engine_core::ParameterKind::Kwargs => {
                    let remaining: IndexMap<String, Value> =
                        std::mem::take(&mut kwargs).into_iter().collect();
                    scope
                        .define(
                            &param.name,
                            Value::Dict(Arc::new(tokio::sync::RwLock::new(remaining))),
                        )
                        .await;
                }
            }
        }

        if arg_idx < args.len() {
            return Err(BlueprintError::ArgumentError {
                message: format!(
                    "too many positional arguments: expected {}, got {}",
                    arg_idx,
                    args.len()
                ),
            });
        }

        if !kwargs.is_empty() {
            let unknown: Vec<_> = kwargs.keys().collect();
            return Err(BlueprintError::ArgumentError {
                message: format!("unexpected keyword arguments: {:?}", unknown),
            });
        }

        Ok(())
    }
}
