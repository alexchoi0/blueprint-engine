use std::sync::Arc;

use blueprint_engine_core::{BlueprintError, Result, Value};
use blueprint_engine_parser::{AstExpr, ExprP};
use blueprint_starlark_syntax::syntax::ast::{ArgumentP, BinOp};

use super::ops;
use super::Evaluator;
use crate::scope::Scope;

impl Evaluator {
    pub async fn eval_match(
        &self,
        match_stmt: &blueprint_starlark_syntax::syntax::ast::MatchP<
            blueprint_starlark_syntax::syntax::ast::AstNoPayload,
        >,
        scope: Arc<Scope>,
    ) -> Result<Value> {
        let subject = self.eval_expr(&match_stmt.subject, scope.clone()).await?;

        for case in &match_stmt.cases {
            let pattern_scope = Scope::new_child(scope.clone(), crate::scope::ScopeKind::Block);

            if self
                .match_pattern(&case.node.pattern, &subject, &pattern_scope)
                .await?
            {
                if let Some(ref guard) = case.node.guard {
                    let guard_val = self.eval_expr(guard, pattern_scope.clone()).await?;
                    if !guard_val.is_truthy() {
                        continue;
                    }
                }

                let bound_vars = pattern_scope.variables_snapshot().await;
                for (name, value) in bound_vars {
                    scope.define(&name, value).await;
                }

                return self.eval_stmt(&case.node.body, scope.clone()).await;
            }
        }

        Ok(Value::None)
    }

    #[async_recursion::async_recursion]
    pub async fn match_pattern(
        &self,
        pattern: &AstExpr,
        subject: &Value,
        scope: &Arc<Scope>,
    ) -> Result<bool> {
        match &pattern.node {
            ExprP::Identifier(ident) => {
                let name = ident.node.ident.as_str();
                match name {
                    "_" => Ok(true),
                    "None" => Ok(matches!(subject, Value::None)),
                    "True" => Ok(matches!(subject, Value::Bool(true))),
                    "False" => Ok(matches!(subject, Value::Bool(false))),
                    _ => {
                        scope.define(name, subject.clone()).await;
                        Ok(true)
                    }
                }
            }

            ExprP::Literal(lit) => {
                let pattern_val = self.eval_literal(lit)?;
                Ok(pattern_val == *subject)
            }

            ExprP::Minus(inner) => {
                let pattern_val = self.eval_literal_negated(inner)?;
                Ok(pattern_val == *subject)
            }

            ExprP::List(patterns) => match subject {
                Value::List(l) => {
                    let items = l.read().await;
                    if items.len() != patterns.len() {
                        return Ok(false);
                    }
                    for (pat, item) in patterns.iter().zip(items.iter()) {
                        if !self.match_pattern(pat, item, scope).await? {
                            return Ok(false);
                        }
                    }
                    Ok(true)
                }
                _ => Ok(false),
            },

            ExprP::Tuple(patterns) => match subject {
                Value::Tuple(t) => {
                    if t.len() != patterns.len() {
                        return Ok(false);
                    }
                    for (pat, item) in patterns.iter().zip(t.iter()) {
                        if !self.match_pattern(pat, item, scope).await? {
                            return Ok(false);
                        }
                    }
                    Ok(true)
                }
                _ => Ok(false),
            },

            ExprP::Dict(pairs) => match subject {
                Value::Dict(d) => {
                    let map = d.read().await;
                    for (key_pat, val_pat) in pairs {
                        let key = self.eval_expr(key_pat, scope.clone()).await?;
                        let key_str = self.value_to_dict_key(&key)?;
                        match map.get(&key_str) {
                            Some(val) => {
                                if !self.match_pattern(val_pat, val, scope).await? {
                                    return Ok(false);
                                }
                            }
                            None => return Ok(false),
                        }
                    }
                    Ok(true)
                }
                _ => Ok(false),
            },

            ExprP::Op(lhs, BinOp::BitOr, rhs) => {
                if self.match_pattern(lhs, subject, scope).await? {
                    return Ok(true);
                }
                self.match_pattern(rhs, subject, scope).await
            }

            ExprP::Call(callee, args) => {
                let name = match &callee.node {
                    ExprP::Identifier(ident) => ident.node.ident.as_str(),
                    _ => {
                        return Err(BlueprintError::ValueError {
                            message: "pattern must use a simple name".into(),
                        })
                    }
                };

                if self.is_type_constraint_pattern(name) {
                    return self
                        .match_type_constraint_pattern(name, args, subject, scope)
                        .await;
                }

                let instance = match subject {
                    Value::StructInstance(inst) if inst.struct_type.name == name => inst,
                    _ => return Ok(false),
                };

                let mut positional_idx = 0;
                for arg in &args.args {
                    match &arg.node {
                        ArgumentP::Named(arg_name, pattern) => {
                            let field_name = arg_name.node.as_str();
                            match instance.fields.get(field_name) {
                                Some(field_val) => {
                                    if !self.match_pattern(pattern, field_val, scope).await? {
                                        return Ok(false);
                                    }
                                }
                                None => return Ok(false),
                            }
                        }
                        ArgumentP::Positional(pattern) => {
                            let field = match instance.struct_type.fields.get(positional_idx) {
                                Some(f) => f,
                                None => {
                                    return Err(BlueprintError::ValueError {
                                        message: "too many positional patterns in struct match"
                                            .into(),
                                    })
                                }
                            };
                            match instance.fields.get(&field.name) {
                                Some(field_val) => {
                                    if !self.match_pattern(pattern, field_val, scope).await? {
                                        return Ok(false);
                                    }
                                }
                                None => return Ok(false),
                            }
                            positional_idx += 1;
                        }
                        _ => {
                            return Err(BlueprintError::ValueError {
                                message:
                                    "only positional and keyword arguments supported in struct patterns"
                                        .into(),
                            })
                        }
                    }
                }

                Ok(true)
            }

            _ => Err(BlueprintError::ValueError {
                message: "unsupported pattern type".into(),
            }),
        }
    }

    pub fn is_type_constraint_pattern(&self, name: &str) -> bool {
        matches!(
            name,
            "str" | "int" | "float" | "bool" | "list" | "tuple" | "dict" | "set"
        )
    }

    #[async_recursion::async_recursion]
    pub async fn match_type_constraint_pattern(
        &self,
        type_name: &str,
        args: &blueprint_starlark_syntax::syntax::ast::CallArgsP<
            blueprint_starlark_syntax::syntax::ast::AstNoPayload,
        >,
        subject: &Value,
        scope: &Arc<Scope>,
    ) -> Result<bool> {
        let type_matches = match type_name {
            "str" => matches!(subject, Value::String(_)),
            "int" => matches!(subject, Value::Int(_)),
            "float" => matches!(subject, Value::Float(_)),
            "bool" => matches!(subject, Value::Bool(_)),
            "list" => matches!(subject, Value::List(_)),
            "tuple" => matches!(subject, Value::Tuple(_)),
            "dict" => matches!(subject, Value::Dict(_)),
            "set" => matches!(subject, Value::Set(_)),
            _ => return Ok(false),
        };

        if !type_matches {
            return Ok(false);
        }

        if args.args.is_empty() {
            return Ok(true);
        }

        if args.args.len() != 1 {
            return Err(BlueprintError::ValueError {
                message: format!(
                    "type constraint pattern {} expects 0 or 1 argument, got {}",
                    type_name,
                    args.args.len()
                ),
            });
        }

        match &args.args[0].node {
            ArgumentP::Positional(inner_pattern) => {
                self.match_pattern(inner_pattern, subject, scope).await
            }
            _ => Err(BlueprintError::ValueError {
                message: "type constraint pattern only supports positional argument".into(),
            }),
        }
    }

    pub fn eval_literal_negated(&self, expr: &AstExpr) -> Result<Value> {
        match &expr.node {
            ExprP::Literal(lit) => {
                let val = self.eval_literal(lit)?;
                ops::eval_unary_minus(val)
            }
            _ => Err(BlueprintError::ValueError {
                message: "invalid negated literal pattern".into(),
            }),
        }
    }
}
