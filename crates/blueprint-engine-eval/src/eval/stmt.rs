use std::sync::Arc;

use blueprint_engine_core::{BlueprintError, Result, Value};
use blueprint_engine_parser::{AstStmt, ParsedModule, StmtP};

use super::ops;
use super::Evaluator;
use crate::scope::{Scope, ScopeKind};

impl Evaluator {
    pub async fn eval(&mut self, module: &ParsedModule, scope: Arc<Scope>) -> Result<Value> {
        self.codemap = Some(module.codemap.clone());
        self.eval_stmt(module.statements(), scope).await
    }

    #[async_recursion::async_recursion]
    pub async fn eval_stmt(&self, stmt: &AstStmt, scope: Arc<Scope>) -> Result<Value> {
        match &stmt.node {
            StmtP::Statements(stmts) => {
                let mut result = Value::None;
                for s in stmts {
                    result = self.eval_stmt(s, scope.clone()).await?;
                }
                Ok(result)
            }

            StmtP::Expression(expr) => self.eval_expr(expr, scope).await,

            StmtP::Assign(assign) => {
                let value = self.eval_expr(&assign.rhs, scope.clone()).await?;
                self.assign_target(&assign.lhs, value, scope).await?;
                Ok(Value::None)
            }

            StmtP::AssignModify(lhs, op, rhs) => {
                let current = self.eval_assign_target_value(lhs, scope.clone()).await?;
                let rhs_val = self.eval_expr(rhs, scope.clone()).await?;
                let new_val = ops::apply_assign_op(*op, current, rhs_val).await?;
                self.assign_target(lhs, new_val, scope).await?;
                Ok(Value::None)
            }

            StmtP::If(cond, then_block) => {
                let cond_val = self.eval_expr(cond, scope.clone()).await?;
                if cond_val.is_truthy() {
                    let block_scope = Scope::new_child(scope, ScopeKind::Block);
                    self.eval_stmt(then_block, block_scope).await?;
                }
                Ok(Value::None)
            }

            StmtP::IfElse(cond, branches) => {
                let (then_block, else_block) = branches.as_ref();
                let cond_val = self.eval_expr(cond, scope.clone()).await?;
                let block_scope = Scope::new_child(scope, ScopeKind::Block);
                if cond_val.is_truthy() {
                    self.eval_stmt(then_block, block_scope).await?;
                } else {
                    self.eval_stmt(else_block, block_scope).await?;
                }
                Ok(Value::None)
            }

            StmtP::For(for_stmt) => {
                let iterable = self.eval_expr(&for_stmt.over, scope.clone()).await?;

                match &iterable {
                    Value::Iterator(iter) => loop {
                        let item = iter.next().await;
                        match item {
                            Some(value) => {
                                let loop_scope = Scope::new_child(scope.clone(), ScopeKind::Loop);
                                self.assign_target(&for_stmt.var, value, loop_scope.clone())
                                    .await?;

                                match self.eval_stmt(&for_stmt.body, loop_scope).await {
                                    Err(BlueprintError::Break) => break,
                                    Err(BlueprintError::Continue) => continue,
                                    Err(e) => return Err(e),
                                    Ok(_) => {}
                                }
                            }
                            None => break,
                        }
                    },
                    Value::Generator(gen) => loop {
                        let item = gen.next().await;
                        match item {
                            Some(value) => {
                                let loop_scope = Scope::new_child(scope.clone(), ScopeKind::Loop);
                                self.assign_target(&for_stmt.var, value, loop_scope.clone())
                                    .await?;

                                match self.eval_stmt(&for_stmt.body, loop_scope).await {
                                    Err(BlueprintError::Break) => break,
                                    Err(BlueprintError::Continue) => continue,
                                    Err(e) => return Err(e),
                                    Ok(_) => {}
                                }
                            }
                            None => break,
                        }
                    },
                    _ => {
                        let items = self.get_iterable(&iterable).await?;

                        for item in items {
                            let loop_scope = Scope::new_child(scope.clone(), ScopeKind::Loop);
                            self.assign_target(&for_stmt.var, item, loop_scope.clone())
                                .await?;

                            match self.eval_stmt(&for_stmt.body, loop_scope).await {
                                Err(BlueprintError::Break) => break,
                                Err(BlueprintError::Continue) => continue,
                                Err(e) => return Err(e),
                                Ok(_) => {}
                            }
                        }
                    }
                }
                Ok(Value::None)
            }

            StmtP::Break => Err(BlueprintError::Break),
            StmtP::Continue => Err(BlueprintError::Continue),

            StmtP::Return(expr) => {
                let value = match expr {
                    Some(e) => self.eval_expr(e, scope).await?,
                    None => Value::None,
                };
                Err(BlueprintError::Return {
                    value: Arc::new(value),
                })
            }

            StmtP::Yield(expr) => self.handle_yield(expr.as_ref(), scope).await,

            StmtP::Pass => Ok(Value::None),

            StmtP::Def(def) => {
                let func = self.create_user_function(def, scope.clone())?;
                scope.define(&def.name.node.ident, func).await;
                Ok(Value::None)
            }

            StmtP::Load(load) => self.eval_load(load, scope).await,

            StmtP::Struct(struct_def) => self.eval_struct_def(struct_def, scope).await,

            StmtP::Match(match_stmt) => self.eval_match(match_stmt, scope).await,
        }
    }
}
