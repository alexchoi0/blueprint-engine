use std::sync::Arc;

use indexmap::{IndexMap, IndexSet};

use blueprint_engine_core::{BlueprintError, Result, Value};
use blueprint_engine_parser::{AstExpr, Clause, ForClause};

use super::Evaluator;
use crate::scope::{Scope, ScopeKind};

impl Evaluator {
    pub async fn eval_list_comprehension(
        &self,
        body: &AstExpr,
        first: &ForClause,
        clauses: &[Clause],
        scope: Arc<Scope>,
    ) -> Result<Value> {
        let mut results = Vec::new();
        self.eval_comprehension_clauses(body, first, clauses, scope, &mut results)
            .await?;
        Ok(Value::List(Arc::new(tokio::sync::RwLock::new(results))))
    }

    pub async fn eval_set_comprehension(
        &self,
        body: &AstExpr,
        first: &ForClause,
        clauses: &[Clause],
        scope: Arc<Scope>,
    ) -> Result<Value> {
        let mut results = Vec::new();
        self.eval_comprehension_clauses(body, first, clauses, scope, &mut results)
            .await?;
        let set: IndexSet<Value> = results.into_iter().collect();
        Ok(Value::Set(Arc::new(tokio::sync::RwLock::new(set))))
    }

    #[async_recursion::async_recursion]
    pub async fn eval_comprehension_clauses(
        &self,
        body: &AstExpr,
        for_clause: &ForClause,
        remaining: &[Clause],
        scope: Arc<Scope>,
        results: &mut Vec<Value>,
    ) -> Result<()> {
        let ForClause { var, over, .. } = for_clause;
        let iterable = self.eval_expr(over, scope.clone()).await?;
        let items = self.get_iterable(&iterable).await?;

        for item in items {
            let iter_scope = Scope::new_child(scope.clone(), ScopeKind::Block);
            self.assign_target(var, item, iter_scope.clone()).await?;

            if remaining.is_empty() {
                let value = self.eval_expr(body, iter_scope).await?;
                results.push(value);
            } else {
                match &remaining[0] {
                    Clause::For(next_for) => {
                        self.eval_comprehension_clauses(
                            body,
                            next_for,
                            &remaining[1..],
                            iter_scope,
                            results,
                        )
                        .await?;
                    }
                    Clause::If(cond) => {
                        let cond_val = self.eval_expr(cond, iter_scope.clone()).await?;
                        if cond_val.is_truthy() {
                            if remaining.len() == 1 {
                                let value = self.eval_expr(body, iter_scope).await?;
                                results.push(value);
                            } else {
                                match &remaining[1] {
                                    Clause::For(next_for) => {
                                        self.eval_comprehension_clauses(
                                            body,
                                            next_for,
                                            &remaining[2..],
                                            iter_scope,
                                            results,
                                        )
                                        .await?;
                                    }
                                    Clause::If(_) => {
                                        return Err(BlueprintError::InternalError {
                                            message: "consecutive if clauses".into(),
                                        });
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        Ok(())
    }

    pub async fn eval_dict_comprehension(
        &self,
        key_expr: &AstExpr,
        val_expr: &AstExpr,
        first: &ForClause,
        clauses: &[Clause],
        scope: Arc<Scope>,
    ) -> Result<Value> {
        let mut results = IndexMap::new();
        self.eval_dict_comprehension_clauses(
            key_expr,
            val_expr,
            first,
            clauses,
            scope,
            &mut results,
        )
        .await?;
        Ok(Value::Dict(Arc::new(tokio::sync::RwLock::new(results))))
    }

    #[async_recursion::async_recursion]
    pub async fn eval_dict_comprehension_clauses(
        &self,
        key_expr: &AstExpr,
        val_expr: &AstExpr,
        for_clause: &ForClause,
        remaining: &[Clause],
        scope: Arc<Scope>,
        results: &mut IndexMap<String, Value>,
    ) -> Result<()> {
        let ForClause { var, over, .. } = for_clause;
        let iterable = self.eval_expr(over, scope.clone()).await?;
        let items = self.get_iterable(&iterable).await?;

        for item in items {
            let iter_scope = Scope::new_child(scope.clone(), ScopeKind::Block);
            self.assign_target(var, item, iter_scope.clone()).await?;

            if remaining.is_empty() {
                let key = self.eval_expr(key_expr, iter_scope.clone()).await?;
                let key_str = self.value_to_dict_key(&key)?;
                let val = self.eval_expr(val_expr, iter_scope).await?;
                results.insert(key_str, val);
            } else {
                match &remaining[0] {
                    Clause::For(next_for) => {
                        self.eval_dict_comprehension_clauses(
                            key_expr,
                            val_expr,
                            next_for,
                            &remaining[1..],
                            iter_scope,
                            results,
                        )
                        .await?;
                    }
                    Clause::If(cond) => {
                        let cond_val = self.eval_expr(cond, iter_scope.clone()).await?;
                        if cond_val.is_truthy() {
                            if remaining.len() == 1 {
                                let key = self.eval_expr(key_expr, iter_scope.clone()).await?;
                                let key_str = self.value_to_dict_key(&key)?;
                                let val = self.eval_expr(val_expr, iter_scope).await?;
                                results.insert(key_str, val);
                            } else if let Clause::For(next_for) = &remaining[1] {
                                self.eval_dict_comprehension_clauses(
                                    key_expr,
                                    val_expr,
                                    next_for,
                                    &remaining[2..],
                                    iter_scope,
                                    results,
                                )
                                .await?;
                            }
                        }
                    }
                }
            }
        }

        Ok(())
    }
}
