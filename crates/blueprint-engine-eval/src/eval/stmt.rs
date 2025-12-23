use std::collections::HashMap;
use std::sync::Arc;

use indexmap::{IndexMap, IndexSet};

use blueprint_engine_core::{
    BlueprintError, Generator, GeneratorMessage, Result, StackFrame,
    StructField, StructType, TypeAnnotation, Value,
};
use blueprint_engine_parser::{
    AstExpr, AstStmt, Clause, ExprP, ForClause, ParsedModule, StmtP,
};
use blueprint_starlark_syntax::syntax::ast::{AstAssignTarget, AssignTargetP, BinOp, ArgumentP};
use tokio::sync::mpsc;

use crate::scope::{Scope, ScopeKind};
use super::Evaluator;
use super::ops;

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
                    Value::Iterator(iter) => {
                        loop {
                            let item = iter.next().await;
                            match item {
                                Some(value) => {
                                    let loop_scope = Scope::new_child(scope.clone(), ScopeKind::Loop);
                                    self.assign_target(&for_stmt.var, value, loop_scope.clone()).await?;

                                    match self.eval_stmt(&for_stmt.body, loop_scope).await {
                                        Err(BlueprintError::Break) => break,
                                        Err(BlueprintError::Continue) => continue,
                                        Err(e) => return Err(e),
                                        Ok(_) => {}
                                    }
                                }
                                None => break,
                            }
                        }
                    }
                    Value::Generator(gen) => {
                        loop {
                            let item = gen.next().await;
                            match item {
                                Some(value) => {
                                    let loop_scope = Scope::new_child(scope.clone(), ScopeKind::Loop);
                                    self.assign_target(&for_stmt.var, value, loop_scope.clone()).await?;

                                    match self.eval_stmt(&for_stmt.body, loop_scope).await {
                                        Err(BlueprintError::Break) => break,
                                        Err(BlueprintError::Continue) => continue,
                                        Err(e) => return Err(e),
                                        Ok(_) => {}
                                    }
                                }
                                None => break,
                            }
                        }
                    }
                    _ => {
                        let items = self.get_iterable(&iterable).await?;

                        for item in items {
                            let loop_scope = Scope::new_child(scope.clone(), ScopeKind::Loop);
                            self.assign_target(&for_stmt.var, item, loop_scope.clone()).await?;

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

            StmtP::Yield(expr) => {
                self.handle_yield(expr.as_ref(), scope).await
            }

            StmtP::Pass => Ok(Value::None),

            StmtP::Def(def) => {
                let func = self.create_user_function(def, scope.clone())?;
                scope.define(&def.name.node.ident, func).await;
                Ok(Value::None)
            }

            StmtP::Load(load) => {
                self.eval_load(load, scope).await
            }

            StmtP::Struct(struct_def) => {
                self.eval_struct_def(struct_def, scope).await
            }

            StmtP::Match(match_stmt) => {
                self.eval_match(match_stmt, scope).await
            }
        }
    }

    pub async fn eval_match(
        &self,
        match_stmt: &blueprint_starlark_syntax::syntax::ast::MatchP<blueprint_starlark_syntax::syntax::ast::AstNoPayload>,
        scope: Arc<Scope>,
    ) -> Result<Value> {
        let subject = self.eval_expr(&match_stmt.subject, scope.clone()).await?;

        for case in &match_stmt.cases {
            let pattern_scope = Scope::new_child(scope.clone(), ScopeKind::Block);

            if self.match_pattern(&case.node.pattern, &subject, &pattern_scope).await? {
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

            ExprP::List(patterns) => {
                match subject {
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
                }
            }

            ExprP::Tuple(patterns) => {
                match subject {
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
                }
            }

            ExprP::Dict(pairs) => {
                match subject {
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
                }
            }

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
                    return self.match_type_constraint_pattern(name, args, subject, scope).await;
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
                                        message: "too many positional patterns in struct match".into(),
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
                                message: "only positional and keyword arguments supported in struct patterns".into(),
                            })
                        }
                    }
                }

                Ok(true)
            }

            _ => Err(BlueprintError::ValueError {
                message: format!("unsupported pattern type"),
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
        args: &blueprint_starlark_syntax::syntax::ast::CallArgsP<blueprint_starlark_syntax::syntax::ast::AstNoPayload>,
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

    pub async fn eval_struct_def(
        &self,
        struct_def: &blueprint_starlark_syntax::syntax::ast::StructP<blueprint_starlark_syntax::syntax::ast::AstNoPayload>,
        scope: Arc<Scope>,
    ) -> Result<Value> {
        let struct_name = struct_def.name.node.ident.clone();

        let mut fields = Vec::new();
        for field in &struct_def.fields {
            let field_name = field.node.name.node.ident.clone();
            let type_annotation = self.convert_type_expr(&field.node.typ)?;
            let default = if let Some(ref default_expr) = field.node.default {
                Some(self.eval_const_expr(default_expr)?)
            } else {
                None
            };

            fields.push(StructField {
                name: field_name,
                typ: type_annotation,
                default,
            });
        }

        let struct_type = StructType {
            name: struct_name.clone(),
            fields,
        };

        let value = Value::StructType(Arc::new(struct_type));
        scope.define(&struct_name, value.clone()).await;
        Ok(value)
    }

    pub fn convert_type_expr(
        &self,
        type_expr: &blueprint_starlark_syntax::syntax::ast::AstTypeExprP<blueprint_starlark_syntax::syntax::ast::AstNoPayload>,
    ) -> Result<TypeAnnotation> {
        self.convert_expr_to_type_annotation(&type_expr.node.expr)
    }

    pub fn convert_expr_to_type_annotation(
        &self,
        expr: &AstExpr,
    ) -> Result<TypeAnnotation> {
        match &expr.node {
            ExprP::Identifier(ident) => {
                Ok(TypeAnnotation::Simple(ident.node.ident.clone()))
            }
            ExprP::Index(pair) => {
                let (base, index) = pair.as_ref();
                let base_name = match &base.node {
                    ExprP::Identifier(ident) => ident.node.ident.clone(),
                    _ => return Err(BlueprintError::ValueError {
                        message: "invalid type annotation".into(),
                    }),
                };

                let params = match &index.node {
                    ExprP::Tuple(items) => {
                        let mut type_params = Vec::new();
                        for item in items {
                            type_params.push(self.convert_expr_to_type_annotation(item)?);
                        }
                        type_params
                    }
                    _ => vec![self.convert_expr_to_type_annotation(index)?],
                };

                Ok(TypeAnnotation::Parameterized(base_name, params))
            }
            ExprP::Op(lhs, BinOp::BitOr, rhs) => {
                if let ExprP::Identifier(ident) = &rhs.node {
                    if ident.node.ident == "None" {
                        let inner = self.convert_expr_to_type_annotation(lhs)?;
                        return Ok(TypeAnnotation::Optional(Box::new(inner)));
                    }
                }
                Err(BlueprintError::ValueError {
                    message: "invalid type annotation with | operator".into(),
                })
            }
            _ => Err(BlueprintError::ValueError {
                message: format!("unsupported type annotation expression"),
            }),
        }
    }

    #[async_recursion::async_recursion]
    pub async fn assign_target(&self, target: &AstAssignTarget, value: Value, scope: Arc<Scope>) -> Result<()> {
        match &target.node {
            AssignTargetP::Identifier(ident) => {
                scope.set(ident.node.ident.as_str(), value).await;
                Ok(())
            }
            AssignTargetP::Tuple(targets) => {
                let values = self.get_iterable(&value).await?;
                if values.len() != targets.len() {
                    return Err(BlueprintError::ValueError {
                        message: format!(
                            "cannot unpack {} values into {} targets",
                            values.len(),
                            targets.len()
                        ),
                    });
                }
                for (t, v) in targets.iter().zip(values) {
                    self.assign_target(t, v, scope.clone()).await?;
                }
                Ok(())
            }
            AssignTargetP::Index(pair) => {
                let (target_expr, index_expr) = pair.as_ref();
                let target_val = self.eval_expr(target_expr, scope.clone()).await?;
                let index_val = self.eval_expr(index_expr, scope).await?;

                match target_val {
                    Value::List(l) => {
                        let idx = index_val.as_int()?;
                        let mut items = l.write().await;
                        let len = items.len() as i64;
                        let actual_idx = if idx < 0 { len + idx } else { idx };
                        if actual_idx < 0 || actual_idx >= len {
                            return Err(BlueprintError::IndexError {
                                message: format!("list index {} out of range", idx),
                            });
                        }
                        items[actual_idx as usize] = value;
                        Ok(())
                    }
                    Value::Dict(d) => {
                        let key = self.value_to_dict_key(&index_val)?;
                        let mut map = d.write().await;
                        map.insert(key, value);
                        Ok(())
                    }
                    _ => Err(BlueprintError::TypeError {
                        expected: "list or dict".into(),
                        actual: target_val.type_name().into(),
                    }),
                }
            }
            AssignTargetP::Dot(_, attr) => {
                Err(BlueprintError::Unsupported {
                    message: format!("attribute assignment to .{} is not supported", attr.node),
                })
            }
        }
    }

    pub async fn eval_assign_target_value(&self, target: &AstAssignTarget, scope: Arc<Scope>) -> Result<Value> {
        match &target.node {
            AssignTargetP::Identifier(ident) => {
                let name = ident.node.ident.as_str();
                scope.get(name).await.ok_or_else(|| BlueprintError::NameError {
                    name: name.to_string(),
                })
            }
            AssignTargetP::Index(pair) => {
                let (target_expr, index_expr) = pair.as_ref();
                let target_val = self.eval_expr(target_expr, scope.clone()).await?;
                let index_val = self.eval_expr(index_expr, scope).await?;
                self.eval_index(target_val, index_val).await
            }
            AssignTargetP::Dot(target_expr, attr) => {
                let target_val = self.eval_expr(target_expr, scope).await?;
                target_val.get_attr(attr.node.as_str()).ok_or_else(|| BlueprintError::AttributeError {
                    type_name: target_val.type_name().into(),
                    attr: attr.node.to_string(),
                })
            }
            AssignTargetP::Tuple(_) => Err(BlueprintError::Unsupported {
                message: "augmented assignment to tuple".into(),
            }),
        }
    }

    pub async fn get_iterable(&self, value: &Value) -> Result<Vec<Value>> {
        match value {
            Value::List(l) => Ok(l.read().await.clone()),
            Value::Tuple(t) => Ok(t.as_ref().clone()),
            Value::String(s) => Ok(s.chars().map(|c| Value::String(Arc::new(c.to_string()))).collect()),
            Value::Dict(d) => {
                let map = d.read().await;
                Ok(map.keys().map(|k| Value::String(Arc::new(k.clone()))).collect())
            }
            Value::Set(s) => {
                let set = s.read().await;
                Ok(set.iter().cloned().collect())
            }
            _ => Err(BlueprintError::TypeError {
                expected: "iterable".into(),
                actual: value.type_name().into(),
            }),
        }
    }

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
                        self.eval_comprehension_clauses(body, next_for, &remaining[1..], iter_scope, results)
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
        self.eval_dict_comprehension_clauses(key_expr, val_expr, first, clauses, scope, &mut results)
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

    pub async fn handle_yield(
        &self,
        expr: Option<&AstExpr>,
        scope: Arc<Scope>,
    ) -> Result<Value> {
        let yield_tx = scope.get_yield_tx().ok_or_else(|| BlueprintError::ArgumentError {
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
        let body = func.body.downcast_ref::<AstStmt>().ok_or_else(|| {
            BlueprintError::InternalError {
                message: "Invalid function body".into(),
            }
        })?;

        if Self::contains_yield(body) {
            return self.create_generator(func, args, kwargs).await;
        }

        let closure_scope = func.closure.as_ref().and_then(|c| c.downcast_ref::<Arc<Scope>>().cloned());
        let base_scope = closure_scope.unwrap_or_else(Scope::new_global);
        let call_scope = Scope::new_child(base_scope, ScopeKind::Function);

        self.bind_parameters(&func.params, args, kwargs, &call_scope).await?;

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

        let closure_scope = func.closure.as_ref().and_then(|c| c.downcast_ref::<Arc<Scope>>().cloned());
        let base_scope = closure_scope.unwrap_or_else(Scope::new_global);
        let gen_scope = Scope::new_generator(base_scope, tx.clone());

        self.bind_parameters(&func.params, args, kwargs, &gen_scope).await?;

        let body = func.body.downcast_ref::<AstStmt>().ok_or_else(|| {
            BlueprintError::InternalError {
                message: "Invalid function body".into(),
            }
        })?.clone();

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
        let closure_scope = func.closure.as_ref().and_then(|c| c.downcast_ref::<Arc<Scope>>().cloned());
        let base_scope = closure_scope.unwrap_or_else(Scope::new_global);
        let call_scope = Scope::new_child(base_scope, ScopeKind::Function);

        self.bind_parameters(&func.params, args, kwargs, &call_scope).await?;

        let body = func.body.downcast_ref::<AstExpr>().ok_or_else(|| {
            BlueprintError::InternalError {
                message: "Invalid lambda body".into(),
            }
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
                        .define(&param.name, Value::List(Arc::new(tokio::sync::RwLock::new(remaining))))
                        .await;
                    arg_idx = args.len();
                }
                blueprint_engine_core::ParameterKind::Kwargs => {
                    let remaining: IndexMap<String, Value> = std::mem::take(&mut kwargs).into_iter().collect();
                    scope
                        .define(&param.name, Value::Dict(Arc::new(tokio::sync::RwLock::new(remaining))))
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
