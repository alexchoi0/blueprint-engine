use std::collections::HashMap;
use std::sync::Arc;

use indexmap::{IndexMap, IndexSet};

use blueprint_engine_core::{BlueprintError, Result, SourceLocation, Value};
use blueprint_engine_parser::AstExpr;
use blueprint_starlark_syntax::syntax::ast::{AstLiteral, BinOp, ExprP};

use super::ops;
use super::Evaluator;
use crate::scope::Scope;

impl Evaluator {
    #[async_recursion::async_recursion]
    pub async fn eval_expr(&self, expr: &AstExpr, scope: Arc<Scope>) -> Result<Value> {
        match &expr.node {
            ExprP::Literal(lit) => self.eval_literal(lit),

            ExprP::Identifier(ident) => {
                let name = ident.node.ident.as_str();

                match name {
                    "True" => return Ok(Value::Bool(true)),
                    "False" => return Ok(Value::Bool(false)),
                    "None" => return Ok(Value::None),
                    _ => {}
                }

                if let Some(value) = scope.get(name).await {
                    return Ok(value);
                }

                if let Some(native) = self.builtins.get(name) {
                    return Ok(Value::NativeFunction(native.clone()));
                }

                if let Some(module_funcs) = self.custom_modules.get(name) {
                    let mut dict = IndexMap::new();
                    for (func_name, func) in module_funcs {
                        dict.insert(func_name.clone(), Value::NativeFunction(func.clone()));
                    }
                    return Ok(Value::Dict(Arc::new(tokio::sync::RwLock::new(dict))));
                }

                Err(BlueprintError::NameError {
                    name: name.to_string(),
                })
            }

            ExprP::Tuple(items) => {
                let mut values = Vec::with_capacity(items.len());
                for item in items {
                    values.push(self.eval_expr(item, scope.clone()).await?);
                }
                Ok(Value::Tuple(Arc::new(values)))
            }

            ExprP::List(items) => {
                let mut values = Vec::with_capacity(items.len());
                for item in items {
                    values.push(self.eval_expr(item, scope.clone()).await?);
                }
                Ok(Value::List(Arc::new(tokio::sync::RwLock::new(values))))
            }

            ExprP::Dict(pairs) => {
                let mut map = IndexMap::new();
                for (key, value) in pairs {
                    let k = self.eval_expr(key, scope.clone()).await?;
                    let k_str = self.value_to_dict_key(&k)?;
                    let v = self.eval_expr(value, scope.clone()).await?;
                    map.insert(k_str, v);
                }
                Ok(Value::Dict(Arc::new(tokio::sync::RwLock::new(map))))
            }

            ExprP::Set(items) => {
                let mut set = IndexSet::new();
                for item in items {
                    let v = self.eval_expr(item, scope.clone()).await?;
                    set.insert(v);
                }
                Ok(Value::Set(Arc::new(tokio::sync::RwLock::new(set))))
            }

            ExprP::Call(callee, args) => {
                let func = self.eval_expr(callee, scope.clone()).await?;
                let (positional, kwargs) = self.eval_call_args(&args.args, scope.clone()).await?;
                self.call_function(func, positional, kwargs, scope).await
            }

            ExprP::Index(pair) => {
                let (target, index) = pair.as_ref();
                let target_val = self.eval_expr(target, scope.clone()).await?;
                let index_val = self.eval_expr(index, scope).await?;
                self.eval_index(target_val, index_val).await.map_err(|e| {
                    let (line, column) = self.get_span_location(&expr.span);
                    let file = self
                        .current_file
                        .as_ref()
                        .map(|p| p.to_string_lossy().to_string());
                    e.with_location(SourceLocation {
                        file,
                        line,
                        column,
                        span: None,
                    })
                })
            }

            ExprP::Index2(triple) => {
                let (target, start, end) = triple.as_ref();
                let target_val = self.eval_expr(target, scope.clone()).await?;
                let start_val = self.eval_expr(start, scope.clone()).await?;
                let end_val = self.eval_expr(end, scope).await?;
                self.eval_slice(target_val, Some(start_val), Some(end_val))
                    .await
            }

            ExprP::Dot(target, attr) => {
                let target_val = self.eval_expr(target, scope).await?;
                let attr_name = attr.node.as_str();

                if let Value::Dict(d) = &target_val {
                    let map = d.read().await;
                    if let Some(v) = map.get(attr_name) {
                        return Ok(v.clone());
                    }
                }

                match target_val.get_attr(attr_name) {
                    Some(v) => Ok(v),
                    None => Err(BlueprintError::AttributeError {
                        type_name: target_val.type_name().into(),
                        attr: attr_name.to_string(),
                    }),
                }
            }

            ExprP::Not(inner) => {
                let value = self.eval_expr(inner, scope).await?;
                Ok(Value::Bool(!value.is_truthy()))
            }

            ExprP::Minus(inner) => {
                let value = self.eval_expr(inner, scope).await?;
                ops::eval_unary_minus(value)
            }

            ExprP::Plus(inner) => {
                let value = self.eval_expr(inner, scope).await?;
                match &value {
                    Value::Int(_) | Value::Float(_) => Ok(value),
                    _ => Err(BlueprintError::TypeError {
                        expected: "number".into(),
                        actual: value.type_name().into(),
                    }),
                }
            }

            ExprP::Op(lhs, op, rhs) => {
                let left = self.eval_expr(lhs, scope.clone()).await?;

                match op {
                    BinOp::And => {
                        if !left.is_truthy() {
                            return Ok(left);
                        }
                        return self.eval_expr(rhs, scope).await;
                    }
                    BinOp::Or => {
                        if left.is_truthy() {
                            return Ok(left);
                        }
                        return self.eval_expr(rhs, scope).await;
                    }
                    BinOp::In => {
                        let right = self.eval_expr(rhs, scope).await?;
                        return self.eval_in(left, right).await;
                    }
                    BinOp::NotIn => {
                        let right = self.eval_expr(rhs, scope).await?;
                        let result = self.eval_in(left, right).await?;
                        return match result {
                            Value::Bool(b) => Ok(Value::Bool(!b)),
                            _ => unreachable!(),
                        };
                    }
                    _ => {}
                }

                let right = self.eval_expr(rhs, scope).await?;
                ops::eval_binary_op(left, *op, right).await.map_err(|e| {
                    let (line, column) = self.get_span_location(&expr.span);
                    let file = self
                        .current_file
                        .as_ref()
                        .map(|p| p.to_string_lossy().to_string());
                    e.with_location(SourceLocation {
                        file,
                        line,
                        column,
                        span: None,
                    })
                })
            }

            ExprP::If(triple) => {
                let (cond, then_expr, else_expr) = triple.as_ref();
                let cond_val = self.eval_expr(cond, scope.clone()).await?;
                if cond_val.is_truthy() {
                    self.eval_expr(then_expr, scope).await
                } else {
                    self.eval_expr(else_expr, scope).await
                }
            }

            ExprP::Lambda(lambda) => {
                let func = self.create_lambda_function(lambda, scope)?;
                Ok(func)
            }

            ExprP::ListComprehension(body, first, clauses) => {
                self.eval_list_comprehension(body, first, clauses, scope)
                    .await
            }

            ExprP::SetComprehension(body, first, clauses) => {
                self.eval_set_comprehension(body, first, clauses, scope)
                    .await
            }

            ExprP::DictComprehension(pair, first, clauses) => {
                let (key_expr, val_expr) = pair.as_ref();
                self.eval_dict_comprehension(key_expr, val_expr, first, clauses, scope)
                    .await
            }

            ExprP::Slice(arr, start, stop, step) => {
                let arr_val = self.eval_expr(arr, scope.clone()).await?;
                let start_val = match start {
                    Some(s) => Some(self.eval_expr(s, scope.clone()).await?),
                    None => None,
                };
                let stop_val = match stop {
                    Some(s) => Some(self.eval_expr(s, scope.clone()).await?),
                    None => None,
                };
                let step_val = match step {
                    Some(s) => Some(self.eval_expr(s, scope).await?),
                    None => None,
                };
                self.eval_slice_with_step(arr_val, start_val, stop_val, step_val)
                    .await
            }

            ExprP::FString(fstring) => {
                let format_str = &fstring.format.node;
                let expressions = &fstring.expressions;
                let mut result = String::new();
                let mut expr_iter = expressions.iter();

                for segment in format_str.split("{}") {
                    result.push_str(segment);
                    if let Some(expr) = expr_iter.next() {
                        let val = self.eval_expr(expr, scope.clone()).await?;
                        result.push_str(&val.to_display_string());
                    }
                }
                Ok(Value::String(Arc::new(result)))
            }

            _ => Err(BlueprintError::InternalError {
                message: format!(
                    "Unhandled expression type: {:?}",
                    std::mem::discriminant(&expr.node)
                ),
            }),
        }
    }

    pub fn eval_literal(&self, lit: &AstLiteral) -> Result<Value> {
        use blueprint_starlark_syntax::lexer::TokenInt;
        match lit {
            AstLiteral::Int(i) => {
                let val =
                    match &i.node {
                        TokenInt::I32(n) => *n as i64,
                        TokenInt::BigInt(n) => n.to_string().parse::<i64>().map_err(|_| {
                            BlueprintError::ValueError {
                                message: "Integer overflow".into(),
                            }
                        })?,
                    };
                Ok(Value::Int(val))
            }
            AstLiteral::Float(f) => Ok(Value::Float(f.node)),
            AstLiteral::String(s) => Ok(Value::String(Arc::new(s.node.clone()))),
            AstLiteral::ByteString(b) => {
                let s: String = b.node.iter().map(|&c| c as char).collect();
                Ok(Value::String(Arc::new(s)))
            }
            AstLiteral::Ellipsis => Ok(Value::None),
        }
    }

    pub async fn eval_index(&self, target: Value, index: Value) -> Result<Value> {
        match &target {
            Value::List(l) => {
                let idx = index.as_int()?;
                let items = l.read().await;
                let len = items.len() as i64;
                let actual_idx = if idx < 0 { len + idx } else { idx };
                if actual_idx < 0 || actual_idx >= len {
                    Err(BlueprintError::IndexError {
                        message: format!("list index {} out of range (len={})", idx, len),
                    })
                } else {
                    Ok(items[actual_idx as usize].clone())
                }
            }
            Value::Tuple(t) => {
                let idx = index.as_int()?;
                let len = t.len() as i64;
                let actual_idx = if idx < 0 { len + idx } else { idx };
                if actual_idx < 0 || actual_idx >= len {
                    Err(BlueprintError::IndexError {
                        message: format!("tuple index {} out of range (len={})", idx, len),
                    })
                } else {
                    Ok(t[actual_idx as usize].clone())
                }
            }
            Value::String(s) => {
                let idx = index.as_int()?;
                let chars: Vec<char> = s.chars().collect();
                let len = chars.len() as i64;
                let actual_idx = if idx < 0 { len + idx } else { idx };
                if actual_idx < 0 || actual_idx >= len {
                    Err(BlueprintError::IndexError {
                        message: format!("string index {} out of range (len={})", idx, len),
                    })
                } else {
                    Ok(Value::String(Arc::new(
                        chars[actual_idx as usize].to_string(),
                    )))
                }
            }
            Value::Dict(d) => {
                let key = self.value_to_dict_key(&index)?;
                let map = d.read().await;
                match map.get(&key) {
                    Some(v) => Ok(v.clone()),
                    None => Err(BlueprintError::KeyError { key }),
                }
            }
            Value::Generator(_) | Value::Iterator(_) => Err(BlueprintError::TypeError {
                expected: "subscriptable (use list() to materialize generator first)".into(),
                actual: target.type_name().into(),
            }),
            _ => Err(BlueprintError::TypeError {
                expected: "subscriptable".into(),
                actual: target.type_name().into(),
            }),
        }
    }

    pub async fn eval_slice(
        &self,
        target: Value,
        start: Option<Value>,
        end: Option<Value>,
    ) -> Result<Value> {
        match &target {
            Value::List(l) => {
                let items = l.read().await;
                let len = items.len() as i64;
                let (start_idx, end_idx) = self.normalize_slice_indices(start, end, len)?;
                let slice: Vec<Value> = items[start_idx..end_idx].to_vec();
                Ok(Value::List(Arc::new(tokio::sync::RwLock::new(slice))))
            }
            Value::String(s) => {
                let chars: Vec<char> = s.chars().collect();
                let len = chars.len() as i64;
                let (start_idx, end_idx) = self.normalize_slice_indices(start, end, len)?;
                let slice: String = chars[start_idx..end_idx].iter().collect();
                Ok(Value::String(Arc::new(slice)))
            }
            Value::Tuple(t) => {
                let len = t.len() as i64;
                let (start_idx, end_idx) = self.normalize_slice_indices(start, end, len)?;
                let slice: Vec<Value> = t[start_idx..end_idx].to_vec();
                Ok(Value::Tuple(Arc::new(slice)))
            }
            _ => Err(BlueprintError::TypeError {
                expected: "sliceable".into(),
                actual: target.type_name().into(),
            }),
        }
    }

    pub fn normalize_slice_indices(
        &self,
        start: Option<Value>,
        end: Option<Value>,
        len: i64,
    ) -> Result<(usize, usize)> {
        let start_idx = match start {
            Some(Value::Int(i)) => {
                if i < 0 {
                    (len + i).max(0) as usize
                } else {
                    i.min(len) as usize
                }
            }
            Some(Value::None) | None => 0,
            Some(v) => {
                return Err(BlueprintError::TypeError {
                    expected: "int or None".into(),
                    actual: v.type_name().into(),
                })
            }
        };

        let end_idx = match end {
            Some(Value::Int(i)) => {
                if i < 0 {
                    (len + i).max(0) as usize
                } else {
                    i.min(len) as usize
                }
            }
            Some(Value::None) | None => len as usize,
            Some(v) => {
                return Err(BlueprintError::TypeError {
                    expected: "int or None".into(),
                    actual: v.type_name().into(),
                })
            }
        };

        Ok((start_idx.min(end_idx), end_idx))
    }

    pub async fn eval_slice_with_step(
        &self,
        target: Value,
        start: Option<Value>,
        end: Option<Value>,
        step: Option<Value>,
    ) -> Result<Value> {
        let step_val = match step {
            Some(Value::Int(s)) if s != 0 => s,
            Some(Value::Int(0)) => {
                return Err(BlueprintError::ValueError {
                    message: "slice step cannot be zero".into(),
                })
            }
            Some(Value::None) | None => 1,
            Some(v) => {
                return Err(BlueprintError::TypeError {
                    expected: "int or None".into(),
                    actual: v.type_name().into(),
                })
            }
        };

        if step_val == 1 {
            return self.eval_slice(target, start, end).await;
        }

        match &target {
            Value::List(l) => {
                let items = l.read().await;
                let len = items.len() as i64;
                let (start_idx, end_idx) = self.get_step_indices(start, end, step_val, len)?;
                let slice = self.collect_with_step(&items, start_idx, end_idx, step_val);
                Ok(Value::List(Arc::new(tokio::sync::RwLock::new(slice))))
            }
            Value::String(s) => {
                let chars: Vec<char> = s.chars().collect();
                let len = chars.len() as i64;
                let (start_idx, end_idx) = self.get_step_indices(start, end, step_val, len)?;
                let char_values: Vec<Value> = chars
                    .iter()
                    .map(|c| Value::String(Arc::new(c.to_string())))
                    .collect();
                let slice = self.collect_with_step(&char_values, start_idx, end_idx, step_val);
                let result: String = slice
                    .into_iter()
                    .filter_map(|v| {
                        if let Value::String(s) = v {
                            Some(s.as_ref().clone())
                        } else {
                            None
                        }
                    })
                    .collect();
                Ok(Value::String(Arc::new(result)))
            }
            Value::Tuple(t) => {
                let len = t.len() as i64;
                let (start_idx, end_idx) = self.get_step_indices(start, end, step_val, len)?;
                let slice = self.collect_with_step(t.as_ref(), start_idx, end_idx, step_val);
                Ok(Value::Tuple(Arc::new(slice)))
            }
            _ => Err(BlueprintError::TypeError {
                expected: "sliceable".into(),
                actual: target.type_name().into(),
            }),
        }
    }

    pub fn get_step_indices(
        &self,
        start: Option<Value>,
        end: Option<Value>,
        step: i64,
        len: i64,
    ) -> Result<(i64, i64)> {
        let (default_start, default_end) = if step > 0 {
            (0, len)
        } else {
            (len - 1, -len - 1)
        };

        let start_idx = match start {
            Some(Value::Int(i)) => {
                if i < 0 {
                    (len + i).max(if step > 0 { 0 } else { -1 })
                } else {
                    i.min(len)
                }
            }
            Some(Value::None) | None => default_start,
            Some(v) => {
                return Err(BlueprintError::TypeError {
                    expected: "int or None".into(),
                    actual: v.type_name().into(),
                })
            }
        };

        let end_idx = match end {
            Some(Value::Int(i)) => {
                if i < 0 {
                    len + i
                } else {
                    i.min(len)
                }
            }
            Some(Value::None) | None => default_end,
            Some(v) => {
                return Err(BlueprintError::TypeError {
                    expected: "int or None".into(),
                    actual: v.type_name().into(),
                })
            }
        };

        Ok((start_idx, end_idx))
    }

    pub fn collect_with_step<T: Clone>(
        &self,
        items: &[T],
        start: i64,
        end: i64,
        step: i64,
    ) -> Vec<T> {
        let mut result = Vec::new();
        let mut i = start;
        if step > 0 {
            while i < end && i >= 0 && (i as usize) < items.len() {
                result.push(items[i as usize].clone());
                i += step;
            }
        } else {
            while i > end && i >= 0 && (i as usize) < items.len() {
                result.push(items[i as usize].clone());
                i += step;
            }
        }
        result
    }

    pub async fn eval_in(&self, left: Value, right: Value) -> Result<Value> {
        ops::eval_in(left, right, |v| self.value_to_dict_key(v)).await
    }

    pub async fn eval_call_args(
        &self,
        args: &[blueprint_engine_parser::AstArgument],
        scope: Arc<Scope>,
    ) -> Result<(Vec<Value>, HashMap<String, Value>)> {
        use blueprint_starlark_syntax::syntax::ast::ArgumentP;

        let mut positional = Vec::new();
        let mut kwargs = HashMap::new();

        for arg in args {
            match &arg.node {
                ArgumentP::Positional(expr) => {
                    positional.push(self.eval_expr(expr, scope.clone()).await?);
                }
                ArgumentP::Named(name, expr) => {
                    let value = self.eval_expr(expr, scope.clone()).await?;
                    kwargs.insert(name.node.clone(), value);
                }
                ArgumentP::Args(expr) => {
                    let value = self.eval_expr(expr, scope.clone()).await?;
                    let items = self.get_iterable(&value).await?;
                    positional.extend(items);
                }
                ArgumentP::KwArgs(expr) => {
                    let value = self.eval_expr(expr, scope.clone()).await?;
                    if let Value::Dict(d) = value {
                        let map = d.read().await;
                        for (k, v) in map.iter() {
                            kwargs.insert(k.clone(), v.clone());
                        }
                    } else {
                        return Err(BlueprintError::TypeError {
                            expected: "dict".into(),
                            actual: value.type_name().into(),
                        });
                    }
                }
            }
        }

        Ok((positional, kwargs))
    }
}
