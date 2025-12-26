use std::sync::Arc;

use blueprint_engine_core::{BlueprintError, Result, Value};
use blueprint_starlark_syntax::syntax::ast::{AssignTargetP, AstAssignTarget};

use super::Evaluator;
use crate::scope::Scope;

impl Evaluator {
    #[async_recursion::async_recursion]
    pub async fn assign_target(
        &self,
        target: &AstAssignTarget,
        value: Value,
        scope: Arc<Scope>,
    ) -> Result<()> {
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
            AssignTargetP::Dot(_, attr) => Err(BlueprintError::Unsupported {
                message: format!("attribute assignment to .{} is not supported", attr.node),
            }),
        }
    }

    pub async fn eval_assign_target_value(
        &self,
        target: &AstAssignTarget,
        scope: Arc<Scope>,
    ) -> Result<Value> {
        match &target.node {
            AssignTargetP::Identifier(ident) => {
                let name = ident.node.ident.as_str();
                scope
                    .get(name)
                    .await
                    .ok_or_else(|| BlueprintError::NameError {
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
                target_val.get_attr(attr.node.as_str()).ok_or_else(|| {
                    BlueprintError::AttributeError {
                        type_name: target_val.type_name().into(),
                        attr: attr.node.to_string(),
                    }
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
            Value::String(s) => Ok(s
                .chars()
                .map(|c| Value::String(Arc::new(c.to_string())))
                .collect()),
            Value::Dict(d) => {
                let map = d.read().await;
                Ok(map
                    .keys()
                    .map(|k| Value::String(Arc::new(k.clone())))
                    .collect())
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
}
