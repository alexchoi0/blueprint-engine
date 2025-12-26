use std::sync::Arc;

use blueprint_engine_core::{BlueprintError, Result, StructField, StructType, TypeAnnotation, Value};
use blueprint_engine_parser::{AstExpr, ExprP};
use blueprint_starlark_syntax::syntax::ast::BinOp;

use super::Evaluator;
use crate::scope::Scope;

impl Evaluator {
    pub async fn eval_struct_def(
        &self,
        struct_def: &blueprint_starlark_syntax::syntax::ast::StructP<
            blueprint_starlark_syntax::syntax::ast::AstNoPayload,
        >,
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
        type_expr: &blueprint_starlark_syntax::syntax::ast::AstTypeExprP<
            blueprint_starlark_syntax::syntax::ast::AstNoPayload,
        >,
    ) -> Result<TypeAnnotation> {
        self.convert_expr_to_type_annotation(&type_expr.node.expr)
    }

    pub fn convert_expr_to_type_annotation(&self, expr: &AstExpr) -> Result<TypeAnnotation> {
        match &expr.node {
            ExprP::Identifier(ident) => Ok(TypeAnnotation::Simple(ident.node.ident.clone())),
            ExprP::Index(pair) => {
                let (base, index) = pair.as_ref();
                let base_name = match &base.node {
                    ExprP::Identifier(ident) => ident.node.ident.clone(),
                    _ => {
                        return Err(BlueprintError::ValueError {
                            message: "invalid type annotation".into(),
                        })
                    }
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
                message: "unsupported type annotation expression".into(),
            }),
        }
    }
}
