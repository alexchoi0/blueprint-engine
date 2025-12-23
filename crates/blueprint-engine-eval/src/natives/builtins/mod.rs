mod control;
mod introspection;
mod iterators;
mod math;
mod types;

use std::collections::HashMap;
use std::sync::Arc;

use blueprint_engine_core::{BlueprintError, NativeFunction, Result, Value};

use crate::eval::Evaluator;
use crate::scope::{Scope, ScopeKind};

pub fn register(evaluator: &mut Evaluator) {
    evaluator.register_native(NativeFunction::new("len", introspection::len));
    evaluator.register_native(NativeFunction::new("str", types::to_str));
    evaluator.register_native(NativeFunction::new("int", types::to_int));
    evaluator.register_native(NativeFunction::new("float", types::to_float));
    evaluator.register_native(NativeFunction::new("bool", types::to_bool));
    evaluator.register_native(NativeFunction::new("list", types::to_list));
    evaluator.register_native(NativeFunction::new("dict", types::to_dict));
    evaluator.register_native(NativeFunction::new("tuple", types::to_tuple));
    evaluator.register_native(NativeFunction::new("set", types::to_set));
    evaluator.register_native(NativeFunction::new("iter", types::to_iter));
    evaluator.register_native(NativeFunction::new("range", iterators::range));
    evaluator.register_native(NativeFunction::new("map", iterators::map_fn));
    evaluator.register_native(NativeFunction::new("filter", iterators::filter_fn));
    evaluator.register_native(NativeFunction::new("enumerate", iterators::enumerate));
    evaluator.register_native(NativeFunction::new("zip", iterators::zip));
    evaluator.register_native(NativeFunction::new("sorted", iterators::sorted));
    evaluator.register_native(NativeFunction::new("reversed", iterators::reversed));
    evaluator.register_native(NativeFunction::new("min", math::min));
    evaluator.register_native(NativeFunction::new("max", math::max));
    evaluator.register_native(NativeFunction::new("sum", math::sum));
    evaluator.register_native(NativeFunction::new("abs", math::abs));
    evaluator.register_native(NativeFunction::new("all", math::all));
    evaluator.register_native(NativeFunction::new("any", math::any));
    evaluator.register_native(NativeFunction::new("type", introspection::type_of));
    evaluator.register_native(NativeFunction::new("hasattr", introspection::hasattr));
    evaluator.register_native(NativeFunction::new("getattr", introspection::getattr));
    evaluator.register_native(NativeFunction::new("repr", introspection::repr));
    evaluator.register_native(NativeFunction::new("fail", control::fail));
    evaluator.register_native(NativeFunction::new("exit", control::exit));
    evaluator.register_native(NativeFunction::new("assert", control::assert_fn));
}

pub async fn call_func(func: &Value, args: Vec<Value>) -> Result<Value> {
    match func {
        Value::Lambda(lambda) => {
            let body = lambda
                .body
                .downcast_ref::<blueprint_engine_parser::AstExpr>()
                .ok_or_else(|| BlueprintError::InternalError {
                    message: "Invalid lambda body".into(),
                })?;

            let closure_scope = lambda
                .closure
                .as_ref()
                .and_then(|c| c.downcast_ref::<Arc<Scope>>().cloned());

            let base_scope = closure_scope.unwrap_or_else(Scope::new_global);
            let call_scope = Scope::new_child(base_scope, ScopeKind::Function);

            for (i, param) in lambda.params.iter().enumerate() {
                let value = args.get(i).cloned().or_else(|| param.default.clone());
                if let Some(v) = value {
                    call_scope.define(&param.name, v).await;
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
                    message: "Invalid function body".into(),
                })?;

            let closure_scope = func
                .closure
                .as_ref()
                .and_then(|c| c.downcast_ref::<Arc<Scope>>().cloned());

            let base_scope = closure_scope.unwrap_or_else(Scope::new_global);
            let call_scope = Scope::new_child(base_scope, ScopeKind::Function);

            for (i, param) in func.params.iter().enumerate() {
                let value = args.get(i).cloned().or_else(|| param.default.clone());
                if let Some(v) = value {
                    call_scope.define(&param.name, v).await;
                }
            }

            let evaluator = Evaluator::new();
            match evaluator.eval_stmt(body, call_scope).await {
                Ok(_) => Ok(Value::None),
                Err(BlueprintError::Return { value }) => Ok((*value).clone()),
                Err(e) => Err(e),
            }
        }
        Value::NativeFunction(native) => native.call(args, HashMap::new()).await,
        _ => Err(BlueprintError::NotCallable {
            type_name: func.type_name().into(),
        }),
    }
}
