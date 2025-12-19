use std::collections::HashMap;

use blueprint_core::{BlueprintError, NativeFunction, Result, Value};

use crate::eval::Evaluator;

pub fn register(evaluator: &mut Evaluator) {
    evaluator.register_native(NativeFunction::new("emit", emit_value));
}

async fn emit_value(_args: Vec<Value>, _kwargs: HashMap<String, Value>) -> Result<Value> {
    Err(BlueprintError::InternalError {
        message: "emit() called outside of generator context. This should be handled by the evaluator.".into(),
    })
}
