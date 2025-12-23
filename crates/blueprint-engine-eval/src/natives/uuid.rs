use std::collections::HashMap;
use std::sync::Arc;

use blueprint_engine_core::{NativeFunction, Result, Value};
use uuid::Uuid;

use crate::eval::Evaluator;

pub fn register(evaluator: &mut Evaluator) {
    evaluator.register_native(NativeFunction::new("uuid", uuid_fn));
    evaluator.register_native(NativeFunction::new("uuid_v4", uuid_v4_fn));
    evaluator.register_native(NativeFunction::new("uuid_v7", uuid_v7_fn));
}

async fn uuid_fn(_args: Vec<Value>, _kwargs: HashMap<String, Value>) -> Result<Value> {
    let id = Uuid::new_v4();
    Ok(Value::String(Arc::new(id.to_string())))
}

async fn uuid_v4_fn(_args: Vec<Value>, _kwargs: HashMap<String, Value>) -> Result<Value> {
    let id = Uuid::new_v4();
    Ok(Value::String(Arc::new(id.to_string())))
}

async fn uuid_v7_fn(_args: Vec<Value>, _kwargs: HashMap<String, Value>) -> Result<Value> {
    let id = Uuid::now_v7();
    Ok(Value::String(Arc::new(id.to_string())))
}
