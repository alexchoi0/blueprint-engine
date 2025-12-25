use std::collections::HashMap;
use std::sync::Arc;

use blueprint_engine_core::{NativeFunction, Result, Value};
use indexmap::IndexMap;
use tokio::sync::RwLock;

use crate::scope::Scope;
use crate::Evaluator;

pub fn get_functions() -> Vec<NativeFunction> {
    vec![
        NativeFunction::new("eval", eval_fn),
    ]
}

async fn eval_fn(args: Vec<Value>, kwargs: HashMap<String, Value>) -> Result<Value> {
    let code = args
        .first()
        .ok_or_else(|| blueprint_engine_core::BlueprintError::TypeError {
            expected: "string".into(),
            actual: "missing argument".into(),
        })?
        .as_string()?;

    let filename = args
        .get(1)
        .and_then(|v| v.as_string().ok())
        .unwrap_or_else(|| "<eval>".to_string());

    let isolated = kwargs
        .get("isolated")
        .map(|v| v.is_truthy())
        .unwrap_or(false);

    let module = blueprint_engine_parser::parse(&filename, &code)?;

    let scope = Scope::new_global();
    let mut evaluator = if isolated {
        Evaluator::new_isolated()
    } else {
        Evaluator::new()
    };
    evaluator.set_file(&filename);
    evaluator.eval(&module, scope.clone()).await?;

    let exports = scope.exports().await;
    let mut dict = IndexMap::new();
    for (k, v) in exports {
        dict.insert(k, v);
    }

    Ok(Value::Dict(Arc::new(RwLock::new(dict))))
}
