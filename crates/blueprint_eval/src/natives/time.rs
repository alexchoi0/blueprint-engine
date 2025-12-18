use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

use blueprint_core::{BlueprintError, NativeFunction, Result, Value};
use tokio::time::{sleep, Duration};

use crate::eval::Evaluator;

pub fn register(evaluator: &mut Evaluator) {
    evaluator.register_native(NativeFunction::new("now", now));
    evaluator.register_native(NativeFunction::new("sleep", sleep_fn));
    evaluator.register_native(NativeFunction::new("time", now));
}

async fn now(_args: Vec<Value>, _kwargs: HashMap<String, Value>) -> Result<Value> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();

    Ok(Value::Float(duration.as_secs_f64()))
}

async fn sleep_fn(args: Vec<Value>, _kwargs: HashMap<String, Value>) -> Result<Value> {
    if args.len() != 1 {
        return Err(BlueprintError::ArgumentError {
            message: format!("sleep() takes exactly 1 argument ({} given)", args.len()),
        });
    }

    let seconds = args[0].as_float()?;

    if seconds < 0.0 {
        return Err(BlueprintError::ValueError {
            message: "sleep() argument must not be negative".into(),
        });
    }

    sleep(Duration::from_secs_f64(seconds)).await;

    Ok(Value::None)
}
