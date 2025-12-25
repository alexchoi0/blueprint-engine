use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

use blueprint_engine_core::{
    validation::{get_float_arg, require_args},
    BlueprintError, NativeFunction, Result, Value,
};
use tokio::time::{sleep, Duration};

pub fn get_functions() -> Vec<NativeFunction> {
    vec![
        NativeFunction::new("now", now),
        NativeFunction::new("sleep", sleep_fn),
        NativeFunction::new("time", now),
    ]
}

async fn now(_args: Vec<Value>, _kwargs: HashMap<String, Value>) -> Result<Value> {
    let duration = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();

    Ok(Value::Float(duration.as_secs_f64()))
}

async fn sleep_fn(args: Vec<Value>, _kwargs: HashMap<String, Value>) -> Result<Value> {
    require_args("time.sleep", &args, 1)?;
    let seconds = get_float_arg("time.sleep", &args, 0)?;

    if seconds < 0.0 {
        return Err(BlueprintError::ValueError {
            message: "sleep() argument must not be negative".into(),
        });
    }

    sleep(Duration::from_secs_f64(seconds)).await;

    Ok(Value::None)
}
