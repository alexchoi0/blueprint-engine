mod builtins;
mod console;
mod file;
mod http;
mod json;
mod parallel;
mod process;
mod time;

use crate::eval::Evaluator;

pub fn register_all(evaluator: &mut Evaluator) {
    builtins::register(evaluator);
    console::register(evaluator);
    file::register(evaluator);
    http::register(evaluator);
    json::register(evaluator);
    parallel::register(evaluator);
    process::register(evaluator);
    time::register(evaluator);
}
