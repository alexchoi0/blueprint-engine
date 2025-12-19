mod approval;
mod builtins;
mod console;
mod crypto;
mod file;
mod generator;
mod http;
mod json;
mod jwt;
mod parallel;
mod process;
mod redact;
mod task;
mod time;
pub mod triggers;
mod websocket;

use crate::eval::Evaluator;

pub fn register_all(evaluator: &mut Evaluator) {
    approval::register(evaluator);
    builtins::register(evaluator);
    console::register(evaluator);
    crypto::register(evaluator);
    file::register(evaluator);
    generator::register(evaluator);
    http::register(evaluator);
    json::register(evaluator);
    jwt::register(evaluator);
    parallel::register(evaluator);
    process::register(evaluator);
    redact::register(evaluator);
    task::register(evaluator);
    time::register(evaluator);
    triggers::register(evaluator);
    websocket::register(evaluator);
}
