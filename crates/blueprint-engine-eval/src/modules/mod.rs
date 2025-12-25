mod approval;
mod builtins;
mod console;
mod crypto;
mod file;
mod http;
mod json;
mod jwt;
mod parallel;
mod process;
mod random;
mod redact;
mod regex;
pub mod registry;
mod socket;
mod task;
mod time;
pub mod triggers;
mod websocket;

pub use registry::ModuleRegistry;

use crate::eval::Evaluator;

pub fn register_builtins(evaluator: &mut Evaluator) {
    builtins::register(evaluator);
    console::register(evaluator);
}

pub fn build_registry() -> ModuleRegistry {
    let mut registry = ModuleRegistry::new();
    registry.register_module("approval", approval::get_functions());
    registry.register_module("crypto", crypto::get_functions());
    registry.register_module("file", file::get_functions());
    registry.register_module("http", http::get_functions());
    registry.register_module("json", json::get_functions());
    registry.register_module("jwt", jwt::get_functions());
    registry.register_module("parallel", parallel::get_functions());
    registry.register_module("process", process::get_functions());
    registry.register_module("random", random::get_functions());
    registry.register_module("redact", redact::get_functions());
    registry.register_module("regex", regex::get_functions());
    registry.register_module("socket", socket::get_functions());
    registry.register_module("task", task::get_functions());
    registry.register_module("time", time::get_functions());
    registry.register_module("triggers", triggers::get_functions());
    registry.register_module("websocket", websocket::get_functions());
    registry
}
