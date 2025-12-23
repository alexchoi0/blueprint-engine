use std::collections::HashMap;
use std::sync::Arc;

use indexmap::IndexMap;
use tokio::sync::RwLock;

use super::Value;

#[derive(Debug, Clone)]
pub struct HttpResponse {
    pub status: i64,
    pub body: String,
    pub headers: HashMap<String, String>,
}

impl HttpResponse {
    pub fn get_attr(&self, name: &str) -> Option<Value> {
        match name {
            "status" => Some(Value::Int(self.status)),
            "body" => Some(Value::String(Arc::new(self.body.clone()))),
            "headers" => {
                let map: IndexMap<String, Value> = self
                    .headers
                    .iter()
                    .map(|(k, v)| (k.clone(), Value::String(Arc::new(v.clone()))))
                    .collect();
                Some(Value::Dict(Arc::new(RwLock::new(map))))
            }
            _ => None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ProcessResult {
    pub code: i64,
    pub stdout: String,
    pub stderr: String,
}

impl ProcessResult {
    pub fn get_attr(&self, name: &str) -> Option<Value> {
        match name {
            "code" => Some(Value::Int(self.code)),
            "stdout" => Some(Value::String(Arc::new(self.stdout.clone()))),
            "stderr" => Some(Value::String(Arc::new(self.stderr.clone()))),
            _ => None,
        }
    }
}
