use std::collections::HashMap;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use crate::error::Result;
use super::Value;

pub type NativeFuture = Pin<Box<dyn Future<Output = Result<Value>> + Send>>;
pub type NativeFn = Arc<dyn Fn(Vec<Value>, HashMap<String, Value>) -> NativeFuture + Send + Sync>;

#[derive(Debug, Clone)]
pub struct Parameter {
    pub name: String,
    pub default: Option<Value>,
    pub kind: ParameterKind,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ParameterKind {
    Positional,
    Args,
    Kwargs,
}

pub struct UserFunction {
    pub name: String,
    pub params: Vec<Parameter>,
    pub body: Box<dyn std::any::Any + Send + Sync>,
    pub closure: Option<Arc<dyn std::any::Any + Send + Sync>>,
}

impl fmt::Debug for UserFunction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("UserFunction")
            .field("name", &self.name)
            .field("params", &self.params)
            .finish()
    }
}

pub struct LambdaFunction {
    pub params: Vec<Parameter>,
    pub body: Box<dyn std::any::Any + Send + Sync>,
    pub closure: Option<Arc<dyn std::any::Any + Send + Sync>>,
}

impl fmt::Debug for LambdaFunction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("LambdaFunction")
            .field("params", &self.params)
            .finish()
    }
}

pub struct NativeFunction {
    pub name: String,
    pub func: NativeFn,
}

impl fmt::Debug for NativeFunction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("NativeFunction")
            .field("name", &self.name)
            .finish()
    }
}

impl NativeFunction {
    pub fn new<F, Fut>(name: impl Into<String>, f: F) -> Self
    where
        F: Fn(Vec<Value>, HashMap<String, Value>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Value>> + Send + 'static,
    {
        NativeFunction {
            name: name.into(),
            func: Arc::new(move |args, kwargs| Box::pin(f(args, kwargs))),
        }
    }

    pub fn new_with_state<F>(name: impl Into<String>, f: F) -> Self
    where
        F: Fn(Vec<Value>, HashMap<String, Value>) -> NativeFuture + Send + Sync + 'static,
    {
        NativeFunction {
            name: name.into(),
            func: Arc::new(f),
        }
    }

    pub async fn call(&self, args: Vec<Value>, kwargs: HashMap<String, Value>) -> Result<Value> {
        (self.func)(args, kwargs).await
    }
}
