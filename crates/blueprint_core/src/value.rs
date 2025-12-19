use std::collections::HashMap;
use std::fmt;
use std::future::Future;
use std::hash::{Hash, Hasher};
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::{mpsc, oneshot, Mutex, RwLock};

use crate::error::{BlueprintError, Result};

pub type NativeFuture = Pin<Box<dyn Future<Output = Result<Value>> + Send>>;
pub type NativeFn = Arc<dyn Fn(Vec<Value>, HashMap<String, Value>) -> NativeFuture + Send + Sync>;

#[derive(Clone)]
pub enum Value {
    None,
    Bool(bool),
    Int(i64),
    Float(f64),
    String(Arc<String>),
    List(Arc<RwLock<Vec<Value>>>),
    Dict(Arc<RwLock<HashMap<String, Value>>>),
    Tuple(Arc<Vec<Value>>),
    Function(Arc<UserFunction>),
    Lambda(Arc<LambdaFunction>),
    NativeFunction(Arc<NativeFunction>),
    Response(Arc<HttpResponse>),
    ProcessResult(Arc<ProcessResult>),
    Iterator(Arc<StreamIterator>),
    Generator(Arc<Generator>),
    StructType(Arc<StructType>),
    StructInstance(Arc<StructInstance>),
}

impl fmt::Debug for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::None => write!(f, "None"),
            Value::Bool(b) => write!(f, "Bool({b})"),
            Value::Int(i) => write!(f, "Int({i})"),
            Value::Float(fl) => write!(f, "Float({fl})"),
            Value::String(s) => write!(f, "String({s:?})"),
            Value::List(_) => write!(f, "List([...])"),
            Value::Dict(_) => write!(f, "Dict({{...}})"),
            Value::Tuple(t) => write!(f, "Tuple({:?})", t.as_ref()),
            Value::Function(func) => write!(f, "Function({})", func.name),
            Value::Lambda(_) => write!(f, "Lambda"),
            Value::NativeFunction(func) => write!(f, "NativeFunction({})", func.name),
            Value::Response(r) => write!(f, "Response(status={})", r.status),
            Value::ProcessResult(r) => write!(f, "ProcessResult(code={})", r.code),
            Value::Iterator(_) => write!(f, "Iterator"),
            Value::Generator(_) => write!(f, "Generator"),
            Value::StructType(s) => write!(f, "StructType({})", s.name),
            Value::StructInstance(s) => write!(f, "StructInstance({})", s.struct_type.name),
        }
    }
}

impl Value {
    pub fn type_name(&self) -> &'static str {
        match self {
            Value::None => "NoneType",
            Value::Bool(_) => "bool",
            Value::Int(_) => "int",
            Value::Float(_) => "float",
            Value::String(_) => "string",
            Value::List(_) => "list",
            Value::Dict(_) => "dict",
            Value::Tuple(_) => "tuple",
            Value::Function(_) => "function",
            Value::Lambda(_) => "function",
            Value::NativeFunction(_) => "builtin_function",
            Value::Response(_) => "Response",
            Value::ProcessResult(_) => "Result",
            Value::Iterator(_) => "iterator",
            Value::Generator(_) => "generator",
            Value::StructType(_) => "type",
            Value::StructInstance(_) => "struct",
        }
    }

    pub fn is_truthy(&self) -> bool {
        match self {
            Value::None => false,
            Value::Bool(b) => *b,
            Value::Int(i) => *i != 0,
            Value::Float(f) => *f != 0.0,
            Value::String(s) => !s.is_empty(),
            Value::List(l) => {
                if let Ok(guard) = l.try_read() {
                    !guard.is_empty()
                } else {
                    true
                }
            }
            Value::Dict(d) => {
                if let Ok(guard) = d.try_read() {
                    !guard.is_empty()
                } else {
                    true
                }
            }
            Value::Tuple(t) => !t.is_empty(),
            _ => true,
        }
    }

    pub async fn is_truthy_async(&self) -> bool {
        match self {
            Value::None => false,
            Value::Bool(b) => *b,
            Value::Int(i) => *i != 0,
            Value::Float(f) => *f != 0.0,
            Value::String(s) => !s.is_empty(),
            Value::List(l) => {
                let guard = l.read().await;
                !guard.is_empty()
            }
            Value::Dict(d) => {
                let guard = d.read().await;
                !guard.is_empty()
            }
            Value::Tuple(t) => !t.is_empty(),
            _ => true,
        }
    }

    pub fn is_none(&self) -> bool {
        matches!(self, Value::None)
    }

    pub fn as_bool(&self) -> Result<bool> {
        match self {
            Value::Bool(b) => Ok(*b),
            _ => Err(BlueprintError::TypeError {
                expected: "bool".into(),
                actual: self.type_name().into(),
            }),
        }
    }

    pub fn as_int(&self) -> Result<i64> {
        match self {
            Value::Int(i) => Ok(*i),
            _ => Err(BlueprintError::TypeError {
                expected: "int".into(),
                actual: self.type_name().into(),
            }),
        }
    }

    pub fn as_float(&self) -> Result<f64> {
        match self {
            Value::Float(f) => Ok(*f),
            Value::Int(i) => Ok(*i as f64),
            _ => Err(BlueprintError::TypeError {
                expected: "float".into(),
                actual: self.type_name().into(),
            }),
        }
    }

    pub fn as_string(&self) -> Result<String> {
        match self {
            Value::String(s) => Ok(s.as_ref().clone()),
            _ => Err(BlueprintError::TypeError {
                expected: "string".into(),
                actual: self.type_name().into(),
            }),
        }
    }

    pub fn as_str(&self) -> Result<&str> {
        match self {
            Value::String(s) => Ok(s.as_ref()),
            _ => Err(BlueprintError::TypeError {
                expected: "string".into(),
                actual: self.type_name().into(),
            }),
        }
    }

    pub fn to_display_string(&self) -> String {
        match self {
            Value::None => "None".into(),
            Value::Bool(b) => if *b { "True" } else { "False" }.into(),
            Value::Int(i) => i.to_string(),
            Value::Float(f) => {
                if f.fract() == 0.0 {
                    format!("{f:.1}")
                } else {
                    f.to_string()
                }
            }
            Value::String(s) => s.as_ref().clone(),
            Value::List(l) => {
                match l.try_read() {
                    Ok(guard) => {
                        let items: Vec<String> = guard.iter().map(|v| v.repr()).collect();
                        format!("[{}]", items.join(", "))
                    }
                    Err(_) => "[<locked>]".into(),
                }
            }
            Value::Dict(d) => {
                match d.try_read() {
                    Ok(guard) => {
                        let items: Vec<String> = guard
                            .iter()
                            .map(|(k, v)| format!("{:?}: {}", k, v.repr()))
                            .collect();
                        format!("{{{}}}", items.join(", "))
                    }
                    Err(_) => "{<locked>}".into(),
                }
            }
            Value::Tuple(t) => {
                let items: Vec<String> = t.iter().map(|v| v.repr()).collect();
                if t.len() == 1 {
                    format!("({},)", items[0])
                } else {
                    format!("({})", items.join(", "))
                }
            }
            Value::Function(f) => format!("<function {}>", f.name),
            Value::Lambda(_) => "<lambda>".into(),
            Value::NativeFunction(f) => format!("<builtin_function {}>", f.name),
            Value::Response(r) => format!("<Response status={}>", r.status),
            Value::ProcessResult(r) => format!("<Result code={}>", r.code),
            Value::Iterator(_) => "<iterator>".into(),
            Value::Generator(_) => "<generator>".into(),
            Value::StructType(s) => format!("<type {}>", s.name),
            Value::StructInstance(s) => s.to_display_string(),
        }
    }

    pub fn repr(&self) -> String {
        match self {
            Value::String(s) => format!("{:?}", s.as_ref()),
            _ => self.to_display_string(),
        }
    }

    pub fn get_attr(&self, name: &str) -> Option<Value> {
        match self {
            Value::Response(r) => r.get_attr(name),
            Value::ProcessResult(r) => r.get_attr(name),
            Value::String(s) => get_string_method(s.clone(), name),
            Value::List(l) => get_list_method(l.clone(), name),
            Value::Dict(d) => get_dict_method(d.clone(), name),
            Value::Iterator(it) => it.get_attr(name),
            Value::StructInstance(s) => s.get_field(name),
            _ => None,
        }
    }

    pub fn has_attr(&self, name: &str) -> bool {
        self.get_attr(name).is_some()
    }
}

impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Value::None, Value::None) => true,
            (Value::Bool(a), Value::Bool(b)) => a == b,
            (Value::Int(a), Value::Int(b)) => a == b,
            (Value::Float(a), Value::Float(b)) => a == b,
            (Value::Int(a), Value::Float(b)) => (*a as f64) == *b,
            (Value::Float(a), Value::Int(b)) => *a == (*b as f64),
            (Value::String(a), Value::String(b)) => a == b,
            (Value::Tuple(a), Value::Tuple(b)) => a == b,
            _ => false,
        }
    }
}

impl Eq for Value {}

impl Hash for Value {
    fn hash<H: Hasher>(&self, state: &mut H) {
        std::mem::discriminant(self).hash(state);
        match self {
            Value::None => {}
            Value::Bool(b) => b.hash(state),
            Value::Int(i) => i.hash(state),
            Value::Float(f) => f.to_bits().hash(state),
            Value::String(s) => s.hash(state),
            Value::Tuple(t) => t.hash(state),
            _ => {}
        }
    }
}

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
                let map: HashMap<String, Value> = self
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

fn get_string_method(s: Arc<String>, name: &str) -> Option<Value> {
    let s_clone = s.clone();
    match name {
        "upper" => Some(Value::NativeFunction(Arc::new(NativeFunction::new_with_state(
            "upper",
            move |_args, _kwargs| {
                let result = s_clone.to_uppercase();
                Box::pin(async move { Ok(Value::String(Arc::new(result))) })
            },
        )))),
        "lower" => {
            let s = s.clone();
            Some(Value::NativeFunction(Arc::new(NativeFunction::new_with_state(
                "lower",
                move |_args, _kwargs| {
                    let result = s.to_lowercase();
                    Box::pin(async move { Ok(Value::String(Arc::new(result))) })
                },
            ))))
        }
        "strip" => {
            let s = s.clone();
            Some(Value::NativeFunction(Arc::new(NativeFunction::new_with_state(
                "strip",
                move |_args, _kwargs| {
                    let result = s.trim().to_string();
                    Box::pin(async move { Ok(Value::String(Arc::new(result))) })
                },
            ))))
        }
        "split" => {
            let s = s.clone();
            Some(Value::NativeFunction(Arc::new(NativeFunction::new_with_state(
                "split",
                move |args, _kwargs| {
                    let sep = if args.is_empty() {
                        None
                    } else {
                        Some(args[0].to_display_string())
                    };
                    let parts: Vec<Value> = match sep {
                        Some(ref sep) => s.split(sep.as_str()).map(|p| Value::String(Arc::new(p.to_string()))).collect(),
                        None => s.split_whitespace().map(|p| Value::String(Arc::new(p.to_string()))).collect(),
                    };
                    Box::pin(async move { Ok(Value::List(Arc::new(tokio::sync::RwLock::new(parts)))) })
                },
            ))))
        }
        "join" => {
            let s = s.clone();
            Some(Value::NativeFunction(Arc::new(NativeFunction::new_with_state(
                "join",
                move |args, _kwargs| {
                    let s = s.clone();
                    Box::pin(async move {
                        if args.is_empty() {
                            return Err(BlueprintError::ArgumentError {
                                message: "join() requires 1 argument".into(),
                            });
                        }
                        let items = match &args[0] {
                            Value::List(l) => l.read().await.clone(),
                            Value::Tuple(t) => t.as_ref().clone(),
                            _ => return Err(BlueprintError::TypeError {
                                expected: "list or tuple".into(),
                                actual: args[0].type_name().into(),
                            }),
                        };
                        let strings: Vec<String> = items.iter().map(|v| v.to_display_string()).collect();
                        Ok(Value::String(Arc::new(strings.join(s.as_str()))))
                    })
                },
            ))))
        }
        "replace" => {
            let s = s.clone();
            Some(Value::NativeFunction(Arc::new(NativeFunction::new_with_state(
                "replace",
                move |args, _kwargs| {
                    let s = s.clone();
                    Box::pin(async move {
                        if args.len() < 2 {
                            return Err(BlueprintError::ArgumentError {
                                message: "replace() requires 2 arguments".into(),
                            });
                        }
                        let old = args[0].to_display_string();
                        let new = args[1].to_display_string();
                        let result = s.replace(&old, &new);
                        Ok(Value::String(Arc::new(result)))
                    })
                },
            ))))
        }
        "startswith" => {
            let s = s.clone();
            Some(Value::NativeFunction(Arc::new(NativeFunction::new_with_state(
                "startswith",
                move |args, _kwargs| {
                    let s = s.clone();
                    Box::pin(async move {
                        if args.is_empty() {
                            return Err(BlueprintError::ArgumentError {
                                message: "startswith() requires 1 argument".into(),
                            });
                        }
                        let prefix = args[0].to_display_string();
                        Ok(Value::Bool(s.starts_with(&prefix)))
                    })
                },
            ))))
        }
        "endswith" => {
            let s = s.clone();
            Some(Value::NativeFunction(Arc::new(NativeFunction::new_with_state(
                "endswith",
                move |args, _kwargs| {
                    let s = s.clone();
                    Box::pin(async move {
                        if args.is_empty() {
                            return Err(BlueprintError::ArgumentError {
                                message: "endswith() requires 1 argument".into(),
                            });
                        }
                        let suffix = args[0].to_display_string();
                        Ok(Value::Bool(s.ends_with(&suffix)))
                    })
                },
            ))))
        }
        "find" => {
            let s = s.clone();
            Some(Value::NativeFunction(Arc::new(NativeFunction::new_with_state(
                "find",
                move |args, _kwargs| {
                    let s = s.clone();
                    Box::pin(async move {
                        if args.is_empty() {
                            return Err(BlueprintError::ArgumentError {
                                message: "find() requires 1 argument".into(),
                            });
                        }
                        let needle = args[0].to_display_string();
                        let result = s.find(&needle).map(|i| i as i64).unwrap_or(-1);
                        Ok(Value::Int(result))
                    })
                },
            ))))
        }
        "format" => {
            let s = s.clone();
            Some(Value::NativeFunction(Arc::new(NativeFunction::new_with_state(
                "format",
                move |args, _kwargs| {
                    let s = s.clone();
                    Box::pin(async move {
                        let mut result = s.as_str().to_string();
                        for arg in args {
                            if let Some(pos) = result.find("{}") {
                                result = format!("{}{}{}", &result[..pos], arg.to_display_string(), &result[pos+2..]);
                            }
                        }
                        Ok(Value::String(Arc::new(result)))
                    })
                },
            ))))
        }
        _ => None,
    }
}

fn get_list_method(_l: Arc<RwLock<Vec<Value>>>, name: &str) -> Option<Value> {
    match name {
        "append" | "extend" | "insert" | "remove" | "pop" | "clear" | "index" | "count"
        | "sort" | "reverse" => {
            None
        }
        _ => None,
    }
}

fn get_dict_method(d: Arc<RwLock<HashMap<String, Value>>>, name: &str) -> Option<Value> {
    match name {
        "get" => {
            let d_clone = d.clone();
            Some(Value::NativeFunction(Arc::new(NativeFunction::new_with_state(
                "get",
                move |args, _kwargs| {
                    let d = d_clone.clone();
                    Box::pin(async move {
                        if args.is_empty() || args.len() > 2 {
                            return Err(BlueprintError::ArgumentError {
                                message: format!("get() takes 1 or 2 arguments ({} given)", args.len()),
                            });
                        }
                        let key = match &args[0] {
                            Value::String(s) => s.as_ref().clone(),
                            v => return Err(BlueprintError::TypeError {
                                expected: "string".into(),
                                actual: v.type_name().into(),
                            }),
                        };
                        let default = if args.len() == 2 {
                            args[1].clone()
                        } else {
                            Value::None
                        };
                        let map = d.read().await;
                        Ok(map.get(&key).cloned().unwrap_or(default))
                    })
                },
            ))))
        }
        "keys" => {
            let d_clone = d.clone();
            Some(Value::NativeFunction(Arc::new(NativeFunction::new_with_state(
                "keys",
                move |_args, _kwargs| {
                    let d = d_clone.clone();
                    Box::pin(async move {
                        let map = d.read().await;
                        let keys: Vec<Value> = map.keys().map(|k| Value::String(Arc::new(k.clone()))).collect();
                        Ok(Value::List(Arc::new(RwLock::new(keys))))
                    })
                },
            ))))
        }
        "values" => {
            let d_clone = d.clone();
            Some(Value::NativeFunction(Arc::new(NativeFunction::new_with_state(
                "values",
                move |_args, _kwargs| {
                    let d = d_clone.clone();
                    Box::pin(async move {
                        let map = d.read().await;
                        let values: Vec<Value> = map.values().cloned().collect();
                        Ok(Value::List(Arc::new(RwLock::new(values))))
                    })
                },
            ))))
        }
        "items" => {
            let d_clone = d.clone();
            Some(Value::NativeFunction(Arc::new(NativeFunction::new_with_state(
                "items",
                move |_args, _kwargs| {
                    let d = d_clone.clone();
                    Box::pin(async move {
                        let map = d.read().await;
                        let items: Vec<Value> = map.iter()
                            .map(|(k, v)| Value::Tuple(Arc::new(vec![Value::String(Arc::new(k.clone())), v.clone()])))
                            .collect();
                        Ok(Value::List(Arc::new(RwLock::new(items))))
                    })
                },
            ))))
        }
        _ => None,
    }
}

pub struct StreamIterator {
    rx: Mutex<mpsc::Receiver<Option<String>>>,
    content: Mutex<String>,
    done: Mutex<bool>,
    result: Mutex<Option<HashMap<String, Value>>>,
}

impl StreamIterator {
    pub fn new(rx: mpsc::Receiver<Option<String>>) -> Self {
        Self {
            rx: Mutex::new(rx),
            content: Mutex::new(String::new()),
            done: Mutex::new(false),
            result: Mutex::new(None),
        }
    }

    pub async fn next(&self) -> Option<Value> {
        let mut done = self.done.lock().await;
        if *done {
            return None;
        }

        let mut rx = self.rx.lock().await;
        match rx.recv().await {
            Some(Some(chunk)) => {
                let mut content = self.content.lock().await;
                content.push_str(&chunk);
                Some(Value::String(Arc::new(chunk)))
            }
            Some(None) | None => {
                *done = true;
                None
            }
        }
    }

    pub async fn set_result(&self, result: HashMap<String, Value>) {
        let mut r = self.result.lock().await;
        *r = Some(result);
    }

    pub fn get_attr(&self, name: &str) -> Option<Value> {
        match name {
            "content" => {
                let content = self.content.try_lock().ok()?;
                Some(Value::String(Arc::new(content.clone())))
            }
            "done" => {
                let done = self.done.try_lock().ok()?;
                Some(Value::Bool(*done))
            }
            "result" => {
                let result = self.result.try_lock().ok()?;
                match result.as_ref() {
                    Some(map) => Some(Value::Dict(Arc::new(RwLock::new(map.clone())))),
                    None => Some(Value::None),
                }
            }
            _ => None,
        }
    }
}

/// Message sent from generator to consumer
pub enum GeneratorMessage {
    /// Generator yielded a value, waiting for resume signal
    Yielded(Value, oneshot::Sender<()>),
    /// Generator completed (returned or finished)
    Complete,
}

/// A generator that yields values lazily
pub struct Generator {
    /// Receives yielded values from the generator task
    rx: Mutex<mpsc::Receiver<GeneratorMessage>>,
    /// Whether the generator has completed
    done: AtomicBool,
    /// The function that creates this generator (for display)
    pub name: String,
}

impl Generator {
    pub fn new(rx: mpsc::Receiver<GeneratorMessage>, name: String) -> Self {
        Self {
            rx: Mutex::new(rx),
            done: AtomicBool::new(false),
            name,
        }
    }

    /// Get the next yielded value, or None if generator is exhausted
    pub async fn next(&self) -> Option<Value> {
        if self.done.load(Ordering::SeqCst) {
            return None;
        }

        let mut rx = self.rx.lock().await;
        match rx.recv().await {
            Some(GeneratorMessage::Yielded(value, resume_tx)) => {
                // Signal the generator to continue after yield
                let _ = resume_tx.send(());
                Some(value)
            }
            Some(GeneratorMessage::Complete) | None => {
                self.done.store(true, Ordering::SeqCst);
                None
            }
        }
    }

    pub fn is_done(&self) -> bool {
        self.done.load(Ordering::SeqCst)
    }
}

impl fmt::Debug for Generator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "<generator {}>", self.name)
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum TypeAnnotation {
    Simple(String),
    Parameterized(String, Vec<TypeAnnotation>),
    Optional(Box<TypeAnnotation>),
    Any,
}

impl TypeAnnotation {
    pub fn matches(&self, value: &Value) -> bool {
        match self {
            TypeAnnotation::Any => true,
            TypeAnnotation::Simple(name) => match name.as_str() {
                "int" => matches!(value, Value::Int(_)),
                "float" => matches!(value, Value::Float(_) | Value::Int(_)),
                "str" => matches!(value, Value::String(_)),
                "bool" => matches!(value, Value::Bool(_)),
                "list" => matches!(value, Value::List(_)),
                "dict" => matches!(value, Value::Dict(_)),
                "tuple" => matches!(value, Value::Tuple(_)),
                "None" | "NoneType" => matches!(value, Value::None),
                struct_name => {
                    if let Value::StructInstance(inst) = value {
                        inst.struct_type.name == struct_name
                    } else {
                        false
                    }
                }
            },
            TypeAnnotation::Parameterized(name, _params) => {
                match name.as_str() {
                    "list" => matches!(value, Value::List(_)),
                    "dict" => matches!(value, Value::Dict(_)),
                    _ => false,
                }
            }
            TypeAnnotation::Optional(inner) => {
                matches!(value, Value::None) || inner.matches(value)
            }
        }
    }

    pub fn type_name(&self) -> String {
        match self {
            TypeAnnotation::Any => "any".to_string(),
            TypeAnnotation::Simple(name) => name.clone(),
            TypeAnnotation::Parameterized(name, params) => {
                let param_strs: Vec<String> = params.iter().map(|p| p.type_name()).collect();
                format!("{}[{}]", name, param_strs.join(", "))
            }
            TypeAnnotation::Optional(inner) => format!("{}?", inner.type_name()),
        }
    }
}

#[derive(Debug, Clone)]
pub struct StructField {
    pub name: String,
    pub typ: TypeAnnotation,
    pub default: Option<Value>,
}

#[derive(Debug, Clone)]
pub struct StructType {
    pub name: String,
    pub fields: Vec<StructField>,
}

impl StructType {
    pub fn instantiate(&self, args: Vec<Value>, kwargs: HashMap<String, Value>) -> Result<StructInstance> {
        let mut field_values: HashMap<String, Value> = HashMap::new();

        let mut positional_idx = 0;
        for field in &self.fields {
            let value = if let Some(v) = kwargs.get(&field.name) {
                v.clone()
            } else if positional_idx < args.len() {
                let v = args[positional_idx].clone();
                positional_idx += 1;
                v
            } else if let Some(default) = &field.default {
                default.clone()
            } else {
                return Err(BlueprintError::ArgumentError {
                    message: format!(
                        "{}() missing required argument: '{}'",
                        self.name, field.name
                    ),
                });
            };

            if !field.typ.matches(&value) {
                return Err(BlueprintError::TypeError {
                    expected: format!(
                        "{} for field '{}' in {}()",
                        field.typ.type_name(),
                        field.name,
                        self.name
                    ),
                    actual: value.type_name().to_string(),
                });
            }

            field_values.insert(field.name.clone(), value);
        }

        if positional_idx < args.len() {
            return Err(BlueprintError::ArgumentError {
                message: format!(
                    "{}() takes {} positional arguments but {} were given",
                    self.name,
                    self.fields.len(),
                    args.len()
                ),
            });
        }

        for key in kwargs.keys() {
            if !self.fields.iter().any(|f| &f.name == key) {
                return Err(BlueprintError::ArgumentError {
                    message: format!("{}() got unexpected keyword argument '{}'", self.name, key),
                });
            }
        }

        Ok(StructInstance {
            struct_type: Arc::new(self.clone()),
            fields: field_values,
        })
    }
}

#[derive(Debug, Clone)]
pub struct StructInstance {
    pub struct_type: Arc<StructType>,
    pub fields: HashMap<String, Value>,
}

impl StructInstance {
    pub fn get_field(&self, name: &str) -> Option<Value> {
        self.fields.get(name).cloned()
    }

    pub fn to_display_string(&self) -> String {
        let field_strs: Vec<String> = self
            .struct_type
            .fields
            .iter()
            .map(|f| {
                let val = self.fields.get(&f.name).map(|v| v.repr()).unwrap_or_default();
                format!("{}={}", f.name, val)
            })
            .collect();
        format!("{}({})", self.struct_type.name, field_strs.join(", "))
    }
}
