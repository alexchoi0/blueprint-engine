mod functions;
mod generator;
mod io;
mod methods;
mod structs;

pub use functions::{LambdaFunction, NativeFunction, NativeFn, NativeFuture, Parameter, ParameterKind, UserFunction};
pub use generator::{Generator, GeneratorMessage, StreamIterator};
pub use io::{HttpResponse, ProcessResult};
pub use structs::{StructField, StructInstance, StructType, TypeAnnotation};

use std::fmt;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use indexmap::{IndexMap, IndexSet};
use tokio::sync::RwLock;

use crate::error::{BlueprintError, Result};

#[derive(Clone)]
pub enum Value {
    None,
    Bool(bool),
    Int(i64),
    Float(f64),
    String(Arc<String>),
    List(Arc<RwLock<Vec<Value>>>),
    Dict(Arc<RwLock<IndexMap<String, Value>>>),
    Set(Arc<RwLock<IndexSet<Value>>>),
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
            Value::Set(_) => write!(f, "Set({{...}})"),
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
            Value::Set(_) => "set",
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
            Value::Set(s) => {
                if let Ok(guard) = s.try_read() {
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
            Value::Set(s) => {
                let guard = s.read().await;
                !guard.is_empty()
            }
            Value::Tuple(t) => !t.is_empty(),
            _ => true,
        }
    }

    pub fn is_none(&self) -> bool {
        matches!(self, Value::None)
    }

    pub async fn deep_copy(&self) -> Value {
        match self {
            Value::List(l) => {
                let items = l.read().await;
                let mut copied = Vec::with_capacity(items.len());
                for item in items.iter() {
                    copied.push(Box::pin(item.deep_copy()).await);
                }
                Value::List(Arc::new(RwLock::new(copied)))
            }
            Value::Dict(d) => {
                let map = d.read().await;
                let mut copied = IndexMap::with_capacity(map.len());
                for (k, v) in map.iter() {
                    copied.insert(k.clone(), Box::pin(v.deep_copy()).await);
                }
                Value::Dict(Arc::new(RwLock::new(copied)))
            }
            Value::Tuple(t) => {
                let mut copied = Vec::with_capacity(t.len());
                for item in t.iter() {
                    copied.push(Box::pin(item.deep_copy()).await);
                }
                Value::Tuple(Arc::new(copied))
            }
            other => other.clone(),
        }
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
            Value::Set(s) => {
                match s.try_read() {
                    Ok(guard) => {
                        let items: Vec<String> = guard.iter().map(|v| v.repr()).collect();
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
            Value::String(s) => methods::get_string_method(s.clone(), name),
            Value::List(l) => methods::get_list_method(l.clone(), name),
            Value::Dict(d) => methods::get_dict_method(d.clone(), name),
            Value::Set(s) => methods::get_set_method(s.clone(), name),
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
