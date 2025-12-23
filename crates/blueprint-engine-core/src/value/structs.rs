use std::collections::HashMap;
use std::sync::Arc;

use indexmap::IndexMap;

use crate::error::{BlueprintError, Result};
use super::Value;

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
        let mut field_values: IndexMap<String, Value> = IndexMap::new();

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
    pub fields: IndexMap<String, Value>,
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
