use std::sync::Arc;
use thiserror::Error;

#[derive(Debug, Clone)]
pub struct Span {
    pub start: usize,
    pub end: usize,
}

#[derive(Debug, Clone)]
pub struct SourceLocation {
    pub file: Option<String>,
    pub line: usize,
    pub column: usize,
    pub span: Option<Span>,
}

impl std::fmt::Display for SourceLocation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.file {
            Some(file) => write!(f, "{}:{}:{}", file, self.line, self.column),
            None => write!(f, "line {}:{}", self.line, self.column),
        }
    }
}

#[derive(Debug, Clone, Error)]
pub enum BlueprintError {
    #[error("Parse error at {location}: {message}")]
    ParseError {
        location: SourceLocation,
        message: String,
    },

    #[error("Type error: expected {expected}, got {actual}")]
    TypeError { expected: String, actual: String },

    #[error("Name error: undefined variable '{name}'")]
    NameError { name: String },

    #[error("Attribute error: '{type_name}' has no attribute '{attr}'")]
    AttributeError { type_name: String, attr: String },

    #[error("Index error: {message}")]
    IndexError { message: String },

    #[error("Key error: key not found: {key}")]
    KeyError { key: String },

    #[error("Value error: {message}")]
    ValueError { message: String },

    #[error("Argument error: {message}")]
    ArgumentError { message: String },

    #[error("Division by zero")]
    DivisionByZero,

    #[error("I/O error: {path}: {message}")]
    IoError { path: String, message: String },

    #[error("HTTP error: {url}: {message}")]
    HttpError { url: String, message: String },

    #[error("Process error: {command}: {message}")]
    ProcessError { command: String, message: String },

    #[error("JSON error: {message}")]
    JsonError { message: String },

    #[error("Glob error: {message}")]
    GlobError { message: String },

    #[error("Assertion failed: {message}")]
    AssertionError { message: String },

    #[error("{message}")]
    UserError { message: String },

    #[error("Not callable: {type_name}")]
    NotCallable { type_name: String },

    #[error("Internal error: {message}")]
    InternalError { message: String },

    #[error("Unsupported: {message}")]
    Unsupported { message: String },

    #[error("break")]
    Break,

    #[error("continue")]
    Continue,

    #[error("return")]
    Return { value: Arc<crate::Value> },
}

impl BlueprintError {
    pub fn with_file(self, file: String) -> Self {
        match self {
            BlueprintError::ParseError { location, message } => BlueprintError::ParseError {
                location: SourceLocation {
                    file: Some(file),
                    ..location
                },
                message,
            },
            other => other,
        }
    }

    pub fn is_control_flow(&self) -> bool {
        matches!(
            self,
            BlueprintError::Break | BlueprintError::Continue | BlueprintError::Return { .. }
        )
    }
}

pub type Result<T> = std::result::Result<T, BlueprintError>;
