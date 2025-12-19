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

#[derive(Debug, Clone)]
pub struct StackFrame {
    pub function_name: String,
    pub file: Option<String>,
    pub line: usize,
    pub column: usize,
}

impl std::fmt::Display for StackFrame {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let location = match &self.file {
            Some(file) => format!("{}:{}:{}", file, self.line, self.column),
            None => format!("line {}:{}", self.line, self.column),
        };
        write!(f, "  at {} ({})", self.function_name, location)
    }
}

#[derive(Debug, Clone, Default)]
pub struct StackTrace {
    pub frames: Vec<StackFrame>,
}

impl StackTrace {
    pub fn new() -> Self {
        Self { frames: Vec::new() }
    }

    pub fn push(&mut self, frame: StackFrame) {
        self.frames.push(frame);
    }

    pub fn is_empty(&self) -> bool {
        self.frames.is_empty()
    }
}

impl std::fmt::Display for StackTrace {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.frames.is_empty() {
            return Ok(());
        }
        writeln!(f, "Stack trace (most recent call last):")?;
        for frame in &self.frames {
            writeln!(f, "{}", frame)?;
        }
        Ok(())
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

    #[error("exit with code {code}")]
    Exit { code: i32 },

    #[error("")]
    Silent,

    #[error("{error}")]
    WithStack {
        error: Box<BlueprintError>,
        stack: StackTrace,
        location: Option<SourceLocation>,
    },
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
            BlueprintError::Break | BlueprintError::Continue | BlueprintError::Return { .. } | BlueprintError::Exit { .. }
        )
    }

    pub fn with_stack_frame(self, frame: StackFrame) -> Self {
        if self.is_control_flow() {
            return self;
        }

        match self {
            BlueprintError::WithStack { error, mut stack, location } => {
                stack.push(frame);
                BlueprintError::WithStack { error, stack, location }
            }
            other => {
                let mut stack = StackTrace::new();
                stack.push(frame);
                BlueprintError::WithStack {
                    error: Box::new(other),
                    stack,
                    location: None,
                }
            }
        }
    }

    pub fn with_location(self, loc: SourceLocation) -> Self {
        if self.is_control_flow() {
            return self;
        }

        match self {
            BlueprintError::WithStack { error, stack, location: _ } => {
                BlueprintError::WithStack { error, stack, location: Some(loc) }
            }
            other => {
                BlueprintError::WithStack {
                    error: Box::new(other),
                    stack: StackTrace::new(),
                    location: Some(loc),
                }
            }
        }
    }

    pub fn stack_trace(&self) -> Option<&StackTrace> {
        match self {
            BlueprintError::WithStack { stack, .. } => Some(stack),
            _ => None,
        }
    }

    pub fn error_location(&self) -> Option<&SourceLocation> {
        match self {
            BlueprintError::WithStack { location, .. } => location.as_ref(),
            BlueprintError::ParseError { location, .. } => Some(location),
            _ => None,
        }
    }

    pub fn inner_error(&self) -> &BlueprintError {
        match self {
            BlueprintError::WithStack { error, .. } => error.inner_error(),
            other => other,
        }
    }

    pub fn format_with_stack(&self) -> String {
        let mut result = String::new();

        if let Some(loc) = self.error_location() {
            result.push_str(&format!("Error at {}: ", loc));
        } else {
            result.push_str("Error: ");
        }

        result.push_str(&format!("{}", self.inner_error()));

        if let Some(stack) = self.stack_trace() {
            if !stack.is_empty() {
                result.push_str("\n\n");
                result.push_str(&format!("{}", stack));
            }
        }

        result
    }
}

pub type Result<T> = std::result::Result<T, BlueprintError>;
