use allocative::Allocative;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Debug, Clone, Copy, Hash, Eq, PartialEq, Serialize, Deserialize, Allocative)]
pub struct OpId(pub u64);

/// A sub-plan represents a nested computation used in iterator transformations.
/// Used for map transforms, filter predicates, reducer functions, etc.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubPlan {
    /// Input parameter names (e.g., ["x"] for map, ["acc", "x"] for reduce)
    pub params: Vec<String>,

    /// Operations in this sub-plan (uses ParamRef to reference params)
    pub ops: Vec<Op>,

    /// Which op produces the output value
    pub output: OpId,
}

impl std::fmt::Display for OpId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Op[{}]", self.0)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Op {
    pub id: OpId,
    pub kind: OpKind,
    pub inputs: Vec<OpId>,
    pub source_location: Option<SourceSpan>,
    /// If set, this operation only executes if the guard operation returns truthy
    pub guard: Option<OpId>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceSpan {
    pub file: String,
    pub line: u32,
    pub column: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum OpKind {
    ReadFile { path: ValueRef },
    WriteFile { path: ValueRef, content: ValueRef },
    AppendFile { path: ValueRef, content: ValueRef },
    DeleteFile { path: ValueRef },
    ListDir { path: ValueRef },
    Mkdir { path: ValueRef, recursive: bool },
    Rmdir { path: ValueRef, recursive: bool },
    CopyFile { src: ValueRef, dst: ValueRef },
    MoveFile { src: ValueRef, dst: ValueRef },
    FileExists { path: ValueRef },
    IsDir { path: ValueRef },
    IsFile { path: ValueRef },
    FileSize { path: ValueRef },

    HttpRequest {
        method: ValueRef,
        url: ValueRef,
        headers: ValueRef,
        body: ValueRef,
    },

    TcpConnect { host: ValueRef, port: ValueRef },
    TcpSend { handle: ValueRef, data: ValueRef },
    TcpRecv { handle: ValueRef, max_bytes: ValueRef },
    TcpClose { handle: ValueRef },
    TcpListen { host: ValueRef, port: ValueRef },
    TcpAccept { listener: ValueRef },

    UdpBind { host: ValueRef, port: ValueRef },
    UdpSendTo {
        handle: ValueRef,
        data: ValueRef,
        host: ValueRef,
        port: ValueRef,
    },
    UdpRecvFrom { handle: ValueRef, max_bytes: ValueRef },
    UdpClose { handle: ValueRef },

    UnixConnect { path: ValueRef },
    UnixSend { handle: ValueRef, data: ValueRef },
    UnixRecv { handle: ValueRef, max_bytes: ValueRef },
    UnixClose { handle: ValueRef },
    UnixListen { path: ValueRef },
    UnixAccept { listener: ValueRef },

    Exec { command: ValueRef, args: ValueRef },
    EnvGet { name: ValueRef, default: ValueRef },
    Sleep { seconds: ValueRef },
    Now,
    Print { message: ValueRef },
    JsonEncode { value: ValueRef },
    JsonDecode { string: ValueRef },

    All { ops: Vec<OpId> },
    Any { ops: Vec<OpId> },
    AtLeast { ops: Vec<OpId>, count: usize },
    AtMost { ops: Vec<OpId>, count: usize },
    After { dependency: OpId, value: OpId },

    // Type conversions
    ToBool { value: ValueRef },
    ToInt { value: ValueRef },
    ToFloat { value: ValueRef },
    ToStr { value: ValueRef },

    // Collection operations
    Len { value: ValueRef },
    Min { values: ValueRef },
    Max { values: ValueRef },
    Sum { values: ValueRef, start: ValueRef },
    Abs { value: ValueRef },
    Sorted { values: ValueRef },
    Reversed { values: ValueRef },

    // Arithmetic
    Add { left: ValueRef, right: ValueRef },
    Sub { left: ValueRef, right: ValueRef },
    Mul { left: ValueRef, right: ValueRef },
    Div { left: ValueRef, right: ValueRef },
    FloorDiv { left: ValueRef, right: ValueRef },
    Mod { left: ValueRef, right: ValueRef },
    Neg { value: ValueRef },

    // Comparisons
    Eq { left: ValueRef, right: ValueRef },
    Ne { left: ValueRef, right: ValueRef },
    Lt { left: ValueRef, right: ValueRef },
    Le { left: ValueRef, right: ValueRef },
    Gt { left: ValueRef, right: ValueRef },
    Ge { left: ValueRef, right: ValueRef },

    // Logic
    Not { value: ValueRef },

    // String operations
    Concat { left: ValueRef, right: ValueRef },
    Contains { haystack: ValueRef, needle: ValueRef },

    // Control flow - ternary expression
    If {
        condition: ValueRef,
        then_value: ValueRef,
        else_value: ValueRef,
    },

    // Control flow - runtime blocks
    /// Runtime for-each loop over a collection
    ForEach {
        /// The collection to iterate over (must resolve to list/tuple)
        items: ValueRef,
        /// Name bound to current item in each iteration
        item_name: String,
        /// Operations to execute for each item
        body: SubPlan,
        /// Whether iterations can run in parallel (determined by dependency analysis)
        parallel: bool,
    },

    /// Transform each item in a list (always parallel)
    Map {
        /// The collection to transform
        items: ValueRef,
        /// Name bound to current item in transform
        item_name: String,
        /// Transform operations to execute for each item
        body: SubPlan,
    },

    /// Filter items based on predicate (always parallel)
    Filter {
        /// The collection to filter
        items: ValueRef,
        /// Name bound to current item in predicate
        item_name: String,
        /// Predicate operations (output determines inclusion)
        predicate: SubPlan,
    },

    /// Runtime conditional block execution
    IfBlock {
        /// Condition to evaluate
        condition: ValueRef,
        /// Operations to execute if condition is truthy
        then_body: SubPlan,
        /// Operations to execute if condition is falsy (optional)
        else_body: Option<SubPlan>,
    },

    /// Break out of the enclosing loop
    Break,

    /// Continue to next iteration of enclosing loop
    Continue,

    // Dynamic indexing
    Index { base: ValueRef, index: ValueRef },
    SetIndex { base: ValueRef, index: ValueRef, value: ValueRef },

    // === Generator Operations ===

    /// Define a generator (produces iterator when called)
    GeneratorDef {
        name: String,
        params: Vec<String>,
        body: SubPlan,
    },

    /// Yield a value from within a generator
    GeneratorYield {
        value: ValueRef,
    },

    /// Conditional yield
    GeneratorYieldIf {
        condition: ValueRef,
        value: ValueRef,
    },

    // === SubPlan Parameter Reference ===

    /// Reference a parameter in a SubPlan (used inside transforms/predicates)
    ParamRef {
        name: String,
    },

    /// Exports a global variable as a frozen value
    FrozenValue {
        name: String,
        value: ValueRef,
    },
}

impl OpKind {
    pub fn name(&self) -> &'static str {
        match self {
            OpKind::ReadFile { .. } => "ReadFile",
            OpKind::WriteFile { .. } => "WriteFile",
            OpKind::AppendFile { .. } => "AppendFile",
            OpKind::DeleteFile { .. } => "DeleteFile",
            OpKind::ListDir { .. } => "ListDir",
            OpKind::Mkdir { .. } => "Mkdir",
            OpKind::Rmdir { .. } => "Rmdir",
            OpKind::CopyFile { .. } => "CopyFile",
            OpKind::MoveFile { .. } => "MoveFile",
            OpKind::FileExists { .. } => "FileExists",
            OpKind::IsDir { .. } => "IsDir",
            OpKind::IsFile { .. } => "IsFile",
            OpKind::FileSize { .. } => "FileSize",
            OpKind::HttpRequest { .. } => "HttpRequest",
            OpKind::TcpConnect { .. } => "TcpConnect",
            OpKind::TcpSend { .. } => "TcpSend",
            OpKind::TcpRecv { .. } => "TcpRecv",
            OpKind::TcpClose { .. } => "TcpClose",
            OpKind::TcpListen { .. } => "TcpListen",
            OpKind::TcpAccept { .. } => "TcpAccept",
            OpKind::UdpBind { .. } => "UdpBind",
            OpKind::UdpSendTo { .. } => "UdpSendTo",
            OpKind::UdpRecvFrom { .. } => "UdpRecvFrom",
            OpKind::UdpClose { .. } => "UdpClose",
            OpKind::UnixConnect { .. } => "UnixConnect",
            OpKind::UnixSend { .. } => "UnixSend",
            OpKind::UnixRecv { .. } => "UnixRecv",
            OpKind::UnixClose { .. } => "UnixClose",
            OpKind::UnixListen { .. } => "UnixListen",
            OpKind::UnixAccept { .. } => "UnixAccept",
            OpKind::Exec { .. } => "Exec",
            OpKind::EnvGet { .. } => "EnvGet",
            OpKind::Sleep { .. } => "Sleep",
            OpKind::Now => "Now",
            OpKind::Print { .. } => "Print",
            OpKind::JsonEncode { .. } => "JsonEncode",
            OpKind::JsonDecode { .. } => "JsonDecode",
            OpKind::All { .. } => "All",
            OpKind::Any { .. } => "Any",
            OpKind::AtLeast { .. } => "AtLeast",
            OpKind::AtMost { .. } => "AtMost",
            OpKind::After { .. } => "After",
            OpKind::ToBool { .. } => "ToBool",
            OpKind::ToInt { .. } => "ToInt",
            OpKind::ToFloat { .. } => "ToFloat",
            OpKind::ToStr { .. } => "ToStr",
            OpKind::Len { .. } => "Len",
            OpKind::Min { .. } => "Min",
            OpKind::Max { .. } => "Max",
            OpKind::Sum { .. } => "Sum",
            OpKind::Abs { .. } => "Abs",
            OpKind::Sorted { .. } => "Sorted",
            OpKind::Reversed { .. } => "Reversed",
            OpKind::Add { .. } => "Add",
            OpKind::Sub { .. } => "Sub",
            OpKind::Mul { .. } => "Mul",
            OpKind::Div { .. } => "Div",
            OpKind::FloorDiv { .. } => "FloorDiv",
            OpKind::Mod { .. } => "Mod",
            OpKind::Neg { .. } => "Neg",
            OpKind::Eq { .. } => "Eq",
            OpKind::Ne { .. } => "Ne",
            OpKind::Lt { .. } => "Lt",
            OpKind::Le { .. } => "Le",
            OpKind::Gt { .. } => "Gt",
            OpKind::Ge { .. } => "Ge",
            OpKind::Not { .. } => "Not",
            OpKind::Concat { .. } => "Concat",
            OpKind::Contains { .. } => "Contains",
            OpKind::If { .. } => "If",
            OpKind::ForEach { .. } => "ForEach",
            OpKind::Map { .. } => "Map",
            OpKind::Filter { .. } => "Filter",
            OpKind::IfBlock { .. } => "IfBlock",
            OpKind::Break => "Break",
            OpKind::Continue => "Continue",
            OpKind::Index { .. } => "Index",
            OpKind::SetIndex { .. } => "SetIndex",
            OpKind::GeneratorDef { .. } => "GeneratorDef",
            OpKind::GeneratorYield { .. } => "GeneratorYield",
            OpKind::GeneratorYieldIf { .. } => "GeneratorYieldIf",
            OpKind::ParamRef { .. } => "ParamRef",
            OpKind::FrozenValue { .. } => "FrozenValue",
        }
    }

    pub fn requires_approval(&self) -> bool {
        match self {
            OpKind::ReadFile { .. }
            | OpKind::WriteFile { .. }
            | OpKind::AppendFile { .. }
            | OpKind::DeleteFile { .. }
            | OpKind::ListDir { .. }
            | OpKind::Mkdir { .. }
            | OpKind::Rmdir { .. }
            | OpKind::CopyFile { .. }
            | OpKind::MoveFile { .. }
            | OpKind::HttpRequest { .. }
            | OpKind::TcpConnect { .. }
            | OpKind::TcpListen { .. }
            | OpKind::UdpBind { .. }
            | OpKind::UdpSendTo { .. }
            | OpKind::UnixConnect { .. }
            | OpKind::UnixListen { .. }
            | OpKind::Exec { .. } => true,

            OpKind::FileExists { .. }
            | OpKind::IsDir { .. }
            | OpKind::IsFile { .. }
            | OpKind::FileSize { .. }
            | OpKind::TcpSend { .. }
            | OpKind::TcpRecv { .. }
            | OpKind::TcpClose { .. }
            | OpKind::TcpAccept { .. }
            | OpKind::UdpRecvFrom { .. }
            | OpKind::UdpClose { .. }
            | OpKind::UnixSend { .. }
            | OpKind::UnixRecv { .. }
            | OpKind::UnixClose { .. }
            | OpKind::UnixAccept { .. }
            | OpKind::EnvGet { .. }
            | OpKind::Sleep { .. }
            | OpKind::Now
            | OpKind::Print { .. }
            | OpKind::JsonEncode { .. }
            | OpKind::JsonDecode { .. }
            | OpKind::All { .. }
            | OpKind::Any { .. }
            | OpKind::AtLeast { .. }
            | OpKind::AtMost { .. }
            | OpKind::After { .. }
            | OpKind::ToBool { .. }
            | OpKind::ToInt { .. }
            | OpKind::ToFloat { .. }
            | OpKind::ToStr { .. }
            | OpKind::Len { .. }
            | OpKind::Min { .. }
            | OpKind::Max { .. }
            | OpKind::Sum { .. }
            | OpKind::Abs { .. }
            | OpKind::Sorted { .. }
            | OpKind::Reversed { .. }
            | OpKind::Add { .. }
            | OpKind::Sub { .. }
            | OpKind::Mul { .. }
            | OpKind::Div { .. }
            | OpKind::FloorDiv { .. }
            | OpKind::Mod { .. }
            | OpKind::Neg { .. }
            | OpKind::Eq { .. }
            | OpKind::Ne { .. }
            | OpKind::Lt { .. }
            | OpKind::Le { .. }
            | OpKind::Gt { .. }
            | OpKind::Ge { .. }
            | OpKind::Not { .. }
            | OpKind::Concat { .. }
            | OpKind::Contains { .. }
            | OpKind::If { .. }
            | OpKind::ForEach { .. }
            | OpKind::Map { .. }
            | OpKind::Filter { .. }
            | OpKind::IfBlock { .. }
            | OpKind::Break
            | OpKind::Continue
            | OpKind::Index { .. }
            | OpKind::SetIndex { .. }
            | OpKind::GeneratorDef { .. }
            | OpKind::GeneratorYield { .. }
            | OpKind::GeneratorYieldIf { .. }
            | OpKind::ParamRef { .. }
            | OpKind::FrozenValue { .. } => false,
        }
    }

    pub fn collect_value_refs(&self) -> Vec<&ValueRef> {
        match self {
            OpKind::ReadFile { path } => vec![path],
            OpKind::WriteFile { path, content } => vec![path, content],
            OpKind::AppendFile { path, content } => vec![path, content],
            OpKind::DeleteFile { path } => vec![path],
            OpKind::ListDir { path } => vec![path],
            OpKind::Mkdir { path, .. } => vec![path],
            OpKind::Rmdir { path, .. } => vec![path],
            OpKind::CopyFile { src, dst } => vec![src, dst],
            OpKind::MoveFile { src, dst } => vec![src, dst],
            OpKind::FileExists { path } => vec![path],
            OpKind::IsDir { path } => vec![path],
            OpKind::IsFile { path } => vec![path],
            OpKind::FileSize { path } => vec![path],
            OpKind::HttpRequest {
                method,
                url,
                headers,
                body,
            } => vec![method, url, headers, body],
            OpKind::TcpConnect { host, port } => vec![host, port],
            OpKind::TcpSend { handle, data } => vec![handle, data],
            OpKind::TcpRecv { handle, max_bytes } => vec![handle, max_bytes],
            OpKind::TcpClose { handle } => vec![handle],
            OpKind::TcpListen { host, port } => vec![host, port],
            OpKind::TcpAccept { listener } => vec![listener],
            OpKind::UdpBind { host, port } => vec![host, port],
            OpKind::UdpSendTo {
                handle,
                data,
                host,
                port,
            } => vec![handle, data, host, port],
            OpKind::UdpRecvFrom { handle, max_bytes } => vec![handle, max_bytes],
            OpKind::UdpClose { handle } => vec![handle],
            OpKind::UnixConnect { path } => vec![path],
            OpKind::UnixSend { handle, data } => vec![handle, data],
            OpKind::UnixRecv { handle, max_bytes } => vec![handle, max_bytes],
            OpKind::UnixClose { handle } => vec![handle],
            OpKind::UnixListen { path } => vec![path],
            OpKind::UnixAccept { listener } => vec![listener],
            OpKind::Exec { command, args } => vec![command, args],
            OpKind::EnvGet { name, default } => vec![name, default],
            OpKind::Sleep { seconds } => vec![seconds],
            OpKind::Now => vec![],
            OpKind::Print { message } => vec![message],
            OpKind::JsonEncode { value } => vec![value],
            OpKind::JsonDecode { string } => vec![string],
            OpKind::All { .. } | OpKind::Any { .. } | OpKind::AtLeast { .. } | OpKind::AtMost { .. } | OpKind::After { .. } => vec![],
            OpKind::ToBool { value } => vec![value],
            OpKind::ToInt { value } => vec![value],
            OpKind::ToFloat { value } => vec![value],
            OpKind::ToStr { value } => vec![value],
            OpKind::Len { value } => vec![value],
            OpKind::Min { values } => vec![values],
            OpKind::Max { values } => vec![values],
            OpKind::Sum { values, start } => vec![values, start],
            OpKind::Abs { value } => vec![value],
            OpKind::Sorted { values } => vec![values],
            OpKind::Reversed { values } => vec![values],
            OpKind::Add { left, right } => vec![left, right],
            OpKind::Sub { left, right } => vec![left, right],
            OpKind::Mul { left, right } => vec![left, right],
            OpKind::Div { left, right } => vec![left, right],
            OpKind::FloorDiv { left, right } => vec![left, right],
            OpKind::Mod { left, right } => vec![left, right],
            OpKind::Neg { value } => vec![value],
            OpKind::Eq { left, right } => vec![left, right],
            OpKind::Ne { left, right } => vec![left, right],
            OpKind::Lt { left, right } => vec![left, right],
            OpKind::Le { left, right } => vec![left, right],
            OpKind::Gt { left, right } => vec![left, right],
            OpKind::Ge { left, right } => vec![left, right],
            OpKind::Not { value } => vec![value],
            OpKind::Concat { left, right } => vec![left, right],
            OpKind::Contains { haystack, needle } => vec![haystack, needle],
            OpKind::If { condition, then_value, else_value } => vec![condition, then_value, else_value],
            OpKind::ForEach { items, .. } => vec![items],
            OpKind::Map { items, .. } => vec![items],
            OpKind::Filter { items, .. } => vec![items],
            OpKind::IfBlock { condition, .. } => vec![condition],
            OpKind::Break | OpKind::Continue => vec![],
            OpKind::Index { base, index } => vec![base, index],
            OpKind::SetIndex { base, index, value } => vec![base, index, value],
            OpKind::GeneratorDef { .. } => vec![],
            OpKind::GeneratorYield { value } => vec![value],
            OpKind::GeneratorYieldIf { condition, value } => vec![condition, value],
            OpKind::ParamRef { .. } => vec![],
            OpKind::FrozenValue { value, .. } => vec![value],
        }
    }

    pub fn collect_value_refs_mut(&mut self) -> Vec<&mut ValueRef> {
        match self {
            OpKind::ReadFile { path } => vec![path],
            OpKind::WriteFile { path, content } => vec![path, content],
            OpKind::AppendFile { path, content } => vec![path, content],
            OpKind::DeleteFile { path } => vec![path],
            OpKind::ListDir { path } => vec![path],
            OpKind::Mkdir { path, .. } => vec![path],
            OpKind::Rmdir { path, .. } => vec![path],
            OpKind::CopyFile { src, dst } => vec![src, dst],
            OpKind::MoveFile { src, dst } => vec![src, dst],
            OpKind::FileExists { path } => vec![path],
            OpKind::IsDir { path } => vec![path],
            OpKind::IsFile { path } => vec![path],
            OpKind::FileSize { path } => vec![path],
            OpKind::HttpRequest {
                method,
                url,
                headers,
                body,
            } => vec![method, url, headers, body],
            OpKind::TcpConnect { host, port } => vec![host, port],
            OpKind::TcpSend { handle, data } => vec![handle, data],
            OpKind::TcpRecv { handle, max_bytes } => vec![handle, max_bytes],
            OpKind::TcpClose { handle } => vec![handle],
            OpKind::TcpListen { host, port } => vec![host, port],
            OpKind::TcpAccept { listener } => vec![listener],
            OpKind::UdpBind { host, port } => vec![host, port],
            OpKind::UdpSendTo {
                handle,
                data,
                host,
                port,
            } => vec![handle, data, host, port],
            OpKind::UdpRecvFrom { handle, max_bytes } => vec![handle, max_bytes],
            OpKind::UdpClose { handle } => vec![handle],
            OpKind::UnixConnect { path } => vec![path],
            OpKind::UnixSend { handle, data } => vec![handle, data],
            OpKind::UnixRecv { handle, max_bytes } => vec![handle, max_bytes],
            OpKind::UnixClose { handle } => vec![handle],
            OpKind::UnixListen { path } => vec![path],
            OpKind::UnixAccept { listener } => vec![listener],
            OpKind::Exec { command, args } => vec![command, args],
            OpKind::EnvGet { name, default } => vec![name, default],
            OpKind::Sleep { seconds } => vec![seconds],
            OpKind::Now => vec![],
            OpKind::Print { message } => vec![message],
            OpKind::JsonEncode { value } => vec![value],
            OpKind::JsonDecode { string } => vec![string],
            OpKind::All { .. } | OpKind::Any { .. } | OpKind::AtLeast { .. } | OpKind::AtMost { .. } | OpKind::After { .. } => vec![],
            OpKind::ToBool { value } => vec![value],
            OpKind::ToInt { value } => vec![value],
            OpKind::ToFloat { value } => vec![value],
            OpKind::ToStr { value } => vec![value],
            OpKind::Len { value } => vec![value],
            OpKind::Min { values } => vec![values],
            OpKind::Max { values } => vec![values],
            OpKind::Sum { values, start } => vec![values, start],
            OpKind::Abs { value } => vec![value],
            OpKind::Sorted { values } => vec![values],
            OpKind::Reversed { values } => vec![values],
            OpKind::Add { left, right } => vec![left, right],
            OpKind::Sub { left, right } => vec![left, right],
            OpKind::Mul { left, right } => vec![left, right],
            OpKind::Div { left, right } => vec![left, right],
            OpKind::FloorDiv { left, right } => vec![left, right],
            OpKind::Mod { left, right } => vec![left, right],
            OpKind::Neg { value } => vec![value],
            OpKind::Eq { left, right } => vec![left, right],
            OpKind::Ne { left, right } => vec![left, right],
            OpKind::Lt { left, right } => vec![left, right],
            OpKind::Le { left, right } => vec![left, right],
            OpKind::Gt { left, right } => vec![left, right],
            OpKind::Ge { left, right } => vec![left, right],
            OpKind::Not { value } => vec![value],
            OpKind::Concat { left, right } => vec![left, right],
            OpKind::Contains { haystack, needle } => vec![haystack, needle],
            OpKind::If { condition, then_value, else_value } => vec![condition, then_value, else_value],
            OpKind::ForEach { items, .. } => vec![items],
            OpKind::Map { items, .. } => vec![items],
            OpKind::Filter { items, .. } => vec![items],
            OpKind::IfBlock { condition, .. } => vec![condition],
            OpKind::Break | OpKind::Continue => vec![],
            OpKind::Index { base, index } => vec![base, index],
            OpKind::SetIndex { base, index, value } => vec![base, index, value],
            OpKind::GeneratorDef { .. } => vec![],
            OpKind::GeneratorYield { value } => vec![value],
            OpKind::GeneratorYieldIf { condition, value } => vec![condition, value],
            OpKind::ParamRef { .. } => vec![],
            OpKind::FrozenValue { value, .. } => vec![value],
        }
    }

    pub fn collect_op_refs(&self) -> Vec<OpId> {
        match self {
            OpKind::All { ops } | OpKind::Any { ops } | OpKind::AtLeast { ops, .. } | OpKind::AtMost { ops, .. } => {
                ops.clone()
            }
            OpKind::After { dependency, value } => vec![*dependency, *value],
            _ => vec![],
        }
    }

    pub fn is_pure(&self) -> bool {
        matches!(
            self,
            // Arithmetic
            OpKind::Add { .. }
            | OpKind::Sub { .. }
            | OpKind::Mul { .. }
            | OpKind::Div { .. }
            | OpKind::FloorDiv { .. }
            | OpKind::Mod { .. }
            | OpKind::Neg { .. }
            | OpKind::Abs { .. }
            // Comparison
            | OpKind::Eq { .. }
            | OpKind::Ne { .. }
            | OpKind::Lt { .. }
            | OpKind::Le { .. }
            | OpKind::Gt { .. }
            | OpKind::Ge { .. }
            // Logic
            | OpKind::Not { .. }
            // String/list operations
            | OpKind::Concat { .. }
            | OpKind::Len { .. }
            | OpKind::Contains { .. }
            | OpKind::Index { .. }
            // Collection operations
            | OpKind::Min { .. }
            | OpKind::Max { .. }
            | OpKind::Sum { .. }
            | OpKind::Sorted { .. }
            | OpKind::Reversed { .. }
            // Type conversions
            | OpKind::ToBool { .. }
            | OpKind::ToInt { .. }
            | OpKind::ToFloat { .. }
            | OpKind::ToStr { .. }
            // JSON (pure transformation)
            | OpKind::JsonEncode { .. }
            | OpKind::JsonDecode { .. }
            // Conditional (pure if all branches are evaluated)
            | OpKind::If { .. }
        )
    }

    pub fn all_inputs_literal(&self) -> bool {
        let value_refs = self.collect_value_refs();
        let op_refs = self.collect_op_refs();
        value_refs.iter().all(|v| v.is_literal()) && op_refs.is_empty()
    }

    pub fn can_fold(&self) -> bool {
        self.is_pure() && self.all_inputs_literal()
    }

    pub fn to_text_fields(&self) -> Vec<(&'static str, String)> {
        match self {
            OpKind::ReadFile { path } => vec![("path", path.to_text())],
            OpKind::WriteFile { path, content } => vec![("path", path.to_text()), ("content", content.to_text())],
            OpKind::AppendFile { path, content } => vec![("path", path.to_text()), ("content", content.to_text())],
            OpKind::DeleteFile { path } => vec![("path", path.to_text())],
            OpKind::ListDir { path } => vec![("path", path.to_text())],
            OpKind::Mkdir { path, recursive } => vec![("path", path.to_text()), ("recursive", recursive.to_string())],
            OpKind::Rmdir { path, recursive } => vec![("path", path.to_text()), ("recursive", recursive.to_string())],
            OpKind::CopyFile { src, dst } => vec![("src", src.to_text()), ("dst", dst.to_text())],
            OpKind::MoveFile { src, dst } => vec![("src", src.to_text()), ("dst", dst.to_text())],
            OpKind::FileExists { path } => vec![("path", path.to_text())],
            OpKind::IsDir { path } => vec![("path", path.to_text())],
            OpKind::IsFile { path } => vec![("path", path.to_text())],
            OpKind::FileSize { path } => vec![("path", path.to_text())],
            OpKind::HttpRequest { method, url, headers, body } => vec![
                ("method", method.to_text()),
                ("url", url.to_text()),
                ("headers", headers.to_text()),
                ("body", body.to_text()),
            ],
            OpKind::TcpConnect { host, port } => vec![("host", host.to_text()), ("port", port.to_text())],
            OpKind::TcpSend { handle, data } => vec![("handle", handle.to_text()), ("data", data.to_text())],
            OpKind::TcpRecv { handle, max_bytes } => vec![("handle", handle.to_text()), ("max_bytes", max_bytes.to_text())],
            OpKind::TcpClose { handle } => vec![("handle", handle.to_text())],
            OpKind::TcpListen { host, port } => vec![("host", host.to_text()), ("port", port.to_text())],
            OpKind::TcpAccept { listener } => vec![("listener", listener.to_text())],
            OpKind::UdpBind { host, port } => vec![("host", host.to_text()), ("port", port.to_text())],
            OpKind::UdpSendTo { handle, data, host, port } => vec![
                ("handle", handle.to_text()),
                ("data", data.to_text()),
                ("host", host.to_text()),
                ("port", port.to_text()),
            ],
            OpKind::UdpRecvFrom { handle, max_bytes } => vec![("handle", handle.to_text()), ("max_bytes", max_bytes.to_text())],
            OpKind::UdpClose { handle } => vec![("handle", handle.to_text())],
            OpKind::UnixConnect { path } => vec![("path", path.to_text())],
            OpKind::UnixSend { handle, data } => vec![("handle", handle.to_text()), ("data", data.to_text())],
            OpKind::UnixRecv { handle, max_bytes } => vec![("handle", handle.to_text()), ("max_bytes", max_bytes.to_text())],
            OpKind::UnixClose { handle } => vec![("handle", handle.to_text())],
            OpKind::UnixListen { path } => vec![("path", path.to_text())],
            OpKind::UnixAccept { listener } => vec![("listener", listener.to_text())],
            OpKind::Exec { command, args } => vec![("command", command.to_text()), ("args", args.to_text())],
            OpKind::EnvGet { name, default } => vec![("name", name.to_text()), ("default", default.to_text())],
            OpKind::Sleep { seconds } => vec![("seconds", seconds.to_text())],
            OpKind::Now => vec![],
            OpKind::Print { message } => vec![("message", message.to_text())],
            OpKind::JsonEncode { value } => vec![("value", value.to_text())],
            OpKind::JsonDecode { string } => vec![("string", string.to_text())],
            OpKind::All { ops } => vec![("ops", format!("{:?}", ops.iter().map(|o| format!("@{}", o.0)).collect::<Vec<_>>()))],
            OpKind::Any { ops } => vec![("ops", format!("{:?}", ops.iter().map(|o| format!("@{}", o.0)).collect::<Vec<_>>()))],
            OpKind::AtLeast { ops, count } => vec![
                ("ops", format!("{:?}", ops.iter().map(|o| format!("@{}", o.0)).collect::<Vec<_>>())),
                ("count", count.to_string()),
            ],
            OpKind::AtMost { ops, count } => vec![
                ("ops", format!("{:?}", ops.iter().map(|o| format!("@{}", o.0)).collect::<Vec<_>>())),
                ("count", count.to_string()),
            ],
            OpKind::After { dependency, value } => vec![
                ("dependency", format!("@{}", dependency.0)),
                ("value", format!("@{}", value.0)),
            ],
            OpKind::ToBool { value } => vec![("value", value.to_text())],
            OpKind::ToInt { value } => vec![("value", value.to_text())],
            OpKind::ToFloat { value } => vec![("value", value.to_text())],
            OpKind::ToStr { value } => vec![("value", value.to_text())],
            OpKind::Len { value } => vec![("value", value.to_text())],
            OpKind::Min { values } => vec![("values", values.to_text())],
            OpKind::Max { values } => vec![("values", values.to_text())],
            OpKind::Sum { values, start } => vec![("values", values.to_text()), ("start", start.to_text())],
            OpKind::Abs { value } => vec![("value", value.to_text())],
            OpKind::Sorted { values } => vec![("values", values.to_text())],
            OpKind::Reversed { values } => vec![("values", values.to_text())],
            OpKind::Add { left, right } => vec![("left", left.to_text()), ("right", right.to_text())],
            OpKind::Sub { left, right } => vec![("left", left.to_text()), ("right", right.to_text())],
            OpKind::Mul { left, right } => vec![("left", left.to_text()), ("right", right.to_text())],
            OpKind::Div { left, right } => vec![("left", left.to_text()), ("right", right.to_text())],
            OpKind::FloorDiv { left, right } => vec![("left", left.to_text()), ("right", right.to_text())],
            OpKind::Mod { left, right } => vec![("left", left.to_text()), ("right", right.to_text())],
            OpKind::Neg { value } => vec![("value", value.to_text())],
            OpKind::Eq { left, right } => vec![("left", left.to_text()), ("right", right.to_text())],
            OpKind::Ne { left, right } => vec![("left", left.to_text()), ("right", right.to_text())],
            OpKind::Lt { left, right } => vec![("left", left.to_text()), ("right", right.to_text())],
            OpKind::Le { left, right } => vec![("left", left.to_text()), ("right", right.to_text())],
            OpKind::Gt { left, right } => vec![("left", left.to_text()), ("right", right.to_text())],
            OpKind::Ge { left, right } => vec![("left", left.to_text()), ("right", right.to_text())],
            OpKind::Not { value } => vec![("value", value.to_text())],
            OpKind::Concat { left, right } => vec![("left", left.to_text()), ("right", right.to_text())],
            OpKind::Contains { haystack, needle } => vec![("haystack", haystack.to_text()), ("needle", needle.to_text())],
            OpKind::If { condition, then_value, else_value } => vec![
                ("condition", condition.to_text()),
                ("then", then_value.to_text()),
                ("else", else_value.to_text()),
            ],
            OpKind::ForEach { items, item_name, parallel, .. } => vec![
                ("items", items.to_text()),
                ("item_name", item_name.clone()),
                ("parallel", parallel.to_string()),
                ("body", "<subplan>".to_string()),
            ],
            OpKind::Map { items, item_name, .. } => vec![
                ("items", items.to_text()),
                ("item_name", item_name.clone()),
                ("body", "<subplan>".to_string()),
            ],
            OpKind::Filter { items, item_name, .. } => vec![
                ("items", items.to_text()),
                ("item_name", item_name.clone()),
                ("predicate", "<subplan>".to_string()),
            ],
            OpKind::IfBlock { condition, else_body, .. } => vec![
                ("condition", condition.to_text()),
                ("then_body", "<subplan>".to_string()),
                ("else_body", if else_body.is_some() { "<subplan>" } else { "none" }.to_string()),
            ],
            OpKind::Break => vec![],
            OpKind::Continue => vec![],
            OpKind::Index { base, index } => vec![("base", base.to_text()), ("index", index.to_text())],
            OpKind::SetIndex { base, index, value } => vec![("base", base.to_text()), ("index", index.to_text()), ("value", value.to_text())],
            OpKind::GeneratorDef { name, params, .. } => vec![
                ("name", name.clone()),
                ("params", format!("{:?}", params)),
                ("body", "<subplan>".to_string()),
            ],
            OpKind::GeneratorYield { value } => vec![("value", value.to_text())],
            OpKind::GeneratorYieldIf { condition, value } => vec![("condition", condition.to_text()), ("value", value.to_text())],
            OpKind::ParamRef { name } => vec![("name", name.clone())],
            OpKind::FrozenValue { name, value } => vec![("name", name.clone()), ("value", value.to_text())],
        }
    }
}

impl std::fmt::Display for OpKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            OpKind::ReadFile { path } => write!(f, "ReadFile({})", path),
            OpKind::WriteFile { path, .. } => write!(f, "WriteFile({})", path),
            OpKind::AppendFile { path, .. } => write!(f, "AppendFile({})", path),
            OpKind::DeleteFile { path } => write!(f, "DeleteFile({})", path),
            OpKind::ListDir { path } => write!(f, "ListDir({})", path),
            OpKind::Mkdir { path, recursive } => {
                if *recursive {
                    write!(f, "MkdirAll({})", path)
                } else {
                    write!(f, "Mkdir({})", path)
                }
            }
            OpKind::Rmdir { path, recursive } => {
                if *recursive {
                    write!(f, "RmdirAll({})", path)
                } else {
                    write!(f, "Rmdir({})", path)
                }
            }
            OpKind::CopyFile { src, dst } => write!(f, "CopyFile({} -> {})", src, dst),
            OpKind::MoveFile { src, dst } => write!(f, "MoveFile({} -> {})", src, dst),
            OpKind::FileExists { path } => write!(f, "FileExists({})", path),
            OpKind::IsDir { path } => write!(f, "IsDir({})", path),
            OpKind::IsFile { path } => write!(f, "IsFile({})", path),
            OpKind::FileSize { path } => write!(f, "FileSize({})", path),
            OpKind::HttpRequest { method, url, .. } => write!(f, "HttpRequest({} {})", method, url),
            OpKind::TcpConnect { host, port } => write!(f, "TcpConnect({}:{})", host, port),
            OpKind::TcpSend { handle, .. } => write!(f, "TcpSend({})", handle),
            OpKind::TcpRecv { handle, .. } => write!(f, "TcpRecv({})", handle),
            OpKind::TcpClose { handle } => write!(f, "TcpClose({})", handle),
            OpKind::TcpListen { host, port } => write!(f, "TcpListen({}:{})", host, port),
            OpKind::TcpAccept { listener } => write!(f, "TcpAccept({})", listener),
            OpKind::UdpBind { host, port } => write!(f, "UdpBind({}:{})", host, port),
            OpKind::UdpSendTo { host, port, .. } => write!(f, "UdpSendTo({}:{})", host, port),
            OpKind::UdpRecvFrom { handle, .. } => write!(f, "UdpRecvFrom({})", handle),
            OpKind::UdpClose { handle } => write!(f, "UdpClose({})", handle),
            OpKind::UnixConnect { path } => write!(f, "UnixConnect({})", path),
            OpKind::UnixSend { handle, .. } => write!(f, "UnixSend({})", handle),
            OpKind::UnixRecv { handle, .. } => write!(f, "UnixRecv({})", handle),
            OpKind::UnixClose { handle } => write!(f, "UnixClose({})", handle),
            OpKind::UnixListen { path } => write!(f, "UnixListen({})", path),
            OpKind::UnixAccept { listener } => write!(f, "UnixAccept({})", listener),
            OpKind::Exec { command, .. } => write!(f, "Exec({})", command),
            OpKind::EnvGet { name, .. } => write!(f, "EnvGet({})", name),
            OpKind::Sleep { seconds } => write!(f, "Sleep({})", seconds),
            OpKind::Now => write!(f, "Now()"),
            OpKind::Print { message } => write!(f, "Print({})", message),
            OpKind::JsonEncode { .. } => write!(f, "JsonEncode(...)"),
            OpKind::JsonDecode { .. } => write!(f, "JsonDecode(...)"),
            OpKind::All { ops } => write!(f, "All({:?})", ops.iter().map(|o| o.0).collect::<Vec<_>>()),
            OpKind::Any { ops } => write!(f, "Any({:?})", ops.iter().map(|o| o.0).collect::<Vec<_>>()),
            OpKind::AtLeast { ops, count } => {
                write!(f, "AtLeast({}, {:?})", count, ops.iter().map(|o| o.0).collect::<Vec<_>>())
            }
            OpKind::AtMost { ops, count } => {
                write!(f, "AtMost({}, {:?})", count, ops.iter().map(|o| o.0).collect::<Vec<_>>())
            }
            OpKind::After { dependency, value } => {
                write!(f, "After({} -> {})", dependency.0, value.0)
            }
            OpKind::ToBool { value } => write!(f, "ToBool({})", value),
            OpKind::ToInt { value } => write!(f, "ToInt({})", value),
            OpKind::ToFloat { value } => write!(f, "ToFloat({})", value),
            OpKind::ToStr { value } => write!(f, "ToStr({})", value),
            OpKind::Len { value } => write!(f, "Len({})", value),
            OpKind::Min { values } => write!(f, "Min({})", values),
            OpKind::Max { values } => write!(f, "Max({})", values),
            OpKind::Sum { values, start } => write!(f, "Sum({}, {})", values, start),
            OpKind::Abs { value } => write!(f, "Abs({})", value),
            OpKind::Sorted { values } => write!(f, "Sorted({})", values),
            OpKind::Reversed { values } => write!(f, "Reversed({})", values),
            OpKind::Add { left, right } => write!(f, "Add({}, {})", left, right),
            OpKind::Sub { left, right } => write!(f, "Sub({}, {})", left, right),
            OpKind::Mul { left, right } => write!(f, "Mul({}, {})", left, right),
            OpKind::Div { left, right } => write!(f, "Div({}, {})", left, right),
            OpKind::FloorDiv { left, right } => write!(f, "FloorDiv({}, {})", left, right),
            OpKind::Mod { left, right } => write!(f, "Mod({}, {})", left, right),
            OpKind::Neg { value } => write!(f, "Neg({})", value),
            OpKind::Eq { left, right } => write!(f, "Eq({}, {})", left, right),
            OpKind::Ne { left, right } => write!(f, "Ne({}, {})", left, right),
            OpKind::Lt { left, right } => write!(f, "Lt({}, {})", left, right),
            OpKind::Le { left, right } => write!(f, "Le({}, {})", left, right),
            OpKind::Gt { left, right } => write!(f, "Gt({}, {})", left, right),
            OpKind::Ge { left, right } => write!(f, "Ge({}, {})", left, right),
            OpKind::Not { value } => write!(f, "Not({})", value),
            OpKind::Concat { left, right } => write!(f, "Concat({}, {})", left, right),
            OpKind::Contains { haystack, needle } => write!(f, "Contains({}, {})", haystack, needle),
            OpKind::If { condition, then_value, else_value } => {
                write!(f, "If({}, {}, {})", condition, then_value, else_value)
            }
            OpKind::ForEach { items, item_name, parallel, .. } => {
                let mode = if *parallel { "parallel" } else { "sequential" };
                write!(f, "ForEach({}, {}, {}, <subplan>)", items, item_name, mode)
            }
            OpKind::Map { items, item_name, .. } => {
                write!(f, "Map({}, {}, <subplan>)", items, item_name)
            }
            OpKind::Filter { items, item_name, .. } => {
                write!(f, "Filter({}, {}, <predicate>)", items, item_name)
            }
            OpKind::IfBlock { condition, else_body, .. } => {
                if else_body.is_some() {
                    write!(f, "IfBlock({}, <then>, <else>)", condition)
                } else {
                    write!(f, "IfBlock({}, <then>)", condition)
                }
            }
            OpKind::Break => write!(f, "Break"),
            OpKind::Continue => write!(f, "Continue"),
            OpKind::Index { base, index } => write!(f, "Index({}, {})", base, index),
            OpKind::SetIndex { base, index, value } => write!(f, "SetIndex({}, {}, {})", base, index, value),
            OpKind::GeneratorDef { name, params, .. } => write!(f, "GeneratorDef({}, {:?}, <subplan>)", name, params),
            OpKind::GeneratorYield { value } => write!(f, "GeneratorYield({})", value),
            OpKind::GeneratorYieldIf { condition, value } => write!(f, "GeneratorYieldIf({}, {})", condition, value),
            OpKind::ParamRef { name } => write!(f, "ParamRef({})", name),
            OpKind::FrozenValue { name, value } => write!(f, "FrozenValue({} = {})", name, value),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum RecordedValue {
    None,
    Bool(bool),
    Int(i64),
    Float(f64),
    String(String),
    Bytes(Vec<u8>),
    List(Vec<RecordedValue>),
    Dict(BTreeMap<String, RecordedValue>),
}

impl RecordedValue {
    pub fn as_string(&self) -> Option<&str> {
        match self {
            RecordedValue::String(s) => Some(s),
            _ => None,
        }
    }

    pub fn as_int(&self) -> Option<i64> {
        match self {
            RecordedValue::Int(i) => Some(*i),
            _ => None,
        }
    }

    pub fn as_float(&self) -> Option<f64> {
        match self {
            RecordedValue::Float(f) => Some(*f),
            _ => None,
        }
    }

    pub fn as_bool(&self) -> Option<bool> {
        match self {
            RecordedValue::Bool(b) => Some(*b),
            _ => None,
        }
    }

    pub fn as_bytes(&self) -> Option<&[u8]> {
        match self {
            RecordedValue::Bytes(b) => Some(b),
            _ => None,
        }
    }

    pub fn as_list(&self) -> Option<&Vec<RecordedValue>> {
        match self {
            RecordedValue::List(l) => Some(l),
            _ => None,
        }
    }

    pub fn as_dict(&self) -> Option<&BTreeMap<String, RecordedValue>> {
        match self {
            RecordedValue::Dict(d) => Some(d),
            _ => None,
        }
    }

    pub fn get(&self, accessor: &Accessor) -> Option<&RecordedValue> {
        match (self, accessor) {
            (RecordedValue::Dict(d), Accessor::Field(key)) => d.get(key),
            (RecordedValue::List(l), Accessor::Index(idx)) => {
                let idx = if *idx < 0 {
                    (l.len() as i64 + idx) as usize
                } else {
                    *idx as usize
                };
                l.get(idx)
            }
            _ => None,
        }
    }

    pub fn get_path(&self, path: &[Accessor]) -> Option<&RecordedValue> {
        let mut current = self;
        for accessor in path {
            current = current.get(accessor)?;
        }
        Some(current)
    }
}

impl Default for RecordedValue {
    fn default() -> Self {
        RecordedValue::None
    }
}

impl std::fmt::Display for RecordedValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RecordedValue::None => write!(f, "None"),
            RecordedValue::Bool(b) => write!(f, "{}", b),
            RecordedValue::Int(i) => write!(f, "{}", i),
            RecordedValue::Float(fl) => write!(f, "{}", fl),
            RecordedValue::String(s) => write!(f, "\"{}\"", s),
            RecordedValue::Bytes(b) => write!(f, "<bytes len={}>", b.len()),
            RecordedValue::List(l) => {
                write!(f, "[")?;
                for (i, v) in l.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{}", v)?;
                }
                write!(f, "]")
            }
            RecordedValue::Dict(d) => {
                write!(f, "{{")?;
                for (i, (k, v)) in d.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "\"{}\": {}", k, v)?;
                }
                write!(f, "}}")
            }
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash, Allocative)]
pub enum Accessor {
    Field(String),
    Index(i64),
}

impl std::fmt::Display for Accessor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Accessor::Field(s) => write!(f, "[\"{}\"]", s),
            Accessor::Index(i) => write!(f, "[{}]", i),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum ValueRef {
    Literal(RecordedValue),
    OpOutput {
        op: OpId,
        path: Vec<Accessor>,
    },
    Dynamic(String),
    List(Vec<ValueRef>),
}

impl ValueRef {
    pub fn literal_string(s: impl Into<String>) -> Self {
        ValueRef::Literal(RecordedValue::String(s.into()))
    }

    pub fn literal_int(i: i64) -> Self {
        ValueRef::Literal(RecordedValue::Int(i))
    }

    pub fn literal_float(f: f64) -> Self {
        ValueRef::Literal(RecordedValue::Float(f))
    }

    pub fn literal_bool(b: bool) -> Self {
        ValueRef::Literal(RecordedValue::Bool(b))
    }

    pub fn literal_none() -> Self {
        ValueRef::Literal(RecordedValue::None)
    }

    pub fn literal_list(items: Vec<RecordedValue>) -> Self {
        ValueRef::Literal(RecordedValue::List(items))
    }

    pub fn literal_dict(items: BTreeMap<String, RecordedValue>) -> Self {
        ValueRef::Literal(RecordedValue::Dict(items))
    }

    pub fn op_output(op: OpId) -> Self {
        ValueRef::OpOutput {
            op,
            path: Vec::new(),
        }
    }

    pub fn op_output_field(op: OpId, field: impl Into<String>) -> Self {
        ValueRef::OpOutput {
            op,
            path: vec![Accessor::Field(field.into())],
        }
    }

    pub fn op_output_index(op: OpId, index: i64) -> Self {
        ValueRef::OpOutput {
            op,
            path: vec![Accessor::Index(index)],
        }
    }

    pub fn with_field(self, field: impl Into<String>) -> Self {
        match self {
            ValueRef::OpOutput { op, mut path } => {
                path.push(Accessor::Field(field.into()));
                ValueRef::OpOutput { op, path }
            }
            other => other,
        }
    }

    pub fn with_index(self, index: i64) -> Self {
        match self {
            ValueRef::OpOutput { op, mut path } => {
                path.push(Accessor::Index(index));
                ValueRef::OpOutput { op, path }
            }
            other => other,
        }
    }

    pub fn referenced_op(&self) -> Option<OpId> {
        match self {
            ValueRef::OpOutput { op, .. } => Some(*op),
            _ => None,
        }
    }

    pub fn referenced_ops(&self) -> Vec<OpId> {
        match self {
            ValueRef::OpOutput { op, .. } => vec![*op],
            ValueRef::List(items) => items.iter().flat_map(|v| v.referenced_ops()).collect(),
            _ => vec![],
        }
    }

    pub fn is_literal(&self) -> bool {
        matches!(self, ValueRef::Literal(_))
    }

    pub fn is_dynamic(&self) -> bool {
        !matches!(self, ValueRef::Literal(_))
    }

    pub fn to_text(&self) -> String {
        match self {
            ValueRef::Literal(v) => v.to_string(),
            ValueRef::OpOutput { op, path } => {
                let mut s = format!("@{}", op.0);
                for accessor in path {
                    s.push_str(&accessor.to_string());
                }
                s
            }
            ValueRef::Dynamic(name) => format!("${{{}}}", name),
            ValueRef::List(items) => {
                let item_strs: Vec<String> = items.iter().map(|v| v.to_text()).collect();
                format!("[{}]", item_strs.join(", "))
            }
        }
    }
}

impl std::fmt::Display for ValueRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ValueRef::Literal(v) => write!(f, "{}", v),
            ValueRef::OpOutput { op, path } => {
                write!(f, "Op[{}]", op.0)?;
                for accessor in path {
                    write!(f, "{}", accessor)?;
                }
                Ok(())
            }
            ValueRef::Dynamic(name) => write!(f, "${}", name),
            ValueRef::List(items) => {
                write!(f, "[")?;
                for (i, item) in items.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{}", item)?;
                }
                write!(f, "]")
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_simple_subplan() -> SubPlan {
        SubPlan {
            params: vec!["x".to_string()],
            ops: vec![Op {
                id: OpId(0),
                kind: OpKind::Print {
                    message: ValueRef::Dynamic("x".to_string()),
                },
                inputs: vec![],
                source_location: None,
                guard: None,
            }],
            output: OpId(0),
        }
    }

    fn make_guarded_subplan() -> SubPlan {
        SubPlan {
            params: vec!["item".to_string()],
            ops: vec![
                Op {
                    id: OpId(0),
                    kind: OpKind::Contains {
                        haystack: ValueRef::Dynamic("item".to_string()),
                        needle: ValueRef::literal_string("test"),
                    },
                    inputs: vec![],
                    source_location: None,
                    guard: None,
                },
                Op {
                    id: OpId(1),
                    kind: OpKind::Print {
                        message: ValueRef::Dynamic("item".to_string()),
                    },
                    inputs: vec![OpId(0)],
                    source_location: None,
                    guard: Some(OpId(0)),
                },
            ],
            output: OpId(1),
        }
    }

    #[test]
    fn test_foreach_creation() {
        let foreach = OpKind::ForEach {
            items: ValueRef::op_output(OpId(0)),
            item_name: "x".to_string(),
            body: make_simple_subplan(),
            parallel: false,
        };

        assert_eq!(foreach.name(), "ForEach");
        assert!(!foreach.requires_approval());
    }

    #[test]
    fn test_foreach_parallel_flag() {
        let sequential = OpKind::ForEach {
            items: ValueRef::op_output(OpId(0)),
            item_name: "x".to_string(),
            body: make_simple_subplan(),
            parallel: false,
        };

        let parallel = OpKind::ForEach {
            items: ValueRef::op_output(OpId(0)),
            item_name: "x".to_string(),
            body: make_simple_subplan(),
            parallel: true,
        };

        let seq_display = format!("{}", sequential);
        let par_display = format!("{}", parallel);

        assert!(seq_display.contains("sequential"));
        assert!(par_display.contains("parallel"));
    }

    #[test]
    fn test_foreach_collect_value_refs() {
        let items_ref = ValueRef::op_output(OpId(5));
        let foreach = OpKind::ForEach {
            items: items_ref.clone(),
            item_name: "x".to_string(),
            body: make_simple_subplan(),
            parallel: false,
        };

        let refs = foreach.collect_value_refs();
        assert_eq!(refs.len(), 1);
        assert_eq!(refs[0], &items_ref);
    }

    #[test]
    fn test_ifblock_creation() {
        let ifblock = OpKind::IfBlock {
            condition: ValueRef::op_output(OpId(0)),
            then_body: make_simple_subplan(),
            else_body: None,
        };

        assert_eq!(ifblock.name(), "IfBlock");
        assert!(!ifblock.requires_approval());
    }

    #[test]
    fn test_ifblock_with_else() {
        let ifblock = OpKind::IfBlock {
            condition: ValueRef::op_output(OpId(0)),
            then_body: make_simple_subplan(),
            else_body: Some(make_simple_subplan()),
        };

        let display = format!("{}", ifblock);
        assert!(display.contains("<then>"));
        assert!(display.contains("<else>"));
    }

    #[test]
    fn test_ifblock_without_else() {
        let ifblock = OpKind::IfBlock {
            condition: ValueRef::op_output(OpId(0)),
            then_body: make_simple_subplan(),
            else_body: None,
        };

        let display = format!("{}", ifblock);
        assert!(display.contains("<then>"));
        assert!(!display.contains("<else>"));
    }

    #[test]
    fn test_ifblock_collect_value_refs() {
        let cond_ref = ValueRef::op_output(OpId(3));
        let ifblock = OpKind::IfBlock {
            condition: cond_ref.clone(),
            then_body: make_simple_subplan(),
            else_body: None,
        };

        let refs = ifblock.collect_value_refs();
        assert_eq!(refs.len(), 1);
        assert_eq!(refs[0], &cond_ref);
    }

    #[test]
    fn test_break_creation() {
        let brk = OpKind::Break;
        assert_eq!(brk.name(), "Break");
        assert!(!brk.requires_approval());
        assert_eq!(format!("{}", brk), "Break");
    }

    #[test]
    fn test_continue_creation() {
        let cont = OpKind::Continue;
        assert_eq!(cont.name(), "Continue");
        assert!(!cont.requires_approval());
        assert_eq!(format!("{}", cont), "Continue");
    }

    #[test]
    fn test_break_continue_no_value_refs() {
        let brk = OpKind::Break;
        let cont = OpKind::Continue;

        assert!(brk.collect_value_refs().is_empty());
        assert!(cont.collect_value_refs().is_empty());
    }

    #[test]
    fn test_op_with_guard() {
        let op = Op {
            id: OpId(1),
            kind: OpKind::Print {
                message: ValueRef::literal_string("hello"),
            },
            inputs: vec![OpId(0)],
            source_location: None,
            guard: Some(OpId(0)),
        };

        assert_eq!(op.guard, Some(OpId(0)));
    }

    #[test]
    fn test_op_without_guard() {
        let op = Op {
            id: OpId(0),
            kind: OpKind::Now,
            inputs: vec![],
            source_location: None,
            guard: None,
        };

        assert_eq!(op.guard, None);
    }

    #[test]
    fn test_subplan_with_params() {
        let subplan = SubPlan {
            params: vec!["a".to_string(), "b".to_string()],
            ops: vec![],
            output: OpId(0),
        };

        assert_eq!(subplan.params.len(), 2);
        assert_eq!(subplan.params[0], "a");
        assert_eq!(subplan.params[1], "b");
    }

    #[test]
    fn test_subplan_with_guarded_ops() {
        let subplan = make_guarded_subplan();

        assert_eq!(subplan.ops.len(), 2);
        assert_eq!(subplan.ops[0].guard, None);
        assert_eq!(subplan.ops[1].guard, Some(OpId(0)));
    }

    #[test]
    fn test_foreach_serialization_roundtrip() {
        let foreach = OpKind::ForEach {
            items: ValueRef::op_output(OpId(0)),
            item_name: "item".to_string(),
            body: make_simple_subplan(),
            parallel: true,
        };

        let serialized = bincode::serialize(&foreach).expect("serialization failed");
        let deserialized: OpKind = bincode::deserialize(&serialized).expect("deserialization failed");

        assert_eq!(deserialized.name(), "ForEach");
        if let OpKind::ForEach { item_name, parallel, .. } = deserialized {
            assert_eq!(item_name, "item");
            assert!(parallel);
        } else {
            panic!("Expected ForEach");
        }
    }

    #[test]
    fn test_ifblock_serialization_roundtrip() {
        let ifblock = OpKind::IfBlock {
            condition: ValueRef::Literal(RecordedValue::Bool(true)),
            then_body: make_simple_subplan(),
            else_body: Some(make_simple_subplan()),
        };

        let serialized = bincode::serialize(&ifblock).expect("serialization failed");
        let deserialized: OpKind = bincode::deserialize(&serialized).expect("deserialization failed");

        assert_eq!(deserialized.name(), "IfBlock");
        if let OpKind::IfBlock { else_body, .. } = deserialized {
            assert!(else_body.is_some());
        } else {
            panic!("Expected IfBlock");
        }
    }

    #[test]
    fn test_break_continue_serialization_roundtrip() {
        let brk = OpKind::Break;
        let cont = OpKind::Continue;

        let brk_ser = bincode::serialize(&brk).expect("serialization failed");
        let cont_ser = bincode::serialize(&cont).expect("serialization failed");

        let brk_de: OpKind = bincode::deserialize(&brk_ser).expect("deserialization failed");
        let cont_de: OpKind = bincode::deserialize(&cont_ser).expect("deserialization failed");

        assert_eq!(brk_de.name(), "Break");
        assert_eq!(cont_de.name(), "Continue");
    }

    #[test]
    fn test_op_with_guard_serialization_roundtrip() {
        let op = Op {
            id: OpId(5),
            kind: OpKind::Print {
                message: ValueRef::literal_string("test"),
            },
            inputs: vec![OpId(0), OpId(1)],
            source_location: Some(SourceSpan {
                file: "test.star".to_string(),
                line: 10,
                column: 5,
            }),
            guard: Some(OpId(2)),
        };

        let serialized = bincode::serialize(&op).expect("serialization failed");
        let deserialized: Op = bincode::deserialize(&serialized).expect("deserialization failed");

        assert_eq!(deserialized.id, OpId(5));
        assert_eq!(deserialized.guard, Some(OpId(2)));
        assert_eq!(deserialized.inputs.len(), 2);
    }

    #[test]
    fn test_nested_subplan_in_foreach() {
        let inner_foreach = OpKind::ForEach {
            items: ValueRef::Dynamic("inner_items".to_string()),
            item_name: "y".to_string(),
            body: make_simple_subplan(),
            parallel: false,
        };

        let nested_subplan = SubPlan {
            params: vec!["x".to_string()],
            ops: vec![Op {
                id: OpId(0),
                kind: inner_foreach,
                inputs: vec![],
                source_location: None,
                guard: None,
            }],
            output: OpId(0),
        };

        let outer_foreach = OpKind::ForEach {
            items: ValueRef::op_output(OpId(0)),
            item_name: "x".to_string(),
            body: nested_subplan,
            parallel: true,
        };

        let serialized = bincode::serialize(&outer_foreach).expect("serialization failed");
        let deserialized: OpKind = bincode::deserialize(&serialized).expect("deserialization failed");

        if let OpKind::ForEach { body, .. } = deserialized {
            if let OpKind::ForEach { item_name, .. } = &body.ops[0].kind {
                assert_eq!(item_name, "y");
            } else {
                panic!("Expected nested ForEach");
            }
        } else {
            panic!("Expected ForEach");
        }
    }

    #[test]
    fn test_foreach_to_text_fields() {
        let foreach = OpKind::ForEach {
            items: ValueRef::op_output(OpId(5)),
            item_name: "item".to_string(),
            body: make_simple_subplan(),
            parallel: true,
        };

        let fields = foreach.to_text_fields();
        let field_names: Vec<_> = fields.iter().map(|(name, _)| *name).collect();

        assert!(field_names.contains(&"items"));
        assert!(field_names.contains(&"item_name"));
        assert!(field_names.contains(&"parallel"));
        assert!(field_names.contains(&"body"));
        assert_eq!(fields.len(), 4);
    }

    #[test]
    fn test_ifblock_to_text_fields() {
        let ifblock_no_else = OpKind::IfBlock {
            condition: ValueRef::op_output(OpId(0)),
            then_body: make_simple_subplan(),
            else_body: None,
        };

        let ifblock_with_else = OpKind::IfBlock {
            condition: ValueRef::op_output(OpId(0)),
            then_body: make_simple_subplan(),
            else_body: Some(make_simple_subplan()),
        };

        let fields_no_else = ifblock_no_else.to_text_fields();
        let fields_with_else = ifblock_with_else.to_text_fields();

        assert_eq!(fields_no_else.len(), 3);
        assert_eq!(fields_with_else.len(), 3);

        let field_names: Vec<_> = fields_with_else.iter().map(|(name, _)| *name).collect();
        assert!(field_names.contains(&"condition"));
        assert!(field_names.contains(&"then_body"));
        assert!(field_names.contains(&"else_body"));
    }

    #[test]
    fn test_break_continue_to_text_fields() {
        let brk = OpKind::Break;
        let cont = OpKind::Continue;

        assert!(brk.to_text_fields().is_empty());
        assert!(cont.to_text_fields().is_empty());
    }

    #[test]
    fn test_control_flow_ops_not_pure() {
        let foreach = OpKind::ForEach {
            items: ValueRef::Literal(RecordedValue::List(vec![])),
            item_name: "x".to_string(),
            body: make_simple_subplan(),
            parallel: false,
        };

        let ifblock = OpKind::IfBlock {
            condition: ValueRef::Literal(RecordedValue::Bool(true)),
            then_body: make_simple_subplan(),
            else_body: None,
        };

        let brk = OpKind::Break;
        let cont = OpKind::Continue;

        assert!(!foreach.is_pure());
        assert!(!ifblock.is_pure());
        assert!(!brk.is_pure());
        assert!(!cont.is_pure());
    }

    #[test]
    fn test_foreach_with_literal_items() {
        let items = ValueRef::Literal(RecordedValue::List(vec![
            RecordedValue::Int(1),
            RecordedValue::Int(2),
            RecordedValue::Int(3),
        ]));

        let foreach = OpKind::ForEach {
            items,
            item_name: "n".to_string(),
            body: make_simple_subplan(),
            parallel: false,
        };

        let refs = foreach.collect_value_refs();
        assert_eq!(refs.len(), 1);
        assert!(refs[0].is_literal());
    }

    #[test]
    fn test_ifblock_with_literal_condition() {
        let ifblock = OpKind::IfBlock {
            condition: ValueRef::Literal(RecordedValue::Bool(true)),
            then_body: make_simple_subplan(),
            else_body: None,
        };

        let refs = ifblock.collect_value_refs();
        assert_eq!(refs.len(), 1);
        assert!(refs[0].is_literal());
    }
}
