use allocative::Allocative;
use crate::op::{Accessor, RecordedValue, SourceSpan};
use serde::{Deserialize, Serialize};
use std::time::{SystemTime, UNIX_EPOCH};

pub const SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, Copy, Hash, Eq, PartialEq, Serialize, Deserialize, Allocative)]
pub struct SchemaOpId(pub u64);

impl std::fmt::Display for SchemaOpId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Schema[{}]", self.0)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum SchemaValue {
    Literal(RecordedValue),
    EnvRef(String),
    ConfigRef(String),
    OpRef { id: SchemaOpId, path: Vec<Accessor> },
    List(Vec<SchemaValue>),
    ParamRef(String),
}

impl SchemaValue {
    pub fn literal_string(s: impl Into<String>) -> Self {
        SchemaValue::Literal(RecordedValue::String(s.into()))
    }

    pub fn literal_int(i: i64) -> Self {
        SchemaValue::Literal(RecordedValue::Int(i))
    }

    pub fn literal_float(f: f64) -> Self {
        SchemaValue::Literal(RecordedValue::Float(f))
    }

    pub fn literal_bool(b: bool) -> Self {
        SchemaValue::Literal(RecordedValue::Bool(b))
    }

    pub fn literal_none() -> Self {
        SchemaValue::Literal(RecordedValue::None)
    }

    pub fn literal_list(items: Vec<RecordedValue>) -> Self {
        SchemaValue::Literal(RecordedValue::List(items))
    }

    pub fn op_ref(id: SchemaOpId) -> Self {
        SchemaValue::OpRef { id, path: Vec::new() }
    }

    pub fn op_ref_with_path(id: SchemaOpId, path: Vec<Accessor>) -> Self {
        SchemaValue::OpRef { id, path }
    }

    pub fn env_ref(name: impl Into<String>) -> Self {
        SchemaValue::EnvRef(name.into())
    }

    pub fn config_ref(key: impl Into<String>) -> Self {
        SchemaValue::ConfigRef(key.into())
    }

    pub fn param_ref(name: impl Into<String>) -> Self {
        SchemaValue::ParamRef(name.into())
    }

    pub fn is_literal(&self) -> bool {
        match self {
            SchemaValue::Literal(_) => true,
            SchemaValue::List(items) => items.iter().all(|v| v.is_literal()),
            _ => false,
        }
    }

    pub fn referenced_op(&self) -> Option<SchemaOpId> {
        match self {
            SchemaValue::OpRef { id, .. } => Some(*id),
            _ => None,
        }
    }

    pub fn referenced_ops(&self) -> Vec<SchemaOpId> {
        match self {
            SchemaValue::OpRef { id, .. } => vec![*id],
            SchemaValue::List(items) => items.iter().flat_map(|v| v.referenced_ops()).collect(),
            _ => vec![],
        }
    }

    pub fn to_text(&self) -> String {
        match self {
            SchemaValue::Literal(v) => v.to_string(),
            SchemaValue::EnvRef(name) => format!("$env{{{}}}", name),
            SchemaValue::ConfigRef(key) => format!("$config{{{}}}", key),
            SchemaValue::OpRef { id, path } => {
                let mut s = format!("@{}", id.0);
                for accessor in path {
                    s.push_str(&accessor.to_string());
                }
                s
            }
            SchemaValue::List(items) => {
                let items_str: Vec<_> = items.iter().map(|v| v.to_text()).collect();
                format!("[{}]", items_str.join(", "))
            }
            SchemaValue::ParamRef(name) => format!("${{{}}}", name),
        }
    }
}

impl std::fmt::Display for SchemaValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SchemaValue::Literal(v) => write!(f, "{}", v),
            SchemaValue::EnvRef(name) => write!(f, "${}", name),
            SchemaValue::ConfigRef(key) => write!(f, "@{}", key),
            SchemaValue::OpRef { id, path } => {
                write!(f, "{}", id)?;
                for accessor in path {
                    write!(f, "{}", accessor)?;
                }
                Ok(())
            }
            SchemaValue::List(items) => {
                write!(f, "[")?;
                for (i, item) in items.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{}", item)?;
                }
                write!(f, "]")
            }
            SchemaValue::ParamRef(name) => write!(f, "${{{}}}", name),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SchemaSubPlan {
    pub params: Vec<String>,
    pub entries: Vec<SchemaSubPlanEntry>,
    pub output: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SchemaSubPlanEntry {
    pub local_id: u64,
    pub op: SchemaOp,
    pub guard: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum SchemaOp {
    // === io module ===
    IoReadFile { path: SchemaValue },
    IoWriteFile { path: SchemaValue, content: SchemaValue },
    IoAppendFile { path: SchemaValue, content: SchemaValue },
    IoDeleteFile { path: SchemaValue },
    IoFileExists { path: SchemaValue },
    IoIsDir { path: SchemaValue },
    IoIsFile { path: SchemaValue },
    IoMkdir { path: SchemaValue, recursive: bool },
    IoRmdir { path: SchemaValue, recursive: bool },
    IoListDir { path: SchemaValue },
    IoCopyFile { src: SchemaValue, dst: SchemaValue },
    IoMoveFile { src: SchemaValue, dst: SchemaValue },
    IoFileSize { path: SchemaValue },

    // === http module ===
    HttpRequest { method: SchemaValue, url: SchemaValue, body: SchemaValue, headers: SchemaValue },

    // === exec module ===
    ExecRun { command: SchemaValue, args: SchemaValue },
    ExecShell { command: SchemaValue },
    ExecEnv { name: SchemaValue, default: SchemaValue },

    // === tcp module ===
    TcpConnect { host: SchemaValue, port: SchemaValue },
    TcpListen { host: SchemaValue, port: SchemaValue },
    TcpSend { handle: SchemaValue, data: SchemaValue },
    TcpRecv { handle: SchemaValue, max_bytes: SchemaValue },
    TcpClose { handle: SchemaValue },
    TcpAccept { listener: SchemaValue },

    // === udp module ===
    UdpBind { host: SchemaValue, port: SchemaValue },
    UdpSendTo { handle: SchemaValue, data: SchemaValue, host: SchemaValue, port: SchemaValue },
    UdpRecvFrom { handle: SchemaValue, max_bytes: SchemaValue },
    UdpClose { handle: SchemaValue },

    // === unix module ===
    UnixConnect { path: SchemaValue },
    UnixListen { path: SchemaValue },
    UnixSend { handle: SchemaValue, data: SchemaValue },
    UnixRecv { handle: SchemaValue, max_bytes: SchemaValue },
    UnixClose { handle: SchemaValue },
    UnixAccept { listener: SchemaValue },

    // === json module ===
    JsonEncode { value: SchemaValue },
    JsonDecode { string: SchemaValue },

    // === bp module (utilities) ===
    BpSleep { seconds: SchemaValue },
    BpNow,
    BpPrint { message: SchemaValue },

    // === ops module (parallel/sync) ===
    OpsAll { ops: Vec<SchemaOpId> },
    OpsAny { ops: Vec<SchemaOpId> },
    OpsAtLeast { ops: Vec<SchemaOpId>, count: usize },
    OpsAtMost { ops: Vec<SchemaOpId>, count: usize },
    OpsAfter { dependency: SchemaOpId, value: SchemaOpId },

    // === Pure operations (arithmetic) ===
    Add { left: SchemaValue, right: SchemaValue },
    Sub { left: SchemaValue, right: SchemaValue },
    Mul { left: SchemaValue, right: SchemaValue },
    Div { left: SchemaValue, right: SchemaValue },
    FloorDiv { left: SchemaValue, right: SchemaValue },
    Mod { left: SchemaValue, right: SchemaValue },
    Neg { value: SchemaValue },

    // === Pure operations (comparisons) ===
    Eq { left: SchemaValue, right: SchemaValue },
    Ne { left: SchemaValue, right: SchemaValue },
    Lt { left: SchemaValue, right: SchemaValue },
    Le { left: SchemaValue, right: SchemaValue },
    Gt { left: SchemaValue, right: SchemaValue },
    Ge { left: SchemaValue, right: SchemaValue },

    // === Pure operations (logic) ===
    Not { value: SchemaValue },

    // === Pure operations (string/collection) ===
    Concat { left: SchemaValue, right: SchemaValue },
    Contains { haystack: SchemaValue, needle: SchemaValue },
    Len { value: SchemaValue },
    Index { base: SchemaValue, index: SchemaValue },
    SetIndex { base: SchemaValue, index: SchemaValue, value: SchemaValue },

    // === Pure operations (collection) ===
    Min { values: SchemaValue },
    Max { values: SchemaValue },
    Sum { values: SchemaValue, start: SchemaValue },
    Abs { value: SchemaValue },
    Sorted { values: SchemaValue },
    Reversed { values: SchemaValue },

    // === Type conversions ===
    ToBool { value: SchemaValue },
    ToInt { value: SchemaValue },
    ToFloat { value: SchemaValue },
    ToStr { value: SchemaValue },

    // === Control flow ===
    If { condition: SchemaValue, then_value: SchemaValue, else_value: SchemaValue },

    // === Runtime control flow ===
    ForEach {
        items: SchemaValue,
        item_name: String,
        body: SchemaSubPlan,
        parallel: bool,
    },

    /// Transform each item in a list (always parallel)
    Map {
        items: SchemaValue,
        item_name: String,
        body: SchemaSubPlan,
    },

    /// Filter items based on predicate (always parallel)
    Filter {
        items: SchemaValue,
        item_name: String,
        predicate: SchemaSubPlan,
    },

    IfBlock {
        condition: SchemaValue,
        then_body: SchemaSubPlan,
        else_body: Option<SchemaSubPlan>,
    },

    Break,

    Continue,

    /// Exports a global variable as a frozen value
    FrozenValue { name: String, value: SchemaValue },
}

impl SchemaOp {
    pub fn name(&self) -> &'static str {
        match self {
            SchemaOp::IoReadFile { .. } => "io.read_file",
            SchemaOp::IoWriteFile { .. } => "io.write_file",
            SchemaOp::IoAppendFile { .. } => "io.append_file",
            SchemaOp::IoDeleteFile { .. } => "io.delete_file",
            SchemaOp::IoFileExists { .. } => "io.file_exists",
            SchemaOp::IoIsDir { .. } => "io.is_dir",
            SchemaOp::IoIsFile { .. } => "io.is_file",
            SchemaOp::IoMkdir { .. } => "io.mkdir",
            SchemaOp::IoRmdir { .. } => "io.rmdir",
            SchemaOp::IoListDir { .. } => "io.list_dir",
            SchemaOp::IoCopyFile { .. } => "io.copy_file",
            SchemaOp::IoMoveFile { .. } => "io.move_file",
            SchemaOp::IoFileSize { .. } => "io.file_size",
            SchemaOp::HttpRequest { .. } => "http.request",
            SchemaOp::ExecRun { .. } => "exec.run",
            SchemaOp::ExecShell { .. } => "exec.shell",
            SchemaOp::ExecEnv { .. } => "exec.env",
            SchemaOp::TcpConnect { .. } => "tcp.connect",
            SchemaOp::TcpListen { .. } => "tcp.listen",
            SchemaOp::TcpSend { .. } => "tcp.send",
            SchemaOp::TcpRecv { .. } => "tcp.recv",
            SchemaOp::TcpClose { .. } => "tcp.close",
            SchemaOp::TcpAccept { .. } => "tcp.accept",
            SchemaOp::UdpBind { .. } => "udp.bind",
            SchemaOp::UdpSendTo { .. } => "udp.send_to",
            SchemaOp::UdpRecvFrom { .. } => "udp.recv_from",
            SchemaOp::UdpClose { .. } => "udp.close",
            SchemaOp::UnixConnect { .. } => "unix.connect",
            SchemaOp::UnixListen { .. } => "unix.listen",
            SchemaOp::UnixSend { .. } => "unix.send",
            SchemaOp::UnixRecv { .. } => "unix.recv",
            SchemaOp::UnixClose { .. } => "unix.close",
            SchemaOp::UnixAccept { .. } => "unix.accept",
            SchemaOp::JsonEncode { .. } => "json.encode",
            SchemaOp::JsonDecode { .. } => "json.decode",
            SchemaOp::BpSleep { .. } => "bp.sleep",
            SchemaOp::BpNow => "bp.now",
            SchemaOp::BpPrint { .. } => "bp.print",
            SchemaOp::OpsAll { .. } => "ops.all",
            SchemaOp::OpsAny { .. } => "ops.any",
            SchemaOp::OpsAtLeast { .. } => "ops.at_least",
            SchemaOp::OpsAtMost { .. } => "ops.at_most",
            SchemaOp::OpsAfter { .. } => "ops.after",
            SchemaOp::Add { .. } => "add",
            SchemaOp::Sub { .. } => "sub",
            SchemaOp::Mul { .. } => "mul",
            SchemaOp::Div { .. } => "div",
            SchemaOp::FloorDiv { .. } => "floordiv",
            SchemaOp::Mod { .. } => "mod",
            SchemaOp::Neg { .. } => "neg",
            SchemaOp::Eq { .. } => "eq",
            SchemaOp::Ne { .. } => "ne",
            SchemaOp::Lt { .. } => "lt",
            SchemaOp::Le { .. } => "le",
            SchemaOp::Gt { .. } => "gt",
            SchemaOp::Ge { .. } => "ge",
            SchemaOp::Not { .. } => "not",
            SchemaOp::Concat { .. } => "concat",
            SchemaOp::Contains { .. } => "contains",
            SchemaOp::Len { .. } => "len",
            SchemaOp::Index { .. } => "index",
            SchemaOp::SetIndex { .. } => "set_index",
            SchemaOp::Min { .. } => "min",
            SchemaOp::Max { .. } => "max",
            SchemaOp::Sum { .. } => "sum",
            SchemaOp::Abs { .. } => "abs",
            SchemaOp::Sorted { .. } => "sorted",
            SchemaOp::Reversed { .. } => "reversed",
            SchemaOp::ToBool { .. } => "bool",
            SchemaOp::ToInt { .. } => "int",
            SchemaOp::ToFloat { .. } => "float",
            SchemaOp::ToStr { .. } => "str",
            SchemaOp::If { .. } => "if",
            SchemaOp::ForEach { .. } => "foreach",
            SchemaOp::Map { .. } => "map",
            SchemaOp::Filter { .. } => "filter",
            SchemaOp::IfBlock { .. } => "if_block",
            SchemaOp::Break => "break",
            SchemaOp::Continue => "continue",
            SchemaOp::FrozenValue { .. } => "frozen_value",
        }
    }

    pub fn collect_value_refs(&self) -> Vec<&SchemaValue> {
        match self {
            SchemaOp::IoReadFile { path } => vec![path],
            SchemaOp::IoWriteFile { path, content } => vec![path, content],
            SchemaOp::IoAppendFile { path, content } => vec![path, content],
            SchemaOp::IoDeleteFile { path } => vec![path],
            SchemaOp::IoFileExists { path } => vec![path],
            SchemaOp::IoIsDir { path } => vec![path],
            SchemaOp::IoIsFile { path } => vec![path],
            SchemaOp::IoMkdir { path, .. } => vec![path],
            SchemaOp::IoRmdir { path, .. } => vec![path],
            SchemaOp::IoListDir { path } => vec![path],
            SchemaOp::IoCopyFile { src, dst } => vec![src, dst],
            SchemaOp::IoMoveFile { src, dst } => vec![src, dst],
            SchemaOp::IoFileSize { path } => vec![path],
            SchemaOp::HttpRequest { method, url, body, headers } => vec![method, url, body, headers],
            SchemaOp::ExecRun { command, args } => vec![command, args],
            SchemaOp::ExecShell { command } => vec![command],
            SchemaOp::ExecEnv { name, default } => vec![name, default],
            SchemaOp::TcpConnect { host, port } => vec![host, port],
            SchemaOp::TcpListen { host, port } => vec![host, port],
            SchemaOp::TcpSend { handle, data } => vec![handle, data],
            SchemaOp::TcpRecv { handle, max_bytes } => vec![handle, max_bytes],
            SchemaOp::TcpClose { handle } => vec![handle],
            SchemaOp::TcpAccept { listener } => vec![listener],
            SchemaOp::UdpBind { host, port } => vec![host, port],
            SchemaOp::UdpSendTo { handle, data, host, port } => vec![handle, data, host, port],
            SchemaOp::UdpRecvFrom { handle, max_bytes } => vec![handle, max_bytes],
            SchemaOp::UdpClose { handle } => vec![handle],
            SchemaOp::UnixConnect { path } => vec![path],
            SchemaOp::UnixListen { path } => vec![path],
            SchemaOp::UnixSend { handle, data } => vec![handle, data],
            SchemaOp::UnixRecv { handle, max_bytes } => vec![handle, max_bytes],
            SchemaOp::UnixClose { handle } => vec![handle],
            SchemaOp::UnixAccept { listener } => vec![listener],
            SchemaOp::JsonEncode { value } => vec![value],
            SchemaOp::JsonDecode { string } => vec![string],
            SchemaOp::BpSleep { seconds } => vec![seconds],
            SchemaOp::BpNow => vec![],
            SchemaOp::BpPrint { message } => vec![message],
            SchemaOp::OpsAll { .. } | SchemaOp::OpsAny { .. } | SchemaOp::OpsAtLeast { .. } | SchemaOp::OpsAtMost { .. } | SchemaOp::OpsAfter { .. } => vec![],
            SchemaOp::Add { left, right } => vec![left, right],
            SchemaOp::Sub { left, right } => vec![left, right],
            SchemaOp::Mul { left, right } => vec![left, right],
            SchemaOp::Div { left, right } => vec![left, right],
            SchemaOp::FloorDiv { left, right } => vec![left, right],
            SchemaOp::Mod { left, right } => vec![left, right],
            SchemaOp::Neg { value } => vec![value],
            SchemaOp::Eq { left, right } => vec![left, right],
            SchemaOp::Ne { left, right } => vec![left, right],
            SchemaOp::Lt { left, right } => vec![left, right],
            SchemaOp::Le { left, right } => vec![left, right],
            SchemaOp::Gt { left, right } => vec![left, right],
            SchemaOp::Ge { left, right } => vec![left, right],
            SchemaOp::Not { value } => vec![value],
            SchemaOp::Concat { left, right } => vec![left, right],
            SchemaOp::Contains { haystack, needle } => vec![haystack, needle],
            SchemaOp::Len { value } => vec![value],
            SchemaOp::Index { base, index } => vec![base, index],
            SchemaOp::SetIndex { base, index, value } => vec![base, index, value],
            SchemaOp::Min { values } => vec![values],
            SchemaOp::Max { values } => vec![values],
            SchemaOp::Sum { values, start } => vec![values, start],
            SchemaOp::Abs { value } => vec![value],
            SchemaOp::Sorted { values } => vec![values],
            SchemaOp::Reversed { values } => vec![values],
            SchemaOp::ToBool { value } => vec![value],
            SchemaOp::ToInt { value } => vec![value],
            SchemaOp::ToFloat { value } => vec![value],
            SchemaOp::ToStr { value } => vec![value],
            SchemaOp::If { condition, then_value, else_value } => vec![condition, then_value, else_value],
            SchemaOp::ForEach { items, .. } => vec![items],
            SchemaOp::Map { items, .. } => vec![items],
            SchemaOp::Filter { items, .. } => vec![items],
            SchemaOp::IfBlock { condition, .. } => vec![condition],
            SchemaOp::Break | SchemaOp::Continue => vec![],
            SchemaOp::FrozenValue { value, .. } => vec![value],
        }
    }

    pub fn collect_op_refs(&self) -> Vec<SchemaOpId> {
        match self {
            SchemaOp::OpsAll { ops } | SchemaOp::OpsAny { ops } | SchemaOp::OpsAtLeast { ops, .. } | SchemaOp::OpsAtMost { ops, .. } => ops.clone(),
            SchemaOp::OpsAfter { dependency, value } => vec![*dependency, *value],
            _ => vec![],
        }
    }

    pub fn requires_approval(&self) -> bool {
        matches!(
            self,
            SchemaOp::IoReadFile { .. }
            | SchemaOp::IoWriteFile { .. }
            | SchemaOp::IoAppendFile { .. }
            | SchemaOp::IoDeleteFile { .. }
            | SchemaOp::IoListDir { .. }
            | SchemaOp::IoMkdir { .. }
            | SchemaOp::IoRmdir { .. }
            | SchemaOp::IoCopyFile { .. }
            | SchemaOp::IoMoveFile { .. }
            | SchemaOp::HttpRequest { .. }
            | SchemaOp::ExecRun { .. }
            | SchemaOp::ExecShell { .. }
            | SchemaOp::TcpConnect { .. }
            | SchemaOp::TcpListen { .. }
            | SchemaOp::UdpBind { .. }
            | SchemaOp::UdpSendTo { .. }
            | SchemaOp::UnixConnect { .. }
            | SchemaOp::UnixListen { .. }
        )
    }

    pub fn to_text_fields(&self) -> Vec<(&'static str, String)> {
        match self {
            SchemaOp::IoReadFile { path } => vec![("path", path.to_text())],
            SchemaOp::IoWriteFile { path, content } => vec![("path", path.to_text()), ("content", content.to_text())],
            SchemaOp::IoAppendFile { path, content } => vec![("path", path.to_text()), ("content", content.to_text())],
            SchemaOp::IoDeleteFile { path } => vec![("path", path.to_text())],
            SchemaOp::IoFileExists { path } => vec![("path", path.to_text())],
            SchemaOp::IoIsDir { path } => vec![("path", path.to_text())],
            SchemaOp::IoIsFile { path } => vec![("path", path.to_text())],
            SchemaOp::IoMkdir { path, recursive } => vec![("path", path.to_text()), ("recursive", recursive.to_string())],
            SchemaOp::IoRmdir { path, recursive } => vec![("path", path.to_text()), ("recursive", recursive.to_string())],
            SchemaOp::IoListDir { path } => vec![("path", path.to_text())],
            SchemaOp::IoCopyFile { src, dst } => vec![("src", src.to_text()), ("dst", dst.to_text())],
            SchemaOp::IoMoveFile { src, dst } => vec![("src", src.to_text()), ("dst", dst.to_text())],
            SchemaOp::IoFileSize { path } => vec![("path", path.to_text())],
            SchemaOp::HttpRequest { method, url, body, headers } => vec![
                ("method", method.to_text()), ("url", url.to_text()), ("body", body.to_text()), ("headers", headers.to_text())
            ],
            SchemaOp::ExecRun { command, args } => vec![("command", command.to_text()), ("args", args.to_text())],
            SchemaOp::ExecShell { command } => vec![("command", command.to_text())],
            SchemaOp::ExecEnv { name, default } => vec![("name", name.to_text()), ("default", default.to_text())],
            SchemaOp::TcpConnect { host, port } => vec![("host", host.to_text()), ("port", port.to_text())],
            SchemaOp::TcpListen { host, port } => vec![("host", host.to_text()), ("port", port.to_text())],
            SchemaOp::TcpSend { handle, data } => vec![("handle", handle.to_text()), ("data", data.to_text())],
            SchemaOp::TcpRecv { handle, max_bytes } => vec![("handle", handle.to_text()), ("max_bytes", max_bytes.to_text())],
            SchemaOp::TcpClose { handle } => vec![("handle", handle.to_text())],
            SchemaOp::TcpAccept { listener } => vec![("listener", listener.to_text())],
            SchemaOp::UdpBind { host, port } => vec![("host", host.to_text()), ("port", port.to_text())],
            SchemaOp::UdpSendTo { handle, data, host, port } => vec![
                ("handle", handle.to_text()), ("data", data.to_text()), ("host", host.to_text()), ("port", port.to_text())
            ],
            SchemaOp::UdpRecvFrom { handle, max_bytes } => vec![("handle", handle.to_text()), ("max_bytes", max_bytes.to_text())],
            SchemaOp::UdpClose { handle } => vec![("handle", handle.to_text())],
            SchemaOp::UnixConnect { path } => vec![("path", path.to_text())],
            SchemaOp::UnixListen { path } => vec![("path", path.to_text())],
            SchemaOp::UnixSend { handle, data } => vec![("handle", handle.to_text()), ("data", data.to_text())],
            SchemaOp::UnixRecv { handle, max_bytes } => vec![("handle", handle.to_text()), ("max_bytes", max_bytes.to_text())],
            SchemaOp::UnixClose { handle } => vec![("handle", handle.to_text())],
            SchemaOp::UnixAccept { listener } => vec![("listener", listener.to_text())],
            SchemaOp::JsonEncode { value } => vec![("value", value.to_text())],
            SchemaOp::JsonDecode { string } => vec![("string", string.to_text())],
            SchemaOp::BpSleep { seconds } => vec![("seconds", seconds.to_text())],
            SchemaOp::BpNow => vec![],
            SchemaOp::BpPrint { message } => vec![("message", message.to_text())],
            SchemaOp::OpsAll { ops } => vec![("ops", format!("{:?}", ops.iter().map(|o| format!("@{}", o.0)).collect::<Vec<_>>()))],
            SchemaOp::OpsAny { ops } => vec![("ops", format!("{:?}", ops.iter().map(|o| format!("@{}", o.0)).collect::<Vec<_>>()))],
            SchemaOp::OpsAtLeast { ops, count } => vec![
                ("ops", format!("{:?}", ops.iter().map(|o| format!("@{}", o.0)).collect::<Vec<_>>())),
                ("count", count.to_string()),
            ],
            SchemaOp::OpsAtMost { ops, count } => vec![
                ("ops", format!("{:?}", ops.iter().map(|o| format!("@{}", o.0)).collect::<Vec<_>>())),
                ("count", count.to_string()),
            ],
            SchemaOp::OpsAfter { dependency, value } => vec![
                ("dependency", format!("@{}", dependency.0)),
                ("value", format!("@{}", value.0)),
            ],
            SchemaOp::Add { left, right } => vec![("left", left.to_text()), ("right", right.to_text())],
            SchemaOp::Sub { left, right } => vec![("left", left.to_text()), ("right", right.to_text())],
            SchemaOp::Mul { left, right } => vec![("left", left.to_text()), ("right", right.to_text())],
            SchemaOp::Div { left, right } => vec![("left", left.to_text()), ("right", right.to_text())],
            SchemaOp::FloorDiv { left, right } => vec![("left", left.to_text()), ("right", right.to_text())],
            SchemaOp::Mod { left, right } => vec![("left", left.to_text()), ("right", right.to_text())],
            SchemaOp::Neg { value } => vec![("value", value.to_text())],
            SchemaOp::Eq { left, right } => vec![("left", left.to_text()), ("right", right.to_text())],
            SchemaOp::Ne { left, right } => vec![("left", left.to_text()), ("right", right.to_text())],
            SchemaOp::Lt { left, right } => vec![("left", left.to_text()), ("right", right.to_text())],
            SchemaOp::Le { left, right } => vec![("left", left.to_text()), ("right", right.to_text())],
            SchemaOp::Gt { left, right } => vec![("left", left.to_text()), ("right", right.to_text())],
            SchemaOp::Ge { left, right } => vec![("left", left.to_text()), ("right", right.to_text())],
            SchemaOp::Not { value } => vec![("value", value.to_text())],
            SchemaOp::Concat { left, right } => vec![("left", left.to_text()), ("right", right.to_text())],
            SchemaOp::Contains { haystack, needle } => vec![("haystack", haystack.to_text()), ("needle", needle.to_text())],
            SchemaOp::Len { value } => vec![("value", value.to_text())],
            SchemaOp::Index { base, index } => vec![("base", base.to_text()), ("index", index.to_text())],
            SchemaOp::SetIndex { base, index, value } => vec![("base", base.to_text()), ("index", index.to_text()), ("value", value.to_text())],
            SchemaOp::Min { values } => vec![("values", values.to_text())],
            SchemaOp::Max { values } => vec![("values", values.to_text())],
            SchemaOp::Sum { values, start } => vec![("values", values.to_text()), ("start", start.to_text())],
            SchemaOp::Abs { value } => vec![("value", value.to_text())],
            SchemaOp::Sorted { values } => vec![("values", values.to_text())],
            SchemaOp::Reversed { values } => vec![("values", values.to_text())],
            SchemaOp::ToBool { value } => vec![("value", value.to_text())],
            SchemaOp::ToInt { value } => vec![("value", value.to_text())],
            SchemaOp::ToFloat { value } => vec![("value", value.to_text())],
            SchemaOp::ToStr { value } => vec![("value", value.to_text())],
            SchemaOp::If { condition, then_value, else_value } => vec![
                ("condition", condition.to_text()),
                ("then", then_value.to_text()),
                ("else", else_value.to_text()),
            ],
            SchemaOp::ForEach { items, item_name, parallel, .. } => vec![
                ("items", items.to_text()),
                ("item", item_name.clone()),
                ("parallel", parallel.to_string()),
                ("body", "<subplan>".to_string()),
            ],
            SchemaOp::Map { items, item_name, .. } => vec![
                ("items", items.to_text()),
                ("item", item_name.clone()),
                ("body", "<subplan>".to_string()),
            ],
            SchemaOp::Filter { items, item_name, .. } => vec![
                ("items", items.to_text()),
                ("item", item_name.clone()),
                ("predicate", "<subplan>".to_string()),
            ],
            SchemaOp::IfBlock { condition, else_body, .. } => {
                let mut fields = vec![
                    ("condition", condition.to_text()),
                    ("then", "<subplan>".to_string()),
                ];
                if else_body.is_some() {
                    fields.push(("else", "<subplan>".to_string()));
                }
                fields
            }
            SchemaOp::Break => vec![],
            SchemaOp::Continue => vec![],
            SchemaOp::FrozenValue { name, value } => vec![("name", name.clone()), ("value", value.to_text())],
        }
    }
}

impl std::fmt::Display for SchemaOp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SchemaOp::IoReadFile { path } => write!(f, "io.read_file({})", path),
            SchemaOp::IoWriteFile { path, content } => write!(f, "io.write_file({}, {})", path, content),
            SchemaOp::IoAppendFile { path, content } => write!(f, "io.append_file({}, {})", path, content),
            SchemaOp::IoDeleteFile { path } => write!(f, "io.delete_file({})", path),
            SchemaOp::IoFileExists { path } => write!(f, "io.file_exists({})", path),
            SchemaOp::IoIsDir { path } => write!(f, "io.is_dir({})", path),
            SchemaOp::IoIsFile { path } => write!(f, "io.is_file({})", path),
            SchemaOp::IoMkdir { path, recursive } => write!(f, "io.mkdir({}, recursive={})", path, recursive),
            SchemaOp::IoRmdir { path, recursive } => write!(f, "io.rmdir({}, recursive={})", path, recursive),
            SchemaOp::IoListDir { path } => write!(f, "io.list_dir({})", path),
            SchemaOp::IoCopyFile { src, dst } => write!(f, "io.copy_file({}, {})", src, dst),
            SchemaOp::IoMoveFile { src, dst } => write!(f, "io.move_file({}, {})", src, dst),
            SchemaOp::IoFileSize { path } => write!(f, "io.file_size({})", path),
            SchemaOp::HttpRequest { method, url, .. } => write!(f, "http.request({}, {})", method, url),
            SchemaOp::ExecRun { command, .. } => write!(f, "exec.run({})", command),
            SchemaOp::ExecShell { command } => write!(f, "exec.shell({})", command),
            SchemaOp::ExecEnv { name, .. } => write!(f, "exec.env({})", name),
            SchemaOp::TcpConnect { host, port } => write!(f, "tcp.connect({}:{})", host, port),
            SchemaOp::TcpListen { host, port } => write!(f, "tcp.listen({}:{})", host, port),
            SchemaOp::TcpSend { handle, .. } => write!(f, "tcp.send({})", handle),
            SchemaOp::TcpRecv { handle, .. } => write!(f, "tcp.recv({})", handle),
            SchemaOp::TcpClose { handle } => write!(f, "tcp.close({})", handle),
            SchemaOp::TcpAccept { listener } => write!(f, "tcp.accept({})", listener),
            SchemaOp::UdpBind { host, port } => write!(f, "udp.bind({}:{})", host, port),
            SchemaOp::UdpSendTo { host, port, .. } => write!(f, "udp.send_to({}:{})", host, port),
            SchemaOp::UdpRecvFrom { handle, .. } => write!(f, "udp.recv_from({})", handle),
            SchemaOp::UdpClose { handle } => write!(f, "udp.close({})", handle),
            SchemaOp::UnixConnect { path } => write!(f, "unix.connect({})", path),
            SchemaOp::UnixListen { path } => write!(f, "unix.listen({})", path),
            SchemaOp::UnixSend { handle, .. } => write!(f, "unix.send({})", handle),
            SchemaOp::UnixRecv { handle, .. } => write!(f, "unix.recv({})", handle),
            SchemaOp::UnixClose { handle } => write!(f, "unix.close({})", handle),
            SchemaOp::UnixAccept { listener } => write!(f, "unix.accept({})", listener),
            SchemaOp::JsonEncode { value } => write!(f, "json.encode({})", value),
            SchemaOp::JsonDecode { string } => write!(f, "json.decode({})", string),
            SchemaOp::BpSleep { seconds } => write!(f, "bp.sleep({})", seconds),
            SchemaOp::BpNow => write!(f, "bp.now()"),
            SchemaOp::BpPrint { message } => write!(f, "bp.print({})", message),
            SchemaOp::OpsAll { ops } => write!(f, "ops.all({:?})", ops.iter().map(|o| o.0).collect::<Vec<_>>()),
            SchemaOp::OpsAny { ops } => write!(f, "ops.any({:?})", ops.iter().map(|o| o.0).collect::<Vec<_>>()),
            SchemaOp::OpsAtLeast { ops, count } => write!(f, "ops.at_least({}, {:?})", count, ops.iter().map(|o| o.0).collect::<Vec<_>>()),
            SchemaOp::OpsAtMost { ops, count } => write!(f, "ops.at_most({}, {:?})", count, ops.iter().map(|o| o.0).collect::<Vec<_>>()),
            SchemaOp::OpsAfter { dependency, value } => write!(f, "ops.after({}, {})", dependency.0, value.0),
            SchemaOp::Add { left, right } => write!(f, "{} + {}", left, right),
            SchemaOp::Sub { left, right } => write!(f, "{} - {}", left, right),
            SchemaOp::Mul { left, right } => write!(f, "{} * {}", left, right),
            SchemaOp::Div { left, right } => write!(f, "{} / {}", left, right),
            SchemaOp::FloorDiv { left, right } => write!(f, "{} // {}", left, right),
            SchemaOp::Mod { left, right } => write!(f, "{} % {}", left, right),
            SchemaOp::Neg { value } => write!(f, "-{}", value),
            SchemaOp::Eq { left, right } => write!(f, "{} == {}", left, right),
            SchemaOp::Ne { left, right } => write!(f, "{} != {}", left, right),
            SchemaOp::Lt { left, right } => write!(f, "{} < {}", left, right),
            SchemaOp::Le { left, right } => write!(f, "{} <= {}", left, right),
            SchemaOp::Gt { left, right } => write!(f, "{} > {}", left, right),
            SchemaOp::Ge { left, right } => write!(f, "{} >= {}", left, right),
            SchemaOp::Not { value } => write!(f, "not {}", value),
            SchemaOp::Concat { left, right } => write!(f, "concat({}, {})", left, right),
            SchemaOp::Contains { haystack, needle } => write!(f, "{} in {}", needle, haystack),
            SchemaOp::Len { value } => write!(f, "len({})", value),
            SchemaOp::Index { base, index } => write!(f, "{}[{}]", base, index),
            SchemaOp::SetIndex { base, index, value } => write!(f, "{}[{}] = {}", base, index, value),
            SchemaOp::Min { values } => write!(f, "min({})", values),
            SchemaOp::Max { values } => write!(f, "max({})", values),
            SchemaOp::Sum { values, start } => write!(f, "sum({}, {})", values, start),
            SchemaOp::Abs { value } => write!(f, "abs({})", value),
            SchemaOp::Sorted { values } => write!(f, "sorted({})", values),
            SchemaOp::Reversed { values } => write!(f, "reversed({})", values),
            SchemaOp::ToBool { value } => write!(f, "bool({})", value),
            SchemaOp::ToInt { value } => write!(f, "int({})", value),
            SchemaOp::ToFloat { value } => write!(f, "float({})", value),
            SchemaOp::ToStr { value } => write!(f, "str({})", value),
            SchemaOp::If { condition, then_value, else_value } => write!(f, "{} if {} else {}", then_value, condition, else_value),
            SchemaOp::ForEach { items, item_name, parallel, .. } => {
                let mode = if *parallel { "parallel" } else { "sequential" };
                write!(f, "foreach {} in {} ({}) {{ ... }}", item_name, items, mode)
            }
            SchemaOp::Map { items, item_name, .. } => {
                write!(f, "map({} => ..., {})", item_name, items)
            }
            SchemaOp::Filter { items, item_name, .. } => {
                write!(f, "filter({} => ..., {})", item_name, items)
            }
            SchemaOp::IfBlock { condition, else_body, .. } => {
                if else_body.is_some() {
                    write!(f, "if {} {{ ... }} else {{ ... }}", condition)
                } else {
                    write!(f, "if {} {{ ... }}", condition)
                }
            }
            SchemaOp::Break => write!(f, "break"),
            SchemaOp::Continue => write!(f, "continue"),
            SchemaOp::FrozenValue { name, value } => write!(f, "frozen({} = {})", name, value),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaEntry {
    pub id: SchemaOpId,
    pub op: SchemaOp,
    pub inputs: Vec<SchemaOpId>,
    pub source_location: Option<SourceSpan>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Schema {
    pub entries: Vec<SchemaEntry>,
    pub next_id: u64,
}

impl Schema {
    pub fn new() -> Self {
        Self {
            entries: Vec::new(),
            next_id: 0,
        }
    }

    pub fn add_op(&mut self, op: SchemaOp, source_location: Option<SourceSpan>) -> SchemaOpId {
        let id = SchemaOpId(self.next_id);
        self.next_id += 1;

        let inputs = self.compute_inputs(&op);

        let entry = SchemaEntry {
            id,
            op,
            inputs,
            source_location,
        };

        self.entries.push(entry);
        id
    }

    fn compute_inputs(&self, op: &SchemaOp) -> Vec<SchemaOpId> {
        let mut inputs = Vec::new();

        for value_ref in op.collect_value_refs() {
            if let Some(op_id) = value_ref.referenced_op() {
                if !inputs.contains(&op_id) {
                    inputs.push(op_id);
                }
            }
        }

        for op_id in op.collect_op_refs() {
            if !inputs.contains(&op_id) {
                inputs.push(op_id);
            }
        }

        inputs
    }

    pub fn get(&self, id: SchemaOpId) -> Option<&SchemaEntry> {
        self.entries.iter().find(|e| e.id == id)
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn entries(&self) -> impl Iterator<Item = &SchemaEntry> {
        self.entries.iter()
    }

    pub fn required_env_vars(&self) -> Vec<String> {
        let mut vars = Vec::new();
        for entry in &self.entries {
            for value in entry.op.collect_value_refs() {
                if let SchemaValue::EnvRef(name) = value {
                    if !vars.contains(name) {
                        vars.push(name.clone());
                    }
                }
            }
        }
        vars
    }

    pub fn required_config_keys(&self) -> Vec<String> {
        let mut keys = Vec::new();
        for entry in &self.entries {
            for value in entry.op.collect_value_refs() {
                if let SchemaValue::ConfigRef(key) = value {
                    if !keys.contains(key) {
                        keys.push(key.clone());
                    }
                }
            }
        }
        keys
    }

    pub fn display(&self) -> String {
        let mut output = String::new();
        output.push_str("=== Blueprint Schema ===\n\n");

        for entry in &self.entries {
            let approval = if entry.op.requires_approval() { " [requires approval]" } else { "" };
            output.push_str(&format!("[{}] {}{}\n", entry.id.0, entry.op, approval));
            if !entry.inputs.is_empty() {
                output.push_str(&format!("    deps: {:?}\n", entry.inputs.iter().map(|i| i.0).collect::<Vec<_>>()));
            }
        }

        let env_vars = self.required_env_vars();
        if !env_vars.is_empty() {
            output.push_str(&format!("\nRequired env vars: {:?}\n", env_vars));
        }

        let config_keys = self.required_config_keys();
        if !config_keys.is_empty() {
            output.push_str(&format!("Required config keys: {:?}\n", config_keys));
        }

        output
    }

    pub fn export_json(&self) -> serde_json::Value {
        serde_json::json!({
            "entries": self.entries.iter().map(|e| {
                serde_json::json!({
                    "id": e.id.0,
                    "op": e.op.name(),
                    "op_display": e.op.to_string(),
                    "inputs": e.inputs.iter().map(|i| i.0).collect::<Vec<_>>(),
                    "requires_approval": e.op.requires_approval(),
                })
            }).collect::<Vec<_>>(),
            "operation_count": self.entries.len(),
            "required_env_vars": self.required_env_vars(),
            "required_config_keys": self.required_config_keys(),
        })
    }

    pub fn to_text(&self) -> String {
        let mut out = String::new();
        out.push_str(".section schema\n\n");

        for entry in &self.entries {
            let mut comments = Vec::new();
            if entry.op.requires_approval() {
                comments.push("[approval]".to_string());
            }
            if !entry.inputs.is_empty() {
                let deps: Vec<String> = entry.inputs.iter().map(|i| format!("@{}", i.0)).collect();
                comments.push(format!("after {}", deps.join(", ")));
            }

            let comment = if comments.is_empty() {
                String::new()
            } else {
                format!("  ; {}", comments.join(" "))
            };

            out.push_str(&format!("@{}: {}{}\n", entry.id.0, entry.op.name(), comment));

            for (name, value) in entry.op.to_text_fields() {
                out.push_str(&format!("    {:12} = {}\n", name, value));
            }
            out.push('\n');
        }

        out.push_str(".section summary\n");
        out.push_str(&format!("    total_ops       = {}\n", self.len()));
        let approval_count = self.entries.iter().filter(|e| e.op.requires_approval()).count();
        out.push_str(&format!("    approval_needed = {}\n", approval_count));

        out
    }
}

impl Default for Schema {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaMetadata {
    pub source_file: Option<String>,
    pub source_content: Option<String>,
    pub required_env: Vec<String>,
    pub required_config: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompiledSchema {
    magic: [u8; 4],
    schema_version: u32,
    source_hash: String,
    compiled_at: u64,
    schema: Schema,
    metadata: Option<SchemaMetadata>,
}

impl CompiledSchema {
    pub fn new(schema: Schema, source_hash: String, metadata: Option<SchemaMetadata>) -> Self {
        let compiled_at = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        Self {
            magic: *b"BS\x00\x01",
            schema_version: SCHEMA_VERSION,
            source_hash,
            compiled_at,
            schema,
            metadata,
        }
    }

    pub fn schema(&self) -> &Schema {
        &self.schema
    }

    pub fn schema_version(&self) -> u32 {
        self.schema_version
    }

    pub fn source_hash(&self) -> &str {
        &self.source_hash
    }

    pub fn compiled_at(&self) -> u64 {
        self.compiled_at
    }

    pub fn metadata(&self) -> Option<&SchemaMetadata> {
        self.metadata.as_ref()
    }

    pub fn save(&self, path: &std::path::Path) -> Result<(), CompiledSchemaError> {
        let encoded = bincode::serialize(self)
            .map_err(|e| CompiledSchemaError::SerializationError(e.to_string()))?;
        std::fs::write(path, encoded)
            .map_err(|e| CompiledSchemaError::IoError(e.to_string()))?;
        Ok(())
    }

    pub fn load(path: &std::path::Path) -> Result<Self, CompiledSchemaError> {
        let data = std::fs::read(path)
            .map_err(|e| CompiledSchemaError::IoError(e.to_string()))?;

        if data.len() < 4 {
            return Err(CompiledSchemaError::InvalidFormat("File too small".to_string()));
        }

        let compiled: CompiledSchema = bincode::deserialize(&data)
            .map_err(|e| CompiledSchemaError::DeserializationError(e.to_string()))?;

        if &compiled.magic != b"BS\x00\x01" {
            return Err(CompiledSchemaError::InvalidFormat("Invalid magic bytes".to_string()));
        }

        if compiled.schema_version != SCHEMA_VERSION {
            return Err(CompiledSchemaError::VersionMismatch {
                expected: SCHEMA_VERSION,
                found: compiled.schema_version,
            });
        }

        Ok(compiled)
    }

    pub fn to_text(&self) -> String {
        let mut out = String::new();
        out.push_str("; Blueprint Schema\n");
        if let Some(meta) = &self.metadata {
            if let Some(src) = &meta.source_file {
                out.push_str(&format!("; Source: {}\n", src));
            }
        }
        out.push_str(&format!("; Hash: {}\n", self.source_hash));
        out.push_str(&format!("; Version: {}\n", self.schema_version));
        out.push_str(&format!("; Operations: {}\n\n", self.schema.len()));

        out.push_str(&self.schema.to_text());

        let env_vars = self.schema.required_env_vars();
        out.push_str("\n.section env\n");
        if env_vars.is_empty() {
            out.push_str("    ; (none)\n");
        } else {
            for var in env_vars {
                out.push_str(&format!("    {}\n", var));
            }
        }

        let config_keys = self.schema.required_config_keys();
        out.push_str("\n.section config\n");
        if config_keys.is_empty() {
            out.push_str("    ; (none)\n");
        } else {
            for key in config_keys {
                out.push_str(&format!("    {}\n", key));
            }
        }

        out
    }
}

#[derive(Debug, Clone)]
pub enum CompiledSchemaError {
    IoError(String),
    SerializationError(String),
    DeserializationError(String),
    InvalidFormat(String),
    VersionMismatch { expected: u32, found: u32 },
}

impl std::fmt::Display for CompiledSchemaError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CompiledSchemaError::IoError(e) => write!(f, "IO error: {}", e),
            CompiledSchemaError::SerializationError(e) => write!(f, "Serialization error: {}", e),
            CompiledSchemaError::DeserializationError(e) => write!(f, "Deserialization error: {}", e),
            CompiledSchemaError::InvalidFormat(e) => write!(f, "Invalid format: {}", e),
            CompiledSchemaError::VersionMismatch { expected, found } => {
                write!(f, "Schema version mismatch: expected {}, found {}", expected, found)
            }
        }
    }
}

impl std::error::Error for CompiledSchemaError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_schema_creation() {
        let mut schema = Schema::new();

        let id1 = schema.add_op(
            SchemaOp::IoReadFile { path: SchemaValue::literal_string("config.json") },
            None,
        );

        let id2 = schema.add_op(
            SchemaOp::JsonDecode { string: SchemaValue::op_ref(id1) },
            None,
        );

        assert_eq!(schema.len(), 2);

        let entry2 = schema.get(id2).unwrap();
        assert!(entry2.inputs.contains(&id1));
    }

    #[test]
    fn test_env_ref_tracking() {
        let mut schema = Schema::new();

        schema.add_op(
            SchemaOp::IoReadFile { path: SchemaValue::env_ref("HOME") },
            None,
        );

        schema.add_op(
            SchemaOp::ExecEnv {
                name: SchemaValue::literal_string("PATH"),
                default: SchemaValue::literal_none(),
            },
            None,
        );

        let env_vars = schema.required_env_vars();
        assert!(env_vars.contains(&"HOME".to_string()));
    }

    #[test]
    fn test_config_ref_tracking() {
        let mut schema = Schema::new();

        schema.add_op(
            SchemaOp::IoWriteFile {
                path: SchemaValue::config_ref("output_dir"),
                content: SchemaValue::literal_string("hello"),
            },
            None,
        );

        let config_keys = schema.required_config_keys();
        assert!(config_keys.contains(&"output_dir".to_string()));
    }

    #[test]
    fn test_schema_display() {
        let mut schema = Schema::new();

        schema.add_op(
            SchemaOp::HttpRequest {
                method: SchemaValue::literal_string("GET"),
                url: SchemaValue::literal_string("https://example.com"),
                body: SchemaValue::literal_none(),
                headers: SchemaValue::literal_none(),
            },
            None,
        );

        let display = schema.display();
        assert!(display.contains("http.request"));
        assert!(display.contains("requires approval"));
    }

    #[test]
    fn test_compiled_schema_roundtrip() {
        let mut schema = Schema::new();
        schema.add_op(
            SchemaOp::BpNow,
            None,
        );

        let compiled = CompiledSchema::new(
            schema,
            "abc123".to_string(),
            None,
        );

        let temp_dir = tempfile::tempdir().unwrap();
        let path = temp_dir.path().join("test.bps");

        compiled.save(&path).unwrap();
        let loaded = CompiledSchema::load(&path).unwrap();

        assert_eq!(loaded.source_hash(), "abc123");
        assert_eq!(loaded.schema().len(), 1);
    }

    fn make_simple_schema_subplan() -> SchemaSubPlan {
        SchemaSubPlan {
            params: vec!["x".to_string()],
            entries: vec![SchemaSubPlanEntry {
                local_id: 0,
                op: SchemaOp::BpPrint {
                    message: SchemaValue::literal_string("test"),
                },
                guard: None,
            }],
            output: 0,
        }
    }

    fn make_guarded_schema_subplan() -> SchemaSubPlan {
        SchemaSubPlan {
            params: vec!["item".to_string()],
            entries: vec![
                SchemaSubPlanEntry {
                    local_id: 0,
                    op: SchemaOp::Contains {
                        haystack: SchemaValue::literal_string("hello world"),
                        needle: SchemaValue::literal_string("hello"),
                    },
                    guard: None,
                },
                SchemaSubPlanEntry {
                    local_id: 1,
                    op: SchemaOp::BpPrint {
                        message: SchemaValue::literal_string("found it"),
                    },
                    guard: Some(0),
                },
            ],
            output: 1,
        }
    }

    #[test]
    fn test_schema_subplan_creation() {
        let subplan = make_simple_schema_subplan();
        assert_eq!(subplan.params.len(), 1);
        assert_eq!(subplan.params[0], "x");
        assert_eq!(subplan.entries.len(), 1);
        assert_eq!(subplan.output, 0);
    }

    #[test]
    fn test_schema_subplan_with_guard() {
        let subplan = make_guarded_schema_subplan();
        assert_eq!(subplan.entries.len(), 2);
        assert_eq!(subplan.entries[0].guard, None);
        assert_eq!(subplan.entries[1].guard, Some(0));
    }

    #[test]
    fn test_schema_foreach_creation() {
        let foreach = SchemaOp::ForEach {
            items: SchemaValue::op_ref(SchemaOpId(0)),
            item_name: "item".to_string(),
            body: make_simple_schema_subplan(),
            parallel: false,
        };

        assert_eq!(foreach.name(), "foreach");
        assert!(!foreach.requires_approval());
    }

    #[test]
    fn test_schema_foreach_parallel() {
        let sequential = SchemaOp::ForEach {
            items: SchemaValue::op_ref(SchemaOpId(0)),
            item_name: "x".to_string(),
            body: make_simple_schema_subplan(),
            parallel: false,
        };

        let parallel = SchemaOp::ForEach {
            items: SchemaValue::op_ref(SchemaOpId(0)),
            item_name: "x".to_string(),
            body: make_simple_schema_subplan(),
            parallel: true,
        };

        let seq_display = format!("{}", sequential);
        let par_display = format!("{}", parallel);

        assert!(seq_display.contains("sequential"));
        assert!(par_display.contains("parallel"));
    }

    #[test]
    fn test_schema_foreach_collect_value_refs() {
        let foreach = SchemaOp::ForEach {
            items: SchemaValue::op_ref(SchemaOpId(5)),
            item_name: "x".to_string(),
            body: make_simple_schema_subplan(),
            parallel: false,
        };

        let refs = foreach.collect_value_refs();
        assert_eq!(refs.len(), 1);
        if let SchemaValue::OpRef { id, .. } = refs[0] {
            assert_eq!(id.0, 5);
        } else {
            panic!("Expected OpRef");
        }
    }

    #[test]
    fn test_schema_ifblock_creation() {
        let ifblock = SchemaOp::IfBlock {
            condition: SchemaValue::op_ref(SchemaOpId(0)),
            then_body: make_simple_schema_subplan(),
            else_body: None,
        };

        assert_eq!(ifblock.name(), "if_block");
        assert!(!ifblock.requires_approval());
    }

    #[test]
    fn test_schema_ifblock_with_else() {
        let ifblock = SchemaOp::IfBlock {
            condition: SchemaValue::literal_bool(true),
            then_body: make_simple_schema_subplan(),
            else_body: Some(make_simple_schema_subplan()),
        };

        let display = format!("{}", ifblock);
        assert!(display.contains("else"));
    }

    #[test]
    fn test_schema_ifblock_without_else() {
        let ifblock = SchemaOp::IfBlock {
            condition: SchemaValue::literal_bool(true),
            then_body: make_simple_schema_subplan(),
            else_body: None,
        };

        let display = format!("{}", ifblock);
        assert!(!display.contains("else"));
    }

    #[test]
    fn test_schema_ifblock_collect_value_refs() {
        let ifblock = SchemaOp::IfBlock {
            condition: SchemaValue::op_ref(SchemaOpId(3)),
            then_body: make_simple_schema_subplan(),
            else_body: None,
        };

        let refs = ifblock.collect_value_refs();
        assert_eq!(refs.len(), 1);
        if let SchemaValue::OpRef { id, .. } = refs[0] {
            assert_eq!(id.0, 3);
        } else {
            panic!("Expected OpRef");
        }
    }

    #[test]
    fn test_schema_break_creation() {
        let brk = SchemaOp::Break;
        assert_eq!(brk.name(), "break");
        assert!(!brk.requires_approval());
        assert_eq!(format!("{}", brk), "break");
    }

    #[test]
    fn test_schema_continue_creation() {
        let cont = SchemaOp::Continue;
        assert_eq!(cont.name(), "continue");
        assert!(!cont.requires_approval());
        assert_eq!(format!("{}", cont), "continue");
    }

    #[test]
    fn test_schema_break_continue_no_value_refs() {
        let brk = SchemaOp::Break;
        let cont = SchemaOp::Continue;

        assert!(brk.collect_value_refs().is_empty());
        assert!(cont.collect_value_refs().is_empty());
    }

    #[test]
    fn test_schema_foreach_to_text_fields() {
        let foreach = SchemaOp::ForEach {
            items: SchemaValue::op_ref(SchemaOpId(5)),
            item_name: "item".to_string(),
            body: make_simple_schema_subplan(),
            parallel: true,
        };

        let fields = foreach.to_text_fields();
        let field_names: Vec<_> = fields.iter().map(|(name, _)| *name).collect();

        assert!(field_names.contains(&"items"));
        assert!(field_names.contains(&"item"));
        assert!(field_names.contains(&"parallel"));
        assert!(field_names.contains(&"body"));
    }

    #[test]
    fn test_schema_ifblock_to_text_fields() {
        let ifblock_no_else = SchemaOp::IfBlock {
            condition: SchemaValue::op_ref(SchemaOpId(0)),
            then_body: make_simple_schema_subplan(),
            else_body: None,
        };

        let ifblock_with_else = SchemaOp::IfBlock {
            condition: SchemaValue::op_ref(SchemaOpId(0)),
            then_body: make_simple_schema_subplan(),
            else_body: Some(make_simple_schema_subplan()),
        };

        let fields_no_else = ifblock_no_else.to_text_fields();
        let fields_with_else = ifblock_with_else.to_text_fields();

        assert_eq!(fields_no_else.len(), 2);
        assert_eq!(fields_with_else.len(), 3);
    }

    #[test]
    fn test_schema_break_continue_to_text_fields() {
        let brk = SchemaOp::Break;
        let cont = SchemaOp::Continue;

        assert!(brk.to_text_fields().is_empty());
        assert!(cont.to_text_fields().is_empty());
    }

    #[test]
    fn test_schema_foreach_serialization_roundtrip() {
        let foreach = SchemaOp::ForEach {
            items: SchemaValue::op_ref(SchemaOpId(0)),
            item_name: "item".to_string(),
            body: make_simple_schema_subplan(),
            parallel: true,
        };

        let serialized = bincode::serialize(&foreach).expect("serialization failed");
        let deserialized: SchemaOp = bincode::deserialize(&serialized).expect("deserialization failed");

        assert_eq!(deserialized.name(), "foreach");
        if let SchemaOp::ForEach { item_name, parallel, .. } = deserialized {
            assert_eq!(item_name, "item");
            assert!(parallel);
        } else {
            panic!("Expected ForEach");
        }
    }

    #[test]
    fn test_schema_ifblock_serialization_roundtrip() {
        let ifblock = SchemaOp::IfBlock {
            condition: SchemaValue::literal_bool(true),
            then_body: make_simple_schema_subplan(),
            else_body: Some(make_simple_schema_subplan()),
        };

        let serialized = bincode::serialize(&ifblock).expect("serialization failed");
        let deserialized: SchemaOp = bincode::deserialize(&serialized).expect("deserialization failed");

        assert_eq!(deserialized.name(), "if_block");
        if let SchemaOp::IfBlock { else_body, .. } = deserialized {
            assert!(else_body.is_some());
        } else {
            panic!("Expected IfBlock");
        }
    }

    #[test]
    fn test_schema_break_continue_serialization_roundtrip() {
        let brk = SchemaOp::Break;
        let cont = SchemaOp::Continue;

        let brk_ser = bincode::serialize(&brk).expect("serialization failed");
        let cont_ser = bincode::serialize(&cont).expect("serialization failed");

        let brk_de: SchemaOp = bincode::deserialize(&brk_ser).expect("deserialization failed");
        let cont_de: SchemaOp = bincode::deserialize(&cont_ser).expect("deserialization failed");

        assert_eq!(brk_de.name(), "break");
        assert_eq!(cont_de.name(), "continue");
    }

    #[test]
    fn test_schema_subplan_serialization_roundtrip() {
        let subplan = make_guarded_schema_subplan();

        let serialized = bincode::serialize(&subplan).expect("serialization failed");
        let deserialized: SchemaSubPlan = bincode::deserialize(&serialized).expect("deserialization failed");

        assert_eq!(deserialized.params.len(), 1);
        assert_eq!(deserialized.entries.len(), 2);
        assert_eq!(deserialized.entries[1].guard, Some(0));
    }

    #[test]
    fn test_schema_with_foreach() {
        let mut schema = Schema::new();

        let read_id = schema.add_op(
            SchemaOp::IoReadFile {
                path: SchemaValue::literal_string("data.txt"),
            },
            None,
        );

        let foreach_id = schema.add_op(
            SchemaOp::ForEach {
                items: SchemaValue::op_ref(read_id),
                item_name: "line".to_string(),
                body: make_simple_schema_subplan(),
                parallel: false,
            },
            None,
        );

        assert_eq!(schema.len(), 2);
        let foreach_entry = schema.get(foreach_id).unwrap();
        assert!(foreach_entry.inputs.contains(&read_id));
    }

    #[test]
    fn test_schema_with_ifblock() {
        let mut schema = Schema::new();

        let cond_id = schema.add_op(
            SchemaOp::IoFileExists {
                path: SchemaValue::literal_string("test.txt"),
            },
            None,
        );

        let ifblock_id = schema.add_op(
            SchemaOp::IfBlock {
                condition: SchemaValue::op_ref(cond_id),
                then_body: make_simple_schema_subplan(),
                else_body: None,
            },
            None,
        );

        assert_eq!(schema.len(), 2);
        let ifblock_entry = schema.get(ifblock_id).unwrap();
        assert!(ifblock_entry.inputs.contains(&cond_id));
    }

    #[test]
    fn test_nested_foreach_in_schema() {
        let inner_body = SchemaSubPlan {
            params: vec!["y".to_string()],
            entries: vec![SchemaSubPlanEntry {
                local_id: 0,
                op: SchemaOp::BpPrint {
                    message: SchemaValue::literal_string("inner"),
                },
                guard: None,
            }],
            output: 0,
        };

        let inner_foreach = SchemaOp::ForEach {
            items: SchemaValue::literal_list(vec![
                RecordedValue::Int(1),
                RecordedValue::Int(2),
            ]),
            item_name: "y".to_string(),
            body: inner_body,
            parallel: false,
        };

        let outer_body = SchemaSubPlan {
            params: vec!["x".to_string()],
            entries: vec![SchemaSubPlanEntry {
                local_id: 0,
                op: inner_foreach,
                guard: None,
            }],
            output: 0,
        };

        let outer_foreach = SchemaOp::ForEach {
            items: SchemaValue::literal_list(vec![
                RecordedValue::String("a".to_string()),
                RecordedValue::String("b".to_string()),
            ]),
            item_name: "x".to_string(),
            body: outer_body,
            parallel: true,
        };

        let serialized = bincode::serialize(&outer_foreach).expect("serialization failed");
        let deserialized: SchemaOp = bincode::deserialize(&serialized).expect("deserialization failed");

        if let SchemaOp::ForEach { body, parallel, .. } = deserialized {
            assert!(parallel);
            if let SchemaOp::ForEach { item_name, .. } = &body.entries[0].op {
                assert_eq!(item_name, "y");
            } else {
                panic!("Expected nested ForEach");
            }
        } else {
            panic!("Expected ForEach");
        }
    }

    #[test]
    fn test_schema_control_flow_not_require_approval() {
        let foreach = SchemaOp::ForEach {
            items: SchemaValue::literal_list(vec![]),
            item_name: "x".to_string(),
            body: make_simple_schema_subplan(),
            parallel: false,
        };

        let ifblock = SchemaOp::IfBlock {
            condition: SchemaValue::literal_bool(true),
            then_body: make_simple_schema_subplan(),
            else_body: None,
        };

        let brk = SchemaOp::Break;
        let cont = SchemaOp::Continue;

        assert!(!foreach.requires_approval());
        assert!(!ifblock.requires_approval());
        assert!(!brk.requires_approval());
        assert!(!cont.requires_approval());
    }
}
