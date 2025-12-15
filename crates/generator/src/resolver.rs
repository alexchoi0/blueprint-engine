use anyhow::{anyhow, Result};
use blueprint_common::{
    ExecutionContext, Op, OpId, OpKind, Plan, RecordedValue, Schema, SchemaOp, SchemaOpId,
    SchemaSubPlan, SchemaValue, SubPlan, ValueRef,
};
use std::collections::HashMap;

pub struct PlanGenerator<'a> {
    context: &'a ExecutionContext,
}

#[derive(Debug, Clone)]
pub enum PlanGeneratorError {
    UnresolvedEnvVar(String),
    UnresolvedConfigKey(String),
    UnknownOpRef(SchemaOpId),
    InvalidValue(String),
}

impl std::fmt::Display for PlanGeneratorError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PlanGeneratorError::UnresolvedEnvVar(name) => {
                write!(f, "Environment variable '{}' not set", name)
            }
            PlanGeneratorError::UnresolvedConfigKey(key) => {
                write!(f, "Config key '{}' not defined", key)
            }
            PlanGeneratorError::UnknownOpRef(id) => {
                write!(f, "Unknown schema op reference: {}", id)
            }
            PlanGeneratorError::InvalidValue(msg) => {
                write!(f, "Invalid value: {}", msg)
            }
        }
    }
}

impl std::error::Error for PlanGeneratorError {}

impl<'a> PlanGenerator<'a> {
    pub fn new(context: &'a ExecutionContext) -> Self {
        Self { context }
    }

    pub fn generate(&self, schema: &Schema) -> Result<Plan> {
        let mut plan = Plan::new();
        let mut id_map: HashMap<SchemaOpId, OpId> = HashMap::new();

        for entry in schema.entries() {
            let op_kinds = self.resolve_op(&entry.op, &id_map)?;

            for (i, op_kind) in op_kinds.into_iter().enumerate() {
                let op_id = plan.add_op(op_kind, entry.source_location.clone());
                if i == 0 {
                    id_map.insert(entry.id, op_id);
                }
            }
        }

        Ok(plan)
    }

    fn resolve_op(
        &self,
        op: &SchemaOp,
        id_map: &HashMap<SchemaOpId, OpId>,
    ) -> Result<Vec<OpKind>> {
        match op {
            // === io module ===
            SchemaOp::IoReadFile { path } => Ok(vec![OpKind::ReadFile {
                path: self.resolve_value(path, id_map)?,
            }]),

            SchemaOp::IoWriteFile { path, content } => Ok(vec![OpKind::WriteFile {
                path: self.resolve_value(path, id_map)?,
                content: self.resolve_value(content, id_map)?,
            }]),

            SchemaOp::IoAppendFile { path, content } => Ok(vec![OpKind::AppendFile {
                path: self.resolve_value(path, id_map)?,
                content: self.resolve_value(content, id_map)?,
            }]),

            SchemaOp::IoDeleteFile { path } => Ok(vec![OpKind::DeleteFile {
                path: self.resolve_value(path, id_map)?,
            }]),

            SchemaOp::IoFileExists { path } => Ok(vec![OpKind::FileExists {
                path: self.resolve_value(path, id_map)?,
            }]),

            SchemaOp::IoIsDir { path } => Ok(vec![OpKind::IsDir {
                path: self.resolve_value(path, id_map)?,
            }]),

            SchemaOp::IoIsFile { path } => Ok(vec![OpKind::IsFile {
                path: self.resolve_value(path, id_map)?,
            }]),

            SchemaOp::IoMkdir { path, recursive } => Ok(vec![OpKind::Mkdir {
                path: self.resolve_value(path, id_map)?,
                recursive: *recursive,
            }]),

            SchemaOp::IoRmdir { path, recursive } => Ok(vec![OpKind::Rmdir {
                path: self.resolve_value(path, id_map)?,
                recursive: *recursive,
            }]),

            SchemaOp::IoListDir { path } => Ok(vec![OpKind::ListDir {
                path: self.resolve_value(path, id_map)?,
            }]),

            SchemaOp::IoCopyFile { src, dst } => Ok(vec![OpKind::CopyFile {
                src: self.resolve_value(src, id_map)?,
                dst: self.resolve_value(dst, id_map)?,
            }]),

            SchemaOp::IoMoveFile { src, dst } => Ok(vec![OpKind::MoveFile {
                src: self.resolve_value(src, id_map)?,
                dst: self.resolve_value(dst, id_map)?,
            }]),

            SchemaOp::IoFileSize { path } => Ok(vec![OpKind::FileSize {
                path: self.resolve_value(path, id_map)?,
            }]),

            // === http module ===
            SchemaOp::HttpRequest {
                method,
                url,
                body,
                headers,
            } => Ok(vec![OpKind::HttpRequest {
                method: self.resolve_value(method, id_map)?,
                url: self.resolve_value(url, id_map)?,
                headers: self.resolve_value(headers, id_map)?,
                body: self.resolve_value(body, id_map)?,
            }]),

            // === exec module ===
            SchemaOp::ExecRun { command, args } => Ok(vec![OpKind::Exec {
                command: self.resolve_value(command, id_map)?,
                args: self.resolve_value(args, id_map)?,
            }]),

            SchemaOp::ExecShell { command } => {
                let shell = if self.context.os == "windows" {
                    "cmd"
                } else {
                    "sh"
                };
                let shell_args = if self.context.os == "windows" {
                    vec![
                        RecordedValue::String("/c".to_string()),
                    ]
                } else {
                    vec![
                        RecordedValue::String("-c".to_string()),
                    ]
                };

                let cmd_value = self.resolve_value(command, id_map)?;

                match cmd_value {
                    ValueRef::Literal(RecordedValue::String(cmd)) => {
                        let mut args = shell_args;
                        args.push(RecordedValue::String(cmd));
                        Ok(vec![OpKind::Exec {
                            command: ValueRef::literal_string(shell),
                            args: ValueRef::Literal(RecordedValue::List(args)),
                        }])
                    }
                    ValueRef::OpOutput { op: _, path: _ } => {
                        Ok(vec![OpKind::Exec {
                            command: ValueRef::literal_string(shell),
                            args: ValueRef::Literal(RecordedValue::List(vec![
                                RecordedValue::String(if self.context.os == "windows" { "/c" } else { "-c" }.to_string()),
                            ])),
                        }])
                    }
                    _ => {
                        Ok(vec![OpKind::Exec {
                            command: ValueRef::literal_string(shell),
                            args: ValueRef::Literal(RecordedValue::List(shell_args)),
                        }])
                    }
                }
            }

            SchemaOp::ExecEnv { name, default } => Ok(vec![OpKind::EnvGet {
                name: self.resolve_value(name, id_map)?,
                default: self.resolve_value(default, id_map)?,
            }]),

            // === tcp module ===
            SchemaOp::TcpConnect { host, port } => Ok(vec![OpKind::TcpConnect {
                host: self.resolve_value(host, id_map)?,
                port: self.resolve_value(port, id_map)?,
            }]),

            SchemaOp::TcpListen { host, port } => Ok(vec![OpKind::TcpListen {
                host: self.resolve_value(host, id_map)?,
                port: self.resolve_value(port, id_map)?,
            }]),

            SchemaOp::TcpSend { handle, data } => Ok(vec![OpKind::TcpSend {
                handle: self.resolve_value(handle, id_map)?,
                data: self.resolve_value(data, id_map)?,
            }]),

            SchemaOp::TcpRecv { handle, max_bytes } => Ok(vec![OpKind::TcpRecv {
                handle: self.resolve_value(handle, id_map)?,
                max_bytes: self.resolve_value(max_bytes, id_map)?,
            }]),

            SchemaOp::TcpClose { handle } => Ok(vec![OpKind::TcpClose {
                handle: self.resolve_value(handle, id_map)?,
            }]),

            SchemaOp::TcpAccept { listener } => Ok(vec![OpKind::TcpAccept {
                listener: self.resolve_value(listener, id_map)?,
            }]),

            // === udp module ===
            SchemaOp::UdpBind { host, port } => Ok(vec![OpKind::UdpBind {
                host: self.resolve_value(host, id_map)?,
                port: self.resolve_value(port, id_map)?,
            }]),

            SchemaOp::UdpSendTo {
                handle,
                data,
                host,
                port,
            } => Ok(vec![OpKind::UdpSendTo {
                handle: self.resolve_value(handle, id_map)?,
                data: self.resolve_value(data, id_map)?,
                host: self.resolve_value(host, id_map)?,
                port: self.resolve_value(port, id_map)?,
            }]),

            SchemaOp::UdpRecvFrom { handle, max_bytes } => Ok(vec![OpKind::UdpRecvFrom {
                handle: self.resolve_value(handle, id_map)?,
                max_bytes: self.resolve_value(max_bytes, id_map)?,
            }]),

            SchemaOp::UdpClose { handle } => Ok(vec![OpKind::UdpClose {
                handle: self.resolve_value(handle, id_map)?,
            }]),

            // === unix module ===
            SchemaOp::UnixConnect { path } => Ok(vec![OpKind::UnixConnect {
                path: self.resolve_value(path, id_map)?,
            }]),

            SchemaOp::UnixListen { path } => Ok(vec![OpKind::UnixListen {
                path: self.resolve_value(path, id_map)?,
            }]),

            SchemaOp::UnixSend { handle, data } => Ok(vec![OpKind::UnixSend {
                handle: self.resolve_value(handle, id_map)?,
                data: self.resolve_value(data, id_map)?,
            }]),

            SchemaOp::UnixRecv { handle, max_bytes } => Ok(vec![OpKind::UnixRecv {
                handle: self.resolve_value(handle, id_map)?,
                max_bytes: self.resolve_value(max_bytes, id_map)?,
            }]),

            SchemaOp::UnixClose { handle } => Ok(vec![OpKind::UnixClose {
                handle: self.resolve_value(handle, id_map)?,
            }]),

            SchemaOp::UnixAccept { listener } => Ok(vec![OpKind::UnixAccept {
                listener: self.resolve_value(listener, id_map)?,
            }]),

            // === json module ===
            SchemaOp::JsonEncode { value } => Ok(vec![OpKind::JsonEncode {
                value: self.resolve_value(value, id_map)?,
            }]),

            SchemaOp::JsonDecode { string } => Ok(vec![OpKind::JsonDecode {
                string: self.resolve_value(string, id_map)?,
            }]),

            // === bp module ===
            SchemaOp::BpSleep { seconds } => Ok(vec![OpKind::Sleep {
                seconds: self.resolve_value(seconds, id_map)?,
            }]),

            SchemaOp::BpNow => Ok(vec![OpKind::Now]),

            SchemaOp::BpPrint { message } => Ok(vec![OpKind::Print {
                message: self.resolve_value(message, id_map)?,
            }]),

            // === ops module ===
            SchemaOp::OpsAll { ops } => {
                let plan_ops: Vec<OpId> = ops
                    .iter()
                    .map(|id| {
                        id_map
                            .get(id)
                            .copied()
                            .ok_or_else(|| anyhow!(PlanGeneratorError::UnknownOpRef(*id)))
                    })
                    .collect::<Result<Vec<_>>>()?;
                Ok(vec![OpKind::All { ops: plan_ops }])
            }

            SchemaOp::OpsAny { ops } => {
                let plan_ops: Vec<OpId> = ops
                    .iter()
                    .map(|id| {
                        id_map
                            .get(id)
                            .copied()
                            .ok_or_else(|| anyhow!(PlanGeneratorError::UnknownOpRef(*id)))
                    })
                    .collect::<Result<Vec<_>>>()?;
                Ok(vec![OpKind::Any { ops: plan_ops }])
            }

            SchemaOp::OpsAtLeast { ops, count } => {
                let plan_ops: Vec<OpId> = ops
                    .iter()
                    .map(|id| {
                        id_map
                            .get(id)
                            .copied()
                            .ok_or_else(|| anyhow!(PlanGeneratorError::UnknownOpRef(*id)))
                    })
                    .collect::<Result<Vec<_>>>()?;
                Ok(vec![OpKind::AtLeast {
                    ops: plan_ops,
                    count: *count,
                }])
            }

            SchemaOp::OpsAtMost { ops, count } => {
                let plan_ops: Vec<OpId> = ops
                    .iter()
                    .map(|id| {
                        id_map
                            .get(id)
                            .copied()
                            .ok_or_else(|| anyhow!(PlanGeneratorError::UnknownOpRef(*id)))
                    })
                    .collect::<Result<Vec<_>>>()?;
                Ok(vec![OpKind::AtMost {
                    ops: plan_ops,
                    count: *count,
                }])
            }

            SchemaOp::OpsAfter { dependency, value } => {
                let dep_id = id_map
                    .get(dependency)
                    .copied()
                    .ok_or_else(|| anyhow!(PlanGeneratorError::UnknownOpRef(*dependency)))?;
                let val_id = id_map
                    .get(value)
                    .copied()
                    .ok_or_else(|| anyhow!(PlanGeneratorError::UnknownOpRef(*value)))?;
                Ok(vec![OpKind::After {
                    dependency: dep_id,
                    value: val_id,
                }])
            }

            // === Pure operations (arithmetic) ===
            SchemaOp::Add { left, right } => Ok(vec![OpKind::Add {
                left: self.resolve_value(left, id_map)?,
                right: self.resolve_value(right, id_map)?,
            }]),

            SchemaOp::Sub { left, right } => Ok(vec![OpKind::Sub {
                left: self.resolve_value(left, id_map)?,
                right: self.resolve_value(right, id_map)?,
            }]),

            SchemaOp::Mul { left, right } => Ok(vec![OpKind::Mul {
                left: self.resolve_value(left, id_map)?,
                right: self.resolve_value(right, id_map)?,
            }]),

            SchemaOp::Div { left, right } => Ok(vec![OpKind::Div {
                left: self.resolve_value(left, id_map)?,
                right: self.resolve_value(right, id_map)?,
            }]),

            SchemaOp::FloorDiv { left, right } => Ok(vec![OpKind::FloorDiv {
                left: self.resolve_value(left, id_map)?,
                right: self.resolve_value(right, id_map)?,
            }]),

            SchemaOp::Mod { left, right } => Ok(vec![OpKind::Mod {
                left: self.resolve_value(left, id_map)?,
                right: self.resolve_value(right, id_map)?,
            }]),

            SchemaOp::Neg { value } => Ok(vec![OpKind::Neg {
                value: self.resolve_value(value, id_map)?,
            }]),

            // === Pure operations (comparisons) ===
            SchemaOp::Eq { left, right } => Ok(vec![OpKind::Eq {
                left: self.resolve_value(left, id_map)?,
                right: self.resolve_value(right, id_map)?,
            }]),

            SchemaOp::Ne { left, right } => Ok(vec![OpKind::Ne {
                left: self.resolve_value(left, id_map)?,
                right: self.resolve_value(right, id_map)?,
            }]),

            SchemaOp::Lt { left, right } => Ok(vec![OpKind::Lt {
                left: self.resolve_value(left, id_map)?,
                right: self.resolve_value(right, id_map)?,
            }]),

            SchemaOp::Le { left, right } => Ok(vec![OpKind::Le {
                left: self.resolve_value(left, id_map)?,
                right: self.resolve_value(right, id_map)?,
            }]),

            SchemaOp::Gt { left, right } => Ok(vec![OpKind::Gt {
                left: self.resolve_value(left, id_map)?,
                right: self.resolve_value(right, id_map)?,
            }]),

            SchemaOp::Ge { left, right } => Ok(vec![OpKind::Ge {
                left: self.resolve_value(left, id_map)?,
                right: self.resolve_value(right, id_map)?,
            }]),

            // === Pure operations (logic) ===
            SchemaOp::Not { value } => Ok(vec![OpKind::Not {
                value: self.resolve_value(value, id_map)?,
            }]),

            // === Pure operations (string/collection) ===
            SchemaOp::Concat { left, right } => Ok(vec![OpKind::Concat {
                left: self.resolve_value(left, id_map)?,
                right: self.resolve_value(right, id_map)?,
            }]),

            SchemaOp::Contains { haystack, needle } => Ok(vec![OpKind::Contains {
                haystack: self.resolve_value(haystack, id_map)?,
                needle: self.resolve_value(needle, id_map)?,
            }]),

            SchemaOp::Len { value } => Ok(vec![OpKind::Len {
                value: self.resolve_value(value, id_map)?,
            }]),

            SchemaOp::Index { base, index } => Ok(vec![OpKind::Index {
                base: self.resolve_value(base, id_map)?,
                index: self.resolve_value(index, id_map)?,
            }]),

            SchemaOp::SetIndex { base, index, value } => Ok(vec![OpKind::SetIndex {
                base: self.resolve_value(base, id_map)?,
                index: self.resolve_value(index, id_map)?,
                value: self.resolve_value(value, id_map)?,
            }]),

            // === Pure operations (collection) ===
            SchemaOp::Min { values } => Ok(vec![OpKind::Min {
                values: self.resolve_value(values, id_map)?,
            }]),

            SchemaOp::Max { values } => Ok(vec![OpKind::Max {
                values: self.resolve_value(values, id_map)?,
            }]),

            SchemaOp::Sum { values, start } => Ok(vec![OpKind::Sum {
                values: self.resolve_value(values, id_map)?,
                start: self.resolve_value(start, id_map)?,
            }]),

            SchemaOp::Abs { value } => Ok(vec![OpKind::Abs {
                value: self.resolve_value(value, id_map)?,
            }]),

            SchemaOp::Sorted { values } => Ok(vec![OpKind::Sorted {
                values: self.resolve_value(values, id_map)?,
            }]),

            SchemaOp::Reversed { values } => Ok(vec![OpKind::Reversed {
                values: self.resolve_value(values, id_map)?,
            }]),

            // === Type conversions ===
            SchemaOp::ToBool { value } => Ok(vec![OpKind::ToBool {
                value: self.resolve_value(value, id_map)?,
            }]),

            SchemaOp::ToInt { value } => Ok(vec![OpKind::ToInt {
                value: self.resolve_value(value, id_map)?,
            }]),

            SchemaOp::ToFloat { value } => Ok(vec![OpKind::ToFloat {
                value: self.resolve_value(value, id_map)?,
            }]),

            SchemaOp::ToStr { value } => Ok(vec![OpKind::ToStr {
                value: self.resolve_value(value, id_map)?,
            }]),

            // === Control flow ===
            SchemaOp::If {
                condition,
                then_value,
                else_value,
            } => Ok(vec![OpKind::If {
                condition: self.resolve_value(condition, id_map)?,
                then_value: self.resolve_value(then_value, id_map)?,
                else_value: self.resolve_value(else_value, id_map)?,
            }]),

            // === Runtime control flow ===
            SchemaOp::ForEach { items, item_name, body, parallel } => {
                Ok(vec![OpKind::ForEach {
                    items: self.resolve_value(items, id_map)?,
                    item_name: item_name.clone(),
                    body: self.resolve_subplan(body, id_map)?,
                    parallel: *parallel,
                }])
            }

            SchemaOp::Map { items, item_name, body } => {
                Ok(vec![OpKind::Map {
                    items: self.resolve_value(items, id_map)?,
                    item_name: item_name.clone(),
                    body: self.resolve_subplan(body, id_map)?,
                }])
            }

            SchemaOp::Filter { items, item_name, predicate } => {
                Ok(vec![OpKind::Filter {
                    items: self.resolve_value(items, id_map)?,
                    item_name: item_name.clone(),
                    predicate: self.resolve_subplan(predicate, id_map)?,
                }])
            }

            SchemaOp::IfBlock { condition, then_body, else_body } => {
                Ok(vec![OpKind::IfBlock {
                    condition: self.resolve_value(condition, id_map)?,
                    then_body: self.resolve_subplan(then_body, id_map)?,
                    else_body: match else_body {
                        Some(body) => Some(self.resolve_subplan(body, id_map)?),
                        None => None,
                    },
                }])
            }

            SchemaOp::Break => Ok(vec![OpKind::Break]),
            SchemaOp::Continue => Ok(vec![OpKind::Continue]),

            SchemaOp::FrozenValue { name, value } => Ok(vec![OpKind::FrozenValue {
                name: name.clone(),
                value: self.resolve_value(value, id_map)?,
            }]),
        }
    }

    fn resolve_value(
        &self,
        value: &SchemaValue,
        id_map: &HashMap<SchemaOpId, OpId>,
    ) -> Result<ValueRef> {
        match value {
            SchemaValue::Literal(v) => Ok(ValueRef::Literal(v.clone())),

            SchemaValue::EnvRef(name) => match self.context.resolve_env(name) {
                Some(v) => Ok(ValueRef::literal_string(v)),
                None => Err(anyhow!(PlanGeneratorError::UnresolvedEnvVar(name.clone()))),
            },

            SchemaValue::ConfigRef(key) => match self.context.resolve_config_path(key) {
                Some(v) => Ok(ValueRef::literal_string(v)),
                None => match self.context.resolve_config_var(key) {
                    Some(v) => Ok(ValueRef::literal_string(v)),
                    None => Err(anyhow!(PlanGeneratorError::UnresolvedConfigKey(key.clone()))),
                },
            },

            SchemaValue::OpRef { id: schema_id, path } => {
                let plan_id = id_map
                    .get(schema_id)
                    .copied()
                    .ok_or_else(|| anyhow!(PlanGeneratorError::UnknownOpRef(*schema_id)))?;
                Ok(ValueRef::OpOutput {
                    op: plan_id,
                    path: path.clone(),
                })
            }

            SchemaValue::List(items) => {
                let resolved: Result<Vec<ValueRef>> = items
                    .iter()
                    .map(|item| self.resolve_value(item, id_map))
                    .collect();
                Ok(ValueRef::List(resolved?))
            }

            SchemaValue::ParamRef(name) => {
                Ok(ValueRef::Dynamic(name.clone()))
            }
        }
    }

    fn resolve_subplan(
        &self,
        schema_subplan: &SchemaSubPlan,
        parent_id_map: &HashMap<SchemaOpId, OpId>,
    ) -> Result<SubPlan> {
        let mut ops = Vec::new();
        let mut local_id_map: HashMap<u64, OpId> = HashMap::new();

        for entry in &schema_subplan.entries {
            let op_id = OpId(entry.local_id);
            local_id_map.insert(entry.local_id, op_id);

            let mut combined_map = parent_id_map.clone();
            for (local_id, op_id) in &local_id_map {
                combined_map.insert(SchemaOpId(*local_id), *op_id);
            }

            let op_kinds = self.resolve_op(&entry.op, &combined_map)?;

            for kind in op_kinds {
                let op = Op {
                    id: op_id,
                    kind,
                    inputs: vec![],
                    source_location: None,
                    guard: entry.guard.map(OpId),
                };
                ops.push(op);
            }
        }

        Ok(SubPlan {
            params: schema_subplan.params.clone(),
            ops,
            output: OpId(schema_subplan.output),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use blueprint_common::{PathMapping, ProjectConfig, Schema, SchemaOp, SchemaValue};

    fn test_context() -> ExecutionContext {
        ExecutionContext::from_current_env()
            .with_env("HOME", "/home/testuser")
            .with_env("API_KEY", "secret123")
    }

    #[test]
    fn test_lower_simple_read() {
        let mut schema = Schema::new();
        schema.add_op(
            SchemaOp::IoReadFile {
                path: SchemaValue::literal_string("config.json"),
            },
            None,
        );

        let ctx = test_context();
        let resolver = Resolver::new(&ctx);
        let plan = resolver.resolve(&schema).unwrap();

        assert_eq!(plan.len(), 1);
        let op = plan.ops().next().unwrap();
        assert!(matches!(op.kind, OpKind::ReadFile { .. }));
    }

    #[test]
    fn test_lower_env_ref() {
        let mut schema = Schema::new();
        schema.add_op(
            SchemaOp::IoReadFile {
                path: SchemaValue::env_ref("HOME"),
            },
            None,
        );

        let ctx = test_context();
        let resolver = Resolver::new(&ctx);
        let plan = resolver.resolve(&schema).unwrap();

        let op = plan.ops().next().unwrap();
        if let OpKind::ReadFile { path } = &op.kind {
            assert!(matches!(path, ValueRef::Literal(RecordedValue::String(s)) if s == "/home/testuser"));
        } else {
            panic!("Expected ReadFile op");
        }
    }

    #[test]
    fn test_lower_config_ref() {
        let config = ProjectConfig::new()
            .with_path(
                "output",
                PathMapping::new("/default/output")
                    .with_linux("/var/output")
                    .with_macos("$HOME/output"),
            );

        let mut ctx = test_context().with_config(config);
        ctx.os = "linux".to_string();

        let mut schema = Schema::new();
        schema.add_op(
            SchemaOp::IoWriteFile {
                path: SchemaValue::config_ref("output"),
                content: SchemaValue::literal_string("hello"),
            },
            None,
        );

        let resolver = Resolver::new(&ctx);
        let plan = resolver.resolve(&schema).unwrap();

        let op = plan.ops().next().unwrap();
        if let OpKind::WriteFile { path, .. } = &op.kind {
            assert!(matches!(path, ValueRef::Literal(RecordedValue::String(s)) if s == "/var/output"));
        } else {
            panic!("Expected WriteFile op");
        }
    }

    #[test]
    fn test_lower_http_request() {
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

        let ctx = test_context();
        let resolver = Resolver::new(&ctx);
        let plan = resolver.resolve(&schema).unwrap();

        let op = plan.ops().next().unwrap();
        if let OpKind::HttpRequest { method, .. } = &op.kind {
            assert!(matches!(method, ValueRef::Literal(RecordedValue::String(s)) if s == "GET"));
        } else {
            panic!("Expected HttpRequest op");
        }
    }

    #[test]
    fn test_resolve_op_ref() {
        let mut schema = Schema::new();
        let read_id = schema.add_op(
            SchemaOp::IoReadFile {
                path: SchemaValue::literal_string("data.json"),
            },
            None,
        );
        schema.add_op(
            SchemaOp::JsonDecode {
                string: SchemaValue::op_ref(read_id),
            },
            None,
        );

        let ctx = test_context();
        let resolver = Resolver::new(&ctx);
        let plan = resolver.resolve(&schema).unwrap();

        assert_eq!(plan.len(), 2);

        let decode_op = plan.ops().nth(1).unwrap();
        if let OpKind::JsonDecode { string } = &decode_op.kind {
            assert!(matches!(string, ValueRef::OpOutput { .. }));
        } else {
            panic!("Expected JsonDecode op");
        }
    }

    #[test]
    fn test_lower_unresolved_env_var() {
        let mut schema = Schema::new();
        schema.add_op(
            SchemaOp::IoReadFile {
                path: SchemaValue::env_ref("NONEXISTENT_VAR"),
            },
            None,
        );

        let ctx = ExecutionContext::from_current_env();
        let resolver = Resolver::new(&ctx);
        let result = resolver.resolve(&schema);

        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("NONEXISTENT_VAR"));
    }

    #[test]
    fn test_lower_arithmetic() {
        let mut schema = Schema::new();
        schema.add_op(
            SchemaOp::Add {
                left: SchemaValue::literal_int(1),
                right: SchemaValue::literal_int(2),
            },
            None,
        );

        let ctx = test_context();
        let resolver = Resolver::new(&ctx);
        let plan = resolver.resolve(&schema).unwrap();

        let op = plan.ops().next().unwrap();
        assert!(matches!(op.kind, OpKind::Add { .. }));
    }

    #[test]
    fn test_resolve_ops_all() {
        let mut schema = Schema::new();
        let op1 = schema.add_op(SchemaOp::BpNow, None);
        let op2 = schema.add_op(SchemaOp::BpNow, None);
        schema.add_op(SchemaOp::OpsAll { ops: vec![op1, op2] }, None);

        let ctx = test_context();
        let resolver = Resolver::new(&ctx);
        let plan = resolver.resolve(&schema).unwrap();

        assert_eq!(plan.len(), 3);
        let all_op = plan.ops().nth(2).unwrap();
        if let OpKind::All { ops } = &all_op.kind {
            assert_eq!(ops.len(), 2);
        } else {
            panic!("Expected All op");
        }
    }

    fn make_simple_schema_subplan() -> SchemaSubPlan {
        SchemaSubPlan {
            params: vec!["x".to_string()],
            entries: vec![blueprint_common::SchemaSubPlanEntry {
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
                blueprint_common::SchemaSubPlanEntry {
                    local_id: 0,
                    op: SchemaOp::Contains {
                        haystack: SchemaValue::literal_string("hello world"),
                        needle: SchemaValue::literal_string("hello"),
                    },
                    guard: None,
                },
                blueprint_common::SchemaSubPlanEntry {
                    local_id: 1,
                    op: SchemaOp::BpPrint {
                        message: SchemaValue::literal_string("found"),
                    },
                    guard: Some(0),
                },
            ],
            output: 1,
        }
    }

    #[test]
    fn test_resolve_foreach() {
        let mut schema = Schema::new();
        let read_id = schema.add_op(
            SchemaOp::IoReadFile {
                path: SchemaValue::literal_string("data.txt"),
            },
            None,
        );

        schema.add_op(
            SchemaOp::ForEach {
                items: SchemaValue::op_ref(read_id),
                item_name: "line".to_string(),
                body: make_simple_schema_subplan(),
                parallel: false,
            },
            None,
        );

        let ctx = test_context();
        let resolver = Resolver::new(&ctx);
        let plan = resolver.resolve(&schema).unwrap();

        assert_eq!(plan.len(), 2);
        let foreach_op = plan.ops().nth(1).unwrap();
        if let OpKind::ForEach { item_name, parallel, body, .. } = &foreach_op.kind {
            assert_eq!(item_name, "line");
            assert!(!parallel);
            assert_eq!(body.params.len(), 1);
            assert_eq!(body.ops.len(), 1);
        } else {
            panic!("Expected ForEach op");
        }
    }

    #[test]
    fn test_resolve_foreach_parallel() {
        let mut schema = Schema::new();
        schema.add_op(
            SchemaOp::ForEach {
                items: SchemaValue::literal_list(vec![
                    RecordedValue::Int(1),
                    RecordedValue::Int(2),
                    RecordedValue::Int(3),
                ]),
                item_name: "n".to_string(),
                body: make_simple_schema_subplan(),
                parallel: true,
            },
            None,
        );

        let ctx = test_context();
        let resolver = Resolver::new(&ctx);
        let plan = resolver.resolve(&schema).unwrap();

        let foreach_op = plan.ops().next().unwrap();
        if let OpKind::ForEach { parallel, .. } = &foreach_op.kind {
            assert!(parallel);
        } else {
            panic!("Expected ForEach op");
        }
    }

    #[test]
    fn test_resolve_ifblock() {
        let mut schema = Schema::new();
        let cond_id = schema.add_op(
            SchemaOp::IoFileExists {
                path: SchemaValue::literal_string("test.txt"),
            },
            None,
        );

        schema.add_op(
            SchemaOp::IfBlock {
                condition: SchemaValue::op_ref(cond_id),
                then_body: make_simple_schema_subplan(),
                else_body: None,
            },
            None,
        );

        let ctx = test_context();
        let resolver = Resolver::new(&ctx);
        let plan = resolver.resolve(&schema).unwrap();

        assert_eq!(plan.len(), 2);
        let ifblock_op = plan.ops().nth(1).unwrap();
        if let OpKind::IfBlock { then_body, else_body, .. } = &ifblock_op.kind {
            assert_eq!(then_body.ops.len(), 1);
            assert!(else_body.is_none());
        } else {
            panic!("Expected IfBlock op");
        }
    }

    #[test]
    fn test_resolve_ifblock_with_else() {
        let mut schema = Schema::new();
        schema.add_op(
            SchemaOp::IfBlock {
                condition: SchemaValue::literal_bool(true),
                then_body: make_simple_schema_subplan(),
                else_body: Some(make_simple_schema_subplan()),
            },
            None,
        );

        let ctx = test_context();
        let resolver = Resolver::new(&ctx);
        let plan = resolver.resolve(&schema).unwrap();

        let ifblock_op = plan.ops().next().unwrap();
        if let OpKind::IfBlock { else_body, .. } = &ifblock_op.kind {
            assert!(else_body.is_some());
        } else {
            panic!("Expected IfBlock op");
        }
    }

    #[test]
    fn test_resolve_break() {
        let mut schema = Schema::new();
        schema.add_op(SchemaOp::Break, None);

        let ctx = test_context();
        let resolver = Resolver::new(&ctx);
        let plan = resolver.resolve(&schema).unwrap();

        let op = plan.ops().next().unwrap();
        assert!(matches!(op.kind, OpKind::Break));
    }

    #[test]
    fn test_resolve_continue() {
        let mut schema = Schema::new();
        schema.add_op(SchemaOp::Continue, None);

        let ctx = test_context();
        let resolver = Resolver::new(&ctx);
        let plan = resolver.resolve(&schema).unwrap();

        let op = plan.ops().next().unwrap();
        assert!(matches!(op.kind, OpKind::Continue));
    }

    #[test]
    fn test_resolve_subplan_with_guard() {
        let mut schema = Schema::new();
        schema.add_op(
            SchemaOp::ForEach {
                items: SchemaValue::literal_list(vec![
                    RecordedValue::String("a".to_string()),
                    RecordedValue::String("b".to_string()),
                ]),
                item_name: "item".to_string(),
                body: make_guarded_schema_subplan(),
                parallel: false,
            },
            None,
        );

        let ctx = test_context();
        let resolver = Resolver::new(&ctx);
        let plan = resolver.resolve(&schema).unwrap();

        let foreach_op = plan.ops().next().unwrap();
        if let OpKind::ForEach { body, .. } = &foreach_op.kind {
            assert_eq!(body.ops.len(), 2);
            assert_eq!(body.ops[0].guard, None);
            assert_eq!(body.ops[1].guard, Some(OpId(0)));
        } else {
            panic!("Expected ForEach op");
        }
    }

    #[test]
    fn test_resolve_nested_foreach() {
        let inner_body = SchemaSubPlan {
            params: vec!["y".to_string()],
            entries: vec![blueprint_common::SchemaSubPlanEntry {
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
            entries: vec![blueprint_common::SchemaSubPlanEntry {
                local_id: 0,
                op: inner_foreach,
                guard: None,
            }],
            output: 0,
        };

        let mut schema = Schema::new();
        schema.add_op(
            SchemaOp::ForEach {
                items: SchemaValue::literal_list(vec![
                    RecordedValue::String("a".to_string()),
                    RecordedValue::String("b".to_string()),
                ]),
                item_name: "x".to_string(),
                body: outer_body,
                parallel: true,
            },
            None,
        );

        let ctx = test_context();
        let resolver = Resolver::new(&ctx);
        let plan = resolver.resolve(&schema).unwrap();

        let outer_op = plan.ops().next().unwrap();
        if let OpKind::ForEach { body, parallel, .. } = &outer_op.kind {
            assert!(parallel);
            if let OpKind::ForEach { item_name, parallel: inner_parallel, .. } = &body.ops[0].kind {
                assert_eq!(item_name, "y");
                assert!(!inner_parallel);
            } else {
                panic!("Expected nested ForEach op");
            }
        } else {
            panic!("Expected ForEach op");
        }
    }

    #[test]
    fn test_resolve_ifblock_in_foreach() {
        let body_with_ifblock = SchemaSubPlan {
            params: vec!["item".to_string()],
            entries: vec![
                blueprint_common::SchemaSubPlanEntry {
                    local_id: 0,
                    op: SchemaOp::Contains {
                        haystack: SchemaValue::literal_string("hello"),
                        needle: SchemaValue::literal_string("e"),
                    },
                    guard: None,
                },
                blueprint_common::SchemaSubPlanEntry {
                    local_id: 1,
                    op: SchemaOp::IfBlock {
                        condition: SchemaValue::OpRef {
                            id: SchemaOpId(0),
                            path: vec![],
                        },
                        then_body: make_simple_schema_subplan(),
                        else_body: None,
                    },
                    guard: None,
                },
            ],
            output: 1,
        };

        let mut schema = Schema::new();
        schema.add_op(
            SchemaOp::ForEach {
                items: SchemaValue::literal_list(vec![
                    RecordedValue::String("a".to_string()),
                ]),
                item_name: "item".to_string(),
                body: body_with_ifblock,
                parallel: false,
            },
            None,
        );

        let ctx = test_context();
        let resolver = Resolver::new(&ctx);
        let plan = resolver.resolve(&schema).unwrap();

        let foreach_op = plan.ops().next().unwrap();
        if let OpKind::ForEach { body, .. } = &foreach_op.kind {
            assert_eq!(body.ops.len(), 2);
            assert!(matches!(body.ops[0].kind, OpKind::Contains { .. }));
            assert!(matches!(body.ops[1].kind, OpKind::IfBlock { .. }));
        } else {
            panic!("Expected ForEach op");
        }
    }
}
