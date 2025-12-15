use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::time::SystemTime;

use thiserror::Error;
use tokio::sync::mpsc;

use blueprint_common::{OpId, OpKind, RecordedValue, ValueRef, SubPlan, Accessor, Plan};

use super::cache::{compute_input_hash, OpCache};
use super::resolver::ValueResolver;

#[derive(Error, Debug)]
pub enum ExecutionError {
    #[error("Operation {0:?} failed: {1}")]
    OpFailed(OpId, String),

    #[error("Cycle detected in plan")]
    CycleDetected,

    #[error("Missing dependency: op {0:?} depends on {1:?} which has no result")]
    MissingDependency(OpId, OpId),

    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),

    #[error("HTTP error: {0}")]
    HttpError(String),

    #[error("JSON error: {0}")]
    JsonError(#[from] serde_json::Error),

    #[error("Value resolution failed for op {0:?}")]
    ResolutionFailed(OpId),

    #[error("Command execution failed: {0}")]
    CommandFailed(String),

    #[error("Invalid operation: {0}")]
    InvalidOp(String),
}

#[derive(Debug, Clone, PartialEq)]
pub enum LoopSignal {
    None,
    Break,
    Continue,
}

pub type ExecutionResult<T> = Result<T, ExecutionError>;

const DEFAULT_MAX_CONCURRENT: usize = 64;

use blueprint_common::Op;

pub struct BlueprintInterpreter {
    ops: HashMap<OpId, Op>,
    dependents: HashMap<OpId, Vec<OpId>>,
    pending_deps: HashMap<OpId, usize>,
    ready: VecDeque<OpId>,
    in_flight: HashSet<OpId>,
    cache: Arc<OpCache>,
    max_concurrent: usize,
    dry_run: bool,
    http_client: reqwest::Client,
}

impl BlueprintInterpreter {
    pub fn new() -> Self {
        Self {
            ops: HashMap::new(),
            dependents: HashMap::new(),
            pending_deps: HashMap::new(),
            ready: VecDeque::new(),
            in_flight: HashSet::new(),
            cache: Arc::new(OpCache::new()),
            max_concurrent: DEFAULT_MAX_CONCURRENT,
            dry_run: false,
            http_client: reqwest::Client::new(),
        }
    }

    pub fn with_max_concurrent(mut self, n: usize) -> Self {
        self.max_concurrent = n;
        self
    }

    pub fn with_dry_run(mut self, dry_run: bool) -> Self {
        self.dry_run = dry_run;
        self
    }

    pub fn with_cache(mut self, cache: OpCache) -> Self {
        self.cache = Arc::new(cache);
        self
    }

    pub fn add_op(&mut self, op: Op) {
        let op_id = op.id;

        if self.ops.contains_key(&op_id) {
            return;
        }

        let unsatisfied = op
            .inputs
            .iter()
            .filter(|id| self.cache.get_value(**id).is_none())
            .count();

        for &input_id in &op.inputs {
            self.dependents.entry(input_id).or_default().push(op_id);
        }

        self.pending_deps.insert(op_id, unsatisfied);
        self.ops.insert(op_id, op);

        if unsatisfied == 0 {
            self.ready.push_back(op_id);
        }
    }

    pub fn add_plan(&mut self, plan: &Plan) {
        for op in plan.ops() {
            self.add_op(op.clone());
        }
    }

    pub fn reset(&mut self) {
        self.ops.clear();
        self.dependents.clear();
        self.pending_deps.clear();
        self.ready.clear();
        self.in_flight.clear();
        self.cache = Arc::new(OpCache::new());
    }

    pub fn cache(&self) -> &OpCache {
        &self.cache
    }

    fn handle_completion(&mut self, op_id: OpId, value: RecordedValue) {
        self.in_flight.remove(&op_id);

        if let Some(op) = self.ops.get(&op_id) {
            let inputs = self.collect_inputs(&op.kind);
            let input_hash = compute_input_hash(&inputs);
            self.cache.insert(op_id, value, input_hash);
        }

        if let Some(deps) = self.dependents.get(&op_id) {
            for &dependent in deps {
                if let Some(count) = self.pending_deps.get_mut(&dependent) {
                    *count -= 1;
                    if *count == 0 {
                        self.ready.push_back(dependent);
                    }
                }
            }
        }
    }

    fn collect_inputs(&self, kind: &OpKind) -> Vec<ValueRef> {
        match kind {
            OpKind::ReadFile { path } => vec![path.clone()],
            OpKind::WriteFile { path, content } => vec![path.clone(), content.clone()],
            OpKind::AppendFile { path, content } => vec![path.clone(), content.clone()],
            OpKind::DeleteFile { path } => vec![path.clone()],
            OpKind::FileExists { path } => vec![path.clone()],
            OpKind::IsDir { path } => vec![path.clone()],
            OpKind::IsFile { path } => vec![path.clone()],
            OpKind::Mkdir { path, .. } => vec![path.clone()],
            OpKind::Rmdir { path, .. } => vec![path.clone()],
            OpKind::ListDir { path } => vec![path.clone()],
            OpKind::CopyFile { src, dst } => vec![src.clone(), dst.clone()],
            OpKind::MoveFile { src, dst } => vec![src.clone(), dst.clone()],
            OpKind::FileSize { path } => vec![path.clone()],
            OpKind::HttpRequest { method, url, headers, body } => {
                vec![method.clone(), url.clone(), headers.clone(), body.clone()]
            }
            OpKind::TcpConnect { host, port } => vec![host.clone(), port.clone()],
            OpKind::TcpSend { handle, data } => vec![handle.clone(), data.clone()],
            OpKind::TcpRecv { handle, max_bytes } => vec![handle.clone(), max_bytes.clone()],
            OpKind::TcpClose { handle } => vec![handle.clone()],
            OpKind::UdpBind { host, port } => vec![host.clone(), port.clone()],
            OpKind::UdpSendTo { handle, data, host, port } => {
                vec![handle.clone(), data.clone(), host.clone(), port.clone()]
            }
            OpKind::UdpRecvFrom { handle, max_bytes } => vec![handle.clone(), max_bytes.clone()],
            OpKind::UdpClose { handle } => vec![handle.clone()],
            OpKind::Exec { command, args } => vec![command.clone(), args.clone()],
            OpKind::EnvGet { name, default } => vec![name.clone(), default.clone()],
            OpKind::Sleep { seconds } => vec![seconds.clone()],
            OpKind::Print { message } => vec![message.clone()],
            OpKind::JsonEncode { value } => vec![value.clone()],
            OpKind::JsonDecode { string } => vec![string.clone()],
            OpKind::Now => vec![],
            OpKind::All { .. } | OpKind::Any { .. } | OpKind::AtLeast { .. } | OpKind::AtMost { .. } | OpKind::After { .. } => vec![],
            OpKind::TcpListen { host, port } => vec![host.clone(), port.clone()],
            OpKind::TcpAccept { listener } => vec![listener.clone()],
            OpKind::UnixConnect { path } => vec![path.clone()],
            OpKind::UnixSend { handle, data } => vec![handle.clone(), data.clone()],
            OpKind::UnixRecv { handle, max_bytes } => vec![handle.clone(), max_bytes.clone()],
            OpKind::UnixClose { handle } => vec![handle.clone()],
            OpKind::UnixListen { path } => vec![path.clone()],
            OpKind::UnixAccept { listener } => vec![listener.clone()],
            OpKind::ToBool { value } => vec![value.clone()],
            OpKind::ToInt { value } => vec![value.clone()],
            OpKind::ToFloat { value } => vec![value.clone()],
            OpKind::ToStr { value } => vec![value.clone()],
            OpKind::Len { value } => vec![value.clone()],
            OpKind::Add { left, right } => vec![left.clone(), right.clone()],
            OpKind::Sub { left, right } => vec![left.clone(), right.clone()],
            OpKind::Mul { left, right } => vec![left.clone(), right.clone()],
            OpKind::Div { left, right } => vec![left.clone(), right.clone()],
            OpKind::FloorDiv { left, right } => vec![left.clone(), right.clone()],
            OpKind::Mod { left, right } => vec![left.clone(), right.clone()],
            OpKind::Neg { value } => vec![value.clone()],
            OpKind::Eq { left, right } => vec![left.clone(), right.clone()],
            OpKind::Ne { left, right } => vec![left.clone(), right.clone()],
            OpKind::Lt { left, right } => vec![left.clone(), right.clone()],
            OpKind::Le { left, right } => vec![left.clone(), right.clone()],
            OpKind::Gt { left, right } => vec![left.clone(), right.clone()],
            OpKind::Ge { left, right } => vec![left.clone(), right.clone()],
            OpKind::Not { value } => vec![value.clone()],
            OpKind::Concat { left, right } => vec![left.clone(), right.clone()],
            OpKind::Contains { haystack, needle } => vec![haystack.clone(), needle.clone()],
            OpKind::If { condition, then_value, else_value } => vec![condition.clone(), then_value.clone(), else_value.clone()],
            OpKind::Index { base, index } => vec![base.clone(), index.clone()],
            OpKind::SetIndex { base, index, value } => vec![base.clone(), index.clone(), value.clone()],
            OpKind::Min { values } => vec![values.clone()],
            OpKind::Max { values } => vec![values.clone()],
            OpKind::Sum { values, start } => vec![values.clone(), start.clone()],
            OpKind::Abs { value } => vec![value.clone()],
            OpKind::Sorted { values } => vec![values.clone()],
            OpKind::Reversed { values } => vec![values.clone()],
            OpKind::GeneratorDef { .. } => vec![],
            OpKind::GeneratorYield { value } => vec![value.clone()],
            OpKind::GeneratorYieldIf { condition, value } => vec![condition.clone(), value.clone()],
            OpKind::ParamRef { .. } => vec![],
            OpKind::ForEach { items, .. } => vec![items.clone()],
            OpKind::Map { items, .. } => vec![items.clone()],
            OpKind::Filter { items, .. } => vec![items.clone()],
            OpKind::IfBlock { condition, .. } => vec![condition.clone()],
            OpKind::Break | OpKind::Continue => vec![],
            OpKind::FrozenValue { value, .. } => vec![value.clone()],
        }
    }

    pub async fn run(&mut self) -> ExecutionResult<()> {
        let (tx, mut rx) = mpsc::channel::<(OpId, ExecutionResult<RecordedValue>)>(self.max_concurrent * 2);

        loop {
            while self.in_flight.len() < self.max_concurrent {
                let Some(op_id) = self.ready.pop_front() else { break };

                let cache = Arc::clone(&self.cache);
                let http_client = self.http_client.clone();
                let dry_run = self.dry_run;
                let op = self.ops.get(&op_id).unwrap().clone();
                let tx = tx.clone();

                tokio::spawn(async move {
                    let result = execute_op(&op.kind, op_id, &cache, &http_client, dry_run).await;
                    let _ = tx.send((op_id, result)).await;
                });

                self.in_flight.insert(op_id);
            }

            if self.in_flight.is_empty() && self.ready.is_empty() {
                break;
            }

            let Some((op_id, result)) = rx.recv().await else {
                break;
            };

            let value = result?;
            self.handle_completion(op_id, value);
        }

        self.cache.sync();
        Ok(())
    }

    pub async fn execute(&mut self, plan: &Plan) -> ExecutionResult<OpCache> {
        self.add_plan(plan);
        self.run().await?;
        Ok((*self.cache).clone())
    }

    // === Unified API Methods ===

    /// All-in-one: Parse script, generate schema, generate plan, and execute
    pub async fn run_script(&mut self, path: &std::path::Path) -> ExecutionResult<OpCache> {
        let plan = self.compile(path)?;
        self.execute(&plan).await
    }

    /// All-in-one from source string: Parse, generate, and execute
    pub async fn run_source(&mut self, source: &str) -> ExecutionResult<OpCache> {
        let plan = self.compile_source(source)?;
        self.execute(&plan).await
    }

    /// Compile script to plan without executing
    pub fn compile(&self, path: &std::path::Path) -> ExecutionResult<Plan> {
        use blueprint_generator::{BlueprintGenerator, PlanGenerator};
        use blueprint_common::ExecutionContext;

        let generator = BlueprintGenerator::new();
        let schema = generator.generate_schema(path)
            .map_err(|e| ExecutionError::InvalidOp(format!("Schema generation failed: {}", e)))?;

        let ctx = ExecutionContext::from_current_env();
        let plan_gen = PlanGenerator::new(&ctx);
        plan_gen.generate(&schema)
            .map_err(|e| ExecutionError::InvalidOp(format!("Plan generation failed: {}", e)))
    }

    /// Compile source string to plan without executing
    pub fn compile_source(&self, source: &str) -> ExecutionResult<Plan> {
        use blueprint_generator::{BlueprintGenerator, PlanGenerator};
        use blueprint_common::ExecutionContext;

        let generator = BlueprintGenerator::new();
        let schema = generator.generate_from_source(source)
            .map_err(|e| ExecutionError::InvalidOp(format!("Schema generation failed: {}", e)))?;

        let ctx = ExecutionContext::from_current_env();
        let plan_gen = PlanGenerator::new(&ctx);
        plan_gen.generate(&schema)
            .map_err(|e| ExecutionError::InvalidOp(format!("Plan generation failed: {}", e)))
    }

    /// Check script syntax without generating plan
    pub fn check(&self, path: &std::path::Path) -> ExecutionResult<()> {
        use blueprint_generator::BlueprintGenerator;

        let generator = BlueprintGenerator::new();
        generator.check(path)
            .map_err(|e| ExecutionError::InvalidOp(format!("Syntax check failed: {}", e)))
    }

    /// Generate schema from script file
    pub fn generate_schema(&self, path: &std::path::Path) -> ExecutionResult<blueprint_common::Schema> {
        use blueprint_generator::BlueprintGenerator;

        let generator = BlueprintGenerator::new();
        generator.generate_schema(path)
            .map_err(|e| ExecutionError::InvalidOp(format!("Schema generation failed: {}", e)))
    }

    /// Generate schema from source string
    pub fn generate_schema_from_source(&self, source: &str) -> ExecutionResult<blueprint_common::Schema> {
        use blueprint_generator::BlueprintGenerator;

        let generator = BlueprintGenerator::new();
        generator.generate_from_source(source)
            .map_err(|e| ExecutionError::InvalidOp(format!("Schema generation failed: {}", e)))
    }

    /// Generate compiled schema (for serialization)
    pub fn generate_compiled_schema(
        &self,
        path: &std::path::Path,
        include_source: bool,
    ) -> ExecutionResult<blueprint_common::CompiledSchema> {
        use blueprint_generator::BlueprintGenerator;

        let generator = BlueprintGenerator::new();
        generator.generate_compiled_schema(path, include_source)
            .map_err(|e| ExecutionError::InvalidOp(format!("Compiled schema generation failed: {}", e)))
    }

    /// Generate compiled plan (for serialization)
    pub fn generate_compiled_plan(
        &self,
        path: &std::path::Path,
        opt_level: blueprint_common::OptLevel,
        include_source: bool,
    ) -> ExecutionResult<blueprint_common::CompiledPlan> {
        use blueprint_generator::{BlueprintGenerator, PlanGenerator};
        use blueprint_common::ExecutionContext;

        let generator = BlueprintGenerator::new();
        let schema = generator.generate_schema(path)
            .map_err(|e| ExecutionError::InvalidOp(format!("Schema generation failed: {}", e)))?;

        let ctx = ExecutionContext::from_current_env();
        let plan_gen = PlanGenerator::new(&ctx);
        let plan = plan_gen.generate(&schema)
            .map_err(|e| ExecutionError::InvalidOp(format!("Plan generation failed: {}", e)))?;

        generator.generate_compiled_plan(path, plan, opt_level, include_source)
            .map_err(|e| ExecutionError::InvalidOp(format!("Compiled plan generation failed: {}", e)))
    }
}

impl Default for BlueprintInterpreter {
    fn default() -> Self {
        Self::new()
    }
}

async fn execute_op(
    kind: &OpKind,
    op_id: OpId,
    cache: &Arc<OpCache>,
    http_client: &reqwest::Client,
    dry_run: bool,
) -> ExecutionResult<RecordedValue> {
    if dry_run {
        println!("[DRY RUN] Would execute: Op {:?} ({:?})", op_id, kind);
        return Ok(RecordedValue::None);
    }

    let resolver = ValueResolver::new(cache);

    match kind {
        OpKind::ReadFile { path } => {
            let path_str = resolver.resolve_to_string(path)
                .ok_or_else(|| ExecutionError::ResolutionFailed(op_id))?;

            let content = tokio::fs::read_to_string(&path_str).await?;
            Ok(RecordedValue::String(content))
        }

        OpKind::WriteFile { path, content } => {
            let path_str = resolver.resolve_to_string(path)
                .ok_or_else(|| ExecutionError::ResolutionFailed(op_id))?;
            let content_str = resolver.resolve_to_string(content)
                .ok_or_else(|| ExecutionError::ResolutionFailed(op_id))?;

            tokio::fs::write(&path_str, &content_str).await?;
            Ok(RecordedValue::None)
        }

        OpKind::AppendFile { path, content } => {
            let path_str = resolver.resolve_to_string(path)
                .ok_or_else(|| ExecutionError::ResolutionFailed(op_id))?;
            let content_str = resolver.resolve_to_string(content)
                .ok_or_else(|| ExecutionError::ResolutionFailed(op_id))?;

            use tokio::io::AsyncWriteExt;
            let mut file = tokio::fs::OpenOptions::new()
                .append(true)
                .create(true)
                .open(&path_str)
                .await?;
            file.write_all(content_str.as_bytes()).await?;
            Ok(RecordedValue::None)
        }

        OpKind::DeleteFile { path } => {
            let path_str = resolver.resolve_to_string(path)
                .ok_or_else(|| ExecutionError::ResolutionFailed(op_id))?;

            tokio::fs::remove_file(&path_str).await?;
            Ok(RecordedValue::None)
        }

        OpKind::FileExists { path } => {
            let path_str = resolver.resolve_to_string(path)
                .ok_or_else(|| ExecutionError::ResolutionFailed(op_id))?;

            let exists = tokio::fs::try_exists(&path_str).await.unwrap_or(false);
            Ok(RecordedValue::Bool(exists))
        }

        OpKind::IsDir { path } => {
            let path_str = resolver.resolve_to_string(path)
                .ok_or_else(|| ExecutionError::ResolutionFailed(op_id))?;

            let metadata = tokio::fs::metadata(&path_str).await;
            Ok(RecordedValue::Bool(metadata.map(|m| m.is_dir()).unwrap_or(false)))
        }

        OpKind::IsFile { path } => {
            let path_str = resolver.resolve_to_string(path)
                .ok_or_else(|| ExecutionError::ResolutionFailed(op_id))?;

            let metadata = tokio::fs::metadata(&path_str).await;
            Ok(RecordedValue::Bool(metadata.map(|m| m.is_file()).unwrap_or(false)))
        }

        OpKind::Mkdir { path, recursive } => {
            let path_str = resolver.resolve_to_string(path)
                .ok_or_else(|| ExecutionError::ResolutionFailed(op_id))?;
            let recursive = *recursive;

            if recursive {
                tokio::fs::create_dir_all(&path_str).await?;
            } else {
                tokio::fs::create_dir(&path_str).await?;
            }
            Ok(RecordedValue::None)
        }

        OpKind::Rmdir { path, recursive } => {
            let path_str = resolver.resolve_to_string(path)
                .ok_or_else(|| ExecutionError::ResolutionFailed(op_id))?;
            let recursive = *recursive;

            if recursive {
                tokio::fs::remove_dir_all(&path_str).await?;
            } else {
                tokio::fs::remove_dir(&path_str).await?;
            }
            Ok(RecordedValue::None)
        }

        OpKind::ListDir { path } => {
            let path_str = resolver.resolve_to_string(path)
                .ok_or_else(|| ExecutionError::ResolutionFailed(op_id))?;

            let mut entries = tokio::fs::read_dir(&path_str).await?;
            let mut items = Vec::new();
            while let Some(entry) = entries.next_entry().await? {
                if let Some(name) = entry.file_name().to_str() {
                    items.push(RecordedValue::String(name.to_string()));
                }
            }
            Ok(RecordedValue::List(items))
        }

        OpKind::CopyFile { src, dst } => {
            let src_str = resolver.resolve_to_string(src)
                .ok_or_else(|| ExecutionError::ResolutionFailed(op_id))?;
            let dst_str = resolver.resolve_to_string(dst)
                .ok_or_else(|| ExecutionError::ResolutionFailed(op_id))?;

            tokio::fs::copy(&src_str, &dst_str).await?;
            Ok(RecordedValue::None)
        }

        OpKind::MoveFile { src, dst } => {
            let src_str = resolver.resolve_to_string(src)
                .ok_or_else(|| ExecutionError::ResolutionFailed(op_id))?;
            let dst_str = resolver.resolve_to_string(dst)
                .ok_or_else(|| ExecutionError::ResolutionFailed(op_id))?;

            tokio::fs::rename(&src_str, &dst_str).await?;
            Ok(RecordedValue::None)
        }

        OpKind::FileSize { path } => {
            let path_str = resolver.resolve_to_string(path)
                .ok_or_else(|| ExecutionError::ResolutionFailed(op_id))?;

            let metadata = tokio::fs::metadata(&path_str).await?;
            Ok(RecordedValue::Int(metadata.len() as i64))
        }

        OpKind::HttpRequest { method, url, headers, body } => {
            let method_str = resolver.resolve_to_string(method)
                .ok_or_else(|| ExecutionError::ResolutionFailed(op_id))?;
            let url_str = resolver.resolve_to_string(url)
                .ok_or_else(|| ExecutionError::ResolutionFailed(op_id))?;
            let body_opt = resolver.resolve_to_string(body);
            let headers_val = resolver.resolve(headers);

            let req_method = method_str.parse::<reqwest::Method>()
                .map_err(|e| ExecutionError::HttpError(e.to_string()))?;

            let mut request = http_client.request(req_method, &url_str);

            if let Some(RecordedValue::Dict(h)) = headers_val {
                for (key, value) in h {
                    if let RecordedValue::String(v) = value {
                        request = request.header(&key, &v);
                    }
                }
            }

            if let Some(b) = body_opt {
                if !b.is_empty() && b != "None" {
                    request = request.body(b);
                }
            }

            let response = request.send().await
                .map_err(|e| ExecutionError::HttpError(e.to_string()))?;

            let status = response.status().as_u16() as i64;
            let mut response_headers = BTreeMap::new();
            for (key, value) in response.headers() {
                if let Ok(v) = value.to_str() {
                    response_headers.insert(
                        key.as_str().to_string(),
                        RecordedValue::String(v.to_string()),
                    );
                }
            }
            let body = response.text().await
                .map_err(|e| ExecutionError::HttpError(e.to_string()))?;

            let mut result = BTreeMap::new();
            result.insert("status".to_string(), RecordedValue::Int(status));
            result.insert("headers".to_string(), RecordedValue::Dict(response_headers));
            result.insert("body".to_string(), RecordedValue::String(body));

            Ok(RecordedValue::Dict(result))
        }

        OpKind::Exec { command, args } => {
            let cmd_str = resolver.resolve_to_string(command)
                .ok_or_else(|| ExecutionError::ResolutionFailed(op_id))?;
            let args_list = resolver.resolve_to_list(args).unwrap_or_default();

            let args_vec: Vec<String> = args_list
                .iter()
                .filter_map(|v| match v {
                    RecordedValue::String(s) => Some(s.clone()),
                    _ => None,
                })
                .collect();

            let output = tokio::process::Command::new(&cmd_str)
                .args(&args_vec)
                .output()
                .await
                .map_err(|e| ExecutionError::CommandFailed(e.to_string()))?;

            let mut result = BTreeMap::new();
            result.insert("code".to_string(), RecordedValue::Int(output.status.code().unwrap_or(-1) as i64));
            result.insert("stdout".to_string(), RecordedValue::String(String::from_utf8_lossy(&output.stdout).to_string()));
            result.insert("stderr".to_string(), RecordedValue::String(String::from_utf8_lossy(&output.stderr).to_string()));

            Ok(RecordedValue::Dict(result))
        }

        OpKind::EnvGet { name, default } => {
            let name_str = resolver.resolve_to_string(name)
                .ok_or_else(|| ExecutionError::ResolutionFailed(op_id))?;
            let default_str = resolver.resolve_to_string(default);

            let value = std::env::var(&name_str)
                .unwrap_or_else(|_| default_str.unwrap_or_default());
            Ok(RecordedValue::String(value))
        }

        OpKind::Sleep { seconds } => {
            let secs = resolver.resolve_to_float(seconds)
                .ok_or_else(|| ExecutionError::ResolutionFailed(op_id))?;

            tokio::time::sleep(std::time::Duration::from_secs_f64(secs)).await;
            Ok(RecordedValue::None)
        }

        OpKind::Print { message } => {
            let msg = resolver.resolve_to_string(message)
                .ok_or_else(|| ExecutionError::ResolutionFailed(op_id))?;

            println!("{}", msg);
            Ok(RecordedValue::None)
        }

        OpKind::Now => {
            let timestamp = SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .map(|d| d.as_secs_f64())
                .unwrap_or(0.0);
            Ok(RecordedValue::Float(timestamp))
        }

        OpKind::JsonEncode { value } => {
            let val = resolver.resolve(value)
                .ok_or_else(|| ExecutionError::ResolutionFailed(op_id))?;

            let json = recorded_value_to_json(&val)?;
            Ok(RecordedValue::String(json.to_string()))
        }

        OpKind::JsonDecode { string } => {
            let s = resolver.resolve_to_string(string)
                .ok_or_else(|| ExecutionError::ResolutionFailed(op_id))?;

            let json: serde_json::Value = serde_json::from_str(&s)?;
            Ok(json_to_recorded_value(&json))
        }

        OpKind::All { ops } => {
            let results: Vec<RecordedValue> = ops
                .iter()
                .filter_map(|op_id| cache.get_value(*op_id))
                .collect();
            Ok(RecordedValue::List(results))
        }

        OpKind::Any { ops } => {
            for op_id in ops {
                if let Some(value) = cache.get_value(*op_id) {
                    return Ok(value);
                }
            }
            Ok(RecordedValue::None)
        }

        OpKind::AtLeast { ops, count } => {
            let completed: Vec<RecordedValue> = ops
                .iter()
                .filter_map(|op_id| cache.get_value(*op_id))
                .collect();

            Ok(RecordedValue::Bool(completed.len() >= *count))
        }

        OpKind::AtMost { ops, count } => {
            let completed: Vec<RecordedValue> = ops
                .iter()
                .filter_map(|op_id| cache.get_value(*op_id))
                .collect();

            Ok(RecordedValue::Bool(completed.len() <= *count))
        }

        OpKind::After { value, .. } => {
            cache
                .get_value(*value)
                .ok_or_else(|| ExecutionError::MissingDependency(op_id, *value))
        }

        OpKind::TcpConnect { .. } | OpKind::TcpSend { .. } | OpKind::TcpRecv { .. } | OpKind::TcpClose { .. } |
        OpKind::TcpListen { .. } | OpKind::TcpAccept { .. } => {
            Err(ExecutionError::InvalidOp("TCP operations require direct execution mode (use builtins)".to_string()))
        }

        OpKind::UdpBind { .. } | OpKind::UdpSendTo { .. } | OpKind::UdpRecvFrom { .. } | OpKind::UdpClose { .. } => {
            Err(ExecutionError::InvalidOp("UDP operations require direct execution mode (use builtins)".to_string()))
        }

        OpKind::UnixConnect { .. } | OpKind::UnixSend { .. } | OpKind::UnixRecv { .. } | OpKind::UnixClose { .. } |
        OpKind::UnixListen { .. } | OpKind::UnixAccept { .. } => {
            Err(ExecutionError::InvalidOp("Unix socket operations require direct execution mode (use builtins)".to_string()))
        }

        OpKind::ToBool { value } => {
            let v = resolver.resolve(value)
                .ok_or_else(|| ExecutionError::ResolutionFailed(op_id))?;
            Ok(RecordedValue::Bool(is_truthy(&v)))
        }

        OpKind::ToInt { value } => {
            let v = resolver.resolve(value)
                .ok_or_else(|| ExecutionError::ResolutionFailed(op_id))?;
            match v {
                RecordedValue::Int(i) => Ok(RecordedValue::Int(i)),
                RecordedValue::Float(f) => Ok(RecordedValue::Int(f as i64)),
                RecordedValue::Bool(b) => Ok(RecordedValue::Int(if b { 1 } else { 0 })),
                RecordedValue::String(s) => {
                    let i = s.trim().parse::<i64>()
                        .map_err(|_| ExecutionError::InvalidOp(format!("Cannot convert '{}' to int", s)))?;
                    Ok(RecordedValue::Int(i))
                }
                _ => Err(ExecutionError::InvalidOp("Cannot convert to int".to_string()))
            }
        }

        OpKind::ToFloat { value } => {
            let v = resolver.resolve(value)
                .ok_or_else(|| ExecutionError::ResolutionFailed(op_id))?;
            match v {
                RecordedValue::Float(f) => Ok(RecordedValue::Float(f)),
                RecordedValue::Int(i) => Ok(RecordedValue::Float(i as f64)),
                RecordedValue::Bool(b) => Ok(RecordedValue::Float(if b { 1.0 } else { 0.0 })),
                RecordedValue::String(s) => {
                    let f = s.trim().parse::<f64>()
                        .map_err(|_| ExecutionError::InvalidOp(format!("Cannot convert '{}' to float", s)))?;
                    Ok(RecordedValue::Float(f))
                }
                _ => Err(ExecutionError::InvalidOp("Cannot convert to float".to_string()))
            }
        }

        OpKind::ToStr { value } => {
            let v = resolver.resolve(value)
                .ok_or_else(|| ExecutionError::ResolutionFailed(op_id))?;
            Ok(RecordedValue::String(recorded_value_to_string(&v)))
        }

        OpKind::Len { value } => {
            let v = resolver.resolve(value)
                .ok_or_else(|| ExecutionError::ResolutionFailed(op_id))?;
            match v {
                RecordedValue::String(s) => Ok(RecordedValue::Int(s.len() as i64)),
                RecordedValue::List(l) => Ok(RecordedValue::Int(l.len() as i64)),
                RecordedValue::Dict(d) => Ok(RecordedValue::Int(d.len() as i64)),
                RecordedValue::Bytes(b) => Ok(RecordedValue::Int(b.len() as i64)),
                _ => Err(ExecutionError::InvalidOp("Cannot get length of this type".to_string()))
            }
        }

        OpKind::Add { left, right } => {
            let l = resolver.resolve(left)
                .ok_or_else(|| ExecutionError::ResolutionFailed(op_id))?;
            let r = resolver.resolve(right)
                .ok_or_else(|| ExecutionError::ResolutionFailed(op_id))?;
            match (l, r) {
                (RecordedValue::Int(a), RecordedValue::Int(b)) => Ok(RecordedValue::Int(a + b)),
                (RecordedValue::Float(a), RecordedValue::Float(b)) => Ok(RecordedValue::Float(a + b)),
                (RecordedValue::Int(a), RecordedValue::Float(b)) => Ok(RecordedValue::Float(a as f64 + b)),
                (RecordedValue::Float(a), RecordedValue::Int(b)) => Ok(RecordedValue::Float(a + b as f64)),
                (RecordedValue::String(a), RecordedValue::String(b)) => Ok(RecordedValue::String(a + &b)),
                (RecordedValue::List(mut a), RecordedValue::List(b)) => {
                    a.extend(b);
                    Ok(RecordedValue::List(a))
                }
                _ => Err(ExecutionError::InvalidOp("Cannot add these types".to_string()))
            }
        }

        OpKind::Sub { left, right } => {
            let l = resolver.resolve(left)
                .ok_or_else(|| ExecutionError::ResolutionFailed(op_id))?;
            let r = resolver.resolve(right)
                .ok_or_else(|| ExecutionError::ResolutionFailed(op_id))?;
            match (l, r) {
                (RecordedValue::Int(a), RecordedValue::Int(b)) => Ok(RecordedValue::Int(a - b)),
                (RecordedValue::Float(a), RecordedValue::Float(b)) => Ok(RecordedValue::Float(a - b)),
                (RecordedValue::Int(a), RecordedValue::Float(b)) => Ok(RecordedValue::Float(a as f64 - b)),
                (RecordedValue::Float(a), RecordedValue::Int(b)) => Ok(RecordedValue::Float(a - b as f64)),
                _ => Err(ExecutionError::InvalidOp("Cannot subtract these types".to_string()))
            }
        }

        OpKind::Mul { left, right } => {
            let l = resolver.resolve(left)
                .ok_or_else(|| ExecutionError::ResolutionFailed(op_id))?;
            let r = resolver.resolve(right)
                .ok_or_else(|| ExecutionError::ResolutionFailed(op_id))?;
            match (l, r) {
                (RecordedValue::Int(a), RecordedValue::Int(b)) => Ok(RecordedValue::Int(a * b)),
                (RecordedValue::Float(a), RecordedValue::Float(b)) => Ok(RecordedValue::Float(a * b)),
                (RecordedValue::Int(a), RecordedValue::Float(b)) => Ok(RecordedValue::Float(a as f64 * b)),
                (RecordedValue::Float(a), RecordedValue::Int(b)) => Ok(RecordedValue::Float(a * b as f64)),
                (RecordedValue::String(s), RecordedValue::Int(n)) => Ok(RecordedValue::String(s.repeat(n.max(0) as usize))),
                (RecordedValue::Int(n), RecordedValue::String(s)) => Ok(RecordedValue::String(s.repeat(n.max(0) as usize))),
                _ => Err(ExecutionError::InvalidOp("Cannot multiply these types".to_string()))
            }
        }

        OpKind::Div { left, right } => {
            let l = resolver.resolve(left)
                .ok_or_else(|| ExecutionError::ResolutionFailed(op_id))?;
            let r = resolver.resolve(right)
                .ok_or_else(|| ExecutionError::ResolutionFailed(op_id))?;
            match (l, r) {
                (RecordedValue::Int(a), RecordedValue::Int(b)) => {
                    if b == 0 {
                        Err(ExecutionError::InvalidOp("Division by zero".to_string()))
                    } else {
                        Ok(RecordedValue::Float(a as f64 / b as f64))
                    }
                }
                (RecordedValue::Float(a), RecordedValue::Float(b)) => {
                    if b == 0.0 {
                        Err(ExecutionError::InvalidOp("Division by zero".to_string()))
                    } else {
                        Ok(RecordedValue::Float(a / b))
                    }
                }
                (RecordedValue::Int(a), RecordedValue::Float(b)) => {
                    if b == 0.0 {
                        Err(ExecutionError::InvalidOp("Division by zero".to_string()))
                    } else {
                        Ok(RecordedValue::Float(a as f64 / b))
                    }
                }
                (RecordedValue::Float(a), RecordedValue::Int(b)) => {
                    if b == 0 {
                        Err(ExecutionError::InvalidOp("Division by zero".to_string()))
                    } else {
                        Ok(RecordedValue::Float(a / b as f64))
                    }
                }
                _ => Err(ExecutionError::InvalidOp("Cannot divide these types".to_string()))
            }
        }

        OpKind::FloorDiv { left, right } => {
            let l = resolver.resolve(left)
                .ok_or_else(|| ExecutionError::ResolutionFailed(op_id))?;
            let r = resolver.resolve(right)
                .ok_or_else(|| ExecutionError::ResolutionFailed(op_id))?;
            match (l, r) {
                (RecordedValue::Int(a), RecordedValue::Int(b)) => {
                    if b == 0 {
                        Err(ExecutionError::InvalidOp("Division by zero".to_string()))
                    } else {
                        Ok(RecordedValue::Int(a / b))
                    }
                }
                (RecordedValue::Float(a), RecordedValue::Float(b)) => {
                    if b == 0.0 {
                        Err(ExecutionError::InvalidOp("Division by zero".to_string()))
                    } else {
                        Ok(RecordedValue::Int((a / b).floor() as i64))
                    }
                }
                (RecordedValue::Int(a), RecordedValue::Float(b)) => {
                    if b == 0.0 {
                        Err(ExecutionError::InvalidOp("Division by zero".to_string()))
                    } else {
                        Ok(RecordedValue::Int((a as f64 / b).floor() as i64))
                    }
                }
                (RecordedValue::Float(a), RecordedValue::Int(b)) => {
                    if b == 0 {
                        Err(ExecutionError::InvalidOp("Division by zero".to_string()))
                    } else {
                        Ok(RecordedValue::Int((a / b as f64).floor() as i64))
                    }
                }
                _ => Err(ExecutionError::InvalidOp("Cannot floor-divide these types".to_string()))
            }
        }

        OpKind::Mod { left, right } => {
            let l = resolver.resolve(left)
                .ok_or_else(|| ExecutionError::ResolutionFailed(op_id))?;
            let r = resolver.resolve(right)
                .ok_or_else(|| ExecutionError::ResolutionFailed(op_id))?;
            match (l, r) {
                (RecordedValue::Int(a), RecordedValue::Int(b)) => {
                    if b == 0 {
                        Err(ExecutionError::InvalidOp("Modulo by zero".to_string()))
                    } else {
                        Ok(RecordedValue::Int(a % b))
                    }
                }
                (RecordedValue::Float(a), RecordedValue::Float(b)) => {
                    if b == 0.0 {
                        Err(ExecutionError::InvalidOp("Modulo by zero".to_string()))
                    } else {
                        Ok(RecordedValue::Float(a % b))
                    }
                }
                _ => Err(ExecutionError::InvalidOp("Cannot modulo these types".to_string()))
            }
        }

        OpKind::Neg { value } => {
            let v = resolver.resolve(value)
                .ok_or_else(|| ExecutionError::ResolutionFailed(op_id))?;
            match v {
                RecordedValue::Int(i) => Ok(RecordedValue::Int(-i)),
                RecordedValue::Float(f) => Ok(RecordedValue::Float(-f)),
                _ => Err(ExecutionError::InvalidOp("Cannot negate this type".to_string()))
            }
        }

        OpKind::Eq { left, right } => {
            let l = resolver.resolve(left)
                .ok_or_else(|| ExecutionError::ResolutionFailed(op_id))?;
            let r = resolver.resolve(right)
                .ok_or_else(|| ExecutionError::ResolutionFailed(op_id))?;
            Ok(RecordedValue::Bool(l == r))
        }

        OpKind::Ne { left, right } => {
            let l = resolver.resolve(left)
                .ok_or_else(|| ExecutionError::ResolutionFailed(op_id))?;
            let r = resolver.resolve(right)
                .ok_or_else(|| ExecutionError::ResolutionFailed(op_id))?;
            Ok(RecordedValue::Bool(l != r))
        }

        OpKind::Lt { left, right } => {
            let l = resolver.resolve(left)
                .ok_or_else(|| ExecutionError::ResolutionFailed(op_id))?;
            let r = resolver.resolve(right)
                .ok_or_else(|| ExecutionError::ResolutionFailed(op_id))?;
            Ok(RecordedValue::Bool(compare_values(&l, &r)? == std::cmp::Ordering::Less))
        }

        OpKind::Le { left, right } => {
            let l = resolver.resolve(left)
                .ok_or_else(|| ExecutionError::ResolutionFailed(op_id))?;
            let r = resolver.resolve(right)
                .ok_or_else(|| ExecutionError::ResolutionFailed(op_id))?;
            Ok(RecordedValue::Bool(compare_values(&l, &r)? != std::cmp::Ordering::Greater))
        }

        OpKind::Gt { left, right } => {
            let l = resolver.resolve(left)
                .ok_or_else(|| ExecutionError::ResolutionFailed(op_id))?;
            let r = resolver.resolve(right)
                .ok_or_else(|| ExecutionError::ResolutionFailed(op_id))?;
            Ok(RecordedValue::Bool(compare_values(&l, &r)? == std::cmp::Ordering::Greater))
        }

        OpKind::Ge { left, right } => {
            let l = resolver.resolve(left)
                .ok_or_else(|| ExecutionError::ResolutionFailed(op_id))?;
            let r = resolver.resolve(right)
                .ok_or_else(|| ExecutionError::ResolutionFailed(op_id))?;
            Ok(RecordedValue::Bool(compare_values(&l, &r)? != std::cmp::Ordering::Less))
        }

        OpKind::Not { value } => {
            let v = resolver.resolve(value)
                .ok_or_else(|| ExecutionError::ResolutionFailed(op_id))?;
            Ok(RecordedValue::Bool(!is_truthy(&v)))
        }

        OpKind::Concat { left, right } => {
            let l = resolver.resolve(left)
                .ok_or_else(|| ExecutionError::ResolutionFailed(op_id))?;
            let r = resolver.resolve(right)
                .ok_or_else(|| ExecutionError::ResolutionFailed(op_id))?;
            match (l, r) {
                (RecordedValue::String(a), RecordedValue::String(b)) => Ok(RecordedValue::String(a + &b)),
                (RecordedValue::List(mut a), RecordedValue::List(b)) => {
                    a.extend(b);
                    Ok(RecordedValue::List(a))
                }
                _ => Err(ExecutionError::InvalidOp("Cannot concatenate these types".to_string()))
            }
        }

        OpKind::Contains { haystack, needle } => {
            let h = resolver.resolve(haystack)
                .ok_or_else(|| ExecutionError::ResolutionFailed(op_id))?;
            let n = resolver.resolve(needle)
                .ok_or_else(|| ExecutionError::ResolutionFailed(op_id))?;
            match h {
                RecordedValue::String(s) => {
                    if let RecordedValue::String(sub) = n {
                        Ok(RecordedValue::Bool(s.contains(&sub)))
                    } else {
                        Err(ExecutionError::InvalidOp("Can only search for string in string".to_string()))
                    }
                }
                RecordedValue::List(l) => Ok(RecordedValue::Bool(l.contains(&n))),
                RecordedValue::Dict(d) => {
                    if let RecordedValue::String(key) = n {
                        Ok(RecordedValue::Bool(d.contains_key(&key)))
                    } else {
                        Err(ExecutionError::InvalidOp("Dict keys must be strings".to_string()))
                    }
                }
                _ => Err(ExecutionError::InvalidOp("Cannot check containment for this type".to_string()))
            }
        }

        OpKind::If { condition, then_value, else_value } => {
            let cond = resolver.resolve(condition)
                .ok_or_else(|| ExecutionError::ResolutionFailed(op_id))?;
            let then_v = resolver.resolve(then_value)
                .ok_or_else(|| ExecutionError::ResolutionFailed(op_id))?;
            let else_v = resolver.resolve(else_value)
                .ok_or_else(|| ExecutionError::ResolutionFailed(op_id))?;

            if is_truthy(&cond) {
                Ok(then_v)
            } else {
                Ok(else_v)
            }
        }

        OpKind::Index { base, index } => {
            let b = resolver.resolve(base)
                .ok_or_else(|| ExecutionError::ResolutionFailed(op_id))?;
            let idx = resolver.resolve(index)
                .ok_or_else(|| ExecutionError::ResolutionFailed(op_id))?;

            match (b, idx) {
                (RecordedValue::List(list), RecordedValue::Int(i)) => {
                    let index = if i < 0 {
                        (list.len() as i64 + i) as usize
                    } else {
                        i as usize
                    };
                    list.get(index).cloned().ok_or_else(|| {
                        ExecutionError::InvalidOp(format!("Index {} out of bounds for list of length {}", i, list.len()))
                    })
                }
                (RecordedValue::Dict(dict), RecordedValue::String(key)) => {
                    dict.get(&key).cloned().ok_or_else(|| {
                        ExecutionError::InvalidOp(format!("Key '{}' not found in dict", key))
                    })
                }
                (RecordedValue::String(s), RecordedValue::Int(i)) => {
                    let chars: Vec<char> = s.chars().collect();
                    let index = if i < 0 {
                        (chars.len() as i64 + i) as usize
                    } else {
                        i as usize
                    };
                    chars.get(index).map(|c| RecordedValue::String(c.to_string())).ok_or_else(|| {
                        ExecutionError::InvalidOp(format!("Index {} out of bounds for string of length {}", i, chars.len()))
                    })
                }
                _ => Err(ExecutionError::InvalidOp("Invalid index operation".to_string()))
            }
        }

        OpKind::SetIndex { base, index, value } => {
            let mut b = resolver.resolve(base)
                .ok_or_else(|| ExecutionError::ResolutionFailed(op_id))?;
            let idx = resolver.resolve(index)
                .ok_or_else(|| ExecutionError::ResolutionFailed(op_id))?;
            let val = resolver.resolve(value)
                .ok_or_else(|| ExecutionError::ResolutionFailed(op_id))?;

            match (&mut b, idx) {
                (RecordedValue::List(ref mut list), RecordedValue::Int(i)) => {
                    let index = if i < 0 {
                        (list.len() as i64 + i) as usize
                    } else {
                        i as usize
                    };
                    if index >= list.len() {
                        return Err(ExecutionError::InvalidOp(format!(
                            "Index {} out of bounds for list of length {}", i, list.len()
                        )));
                    }
                    list[index] = val;
                    Ok(b)
                }
                (RecordedValue::Dict(ref mut dict), RecordedValue::String(key)) => {
                    dict.insert(key, val);
                    Ok(b)
                }
                _ => Err(ExecutionError::InvalidOp("Invalid set index operation".to_string()))
            }
        }

        OpKind::Min { values } => {
            let vals = resolver.resolve(values)
                .ok_or_else(|| ExecutionError::ResolutionFailed(op_id))?;
            match vals {
                RecordedValue::List(list) if !list.is_empty() => {
                    list.iter().min_by(|a, b| compare_recorded_values(a, b)).cloned()
                        .ok_or_else(|| ExecutionError::InvalidOp("min() arg is empty".to_string()))
                }
                _ => Err(ExecutionError::InvalidOp("min() requires non-empty list".to_string()))
            }
        }

        OpKind::Max { values } => {
            let vals = resolver.resolve(values)
                .ok_or_else(|| ExecutionError::ResolutionFailed(op_id))?;
            match vals {
                RecordedValue::List(list) if !list.is_empty() => {
                    list.iter().max_by(|a, b| compare_recorded_values(a, b)).cloned()
                        .ok_or_else(|| ExecutionError::InvalidOp("max() arg is empty".to_string()))
                }
                _ => Err(ExecutionError::InvalidOp("max() requires non-empty list".to_string()))
            }
        }

        OpKind::Sum { values, start } => {
            let vals = resolver.resolve(values)
                .ok_or_else(|| ExecutionError::ResolutionFailed(op_id))?;
            let start_val = resolver.resolve(start)
                .ok_or_else(|| ExecutionError::ResolutionFailed(op_id))?;
            let start_int = match start_val {
                RecordedValue::Int(i) => i,
                _ => 0,
            };
            match vals {
                RecordedValue::List(list) => {
                    let sum: i64 = list.iter().filter_map(|v| {
                        if let RecordedValue::Int(i) = v { Some(*i) } else { None }
                    }).sum();
                    Ok(RecordedValue::Int(start_int + sum))
                }
                _ => Err(ExecutionError::InvalidOp("sum() requires list".to_string()))
            }
        }

        OpKind::Abs { value } => {
            let val = resolver.resolve(value)
                .ok_or_else(|| ExecutionError::ResolutionFailed(op_id))?;
            match val {
                RecordedValue::Int(i) => Ok(RecordedValue::Int(i.abs())),
                RecordedValue::Float(f) => Ok(RecordedValue::Float(f.abs())),
                _ => Err(ExecutionError::InvalidOp("abs() requires number".to_string()))
            }
        }

        OpKind::Sorted { values } => {
            let vals = resolver.resolve(values)
                .ok_or_else(|| ExecutionError::ResolutionFailed(op_id))?;
            match vals {
                RecordedValue::List(mut list) => {
                    list.sort_by(compare_recorded_values);
                    Ok(RecordedValue::List(list))
                }
                _ => Err(ExecutionError::InvalidOp("sorted() requires list".to_string()))
            }
        }

        OpKind::Reversed { values } => {
            let vals = resolver.resolve(values)
                .ok_or_else(|| ExecutionError::ResolutionFailed(op_id))?;
            match vals {
                RecordedValue::List(mut list) => {
                    list.reverse();
                    Ok(RecordedValue::List(list))
                }
                RecordedValue::String(s) => {
                    Ok(RecordedValue::String(s.chars().rev().collect()))
                }
                _ => Err(ExecutionError::InvalidOp("reversed() requires list or string".to_string()))
            }
        }

        // Generator ops
        OpKind::GeneratorDef { name, params, body } => {
            // Store the generator definition as a spec
            let mut spec = BTreeMap::new();
            spec.insert("generator_kind".to_string(), RecordedValue::String("definition".to_string()));
            spec.insert("name".to_string(), RecordedValue::String(name.clone()));
            spec.insert("params".to_string(), RecordedValue::List(
                params.iter().map(|p| RecordedValue::String(p.clone())).collect()
            ));
            spec.insert("body".to_string(), subplan_to_spec(body));
            Ok(RecordedValue::Dict(spec))
        }

        OpKind::GeneratorYield { .. } => {
            // GeneratorYield should only be executed inside a SubPlan
            Err(ExecutionError::InvalidOp("GeneratorYield should only be used inside generator bodies".to_string()))
        }

        OpKind::GeneratorYieldIf { .. } => {
            // GeneratorYieldIf should only be executed inside a SubPlan
            Err(ExecutionError::InvalidOp("GeneratorYieldIf should only be used inside generator bodies".to_string()))
        }

        OpKind::ParamRef { .. } => {
            Err(ExecutionError::InvalidOp("ParamRef should only be used inside SubPlans".to_string()))
        }

        OpKind::ForEach { items, item_name, body, parallel } => {
            let items_value = resolver.resolve(items)
                .ok_or_else(|| ExecutionError::ResolutionFailed(op_id))?;

            let items_list = match items_value {
                RecordedValue::List(l) => l,
                _ => return Err(ExecutionError::InvalidOp(
                    format!("ForEach items must be a list, got {:?}", items_value)
                )),
            };

            if *parallel && items_list.len() > 1 {
                execute_foreach_parallel(
                    &items_list,
                    item_name,
                    body,
                    cache,
                    http_client,
                ).await
            } else {
                execute_foreach_sequential(
                    &items_list,
                    item_name,
                    body,
                    cache,
                    http_client,
                ).await
            }
        }

        OpKind::Map { items, item_name, body } => {
            let items_value = resolver.resolve(items)
                .ok_or_else(|| ExecutionError::ResolutionFailed(op_id))?;

            let items_list = match items_value {
                RecordedValue::List(l) => l,
                _ => return Err(ExecutionError::InvalidOp(
                    format!("Map items must be a list, got {:?}", items_value)
                )),
            };

            execute_map_parallel(&items_list, item_name, body, cache, http_client).await
        }

        OpKind::Filter { items, item_name, predicate } => {
            let items_value = resolver.resolve(items)
                .ok_or_else(|| ExecutionError::ResolutionFailed(op_id))?;

            let items_list = match items_value {
                RecordedValue::List(l) => l,
                _ => return Err(ExecutionError::InvalidOp(
                    format!("Filter items must be a list, got {:?}", items_value)
                )),
            };

            execute_filter_parallel(&items_list, item_name, predicate, cache, http_client).await
        }

        OpKind::IfBlock { condition, then_body, else_body } => {
            let cond_value = resolver.resolve(condition)
                .ok_or_else(|| ExecutionError::ResolutionFailed(op_id))?;

            let body_to_execute = if is_truthy(&cond_value) {
                Some(then_body)
            } else {
                else_body.as_ref()
            };

            match body_to_execute {
                Some(body) => {
                    let params = HashMap::new();
                    let (result, _signal) = execute_subplan_with_signal(
                        body,
                        &params,
                        cache,
                        http_client,
                    ).await?;
                    Ok(result)
                }
                None => Ok(RecordedValue::None),
            }
        }

        OpKind::Break => {
            Err(ExecutionError::InvalidOp("Break should only be used inside loop bodies".to_string()))
        }

        OpKind::Continue => {
            Err(ExecutionError::InvalidOp("Continue should only be used inside loop bodies".to_string()))
        }

        OpKind::FrozenValue { value, .. } => {
            let result_value = resolver.resolve(value)
                .ok_or_else(|| ExecutionError::ResolutionFailed(op_id))?;
            Ok(result_value)
        }
    }
}

fn compare_recorded_values(a: &RecordedValue, b: &RecordedValue) -> std::cmp::Ordering {
    match (a, b) {
        (RecordedValue::Int(x), RecordedValue::Int(y)) => x.cmp(y),
        (RecordedValue::Float(x), RecordedValue::Float(y)) => x.partial_cmp(y).unwrap_or(std::cmp::Ordering::Equal),
        (RecordedValue::String(x), RecordedValue::String(y)) => x.cmp(y),
        _ => std::cmp::Ordering::Equal,
    }
}

fn is_truthy(value: &RecordedValue) -> bool {
    match value {
        RecordedValue::None => false,
        RecordedValue::Bool(b) => *b,
        RecordedValue::Int(i) => *i != 0,
        RecordedValue::Float(f) => *f != 0.0,
        RecordedValue::String(s) => !s.is_empty(),
        RecordedValue::Bytes(b) => !b.is_empty(),
        RecordedValue::List(l) => !l.is_empty(),
        RecordedValue::Dict(d) => !d.is_empty(),
    }
}

fn recorded_value_to_string(value: &RecordedValue) -> String {
    match value {
        RecordedValue::None => "None".to_string(),
        RecordedValue::Bool(b) => if *b { "True".to_string() } else { "False".to_string() },
        RecordedValue::Int(i) => i.to_string(),
        RecordedValue::Float(f) => f.to_string(),
        RecordedValue::String(s) => s.clone(),
        RecordedValue::Bytes(b) => format!("<bytes len={}>", b.len()),
        RecordedValue::List(l) => {
            let items: Vec<String> = l.iter().map(recorded_value_to_string).collect();
            format!("[{}]", items.join(", "))
        }
        RecordedValue::Dict(d) => {
            let items: Vec<String> = d.iter()
                .map(|(k, v)| format!("{}: {}", k, recorded_value_to_string(v)))
                .collect();
            format!("{{{}}}", items.join(", "))
        }
    }
}

fn compare_values(left: &RecordedValue, right: &RecordedValue) -> ExecutionResult<std::cmp::Ordering> {
    match (left, right) {
        (RecordedValue::Int(a), RecordedValue::Int(b)) => Ok(a.cmp(b)),
        (RecordedValue::Float(a), RecordedValue::Float(b)) => {
            Ok(a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal))
        }
        (RecordedValue::Int(a), RecordedValue::Float(b)) => {
            Ok((*a as f64).partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal))
        }
        (RecordedValue::Float(a), RecordedValue::Int(b)) => {
            Ok(a.partial_cmp(&(*b as f64)).unwrap_or(std::cmp::Ordering::Equal))
        }
        (RecordedValue::String(a), RecordedValue::String(b)) => Ok(a.cmp(b)),
        _ => Err(ExecutionError::InvalidOp("Cannot compare these types".to_string()))
    }
}

fn recorded_value_to_json(value: &RecordedValue) -> serde_json::Result<serde_json::Value> {
    Ok(match value {
        RecordedValue::None => serde_json::Value::Null,
        RecordedValue::Bool(b) => serde_json::Value::Bool(*b),
        RecordedValue::Int(i) => serde_json::Value::Number(serde_json::Number::from(*i)),
        RecordedValue::Float(f) => {
            serde_json::Number::from_f64(*f)
                .map(serde_json::Value::Number)
                .unwrap_or(serde_json::Value::Null)
        }
        RecordedValue::String(s) => serde_json::Value::String(s.clone()),
        RecordedValue::Bytes(b) => serde_json::Value::String(base64_encode(b)),
        RecordedValue::List(list) => {
            let items: Result<Vec<_>, _> = list.iter().map(recorded_value_to_json).collect();
            serde_json::Value::Array(items?)
        }
        RecordedValue::Dict(dict) => {
            let mut map = serde_json::Map::new();
            for (k, v) in dict {
                map.insert(k.clone(), recorded_value_to_json(v)?);
            }
            serde_json::Value::Object(map)
        }
    })
}

fn json_to_recorded_value(json: &serde_json::Value) -> RecordedValue {
    match json {
        serde_json::Value::Null => RecordedValue::None,
        serde_json::Value::Bool(b) => RecordedValue::Bool(*b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                RecordedValue::Int(i)
            } else if let Some(f) = n.as_f64() {
                RecordedValue::Float(f)
            } else {
                RecordedValue::None
            }
        }
        serde_json::Value::String(s) => RecordedValue::String(s.clone()),
        serde_json::Value::Array(arr) => {
            RecordedValue::List(arr.iter().map(json_to_recorded_value).collect())
        }
        serde_json::Value::Object(obj) => {
            let mut dict = BTreeMap::new();
            for (k, v) in obj {
                dict.insert(k.clone(), json_to_recorded_value(v));
            }
            RecordedValue::Dict(dict)
        }
    }
}

/// Execute a SubPlan synchronously with given parameter bindings.
/// Returns (output_value, yields) where yields are collected GeneratorYield values.
#[allow(dead_code)]
fn execute_subplan_sync(
    subplan: &SubPlan,
    params: &HashMap<String, RecordedValue>,
    parent_cache: &OpCache,
) -> ExecutionResult<(RecordedValue, Vec<RecordedValue>)> {
    let mut local_cache: HashMap<OpId, RecordedValue> = HashMap::new();
    let mut yields: Vec<RecordedValue> = Vec::new();

    // Execute ops in order (SubPlans are assumed to be topologically sorted)
    for op in &subplan.ops {
        let value = execute_subplan_op(&op.kind, op.id, &local_cache, params, parent_cache, &mut yields)?;
        local_cache.insert(op.id, value);
    }

    // Get output value
    let output = local_cache.get(&subplan.output)
        .cloned()
        .unwrap_or(RecordedValue::None);

    Ok((output, yields))
}

fn resolve_value_ref_subplan(
    vref: &ValueRef,
    local_cache: &HashMap<OpId, RecordedValue>,
    parent_cache: &OpCache,
) -> Option<RecordedValue> {
    match vref {
        ValueRef::Literal(v) => Some(v.clone()),
        ValueRef::OpOutput { op, path } => {
            let base = local_cache.get(op)
                .cloned()
                .or_else(|| parent_cache.get_value(*op))?;

            let mut current = base;
            for accessor in path {
                current = match accessor {
                    Accessor::Field(name) => {
                        if let RecordedValue::Dict(d) = current {
                            d.get(name)?.clone()
                        } else {
                            return None;
                        }
                    }
                    Accessor::Index(i) => {
                        if let RecordedValue::List(l) = current {
                            let idx = if *i < 0 { (l.len() as i64 + *i) as usize } else { *i as usize };
                            l.get(idx)?.clone()
                        } else {
                            return None;
                        }
                    }
                };
            }
            Some(current)
        }
        ValueRef::Dynamic(_) => None,
        ValueRef::List(items) => {
            let resolved: Option<Vec<RecordedValue>> = items.iter()
                .map(|item| resolve_value_ref_subplan(item, local_cache, parent_cache))
                .collect();
            resolved.map(RecordedValue::List)
        }
    }
}

/// Execute a single op within a SubPlan context
#[allow(dead_code)]
fn execute_subplan_op(
    kind: &OpKind,
    op_id: OpId,
    local_cache: &HashMap<OpId, RecordedValue>,
    params: &HashMap<String, RecordedValue>,
    parent_cache: &OpCache,
    yields: &mut Vec<RecordedValue>,
) -> ExecutionResult<RecordedValue> {
    let resolve = |vref: &ValueRef| -> Option<RecordedValue> {
        resolve_value_ref_subplan(vref, local_cache, parent_cache)
    };

    match kind {
        // ParamRef - look up parameter value
        OpKind::ParamRef { name } => {
            params.get(name)
                .cloned()
                .ok_or_else(|| ExecutionError::InvalidOp(format!("Unknown parameter: {}", name)))
        }

        // GeneratorYield - collect the value
        OpKind::GeneratorYield { value } => {
            let v = resolve(value)
                .ok_or_else(|| ExecutionError::ResolutionFailed(op_id))?;
            yields.push(v.clone());
            Ok(v)
        }

        // GeneratorYieldIf - conditionally collect value
        OpKind::GeneratorYieldIf { condition, value } => {
            let cond = resolve(condition)
                .ok_or_else(|| ExecutionError::ResolutionFailed(op_id))?;
            let v = resolve(value)
                .ok_or_else(|| ExecutionError::ResolutionFailed(op_id))?;

            if is_truthy(&cond) {
                yields.push(v.clone());
            }
            Ok(v)
        }

        // Basic arithmetic ops for SubPlan
        OpKind::Add { left, right } => {
            let l = resolve(left).ok_or_else(|| ExecutionError::ResolutionFailed(op_id))?;
            let r = resolve(right).ok_or_else(|| ExecutionError::ResolutionFailed(op_id))?;
            match (l, r) {
                (RecordedValue::Int(a), RecordedValue::Int(b)) => Ok(RecordedValue::Int(a + b)),
                (RecordedValue::Float(a), RecordedValue::Float(b)) => Ok(RecordedValue::Float(a + b)),
                (RecordedValue::Int(a), RecordedValue::Float(b)) => Ok(RecordedValue::Float(a as f64 + b)),
                (RecordedValue::Float(a), RecordedValue::Int(b)) => Ok(RecordedValue::Float(a + b as f64)),
                (RecordedValue::String(a), RecordedValue::String(b)) => Ok(RecordedValue::String(a + &b)),
                _ => Err(ExecutionError::InvalidOp("Cannot add these types".to_string()))
            }
        }

        OpKind::Sub { left, right } => {
            let l = resolve(left).ok_or_else(|| ExecutionError::ResolutionFailed(op_id))?;
            let r = resolve(right).ok_or_else(|| ExecutionError::ResolutionFailed(op_id))?;
            match (l, r) {
                (RecordedValue::Int(a), RecordedValue::Int(b)) => Ok(RecordedValue::Int(a - b)),
                (RecordedValue::Float(a), RecordedValue::Float(b)) => Ok(RecordedValue::Float(a - b)),
                (RecordedValue::Int(a), RecordedValue::Float(b)) => Ok(RecordedValue::Float(a as f64 - b)),
                (RecordedValue::Float(a), RecordedValue::Int(b)) => Ok(RecordedValue::Float(a - b as f64)),
                _ => Err(ExecutionError::InvalidOp("Cannot subtract these types".to_string()))
            }
        }

        OpKind::Mul { left, right } => {
            let l = resolve(left).ok_or_else(|| ExecutionError::ResolutionFailed(op_id))?;
            let r = resolve(right).ok_or_else(|| ExecutionError::ResolutionFailed(op_id))?;
            match (l, r) {
                (RecordedValue::Int(a), RecordedValue::Int(b)) => Ok(RecordedValue::Int(a * b)),
                (RecordedValue::Float(a), RecordedValue::Float(b)) => Ok(RecordedValue::Float(a * b)),
                (RecordedValue::Int(a), RecordedValue::Float(b)) => Ok(RecordedValue::Float(a as f64 * b)),
                (RecordedValue::Float(a), RecordedValue::Int(b)) => Ok(RecordedValue::Float(a * b as f64)),
                _ => Err(ExecutionError::InvalidOp("Cannot multiply these types".to_string()))
            }
        }

        OpKind::Mod { left, right } => {
            let l = resolve(left).ok_or_else(|| ExecutionError::ResolutionFailed(op_id))?;
            let r = resolve(right).ok_or_else(|| ExecutionError::ResolutionFailed(op_id))?;
            match (l, r) {
                (RecordedValue::Int(a), RecordedValue::Int(b)) => {
                    if b == 0 { Err(ExecutionError::InvalidOp("Modulo by zero".to_string())) }
                    else { Ok(RecordedValue::Int(a % b)) }
                }
                _ => Err(ExecutionError::InvalidOp("Modulo requires integers".to_string()))
            }
        }

        // Comparisons
        OpKind::Lt { left, right } => {
            let l = resolve(left).ok_or_else(|| ExecutionError::ResolutionFailed(op_id))?;
            let r = resolve(right).ok_or_else(|| ExecutionError::ResolutionFailed(op_id))?;
            Ok(RecordedValue::Bool(compare_values(&l, &r)? == std::cmp::Ordering::Less))
        }

        OpKind::Le { left, right } => {
            let l = resolve(left).ok_or_else(|| ExecutionError::ResolutionFailed(op_id))?;
            let r = resolve(right).ok_or_else(|| ExecutionError::ResolutionFailed(op_id))?;
            Ok(RecordedValue::Bool(compare_values(&l, &r)? != std::cmp::Ordering::Greater))
        }

        OpKind::Gt { left, right } => {
            let l = resolve(left).ok_or_else(|| ExecutionError::ResolutionFailed(op_id))?;
            let r = resolve(right).ok_or_else(|| ExecutionError::ResolutionFailed(op_id))?;
            Ok(RecordedValue::Bool(compare_values(&l, &r)? == std::cmp::Ordering::Greater))
        }

        OpKind::Ge { left, right } => {
            let l = resolve(left).ok_or_else(|| ExecutionError::ResolutionFailed(op_id))?;
            let r = resolve(right).ok_or_else(|| ExecutionError::ResolutionFailed(op_id))?;
            Ok(RecordedValue::Bool(compare_values(&l, &r)? != std::cmp::Ordering::Less))
        }

        OpKind::Eq { left, right } => {
            let l = resolve(left).ok_or_else(|| ExecutionError::ResolutionFailed(op_id))?;
            let r = resolve(right).ok_or_else(|| ExecutionError::ResolutionFailed(op_id))?;
            Ok(RecordedValue::Bool(l == r))
        }

        OpKind::Ne { left, right } => {
            let l = resolve(left).ok_or_else(|| ExecutionError::ResolutionFailed(op_id))?;
            let r = resolve(right).ok_or_else(|| ExecutionError::ResolutionFailed(op_id))?;
            Ok(RecordedValue::Bool(l != r))
        }

        OpKind::Not { value } => {
            let v = resolve(value).ok_or_else(|| ExecutionError::ResolutionFailed(op_id))?;
            Ok(RecordedValue::Bool(!is_truthy(&v)))
        }

        OpKind::If { condition, then_value, else_value } => {
            let cond = resolve(condition).ok_or_else(|| ExecutionError::ResolutionFailed(op_id))?;
            let then_v = resolve(then_value).ok_or_else(|| ExecutionError::ResolutionFailed(op_id))?;
            let else_v = resolve(else_value).ok_or_else(|| ExecutionError::ResolutionFailed(op_id))?;
            Ok(if is_truthy(&cond) { then_v } else { else_v })
        }

        // Other ops that might be used in SubPlans
        _ => Err(ExecutionError::InvalidOp(format!("Op {:?} not supported in SubPlan", kind)))
    }
}

/// Serialize a SubPlan to a spec for storage in cache
fn subplan_to_spec(subplan: &SubPlan) -> RecordedValue {
    let ops_json = serde_json::to_string(&subplan.ops).unwrap_or_default();
    let mut spec = BTreeMap::new();
    spec.insert("params".to_string(), RecordedValue::List(
        subplan.params.iter().map(|p| RecordedValue::String(p.clone())).collect()
    ));
    spec.insert("ops_json".to_string(), RecordedValue::String(ops_json));
    spec.insert("output".to_string(), RecordedValue::Int(subplan.output.0 as i64));
    RecordedValue::Dict(spec)
}

/// Deserialize a SubPlan from a spec
#[allow(dead_code)]
fn spec_to_subplan(spec: &RecordedValue) -> ExecutionResult<SubPlan> {
    let dict = match spec {
        RecordedValue::Dict(d) => d,
        _ => return Err(ExecutionError::InvalidOp("Invalid SubPlan spec".to_string())),
    };

    let params = match dict.get("params") {
        Some(RecordedValue::List(l)) => l.iter().filter_map(|v| {
            if let RecordedValue::String(s) = v { Some(s.clone()) } else { None }
        }).collect(),
        _ => return Err(ExecutionError::InvalidOp("Invalid SubPlan params".to_string())),
    };

    let ops_json = match dict.get("ops_json") {
        Some(RecordedValue::String(s)) => s,
        _ => return Err(ExecutionError::InvalidOp("Invalid SubPlan ops".to_string())),
    };

    let ops: Vec<blueprint_common::Op> = serde_json::from_str(ops_json)
        .map_err(|e| ExecutionError::InvalidOp(format!("Failed to parse SubPlan ops: {}", e)))?;

    let output = match dict.get("output") {
        Some(RecordedValue::Int(n)) => OpId(*n as u64),
        _ => return Err(ExecutionError::InvalidOp("Invalid SubPlan output".to_string())),
    };

    Ok(SubPlan { params, ops, output })
}

fn execute_foreach_sequential<'a>(
    items: &'a [RecordedValue],
    item_name: &'a str,
    body: &'a SubPlan,
    cache: &'a Arc<OpCache>,
    http_client: &'a reqwest::Client,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = ExecutionResult<RecordedValue>> + Send + 'a>> {
    Box::pin(async move {
        let mut results = Vec::new();

        for item in items {
            let mut params = HashMap::new();
            params.insert(item_name.to_string(), item.clone());

            let (result, signal) = execute_subplan_with_signal(body, &params, cache, http_client).await?;
            results.push(result);

            match signal {
                LoopSignal::Break => break,
                LoopSignal::Continue => continue,
                LoopSignal::None => {}
            }
        }

        Ok(RecordedValue::List(results))
    })
}

fn execute_foreach_parallel<'a>(
    items: &'a [RecordedValue],
    item_name: &'a str,
    body: &'a SubPlan,
    cache: &'a Arc<OpCache>,
    http_client: &'a reqwest::Client,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = ExecutionResult<RecordedValue>> + Send + 'a>> {
    Box::pin(async move {
        use futures::future::join_all;

        let futures: Vec<_> = items.iter().map(|item| {
            let mut params = HashMap::new();
            params.insert(item_name.to_string(), item.clone());
            let body = body.clone();
            let cache = Arc::clone(cache);
            let http_client = http_client.clone();

            async move {
                execute_subplan_with_signal(&body, &params, &cache, &http_client).await
            }
        }).collect();

        let results: Result<Vec<_>, _> = join_all(futures)
            .await
            .into_iter()
            .collect();

        let results = results?;
        Ok(RecordedValue::List(results.into_iter().map(|(v, _)| v).collect()))
    })
}

fn execute_map_parallel<'a>(
    items: &'a [RecordedValue],
    item_name: &'a str,
    body: &'a SubPlan,
    cache: &'a Arc<OpCache>,
    http_client: &'a reqwest::Client,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = ExecutionResult<RecordedValue>> + Send + 'a>> {
    Box::pin(async move {
        use futures::future::join_all;

        let futures: Vec<_> = items.iter().map(|item| {
            let mut params = HashMap::new();
            params.insert(item_name.to_string(), item.clone());
            let body = body.clone();
            let cache = Arc::clone(cache);
            let http_client = http_client.clone();

            async move {
                execute_subplan_with_signal(&body, &params, &cache, &http_client).await
            }
        }).collect();

        let results: Result<Vec<_>, _> = join_all(futures)
            .await
            .into_iter()
            .collect();

        let results = results?;
        Ok(RecordedValue::List(results.into_iter().map(|(v, _)| v).collect()))
    })
}

fn execute_filter_parallel<'a>(
    items: &'a [RecordedValue],
    item_name: &'a str,
    predicate: &'a SubPlan,
    cache: &'a Arc<OpCache>,
    http_client: &'a reqwest::Client,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = ExecutionResult<RecordedValue>> + Send + 'a>> {
    Box::pin(async move {
        use futures::future::join_all;

        let futures: Vec<_> = items.iter().map(|item| {
            let mut params = HashMap::new();
            params.insert(item_name.to_string(), item.clone());
            let predicate = predicate.clone();
            let cache = Arc::clone(cache);
            let http_client = http_client.clone();
            let item_clone = item.clone();

            async move {
                let (result, _) = execute_subplan_with_signal(&predicate, &params, &cache, &http_client).await?;
                Ok::<_, ExecutionError>((item_clone, is_truthy(&result)))
            }
        }).collect();

        let results: Result<Vec<_>, _> = join_all(futures)
            .await
            .into_iter()
            .collect();

        let results = results?;
        let filtered: Vec<RecordedValue> = results
            .into_iter()
            .filter_map(|(item, include)| if include { Some(item) } else { None })
            .collect();

        Ok(RecordedValue::List(filtered))
    })
}

fn execute_subplan_with_signal<'a>(
    subplan: &'a SubPlan,
    params: &'a HashMap<String, RecordedValue>,
    parent_cache: &'a Arc<OpCache>,
    http_client: &'a reqwest::Client,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = ExecutionResult<(RecordedValue, LoopSignal)>> + Send + 'a>> {
    Box::pin(async move {
        let mut local_results: HashMap<OpId, RecordedValue> = HashMap::new();
        let mut signal = LoopSignal::None;

        for op in &subplan.ops {
            if let Some(guard_id) = op.guard {
                let guard_value = local_results.get(&guard_id)
                    .cloned()
                    .or_else(|| parent_cache.get_value(guard_id));

                if let Some(gv) = guard_value {
                    if !is_truthy(&gv) {
                        local_results.insert(op.id, RecordedValue::None);
                        continue;
                    }
                } else {
                    local_results.insert(op.id, RecordedValue::None);
                    continue;
                }
            }

            match &op.kind {
                OpKind::Break => {
                    signal = LoopSignal::Break;
                    local_results.insert(op.id, RecordedValue::None);
                    break;
                }
                OpKind::Continue => {
                    signal = LoopSignal::Continue;
                    local_results.insert(op.id, RecordedValue::None);
                    break;
                }
                _ => {
                    let value = execute_subplan_op_async(
                        &op.kind,
                        op.id,
                        &local_results,
                        params,
                        parent_cache,
                        http_client,
                    ).await?;
                    local_results.insert(op.id, value);
                }
            }
        }

        let output = local_results.get(&subplan.output)
            .cloned()
            .unwrap_or(RecordedValue::None);

        Ok((output, signal))
    })
}

fn resolve_value_ref_async(
    vref: &ValueRef,
    local_results: &HashMap<OpId, RecordedValue>,
    params: &HashMap<String, RecordedValue>,
    parent_cache: &Arc<OpCache>,
) -> Option<RecordedValue> {
    match vref {
        ValueRef::Literal(v) => Some(v.clone()),
        ValueRef::OpOutput { op, path } => {
            let base = local_results.get(op)
                .cloned()
                .or_else(|| parent_cache.get_value(*op))?;

            let mut current = base;
            for accessor in path {
                current = match accessor {
                    Accessor::Field(name) => {
                        if let RecordedValue::Dict(d) = current {
                            d.get(name)?.clone()
                        } else {
                            return None;
                        }
                    }
                    Accessor::Index(i) => {
                        if let RecordedValue::List(l) = current {
                            let idx = if *i < 0 { (l.len() as i64 + *i) as usize } else { *i as usize };
                            l.get(idx)?.clone()
                        } else {
                            return None;
                        }
                    }
                };
            }
            Some(current)
        }
        ValueRef::Dynamic(name) => params.get(name).cloned(),
        ValueRef::List(items) => {
            let resolved: Option<Vec<RecordedValue>> = items.iter()
                .map(|item| resolve_value_ref_async(item, local_results, params, parent_cache))
                .collect();
            resolved.map(RecordedValue::List)
        }
    }
}

fn execute_subplan_op_async<'a>(
    kind: &'a OpKind,
    op_id: OpId,
    local_results: &'a HashMap<OpId, RecordedValue>,
    params: &'a HashMap<String, RecordedValue>,
    parent_cache: &'a Arc<OpCache>,
    http_client: &'a reqwest::Client,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = ExecutionResult<RecordedValue>> + Send + 'a>> {
    Box::pin(async move {
    let resolve = |vref: &ValueRef| -> Option<RecordedValue> {
        resolve_value_ref_async(vref, local_results, params, parent_cache)
    };

    match kind {
        OpKind::ParamRef { name } => {
            params.get(name)
                .cloned()
                .ok_or_else(|| ExecutionError::InvalidOp(format!("Unknown parameter: {}", name)))
        }

        OpKind::Add { left, right } => {
            let l = resolve(left).ok_or_else(|| ExecutionError::ResolutionFailed(op_id))?;
            let r = resolve(right).ok_or_else(|| ExecutionError::ResolutionFailed(op_id))?;
            match (l, r) {
                (RecordedValue::Int(a), RecordedValue::Int(b)) => Ok(RecordedValue::Int(a + b)),
                (RecordedValue::Float(a), RecordedValue::Float(b)) => Ok(RecordedValue::Float(a + b)),
                (RecordedValue::Int(a), RecordedValue::Float(b)) => Ok(RecordedValue::Float(a as f64 + b)),
                (RecordedValue::Float(a), RecordedValue::Int(b)) => Ok(RecordedValue::Float(a + b as f64)),
                (RecordedValue::String(a), RecordedValue::String(b)) => Ok(RecordedValue::String(a + &b)),
                _ => Err(ExecutionError::InvalidOp("Cannot add these types".to_string()))
            }
        }

        OpKind::Sub { left, right } => {
            let l = resolve(left).ok_or_else(|| ExecutionError::ResolutionFailed(op_id))?;
            let r = resolve(right).ok_or_else(|| ExecutionError::ResolutionFailed(op_id))?;
            match (l, r) {
                (RecordedValue::Int(a), RecordedValue::Int(b)) => Ok(RecordedValue::Int(a - b)),
                (RecordedValue::Float(a), RecordedValue::Float(b)) => Ok(RecordedValue::Float(a - b)),
                (RecordedValue::Int(a), RecordedValue::Float(b)) => Ok(RecordedValue::Float(a as f64 - b)),
                (RecordedValue::Float(a), RecordedValue::Int(b)) => Ok(RecordedValue::Float(a - b as f64)),
                _ => Err(ExecutionError::InvalidOp("Cannot subtract these types".to_string()))
            }
        }

        OpKind::Mul { left, right } => {
            let l = resolve(left).ok_or_else(|| ExecutionError::ResolutionFailed(op_id))?;
            let r = resolve(right).ok_or_else(|| ExecutionError::ResolutionFailed(op_id))?;
            match (l, r) {
                (RecordedValue::Int(a), RecordedValue::Int(b)) => Ok(RecordedValue::Int(a * b)),
                (RecordedValue::Float(a), RecordedValue::Float(b)) => Ok(RecordedValue::Float(a * b)),
                (RecordedValue::Int(a), RecordedValue::Float(b)) => Ok(RecordedValue::Float(a as f64 * b)),
                (RecordedValue::Float(a), RecordedValue::Int(b)) => Ok(RecordedValue::Float(a * b as f64)),
                _ => Err(ExecutionError::InvalidOp("Cannot multiply these types".to_string()))
            }
        }

        OpKind::Div { left, right } => {
            let l = resolve(left).ok_or_else(|| ExecutionError::ResolutionFailed(op_id))?;
            let r = resolve(right).ok_or_else(|| ExecutionError::ResolutionFailed(op_id))?;
            match (l, r) {
                (RecordedValue::Int(a), RecordedValue::Int(b)) => {
                    if b == 0 { Err(ExecutionError::InvalidOp("Division by zero".to_string())) }
                    else { Ok(RecordedValue::Float(a as f64 / b as f64)) }
                }
                (RecordedValue::Float(a), RecordedValue::Float(b)) => {
                    if b == 0.0 { Err(ExecutionError::InvalidOp("Division by zero".to_string())) }
                    else { Ok(RecordedValue::Float(a / b)) }
                }
                _ => Err(ExecutionError::InvalidOp("Cannot divide these types".to_string()))
            }
        }

        OpKind::Mod { left, right } => {
            let l = resolve(left).ok_or_else(|| ExecutionError::ResolutionFailed(op_id))?;
            let r = resolve(right).ok_or_else(|| ExecutionError::ResolutionFailed(op_id))?;
            match (l, r) {
                (RecordedValue::Int(a), RecordedValue::Int(b)) => {
                    if b == 0 { Err(ExecutionError::InvalidOp("Modulo by zero".to_string())) }
                    else { Ok(RecordedValue::Int(a % b)) }
                }
                _ => Err(ExecutionError::InvalidOp("Modulo requires integers".to_string()))
            }
        }

        OpKind::Lt { left, right } => {
            let l = resolve(left).ok_or_else(|| ExecutionError::ResolutionFailed(op_id))?;
            let r = resolve(right).ok_or_else(|| ExecutionError::ResolutionFailed(op_id))?;
            Ok(RecordedValue::Bool(compare_values(&l, &r)? == std::cmp::Ordering::Less))
        }

        OpKind::Le { left, right } => {
            let l = resolve(left).ok_or_else(|| ExecutionError::ResolutionFailed(op_id))?;
            let r = resolve(right).ok_or_else(|| ExecutionError::ResolutionFailed(op_id))?;
            Ok(RecordedValue::Bool(compare_values(&l, &r)? != std::cmp::Ordering::Greater))
        }

        OpKind::Gt { left, right } => {
            let l = resolve(left).ok_or_else(|| ExecutionError::ResolutionFailed(op_id))?;
            let r = resolve(right).ok_or_else(|| ExecutionError::ResolutionFailed(op_id))?;
            Ok(RecordedValue::Bool(compare_values(&l, &r)? == std::cmp::Ordering::Greater))
        }

        OpKind::Ge { left, right } => {
            let l = resolve(left).ok_or_else(|| ExecutionError::ResolutionFailed(op_id))?;
            let r = resolve(right).ok_or_else(|| ExecutionError::ResolutionFailed(op_id))?;
            Ok(RecordedValue::Bool(compare_values(&l, &r)? != std::cmp::Ordering::Less))
        }

        OpKind::Eq { left, right } => {
            let l = resolve(left).ok_or_else(|| ExecutionError::ResolutionFailed(op_id))?;
            let r = resolve(right).ok_or_else(|| ExecutionError::ResolutionFailed(op_id))?;
            Ok(RecordedValue::Bool(l == r))
        }

        OpKind::Ne { left, right } => {
            let l = resolve(left).ok_or_else(|| ExecutionError::ResolutionFailed(op_id))?;
            let r = resolve(right).ok_or_else(|| ExecutionError::ResolutionFailed(op_id))?;
            Ok(RecordedValue::Bool(l != r))
        }

        OpKind::Not { value } => {
            let v = resolve(value).ok_or_else(|| ExecutionError::ResolutionFailed(op_id))?;
            Ok(RecordedValue::Bool(!is_truthy(&v)))
        }

        OpKind::If { condition, then_value, else_value } => {
            let cond = resolve(condition).ok_or_else(|| ExecutionError::ResolutionFailed(op_id))?;
            let then_v = resolve(then_value).ok_or_else(|| ExecutionError::ResolutionFailed(op_id))?;
            let else_v = resolve(else_value).ok_or_else(|| ExecutionError::ResolutionFailed(op_id))?;
            Ok(if is_truthy(&cond) { then_v } else { else_v })
        }

        OpKind::Contains { haystack, needle } => {
            let h = resolve(haystack).ok_or_else(|| ExecutionError::ResolutionFailed(op_id))?;
            let n = resolve(needle).ok_or_else(|| ExecutionError::ResolutionFailed(op_id))?;
            match h {
                RecordedValue::String(s) => {
                    if let RecordedValue::String(sub) = n {
                        Ok(RecordedValue::Bool(s.contains(&sub)))
                    } else {
                        Err(ExecutionError::InvalidOp("Can only search for string in string".to_string()))
                    }
                }
                RecordedValue::List(l) => Ok(RecordedValue::Bool(l.contains(&n))),
                RecordedValue::Dict(d) => {
                    if let RecordedValue::String(key) = n {
                        Ok(RecordedValue::Bool(d.contains_key(&key)))
                    } else {
                        Err(ExecutionError::InvalidOp("Dict keys must be strings".to_string()))
                    }
                }
                _ => Err(ExecutionError::InvalidOp("Cannot check containment for this type".to_string()))
            }
        }

        OpKind::Index { base, index } => {
            let b = resolve(base).ok_or_else(|| ExecutionError::ResolutionFailed(op_id))?;
            let idx = resolve(index).ok_or_else(|| ExecutionError::ResolutionFailed(op_id))?;
            match (b, idx) {
                (RecordedValue::List(list), RecordedValue::Int(i)) => {
                    let index = if i < 0 { (list.len() as i64 + i) as usize } else { i as usize };
                    list.get(index).cloned().ok_or_else(|| {
                        ExecutionError::InvalidOp(format!("Index {} out of bounds", i))
                    })
                }
                (RecordedValue::Dict(dict), RecordedValue::String(key)) => {
                    dict.get(&key).cloned().ok_or_else(|| {
                        ExecutionError::InvalidOp(format!("Key '{}' not found", key))
                    })
                }
                (RecordedValue::String(s), RecordedValue::Int(i)) => {
                    let chars: Vec<char> = s.chars().collect();
                    let index = if i < 0 { (chars.len() as i64 + i) as usize } else { i as usize };
                    chars.get(index).map(|c| RecordedValue::String(c.to_string())).ok_or_else(|| {
                        ExecutionError::InvalidOp(format!("Index {} out of bounds", i))
                    })
                }
                _ => Err(ExecutionError::InvalidOp("Invalid index operation".to_string()))
            }
        }

        OpKind::SetIndex { base, index, value } => {
            let mut b = resolve(base).ok_or_else(|| ExecutionError::ResolutionFailed(op_id))?;
            let idx = resolve(index).ok_or_else(|| ExecutionError::ResolutionFailed(op_id))?;
            let val = resolve(value).ok_or_else(|| ExecutionError::ResolutionFailed(op_id))?;

            match (&mut b, idx) {
                (RecordedValue::List(ref mut list), RecordedValue::Int(i)) => {
                    let index = if i < 0 { (list.len() as i64 + i) as usize } else { i as usize };
                    if index >= list.len() {
                        return Err(ExecutionError::InvalidOp(format!(
                            "Index {} out of bounds for list of length {}", i, list.len()
                        )));
                    }
                    list[index] = val;
                    Ok(b)
                }
                (RecordedValue::Dict(ref mut dict), RecordedValue::String(key)) => {
                    dict.insert(key, val);
                    Ok(b)
                }
                _ => Err(ExecutionError::InvalidOp("Invalid set index operation".to_string()))
            }
        }

        OpKind::Len { value } => {
            let v = resolve(value).ok_or_else(|| ExecutionError::ResolutionFailed(op_id))?;
            match v {
                RecordedValue::String(s) => Ok(RecordedValue::Int(s.len() as i64)),
                RecordedValue::List(l) => Ok(RecordedValue::Int(l.len() as i64)),
                RecordedValue::Dict(d) => Ok(RecordedValue::Int(d.len() as i64)),
                RecordedValue::Bytes(b) => Ok(RecordedValue::Int(b.len() as i64)),
                _ => Err(ExecutionError::InvalidOp("Cannot get length of this type".to_string()))
            }
        }

        OpKind::Concat { left, right } => {
            let l = resolve(left).ok_or_else(|| ExecutionError::ResolutionFailed(op_id))?;
            let r = resolve(right).ok_or_else(|| ExecutionError::ResolutionFailed(op_id))?;
            match (l, r) {
                (RecordedValue::String(a), RecordedValue::String(b)) => Ok(RecordedValue::String(a + &b)),
                (RecordedValue::List(mut a), RecordedValue::List(b)) => {
                    a.extend(b);
                    Ok(RecordedValue::List(a))
                }
                _ => Err(ExecutionError::InvalidOp("Cannot concatenate these types".to_string()))
            }
        }

        OpKind::Print { message } => {
            let msg = match resolve(message) {
                Some(RecordedValue::String(s)) => s,
                Some(v) => format!("{}", v),
                None => return Err(ExecutionError::ResolutionFailed(op_id)),
            };
            println!("{}", msg);
            Ok(RecordedValue::None)
        }

        OpKind::ToBool { value } => {
            let v = resolve(value).ok_or_else(|| ExecutionError::ResolutionFailed(op_id))?;
            Ok(RecordedValue::Bool(is_truthy(&v)))
        }

        OpKind::ToInt { value } => {
            let v = resolve(value).ok_or_else(|| ExecutionError::ResolutionFailed(op_id))?;
            match v {
                RecordedValue::Int(i) => Ok(RecordedValue::Int(i)),
                RecordedValue::Float(f) => Ok(RecordedValue::Int(f as i64)),
                RecordedValue::Bool(b) => Ok(RecordedValue::Int(if b { 1 } else { 0 })),
                RecordedValue::String(s) => {
                    s.trim().parse::<i64>()
                        .map(RecordedValue::Int)
                        .map_err(|_| ExecutionError::InvalidOp(format!("Cannot convert '{}' to int", s)))
                }
                _ => Err(ExecutionError::InvalidOp("Cannot convert to int".to_string()))
            }
        }

        OpKind::ToStr { value } => {
            let v = resolve(value).ok_or_else(|| ExecutionError::ResolutionFailed(op_id))?;
            Ok(RecordedValue::String(recorded_value_to_string(&v)))
        }

        OpKind::WriteFile { path, content } => {
            let path_str = match resolve(path) {
                Some(RecordedValue::String(s)) => s,
                Some(v) => format!("{}", v),
                None => return Err(ExecutionError::ResolutionFailed(op_id)),
            };
            let content_str = match resolve(content) {
                Some(RecordedValue::String(s)) => s,
                Some(v) => format!("{}", v),
                None => return Err(ExecutionError::ResolutionFailed(op_id)),
            };
            tokio::fs::write(&path_str, &content_str).await?;
            Ok(RecordedValue::None)
        }

        OpKind::ReadFile { path } => {
            let path_str = match resolve(path) {
                Some(RecordedValue::String(s)) => s,
                Some(v) => format!("{}", v),
                None => return Err(ExecutionError::ResolutionFailed(op_id)),
            };
            let content = tokio::fs::read_to_string(&path_str).await?;
            Ok(RecordedValue::String(content))
        }

        OpKind::HttpRequest { method, url, headers, body } => {
            let method_str = match resolve(method) {
                Some(RecordedValue::String(s)) => s,
                _ => return Err(ExecutionError::ResolutionFailed(op_id)),
            };
            let url_str = match resolve(url) {
                Some(RecordedValue::String(s)) => s,
                _ => return Err(ExecutionError::ResolutionFailed(op_id)),
            };
            let body_opt = match resolve(body) {
                Some(RecordedValue::String(s)) if !s.is_empty() && s != "None" => Some(s),
                _ => None,
            };
            let headers_val = resolve(headers);

            let req_method = method_str.parse::<reqwest::Method>()
                .map_err(|e| ExecutionError::HttpError(e.to_string()))?;

            let mut request = http_client.request(req_method, &url_str);

            if let Some(RecordedValue::Dict(h)) = headers_val {
                for (key, value) in h {
                    if let RecordedValue::String(v) = value {
                        request = request.header(&key, &v);
                    }
                }
            }

            if let Some(b) = body_opt {
                request = request.body(b);
            }

            let response = request.send().await
                .map_err(|e| ExecutionError::HttpError(e.to_string()))?;

            let status = response.status().as_u16() as i64;
            let mut response_headers = BTreeMap::new();
            for (key, value) in response.headers() {
                if let Ok(v) = value.to_str() {
                    response_headers.insert(key.as_str().to_string(), RecordedValue::String(v.to_string()));
                }
            }
            let body = response.text().await.map_err(|e| ExecutionError::HttpError(e.to_string()))?;

            let mut result = BTreeMap::new();
            result.insert("status".to_string(), RecordedValue::Int(status));
            result.insert("headers".to_string(), RecordedValue::Dict(response_headers));
            result.insert("body".to_string(), RecordedValue::String(body));

            Ok(RecordedValue::Dict(result))
        }

        OpKind::ForEach { items, item_name, body, parallel } => {
            let items_value = resolve(items)
                .ok_or_else(|| ExecutionError::ResolutionFailed(op_id))?;

            let items_list = match items_value {
                RecordedValue::List(l) => l,
                _ => return Err(ExecutionError::InvalidOp(
                    format!("ForEach items must be a list, got {:?}", items_value)
                )),
            };

            if *parallel && items_list.len() > 1 {
                execute_foreach_parallel(&items_list, item_name, body, parent_cache, http_client).await
            } else {
                execute_foreach_sequential(&items_list, item_name, body, parent_cache, http_client).await
            }
        }

        OpKind::IfBlock { condition, then_body, else_body } => {
            let cond_value = resolve(condition)
                .ok_or_else(|| ExecutionError::ResolutionFailed(op_id))?;

            let body_to_execute = if is_truthy(&cond_value) {
                Some(then_body)
            } else {
                else_body.as_ref()
            };

            match body_to_execute {
                Some(body) => {
                    let (result, _signal) = execute_subplan_with_signal(body, params, parent_cache, http_client).await?;
                    Ok(result)
                }
                None => Ok(RecordedValue::None),
            }
        }

        OpKind::GeneratorYield { value } => {
            let v = resolve(value).ok_or_else(|| ExecutionError::ResolutionFailed(op_id))?;
            Ok(v)
        }

        OpKind::GeneratorYieldIf { condition, value } => {
            let cond = resolve(condition).ok_or_else(|| ExecutionError::ResolutionFailed(op_id))?;
            let v = resolve(value).ok_or_else(|| ExecutionError::ResolutionFailed(op_id))?;
            if is_truthy(&cond) { Ok(v) } else { Ok(RecordedValue::None) }
        }

        _ => Err(ExecutionError::InvalidOp(format!("Op {:?} not yet supported in SubPlan", kind.name())))
    }
    })
}

fn base64_encode(bytes: &[u8]) -> String {
    const ALPHABET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut result = String::new();

    for chunk in bytes.chunks(3) {
        let b0 = chunk[0] as usize;
        let b1 = chunk.get(1).copied().unwrap_or(0) as usize;
        let b2 = chunk.get(2).copied().unwrap_or(0) as usize;

        result.push(ALPHABET[b0 >> 2] as char);
        result.push(ALPHABET[((b0 & 0x03) << 4) | (b1 >> 4)] as char);

        if chunk.len() > 1 {
            result.push(ALPHABET[((b1 & 0x0f) << 2) | (b2 >> 6)] as char);
        } else {
            result.push('=');
        }

        if chunk.len() > 2 {
            result.push(ALPHABET[b2 & 0x3f] as char);
        } else {
            result.push('=');
        }
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use blueprint_common::Plan;

    #[tokio::test]
    async fn test_executor_creation() {
        let executor = BlueprintInterpreter::new();
        assert!(!executor.dry_run);
    }

    #[tokio::test]
    async fn test_dry_run_mode() {
        let executor = BlueprintInterpreter::new().with_dry_run(true);
        assert!(executor.dry_run);
    }

    #[tokio::test]
    async fn test_execute_empty_plan() {
        let mut executor = BlueprintInterpreter::new();
        let plan = Plan::new();
        executor.add_plan(&plan);

        let result = executor.run().await;
        assert!(result.is_ok());
        assert!(executor.cache().is_empty());
    }

    #[tokio::test]
    async fn test_execute_now_op() {
        let mut executor = BlueprintInterpreter::new();
        let mut plan = Plan::new();
        plan.add_op(OpKind::Now, None);
        executor.add_plan(&plan);

        let result = executor.run().await;
        assert!(result.is_ok());

        let value = executor.cache().get_value(OpId(0));
        assert!(matches!(value, Some(RecordedValue::Float(_))));
    }

    #[test]
    fn test_json_roundtrip() {
        let value = RecordedValue::Dict({
            let mut map = BTreeMap::new();
            map.insert("key".to_string(), RecordedValue::String("value".to_string()));
            map.insert("number".to_string(), RecordedValue::Int(42));
            map
        });

        let json = recorded_value_to_json(&value).unwrap();
        let back = json_to_recorded_value(&json);

        assert_eq!(value, back);
    }

    #[tokio::test]
    async fn test_streaming_executor_creation() {
        let executor = BlueprintInterpreter::new();
        assert!(!executor.dry_run);
        assert_eq!(executor.max_concurrent, DEFAULT_MAX_CONCURRENT);
    }

    #[tokio::test]
    async fn test_streaming_executor_dry_run() {
        let executor = BlueprintInterpreter::new().with_dry_run(true);
        assert!(executor.dry_run);
    }

    #[tokio::test]
    async fn test_streaming_executor_max_concurrent() {
        let executor = BlueprintInterpreter::new().with_max_concurrent(8);
        assert_eq!(executor.max_concurrent, 8);
    }

    #[tokio::test]
    async fn test_streaming_execute_empty_plan() {
        let mut executor = BlueprintInterpreter::new();
        let plan = Plan::new();

        let result = executor.execute(&plan).await;
        assert!(result.is_ok());
        assert!(result.unwrap().is_empty());
    }

    #[tokio::test]
    async fn test_streaming_execute_now_op() {
        let mut executor = BlueprintInterpreter::new();
        let mut plan = Plan::new();
        plan.add_op(OpKind::Now, None);

        let result = executor.execute(&plan).await;
        assert!(result.is_ok());

        let cache = result.unwrap();
        let value = cache.get_value(OpId(0));
        assert!(matches!(value, Some(RecordedValue::Float(_))));
    }

    #[tokio::test]
    async fn test_streaming_execute_chain() {
        use blueprint_common::op::ValueRef;

        let mut executor = BlueprintInterpreter::new();
        let mut plan = Plan::new();

        let op0 = plan.add_op(OpKind::Now, None);
        let _op1 = plan.add_op(
            OpKind::Print {
                message: ValueRef::op_output(op0),
            },
            None,
        );

        let result = executor.execute(&plan).await;
        assert!(result.is_ok());

        let cache = result.unwrap();
        assert_eq!(cache.len(), 2);
    }

    #[tokio::test]
    async fn test_streaming_execute_parallel() {
        let mut executor = BlueprintInterpreter::new();
        let mut plan = Plan::new();

        plan.add_op(OpKind::Now, None);
        plan.add_op(OpKind::Now, None);
        plan.add_op(OpKind::Now, None);

        let result = executor.execute(&plan).await;
        assert!(result.is_ok());

        let cache = result.unwrap();
        assert_eq!(cache.len(), 3);
    }

    #[tokio::test]
    async fn test_streaming_execute_diamond_dag() {
        use blueprint_common::op::ValueRef;

        let mut executor = BlueprintInterpreter::new();
        let mut plan = Plan::new();

        let a = plan.add_op(OpKind::Now, None);
        let b = plan.add_op(
            OpKind::Print {
                message: ValueRef::op_output(a),
            },
            None,
        );
        let c = plan.add_op(
            OpKind::Print {
                message: ValueRef::op_output(a),
            },
            None,
        );
        let _d = plan.add_op(
            OpKind::All {
                ops: vec![b, c],
            },
            None,
        );

        let result = executor.execute(&plan).await;
        assert!(result.is_ok());

        let cache = result.unwrap();
        assert_eq!(cache.len(), 4);
    }

    #[tokio::test]
    async fn test_streaming_incremental_add_op() {
        use blueprint_common::Op;

        let mut executor = BlueprintInterpreter::new();

        let op = Op {
            id: OpId(0),
            kind: OpKind::Now,
            inputs: vec![],
            source_location: None,
            guard: None,
        };

        executor.add_op(op);
        executor.run().await.unwrap();

        let value = executor.cache().get_value(OpId(0));
        assert!(matches!(value, Some(RecordedValue::Float(_))));
    }

    #[tokio::test]
    async fn test_streaming_incremental_multiple_runs() {
        use blueprint_common::Op;
        use blueprint_common::op::ValueRef;

        let mut executor = BlueprintInterpreter::new();

        let op0 = Op {
            id: OpId(0),
            kind: OpKind::Now,
            inputs: vec![],
            guard: None,
            source_location: None,
        };
        executor.add_op(op0);
        executor.run().await.unwrap();

        assert!(executor.cache().get_value(OpId(0)).is_some());

        let op1 = Op {
            id: OpId(1),
            kind: OpKind::Print {
                message: ValueRef::op_output(OpId(0)),
            },
            inputs: vec![OpId(0)],
            source_location: None,
            guard: None,
        };
        executor.add_op(op1);
        executor.run().await.unwrap();

        assert!(executor.cache().get_value(OpId(1)).is_some());
        assert_eq!(executor.cache().len(), 2);
    }

    #[tokio::test]
    async fn test_streaming_reset() {
        let mut executor = BlueprintInterpreter::new();
        let mut plan = Plan::new();
        plan.add_op(OpKind::Now, None);

        executor.execute(&plan).await.unwrap();
        assert_eq!(executor.cache().len(), 1);

        executor.reset();
        assert_eq!(executor.cache().len(), 0);
        assert!(executor.ops.is_empty());
    }

    #[tokio::test]
    async fn test_streaming_deps_already_satisfied() {
        use blueprint_common::Op;
        use blueprint_common::op::ValueRef;

        let mut executor = BlueprintInterpreter::new();

        let op0 = Op {
            id: OpId(0),
            kind: OpKind::Now,
            inputs: vec![],
            source_location: None,
            guard: None,
        };
        executor.add_op(op0);
        executor.run().await.unwrap();

        let op1 = Op {
            id: OpId(1),
            kind: OpKind::Print {
                message: ValueRef::op_output(OpId(0)),
            },
            inputs: vec![OpId(0)],
            source_location: None,
            guard: None,
        };
        executor.add_op(op1);

        assert!(executor.ready.contains(&OpId(1)));
    }

    #[tokio::test]
    async fn test_streaming_duplicate_add_op_ignored() {
        use blueprint_common::Op;

        let mut executor = BlueprintInterpreter::new();

        let op = Op {
            id: OpId(0),
            kind: OpKind::Now,
            inputs: vec![],
            source_location: None,
            guard: None,
        };

        executor.add_op(op.clone());
        executor.add_op(op);

        assert_eq!(executor.ops.len(), 1);
    }

    #[tokio::test]
    async fn test_foreach_sequential_execution() {
        use blueprint_common::Op;
        use blueprint_common::op::ValueRef;

        let cache = Arc::new(OpCache::new());
        let http_client = reqwest::Client::new();

        let items = vec![
            RecordedValue::Int(1),
            RecordedValue::Int(2),
            RecordedValue::Int(3),
        ];

        let body = SubPlan {
            params: vec!["x".to_string()],
            ops: vec![Op {
                id: OpId(0),
                kind: OpKind::Add {
                    left: ValueRef::Dynamic("x".to_string()),
                    right: ValueRef::literal_int(10),
                },
                inputs: vec![],
                source_location: None,
                guard: None,
            }],
            output: OpId(0),
        };

        let result = execute_foreach_sequential(&items, "x", &body, &cache, &http_client).await;
        assert!(result.is_ok());

        let list = result.unwrap();
        if let RecordedValue::List(results) = list {
            assert_eq!(results.len(), 3);
            assert_eq!(results[0], RecordedValue::Int(11));
            assert_eq!(results[1], RecordedValue::Int(12));
            assert_eq!(results[2], RecordedValue::Int(13));
        } else {
            panic!("Expected list result");
        }
    }

    #[tokio::test]
    async fn test_foreach_with_break() {
        use blueprint_common::Op;
        use blueprint_common::op::ValueRef;

        let cache = Arc::new(OpCache::new());
        let http_client = reqwest::Client::new();

        let items = vec![
            RecordedValue::Int(1),
            RecordedValue::Int(2),
            RecordedValue::Int(3),
        ];

        let body = SubPlan {
            params: vec!["x".to_string()],
            ops: vec![
                Op {
                    id: OpId(0),
                    kind: OpKind::Eq {
                        left: ValueRef::Dynamic("x".to_string()),
                        right: ValueRef::literal_int(2),
                    },
                    inputs: vec![],
                    source_location: None,
                    guard: None,
                },
                Op {
                    id: OpId(1),
                    kind: OpKind::Break,
                    inputs: vec![OpId(0)],
                    source_location: None,
                    guard: Some(OpId(0)),
                },
                Op {
                    id: OpId(2),
                    kind: OpKind::Add {
                        left: ValueRef::Dynamic("x".to_string()),
                        right: ValueRef::literal_int(10),
                    },
                    inputs: vec![],
                    source_location: None,
                    guard: None,
                },
            ],
            output: OpId(2),
        };

        let result = execute_foreach_sequential(&items, "x", &body, &cache, &http_client).await;
        assert!(result.is_ok());

        let list = result.unwrap();
        if let RecordedValue::List(results) = list {
            assert_eq!(results.len(), 2);
            assert_eq!(results[0], RecordedValue::Int(11));
            assert_eq!(results[1], RecordedValue::None);
        } else {
            panic!("Expected list result");
        }
    }

    #[tokio::test]
    async fn test_foreach_parallel_execution() {
        use blueprint_common::Op;
        use blueprint_common::op::ValueRef;

        let cache = Arc::new(OpCache::new());
        let http_client = reqwest::Client::new();

        let items = vec![
            RecordedValue::Int(1),
            RecordedValue::Int(2),
            RecordedValue::Int(3),
        ];

        let body = SubPlan {
            params: vec!["x".to_string()],
            ops: vec![Op {
                id: OpId(0),
                kind: OpKind::Mul {
                    left: ValueRef::Dynamic("x".to_string()),
                    right: ValueRef::literal_int(2),
                },
                inputs: vec![],
                source_location: None,
                guard: None,
            }],
            output: OpId(0),
        };

        let result = execute_foreach_parallel(&items, "x", &body, &cache, &http_client).await;
        assert!(result.is_ok());

        let list = result.unwrap();
        if let RecordedValue::List(results) = list {
            assert_eq!(results.len(), 3);
            assert!(results.contains(&RecordedValue::Int(2)));
            assert!(results.contains(&RecordedValue::Int(4)));
            assert!(results.contains(&RecordedValue::Int(6)));
        } else {
            panic!("Expected list result");
        }
    }

    #[tokio::test]
    async fn test_ifblock_then_branch() {
        use blueprint_common::Op;
        use blueprint_common::op::ValueRef;

        let cache = Arc::new(OpCache::new());
        let http_client = reqwest::Client::new();

        let then_body = SubPlan {
            params: vec![],
            ops: vec![Op {
                id: OpId(0),
                kind: OpKind::Add {
                    left: ValueRef::literal_int(1),
                    right: ValueRef::literal_int(2),
                },
                inputs: vec![],
                source_location: None,
                guard: None,
            }],
            output: OpId(0),
        };

        let params = HashMap::new();
        let (result, signal) = execute_subplan_with_signal(&then_body, &params, &cache, &http_client).await.unwrap();

        assert_eq!(result, RecordedValue::Int(3));
        assert_eq!(signal, LoopSignal::None);
    }

    #[tokio::test]
    async fn test_subplan_with_guard() {
        use blueprint_common::Op;
        use blueprint_common::op::ValueRef;

        let cache = Arc::new(OpCache::new());
        let http_client = reqwest::Client::new();

        let subplan = SubPlan {
            params: vec![],
            ops: vec![
                Op {
                    id: OpId(0),
                    kind: OpKind::ToBool {
                        value: ValueRef::literal_bool(false),
                    },
                    inputs: vec![],
                    source_location: None,
                    guard: None,
                },
                Op {
                    id: OpId(1),
                    kind: OpKind::Add {
                        left: ValueRef::literal_int(100),
                        right: ValueRef::literal_int(200),
                    },
                    inputs: vec![OpId(0)],
                    source_location: None,
                    guard: Some(OpId(0)),
                },
            ],
            output: OpId(1),
        };

        let params = HashMap::new();
        let (result, _signal) = execute_subplan_with_signal(&subplan, &params, &cache, &http_client).await.unwrap();

        assert_eq!(result, RecordedValue::None);
    }

    #[tokio::test]
    async fn test_subplan_guard_passes() {
        use blueprint_common::Op;
        use blueprint_common::op::ValueRef;

        let cache = Arc::new(OpCache::new());
        let http_client = reqwest::Client::new();

        let subplan = SubPlan {
            params: vec![],
            ops: vec![
                Op {
                    id: OpId(0),
                    kind: OpKind::ToBool {
                        value: ValueRef::literal_bool(true),
                    },
                    inputs: vec![],
                    source_location: None,
                    guard: None,
                },
                Op {
                    id: OpId(1),
                    kind: OpKind::Add {
                        left: ValueRef::literal_int(100),
                        right: ValueRef::literal_int(200),
                    },
                    inputs: vec![OpId(0)],
                    source_location: None,
                    guard: Some(OpId(0)),
                },
            ],
            output: OpId(1),
        };

        let params = HashMap::new();
        let (result, _signal) = execute_subplan_with_signal(&subplan, &params, &cache, &http_client).await.unwrap();

        assert_eq!(result, RecordedValue::Int(300));
    }

    #[tokio::test]
    async fn test_loop_signal_none() {
        assert_eq!(LoopSignal::None, LoopSignal::None);
        assert_ne!(LoopSignal::Break, LoopSignal::Continue);
    }

    #[tokio::test]
    async fn test_foreach_empty_items() {
        use blueprint_common::Op;

        let cache = Arc::new(OpCache::new());
        let http_client = reqwest::Client::new();

        let items: Vec<RecordedValue> = vec![];

        let body = SubPlan {
            params: vec!["x".to_string()],
            ops: vec![Op {
                id: OpId(0),
                kind: OpKind::Now,
                inputs: vec![],
                source_location: None,
                guard: None,
            }],
            output: OpId(0),
        };

        let result = execute_foreach_sequential(&items, "x", &body, &cache, &http_client).await;
        assert!(result.is_ok());

        let list = result.unwrap();
        if let RecordedValue::List(results) = list {
            assert!(results.is_empty());
        } else {
            panic!("Expected empty list result");
        }
    }

    #[tokio::test]
    async fn test_subplan_op_paramref() {
        let cache = Arc::new(OpCache::new());
        let http_client = reqwest::Client::new();

        let mut params = HashMap::new();
        params.insert("my_param".to_string(), RecordedValue::String("hello".to_string()));

        let local_results = HashMap::new();

        let kind = OpKind::ParamRef { name: "my_param".to_string() };
        let result = execute_subplan_op_async(&kind, OpId(0), &local_results, &params, &cache, &http_client).await;

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), RecordedValue::String("hello".to_string()));
    }

    #[tokio::test]
    async fn test_subplan_op_contains() {
        use blueprint_common::op::ValueRef;

        let cache = Arc::new(OpCache::new());
        let http_client = reqwest::Client::new();

        let params = HashMap::new();
        let local_results = HashMap::new();

        let kind = OpKind::Contains {
            haystack: ValueRef::literal_string("hello world"),
            needle: ValueRef::literal_string("world"),
        };

        let result = execute_subplan_op_async(&kind, OpId(0), &local_results, &params, &cache, &http_client).await;

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), RecordedValue::Bool(true));
    }
}
