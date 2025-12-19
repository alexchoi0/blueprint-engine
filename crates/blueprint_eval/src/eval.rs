use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock};

use blueprint_core::{BlueprintError, NativeFunction, Result, Value};
use blueprint_parser::{
    AstExpr, AstParameter, AssignOp, AssignTargetP, Clause,
    ExprP, ForClause, ParameterP, ParsedModule, StmtP,
};
use starlark_syntax::codemap::CodeMap;
use starlark_syntax::syntax::ast::{AstLiteral, AstAssignTarget, ArgumentP, BinOp};
use tokio::sync::RwLock;

use crate::scope::{Scope, ScopeKind};

pub struct FrozenModule {
    exports: HashMap<String, Value>,
}

static MODULE_CACHE: OnceLock<RwLock<HashMap<String, Arc<FrozenModule>>>> = OnceLock::new();

fn get_module_cache() -> &'static RwLock<HashMap<String, Arc<FrozenModule>>> {
    MODULE_CACHE.get_or_init(|| RwLock::new(HashMap::new()))
}

pub struct Evaluator {
    natives: HashMap<String, Arc<NativeFunction>>,
    modules: HashMap<String, HashMap<String, Arc<NativeFunction>>>,
    codemap: Option<CodeMap>,
    current_file: Option<PathBuf>,
}

impl Evaluator {
    pub fn new() -> Self {
        let mut evaluator = Self {
            natives: HashMap::new(),
            modules: HashMap::new(),
            codemap: None,
            current_file: None,
        };
        evaluator.register_builtins();
        evaluator
    }

    pub fn with_file(mut self, path: impl AsRef<Path>) -> Self {
        self.current_file = Some(path.as_ref().to_path_buf());
        self
    }

    pub fn set_file(&mut self, path: impl AsRef<Path>) {
        self.current_file = Some(path.as_ref().to_path_buf());
    }

    pub fn register_native(&mut self, func: NativeFunction) {
        self.natives.insert(func.name.clone(), Arc::new(func));
    }

    pub fn register_module_native(&mut self, module: &str, func: NativeFunction) {
        self.modules
            .entry(module.to_string())
            .or_default()
            .insert(func.name.clone(), Arc::new(func));
    }

    pub async fn eval(&mut self, module: &ParsedModule, scope: Arc<Scope>) -> Result<Value> {
        self.codemap = Some(module.codemap.clone());
        self.eval_stmt(module.statements(), scope).await
    }

    #[async_recursion::async_recursion]
    pub async fn eval_stmt(&self, stmt: &blueprint_parser::AstStmt, scope: Arc<Scope>) -> Result<Value> {
        match &stmt.node {
            StmtP::Statements(stmts) => {
                let mut result = Value::None;
                for s in stmts {
                    result = self.eval_stmt(s, scope.clone()).await?;
                }
                Ok(result)
            }

            StmtP::Expression(expr) => self.eval_expr(expr, scope).await,

            StmtP::Assign(assign) => {
                let value = self.eval_expr(&assign.rhs, scope.clone()).await?;
                self.assign_target(&assign.lhs, value, scope).await?;
                Ok(Value::None)
            }

            StmtP::AssignModify(lhs, op, rhs) => {
                let current = self.eval_assign_target_value(lhs, scope.clone()).await?;
                let rhs_val = self.eval_expr(rhs, scope.clone()).await?;
                let new_val = self.apply_assign_op(*op, current, rhs_val).await?;
                self.assign_target(lhs, new_val, scope).await?;
                Ok(Value::None)
            }

            StmtP::If(cond, then_block) => {
                let cond_val = self.eval_expr(cond, scope.clone()).await?;
                if cond_val.is_truthy() {
                    let block_scope = Scope::new_child(scope, ScopeKind::Block);
                    self.eval_stmt(then_block, block_scope).await?;
                }
                Ok(Value::None)
            }

            StmtP::IfElse(cond, branches) => {
                let (then_block, else_block) = branches.as_ref();
                let cond_val = self.eval_expr(cond, scope.clone()).await?;
                let block_scope = Scope::new_child(scope, ScopeKind::Block);
                if cond_val.is_truthy() {
                    self.eval_stmt(then_block, block_scope).await?;
                } else {
                    self.eval_stmt(else_block, block_scope).await?;
                }
                Ok(Value::None)
            }

            StmtP::For(for_stmt) => {
                let iterable = self.eval_expr(&for_stmt.over, scope.clone()).await?;
                let items = self.get_iterable(&iterable).await?;

                for item in items {
                    let loop_scope = Scope::new_child(scope.clone(), ScopeKind::Loop);
                    self.assign_target(&for_stmt.var, item, loop_scope.clone()).await?;

                    match self.eval_stmt(&for_stmt.body, loop_scope).await {
                        Err(BlueprintError::Break) => break,
                        Err(BlueprintError::Continue) => continue,
                        Err(e) => return Err(e),
                        Ok(_) => {}
                    }
                }
                Ok(Value::None)
            }

            StmtP::Break => Err(BlueprintError::Break),
            StmtP::Continue => Err(BlueprintError::Continue),

            StmtP::Return(expr) => {
                let value = match expr {
                    Some(e) => self.eval_expr(e, scope).await?,
                    None => Value::None,
                };
                Err(BlueprintError::Return {
                    value: Arc::new(value),
                })
            }

            StmtP::Pass => Ok(Value::None),

            StmtP::Def(def) => {
                let func = self.create_user_function(def, scope.clone())?;
                scope.define(&def.name.node.ident, func).await;
                Ok(Value::None)
            }

            StmtP::Load(load) => {
                self.eval_load(load, scope).await
            }
        }
    }

    async fn eval_load(
        &self,
        load: &starlark_syntax::syntax::ast::LoadP<starlark_syntax::syntax::ast::AstNoPayload>,
        scope: Arc<Scope>,
    ) -> Result<Value> {
        let module_path = &load.module.node;
        let resolved_path = self.resolve_module_path(module_path)?;
        let canonical_path = std::fs::canonicalize(&resolved_path)
            .unwrap_or_else(|_| resolved_path.clone())
            .to_string_lossy()
            .to_string();

        let cache = get_module_cache();

        {
            let cache_read = cache.read().await;
            if let Some(frozen) = cache_read.get(&canonical_path) {
                return self.bind_load_args(load, &frozen.exports, scope, module_path).await;
            }
        }

        let mut cache_write = cache.write().await;

        if let Some(frozen) = cache_write.get(&canonical_path) {
            return self.bind_load_args(load, &frozen.exports, scope, module_path).await;
        }

        let source = tokio::fs::read_to_string(&resolved_path)
            .await
            .map_err(|e| BlueprintError::IoError {
                path: resolved_path.to_string_lossy().to_string(),
                message: e.to_string(),
            })?;

        let filename = resolved_path.to_string_lossy().to_string();
        let module = blueprint_parser::parse(&filename, &source)?;

        let module_scope = Scope::new_global();
        module_scope
            .define("__file__", Value::String(Arc::new(canonical_path.clone())))
            .await;

        let mut module_evaluator = Evaluator::new();
        module_evaluator.set_file(&resolved_path);
        module_evaluator.eval(&module, module_scope.clone()).await?;

        let exports = module_scope.exports().await;
        let frozen = Arc::new(FrozenModule { exports });

        cache_write.insert(canonical_path, frozen.clone());

        self.bind_load_args(load, &frozen.exports, scope, module_path).await
    }

    async fn bind_load_args(
        &self,
        load: &starlark_syntax::syntax::ast::LoadP<starlark_syntax::syntax::ast::AstNoPayload>,
        exports: &HashMap<String, Value>,
        scope: Arc<Scope>,
        module_path: &str,
    ) -> Result<Value> {
        for arg in &load.args {
            let local_name = arg.local.node.ident.as_str();
            let their_name = &arg.their.node;

            let value = exports.get(their_name).cloned().ok_or_else(|| {
                BlueprintError::NameError {
                    name: format!(
                        "'{}' not found in module '{}'",
                        their_name, module_path
                    ),
                }
            })?;

            scope.define(local_name, value).await;
        }

        Ok(Value::None)
    }

    fn resolve_module_path(&self, module_path: &str) -> Result<PathBuf> {
        if let Some(stdlib_path) = module_path.strip_prefix("@bp/") {
            return self.resolve_stdlib_path(stdlib_path);
        }

        if module_path.starts_with('@') && !module_path.starts_with("@bp/") {
            return self.resolve_package_path(module_path);
        }

        let current_dir = if let Some(ref current_file) = self.current_file {
            current_file
                .parent()
                .map(|p| p.to_path_buf())
                .unwrap_or_else(|| PathBuf::from("."))
        } else {
            PathBuf::from(".")
        };

        let resolved = if module_path.starts_with("./") || module_path.starts_with("../") {
            current_dir.join(module_path)
        } else {
            current_dir.join(module_path)
        };

        Ok(resolved)
    }

    fn resolve_package_path(&self, module_path: &str) -> Result<PathBuf> {
        let path = module_path.strip_prefix('@').unwrap_or(module_path);

        let (repo_path, version) = if let Some(idx) = path.find('#') {
            (&path[..idx], Some(&path[idx + 1..]))
        } else {
            (path, None)
        };

        let parts: Vec<&str> = repo_path.splitn(2, '/').collect();
        if parts.len() != 2 {
            return Err(BlueprintError::IoError {
                path: module_path.to_string(),
                message: "Invalid package format. Expected @user/repo or @user/repo#version".into(),
            });
        }

        let user = parts[0];
        let repo = parts[1];
        let version_str = version.unwrap_or("main");

        let home = std::env::var("HOME").unwrap_or_else(|_| ".".to_string());
        let package_dir = PathBuf::from(&home)
            .join(".blueprint")
            .join("packages")
            .join(user)
            .join(format!("{}#{}", repo, version_str));

        let lib_path = package_dir.join("lib.bp");

        if lib_path.exists() {
            return Ok(lib_path);
        }

        eprintln!("Installing package @{}/{}#{}...", user, repo, version_str);
        self.fetch_package(user, repo, version_str, &package_dir)?;

        if lib_path.exists() {
            Ok(lib_path)
        } else {
            Err(BlueprintError::IoError {
                path: module_path.to_string(),
                message: format!("Package does not contain lib.bp"),
            })
        }
    }

    fn fetch_package(&self, user: &str, repo: &str, version: &str, dest: &PathBuf) -> Result<()> {
        let repo_url = format!("https://github.com/{}/{}.git", user, repo);

        let output = std::process::Command::new("git")
            .args(["clone", "--depth", "1", "--branch", version, &repo_url])
            .arg(dest)
            .output()
            .map_err(|e| BlueprintError::IoError {
                path: repo_url.clone(),
                message: e.to_string(),
            })?;

        if !output.status.success() {
            std::fs::remove_dir_all(dest).ok();
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(BlueprintError::IoError {
                path: repo_url,
                message: format!("Failed to clone: {}", stderr.trim()),
            });
        }

        std::fs::remove_dir_all(dest.join(".git")).ok();

        eprintln!("Installed @{}/{}#{}", user, repo, version);
        Ok(())
    }

    fn resolve_stdlib_path(&self, module_name: &str) -> Result<PathBuf> {
        let filename = format!("{}.bp", module_name);

        if let Ok(exe_path) = std::env::current_exe() {
            if let Some(exe_dir) = exe_path.parent() {
                let stdlib_path = exe_dir.join("stdlib").join(&filename);
                if stdlib_path.exists() {
                    return Ok(stdlib_path);
                }

                let stdlib_path = exe_dir.join("../stdlib").join(&filename);
                if stdlib_path.exists() {
                    return Ok(stdlib_path);
                }
            }
        }

        if let Ok(cwd) = std::env::current_dir() {
            let stdlib_path = cwd.join("stdlib").join(&filename);
            if stdlib_path.exists() {
                return Ok(stdlib_path);
            }
        }

        if let Ok(home) = std::env::var("HOME") {
            let stdlib_path = PathBuf::from(home)
                .join(".blueprint")
                .join("stdlib")
                .join(&filename);
            if stdlib_path.exists() {
                return Ok(stdlib_path);
            }
        }

        Err(BlueprintError::IoError {
            path: format!("@bp/{}", module_name),
            message: format!("stdlib module '{}' not found", module_name),
        })
    }

    #[async_recursion::async_recursion]
    pub async fn eval_expr(&self, expr: &AstExpr, scope: Arc<Scope>) -> Result<Value> {
        match &expr.node {
            ExprP::Literal(lit) => self.eval_literal(lit),

            ExprP::Identifier(ident) => {
                let name = ident.node.ident.as_str();

                match name {
                    "True" => return Ok(Value::Bool(true)),
                    "False" => return Ok(Value::Bool(false)),
                    "None" => return Ok(Value::None),
                    _ => {}
                }

                if let Some(value) = scope.get(name).await {
                    return Ok(value);
                }

                if let Some(native) = self.natives.get(name) {
                    return Ok(Value::NativeFunction(native.clone()));
                }

                if let Some(module_funcs) = self.modules.get(name) {
                    let mut dict = HashMap::new();
                    for (func_name, func) in module_funcs {
                        dict.insert(func_name.clone(), Value::NativeFunction(func.clone()));
                    }
                    return Ok(Value::Dict(Arc::new(tokio::sync::RwLock::new(dict))));
                }

                Err(BlueprintError::NameError {
                    name: name.to_string(),
                })
            }

            ExprP::Tuple(items) => {
                let mut values = Vec::with_capacity(items.len());
                for item in items {
                    values.push(self.eval_expr(item, scope.clone()).await?);
                }
                Ok(Value::Tuple(Arc::new(values)))
            }

            ExprP::List(items) => {
                let mut values = Vec::with_capacity(items.len());
                for item in items {
                    values.push(self.eval_expr(item, scope.clone()).await?);
                }
                Ok(Value::List(Arc::new(tokio::sync::RwLock::new(values))))
            }

            ExprP::Dict(pairs) => {
                let mut map = HashMap::new();
                for (key, value) in pairs {
                    let k = self.eval_expr(key, scope.clone()).await?;
                    let k_str = self.value_to_dict_key(&k)?;
                    let v = self.eval_expr(value, scope.clone()).await?;
                    map.insert(k_str, v);
                }
                Ok(Value::Dict(Arc::new(tokio::sync::RwLock::new(map))))
            }

            ExprP::Call(callee, args) => {
                let func = self.eval_expr(callee, scope.clone()).await?;
                let (positional, kwargs) = self.eval_call_args(&args.args, scope.clone()).await?;
                self.call_function(func, positional, kwargs, scope).await
            }

            ExprP::Index(pair) => {
                let (target, index) = pair.as_ref();
                let target_val = self.eval_expr(target, scope.clone()).await?;
                let index_val = self.eval_expr(index, scope).await?;
                self.eval_index(target_val, index_val).await
            }

            ExprP::Index2(triple) => {
                let (target, start, end) = triple.as_ref();
                let target_val = self.eval_expr(target, scope.clone()).await?;
                let start_val = self.eval_expr(start, scope.clone()).await?;
                let end_val = self.eval_expr(end, scope).await?;
                self.eval_slice(target_val, Some(start_val), Some(end_val))
            }

            ExprP::Dot(target, attr) => {
                let target_val = self.eval_expr(target, scope).await?;
                let attr_name = attr.node.as_str();

                if let Value::Dict(d) = &target_val {
                    let map = d.read().await;
                    if let Some(v) = map.get(attr_name) {
                        return Ok(v.clone());
                    }
                }

                match target_val.get_attr(attr_name) {
                    Some(v) => Ok(v),
                    None => Err(BlueprintError::AttributeError {
                        type_name: target_val.type_name().into(),
                        attr: attr_name.to_string(),
                    }),
                }
            }

            ExprP::Not(inner) => {
                let value = self.eval_expr(inner, scope).await?;
                Ok(Value::Bool(!value.is_truthy()))
            }

            ExprP::Minus(inner) => {
                let value = self.eval_expr(inner, scope).await?;
                self.eval_unary_minus(value)
            }

            ExprP::Plus(inner) => {
                let value = self.eval_expr(inner, scope).await?;
                match &value {
                    Value::Int(_) | Value::Float(_) => Ok(value),
                    _ => Err(BlueprintError::TypeError {
                        expected: "number".into(),
                        actual: value.type_name().into(),
                    }),
                }
            }

            ExprP::Op(lhs, op, rhs) => {
                let left = self.eval_expr(lhs, scope.clone()).await?;

                match op {
                    BinOp::And => {
                        if !left.is_truthy() {
                            return Ok(left);
                        }
                        return self.eval_expr(rhs, scope).await;
                    }
                    BinOp::Or => {
                        if left.is_truthy() {
                            return Ok(left);
                        }
                        return self.eval_expr(rhs, scope).await;
                    }
                    BinOp::In => {
                        let right = self.eval_expr(rhs, scope).await?;
                        return self.eval_in(left, right).await;
                    }
                    BinOp::NotIn => {
                        let right = self.eval_expr(rhs, scope).await?;
                        let result = self.eval_in(left, right).await?;
                        return match result {
                            Value::Bool(b) => Ok(Value::Bool(!b)),
                            _ => unreachable!(),
                        };
                    }
                    _ => {}
                }

                let right = self.eval_expr(rhs, scope).await?;
                self.eval_binary_op(left, *op, right).await
            }

            ExprP::If(triple) => {
                let (cond, then_expr, else_expr) = triple.as_ref();
                let cond_val = self.eval_expr(cond, scope.clone()).await?;
                if cond_val.is_truthy() {
                    self.eval_expr(then_expr, scope).await
                } else {
                    self.eval_expr(else_expr, scope).await
                }
            }

            ExprP::Lambda(lambda) => {
                let func = self.create_lambda_function(lambda, scope)?;
                Ok(func)
            }

            ExprP::ListComprehension(body, first, clauses) => {
                self.eval_list_comprehension(body, first, clauses, scope)
                    .await
            }

            ExprP::DictComprehension(pair, first, clauses) => {
                let (key_expr, val_expr) = pair.as_ref();
                self.eval_dict_comprehension(key_expr, val_expr, first, clauses, scope)
                    .await
            }

            ExprP::Slice(arr, start, stop, step) => {
                let arr_val = self.eval_expr(arr, scope.clone()).await?;
                let start_val = match start {
                    Some(s) => Some(self.eval_expr(s, scope.clone()).await?),
                    None => None,
                };
                let stop_val = match stop {
                    Some(s) => Some(self.eval_expr(s, scope.clone()).await?),
                    None => None,
                };
                let step_val = match step {
                    Some(s) => Some(self.eval_expr(s, scope).await?),
                    None => None,
                };
                self.eval_slice_with_step(arr_val, start_val, stop_val, step_val)
            }

            ExprP::FString(fstring) => {
                let format_str = &fstring.format.node;
                let expressions = &fstring.expressions;
                let mut result = String::new();
                let mut expr_iter = expressions.iter();

                for segment in format_str.split("{}") {
                    result.push_str(segment);
                    if let Some(expr) = expr_iter.next() {
                        let val = self.eval_expr(expr, scope.clone()).await?;
                        result.push_str(&val.to_display_string());
                    }
                }
                Ok(Value::String(Arc::new(result)))
            }

            _ => Err(BlueprintError::InternalError {
                message: format!("Unhandled expression type: {:?}", std::mem::discriminant(&expr.node)),
            }),
        }
    }

    fn eval_literal(&self, lit: &AstLiteral) -> Result<Value> {
        use starlark_syntax::lexer::TokenInt;
        match lit {
            AstLiteral::Int(i) => {
                let val = match &i.node {
                    TokenInt::I32(n) => *n as i64,
                    TokenInt::BigInt(n) => n.to_string().parse::<i64>().map_err(|_| {
                        BlueprintError::ValueError {
                            message: "Integer overflow".into(),
                        }
                    })?,
                };
                Ok(Value::Int(val))
            }
            AstLiteral::Float(f) => Ok(Value::Float(f.node)),
            AstLiteral::String(s) => Ok(Value::String(Arc::new(s.node.clone()))),
            AstLiteral::Ellipsis => Ok(Value::None),
        }
    }

    fn eval_unary_minus(&self, value: Value) -> Result<Value> {
        match value {
            Value::Int(i) => Ok(Value::Int(-i)),
            Value::Float(f) => Ok(Value::Float(-f)),
            _ => Err(BlueprintError::TypeError {
                expected: "number".into(),
                actual: value.type_name().into(),
            }),
        }
    }

    async fn eval_binary_op(&self, left: Value, op: BinOp, right: Value) -> Result<Value> {
        match op {
            BinOp::Add => self.eval_add(left, right).await,
            BinOp::Subtract => self.eval_sub(left, right),
            BinOp::Multiply => self.eval_mul(left, right),
            BinOp::Divide => self.eval_div(left, right),
            BinOp::FloorDivide => self.eval_floor_div(left, right),
            BinOp::Percent => self.eval_mod(left, right),
            BinOp::Equal => Ok(Value::Bool(left == right)),
            BinOp::NotEqual => Ok(Value::Bool(left != right)),
            BinOp::Less => self.eval_compare(left, right, |o| o.is_lt()),
            BinOp::LessOrEqual => self.eval_compare(left, right, |o| o.is_le()),
            BinOp::Greater => self.eval_compare(left, right, |o| o.is_gt()),
            BinOp::GreaterOrEqual => self.eval_compare(left, right, |o| o.is_ge()),
            BinOp::In | BinOp::NotIn => unreachable!("handled in eval_expr"),
            BinOp::BitAnd => self.eval_bit_and(left, right),
            BinOp::BitOr => self.eval_bit_or(left, right),
            BinOp::BitXor => self.eval_bit_xor(left, right),
            BinOp::LeftShift => self.eval_left_shift(left, right),
            BinOp::RightShift => self.eval_right_shift(left, right),
            BinOp::And | BinOp::Or => unreachable!("Short-circuit handled above"),
        }
    }

    async fn eval_add(&self, left: Value, right: Value) -> Result<Value> {
        match (&left, &right) {
            (Value::Int(a), Value::Int(b)) => Ok(Value::Int(a + b)),
            (Value::Float(a), Value::Float(b)) => Ok(Value::Float(a + b)),
            (Value::Int(a), Value::Float(b)) => Ok(Value::Float(*a as f64 + b)),
            (Value::Float(a), Value::Int(b)) => Ok(Value::Float(a + *b as f64)),
            (Value::String(a), Value::String(b)) => {
                Ok(Value::String(Arc::new(format!("{}{}", a, b))))
            }
            (Value::List(a), Value::List(b)) => {
                let mut result = a.read().await.clone();
                result.extend(b.read().await.iter().cloned());
                Ok(Value::List(Arc::new(tokio::sync::RwLock::new(result))))
            }
            _ => Err(BlueprintError::TypeError {
                expected: format!("compatible types for +"),
                actual: format!("{} and {}", left.type_name(), right.type_name()),
            }),
        }
    }

    fn eval_sub(&self, left: Value, right: Value) -> Result<Value> {
        match (&left, &right) {
            (Value::Int(a), Value::Int(b)) => Ok(Value::Int(a - b)),
            (Value::Float(a), Value::Float(b)) => Ok(Value::Float(a - b)),
            (Value::Int(a), Value::Float(b)) => Ok(Value::Float(*a as f64 - b)),
            (Value::Float(a), Value::Int(b)) => Ok(Value::Float(a - *b as f64)),
            _ => Err(BlueprintError::TypeError {
                expected: "numbers".into(),
                actual: format!("{} and {}", left.type_name(), right.type_name()),
            }),
        }
    }

    fn eval_mul(&self, left: Value, right: Value) -> Result<Value> {
        match (&left, &right) {
            (Value::Int(a), Value::Int(b)) => Ok(Value::Int(a * b)),
            (Value::Float(a), Value::Float(b)) => Ok(Value::Float(a * b)),
            (Value::Int(a), Value::Float(b)) => Ok(Value::Float(*a as f64 * b)),
            (Value::Float(a), Value::Int(b)) => Ok(Value::Float(a * *b as f64)),
            (Value::String(s), Value::Int(n)) | (Value::Int(n), Value::String(s)) => {
                if *n <= 0 {
                    Ok(Value::String(Arc::new(String::new())))
                } else {
                    Ok(Value::String(Arc::new(s.repeat(*n as usize))))
                }
            }
            (Value::List(l), Value::Int(n)) | (Value::Int(n), Value::List(l)) => {
                if *n <= 0 {
                    Ok(Value::List(Arc::new(tokio::sync::RwLock::new(vec![]))))
                } else {
                    let items = l.blocking_read();
                    let mut result = Vec::with_capacity(items.len() * (*n as usize));
                    for _ in 0..*n {
                        result.extend(items.iter().cloned());
                    }
                    Ok(Value::List(Arc::new(tokio::sync::RwLock::new(result))))
                }
            }
            _ => Err(BlueprintError::TypeError {
                expected: "compatible types for *".into(),
                actual: format!("{} and {}", left.type_name(), right.type_name()),
            }),
        }
    }

    fn eval_div(&self, left: Value, right: Value) -> Result<Value> {
        match (&left, &right) {
            (Value::Int(a), Value::Int(b)) => {
                if *b == 0 {
                    Err(BlueprintError::DivisionByZero)
                } else {
                    Ok(Value::Float(*a as f64 / *b as f64))
                }
            }
            (Value::Float(a), Value::Float(b)) => {
                if *b == 0.0 {
                    Err(BlueprintError::DivisionByZero)
                } else {
                    Ok(Value::Float(a / b))
                }
            }
            (Value::Int(a), Value::Float(b)) => {
                if *b == 0.0 {
                    Err(BlueprintError::DivisionByZero)
                } else {
                    Ok(Value::Float(*a as f64 / b))
                }
            }
            (Value::Float(a), Value::Int(b)) => {
                if *b == 0 {
                    Err(BlueprintError::DivisionByZero)
                } else {
                    Ok(Value::Float(a / *b as f64))
                }
            }
            _ => Err(BlueprintError::TypeError {
                expected: "numbers".into(),
                actual: format!("{} and {}", left.type_name(), right.type_name()),
            }),
        }
    }

    fn eval_floor_div(&self, left: Value, right: Value) -> Result<Value> {
        match (&left, &right) {
            (Value::Int(a), Value::Int(b)) => {
                if *b == 0 {
                    Err(BlueprintError::DivisionByZero)
                } else {
                    Ok(Value::Int(a.div_euclid(*b)))
                }
            }
            (Value::Float(a), Value::Float(b)) => {
                if *b == 0.0 {
                    Err(BlueprintError::DivisionByZero)
                } else {
                    Ok(Value::Float((a / b).floor()))
                }
            }
            (Value::Int(a), Value::Float(b)) => {
                if *b == 0.0 {
                    Err(BlueprintError::DivisionByZero)
                } else {
                    Ok(Value::Float((*a as f64 / b).floor()))
                }
            }
            (Value::Float(a), Value::Int(b)) => {
                if *b == 0 {
                    Err(BlueprintError::DivisionByZero)
                } else {
                    Ok(Value::Float((a / *b as f64).floor()))
                }
            }
            _ => Err(BlueprintError::TypeError {
                expected: "numbers".into(),
                actual: format!("{} and {}", left.type_name(), right.type_name()),
            }),
        }
    }

    fn eval_mod(&self, left: Value, right: Value) -> Result<Value> {
        match (&left, &right) {
            (Value::Int(a), Value::Int(b)) => {
                if *b == 0 {
                    Err(BlueprintError::DivisionByZero)
                } else {
                    Ok(Value::Int(a.rem_euclid(*b)))
                }
            }
            (Value::Float(a), Value::Float(b)) => {
                if *b == 0.0 {
                    Err(BlueprintError::DivisionByZero)
                } else {
                    Ok(Value::Float(a.rem_euclid(*b)))
                }
            }
            (Value::String(fmt), _) => self.format_string(fmt, &right),
            _ => Err(BlueprintError::TypeError {
                expected: "numbers or string formatting".into(),
                actual: format!("{} and {}", left.type_name(), right.type_name()),
            }),
        }
    }

    fn format_string(&self, fmt: &str, args: &Value) -> Result<Value> {
        let arg_list = match args {
            Value::Tuple(t) => t.as_ref().clone(),
            other => vec![other.clone()],
        };

        let mut result = String::new();
        let mut arg_idx = 0;
        let mut chars = fmt.chars().peekable();

        while let Some(c) = chars.next() {
            if c == '%' {
                if chars.peek() == Some(&'%') {
                    chars.next();
                    result.push('%');
                } else {
                    while chars.peek().map(|c| c.is_ascii_digit() || *c == '-' || *c == '+' || *c == ' ' || *c == '.').unwrap_or(false) {
                        chars.next();
                    }

                    let spec = chars.next().ok_or_else(|| BlueprintError::ValueError {
                        message: "incomplete format".into(),
                    })?;

                    if arg_idx >= arg_list.len() {
                        return Err(BlueprintError::ValueError {
                            message: "not enough arguments for format string".into(),
                        });
                    }

                    let arg = &arg_list[arg_idx];
                    arg_idx += 1;

                    match spec {
                        's' => result.push_str(&arg.to_display_string()),
                        'd' | 'i' => {
                            let i = arg.as_int()?;
                            result.push_str(&i.to_string());
                        }
                        'f' => {
                            let f = arg.as_float()?;
                            result.push_str(&f.to_string());
                        }
                        'r' => result.push_str(&arg.repr()),
                        _ => {
                            return Err(BlueprintError::ValueError {
                                message: format!("unsupported format character: {}", spec),
                            })
                        }
                    }
                }
            } else {
                result.push(c);
            }
        }

        Ok(Value::String(Arc::new(result)))
    }

    fn eval_compare<F>(&self, left: Value, right: Value, cmp: F) -> Result<Value>
    where
        F: Fn(std::cmp::Ordering) -> bool,
    {
        let ordering = match (&left, &right) {
            (Value::Int(a), Value::Int(b)) => a.cmp(b),
            (Value::Float(a), Value::Float(b)) => a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal),
            (Value::Int(a), Value::Float(b)) => (*a as f64).partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal),
            (Value::Float(a), Value::Int(b)) => a.partial_cmp(&(*b as f64)).unwrap_or(std::cmp::Ordering::Equal),
            (Value::String(a), Value::String(b)) => a.cmp(b),
            _ => {
                return Err(BlueprintError::TypeError {
                    expected: "comparable types".into(),
                    actual: format!("{} and {}", left.type_name(), right.type_name()),
                })
            }
        };
        Ok(Value::Bool(cmp(ordering)))
    }

    async fn eval_in(&self, left: Value, right: Value) -> Result<Value> {
        match &right {
            Value::List(l) => {
                let items = l.read().await;
                Ok(Value::Bool(items.iter().any(|item| *item == left)))
            }
            Value::Dict(d) => {
                let key = self.value_to_dict_key(&left)?;
                let map = d.read().await;
                Ok(Value::Bool(map.contains_key(&key)))
            }
            Value::String(s) => {
                let needle = left.as_string()?;
                Ok(Value::Bool(s.contains(&needle)))
            }
            Value::Tuple(t) => Ok(Value::Bool(t.iter().any(|item| *item == left))),
            _ => Err(BlueprintError::TypeError {
                expected: "iterable".into(),
                actual: right.type_name().into(),
            }),
        }
    }

    fn eval_bit_and(&self, left: Value, right: Value) -> Result<Value> {
        match (&left, &right) {
            (Value::Int(a), Value::Int(b)) => Ok(Value::Int(a & b)),
            _ => Err(BlueprintError::TypeError {
                expected: "integers".into(),
                actual: format!("{} and {}", left.type_name(), right.type_name()),
            }),
        }
    }

    fn eval_bit_or(&self, left: Value, right: Value) -> Result<Value> {
        match (&left, &right) {
            (Value::Int(a), Value::Int(b)) => Ok(Value::Int(a | b)),
            _ => Err(BlueprintError::TypeError {
                expected: "integers".into(),
                actual: format!("{} and {}", left.type_name(), right.type_name()),
            }),
        }
    }

    fn eval_bit_xor(&self, left: Value, right: Value) -> Result<Value> {
        match (&left, &right) {
            (Value::Int(a), Value::Int(b)) => Ok(Value::Int(a ^ b)),
            _ => Err(BlueprintError::TypeError {
                expected: "integers".into(),
                actual: format!("{} and {}", left.type_name(), right.type_name()),
            }),
        }
    }

    fn eval_left_shift(&self, left: Value, right: Value) -> Result<Value> {
        match (&left, &right) {
            (Value::Int(a), Value::Int(b)) => {
                if *b < 0 {
                    Err(BlueprintError::ValueError {
                        message: "negative shift count".into(),
                    })
                } else {
                    Ok(Value::Int(a << b))
                }
            }
            _ => Err(BlueprintError::TypeError {
                expected: "integers".into(),
                actual: format!("{} and {}", left.type_name(), right.type_name()),
            }),
        }
    }

    fn eval_right_shift(&self, left: Value, right: Value) -> Result<Value> {
        match (&left, &right) {
            (Value::Int(a), Value::Int(b)) => {
                if *b < 0 {
                    Err(BlueprintError::ValueError {
                        message: "negative shift count".into(),
                    })
                } else {
                    Ok(Value::Int(a >> b))
                }
            }
            _ => Err(BlueprintError::TypeError {
                expected: "integers".into(),
                actual: format!("{} and {}", left.type_name(), right.type_name()),
            }),
        }
    }

    async fn eval_index(&self, target: Value, index: Value) -> Result<Value> {
        match &target {
            Value::List(l) => {
                let idx = index.as_int()?;
                let items = l.read().await;
                let len = items.len() as i64;
                let actual_idx = if idx < 0 { len + idx } else { idx };
                if actual_idx < 0 || actual_idx >= len {
                    Err(BlueprintError::IndexError {
                        message: format!("list index {} out of range (len={})", idx, len),
                    })
                } else {
                    Ok(items[actual_idx as usize].clone())
                }
            }
            Value::Tuple(t) => {
                let idx = index.as_int()?;
                let len = t.len() as i64;
                let actual_idx = if idx < 0 { len + idx } else { idx };
                if actual_idx < 0 || actual_idx >= len {
                    Err(BlueprintError::IndexError {
                        message: format!("tuple index {} out of range (len={})", idx, len),
                    })
                } else {
                    Ok(t[actual_idx as usize].clone())
                }
            }
            Value::String(s) => {
                let idx = index.as_int()?;
                let chars: Vec<char> = s.chars().collect();
                let len = chars.len() as i64;
                let actual_idx = if idx < 0 { len + idx } else { idx };
                if actual_idx < 0 || actual_idx >= len {
                    Err(BlueprintError::IndexError {
                        message: format!("string index {} out of range (len={})", idx, len),
                    })
                } else {
                    Ok(Value::String(Arc::new(chars[actual_idx as usize].to_string())))
                }
            }
            Value::Dict(d) => {
                let key = self.value_to_dict_key(&index)?;
                let map = d.read().await;
                match map.get(&key) {
                    Some(v) => Ok(v.clone()),
                    None => Err(BlueprintError::KeyError { key }),
                }
            }
            _ => Err(BlueprintError::TypeError {
                expected: "subscriptable".into(),
                actual: target.type_name().into(),
            }),
        }
    }

    fn eval_slice(&self, target: Value, start: Option<Value>, end: Option<Value>) -> Result<Value> {
        match &target {
            Value::List(l) => {
                let items = l.blocking_read();
                let len = items.len() as i64;
                let (start_idx, end_idx) = self.normalize_slice_indices(start, end, len)?;
                let slice: Vec<Value> = items[start_idx..end_idx].to_vec();
                Ok(Value::List(Arc::new(tokio::sync::RwLock::new(slice))))
            }
            Value::String(s) => {
                let chars: Vec<char> = s.chars().collect();
                let len = chars.len() as i64;
                let (start_idx, end_idx) = self.normalize_slice_indices(start, end, len)?;
                let slice: String = chars[start_idx..end_idx].iter().collect();
                Ok(Value::String(Arc::new(slice)))
            }
            Value::Tuple(t) => {
                let len = t.len() as i64;
                let (start_idx, end_idx) = self.normalize_slice_indices(start, end, len)?;
                let slice: Vec<Value> = t[start_idx..end_idx].to_vec();
                Ok(Value::Tuple(Arc::new(slice)))
            }
            _ => Err(BlueprintError::TypeError {
                expected: "sliceable".into(),
                actual: target.type_name().into(),
            }),
        }
    }

    fn normalize_slice_indices(
        &self,
        start: Option<Value>,
        end: Option<Value>,
        len: i64,
    ) -> Result<(usize, usize)> {
        let start_idx = match start {
            Some(Value::Int(i)) => {
                if i < 0 {
                    (len + i).max(0) as usize
                } else {
                    i.min(len) as usize
                }
            }
            Some(Value::None) | None => 0,
            Some(v) => {
                return Err(BlueprintError::TypeError {
                    expected: "int or None".into(),
                    actual: v.type_name().into(),
                })
            }
        };

        let end_idx = match end {
            Some(Value::Int(i)) => {
                if i < 0 {
                    (len + i).max(0) as usize
                } else {
                    i.min(len) as usize
                }
            }
            Some(Value::None) | None => len as usize,
            Some(v) => {
                return Err(BlueprintError::TypeError {
                    expected: "int or None".into(),
                    actual: v.type_name().into(),
                })
            }
        };

        Ok((start_idx.min(end_idx), end_idx))
    }

    fn eval_slice_with_step(
        &self,
        target: Value,
        start: Option<Value>,
        end: Option<Value>,
        step: Option<Value>,
    ) -> Result<Value> {
        let step_val = match step {
            Some(Value::Int(s)) if s != 0 => s,
            Some(Value::Int(0)) => {
                return Err(BlueprintError::ValueError {
                    message: "slice step cannot be zero".into(),
                })
            }
            Some(Value::None) | None => 1,
            Some(v) => {
                return Err(BlueprintError::TypeError {
                    expected: "int or None".into(),
                    actual: v.type_name().into(),
                })
            }
        };

        if step_val == 1 {
            return self.eval_slice(target, start, end);
        }

        match &target {
            Value::List(l) => {
                let items = l.blocking_read();
                let len = items.len() as i64;
                let (start_idx, end_idx) = self.get_step_indices(start, end, step_val, len)?;
                let slice = self.collect_with_step(&items, start_idx, end_idx, step_val);
                Ok(Value::List(Arc::new(tokio::sync::RwLock::new(slice))))
            }
            Value::String(s) => {
                let chars: Vec<char> = s.chars().collect();
                let len = chars.len() as i64;
                let (start_idx, end_idx) = self.get_step_indices(start, end, step_val, len)?;
                let char_values: Vec<Value> = chars.iter().map(|c| Value::String(Arc::new(c.to_string()))).collect();
                let slice = self.collect_with_step(&char_values, start_idx, end_idx, step_val);
                let result: String = slice.into_iter().filter_map(|v| {
                    if let Value::String(s) = v { Some(s.as_ref().clone()) } else { None }
                }).collect();
                Ok(Value::String(Arc::new(result)))
            }
            Value::Tuple(t) => {
                let len = t.len() as i64;
                let (start_idx, end_idx) = self.get_step_indices(start, end, step_val, len)?;
                let slice = self.collect_with_step(t.as_ref(), start_idx, end_idx, step_val);
                Ok(Value::Tuple(Arc::new(slice)))
            }
            _ => Err(BlueprintError::TypeError {
                expected: "sliceable".into(),
                actual: target.type_name().into(),
            }),
        }
    }

    fn get_step_indices(
        &self,
        start: Option<Value>,
        end: Option<Value>,
        step: i64,
        len: i64,
    ) -> Result<(i64, i64)> {
        let (default_start, default_end) = if step > 0 { (0, len) } else { (len - 1, -len - 1) };

        let start_idx = match start {
            Some(Value::Int(i)) => {
                if i < 0 { (len + i).max(if step > 0 { 0 } else { -1 }) } else { i.min(len) }
            }
            Some(Value::None) | None => default_start,
            Some(v) => {
                return Err(BlueprintError::TypeError {
                    expected: "int or None".into(),
                    actual: v.type_name().into(),
                })
            }
        };

        let end_idx = match end {
            Some(Value::Int(i)) => {
                if i < 0 { len + i } else { i.min(len) }
            }
            Some(Value::None) | None => default_end,
            Some(v) => {
                return Err(BlueprintError::TypeError {
                    expected: "int or None".into(),
                    actual: v.type_name().into(),
                })
            }
        };

        Ok((start_idx, end_idx))
    }

    fn collect_with_step<T: Clone>(&self, items: &[T], start: i64, end: i64, step: i64) -> Vec<T> {
        let mut result = Vec::new();
        let mut i = start;
        if step > 0 {
            while i < end && i >= 0 && (i as usize) < items.len() {
                result.push(items[i as usize].clone());
                i += step;
            }
        } else {
            while i > end && i >= 0 && (i as usize) < items.len() {
                result.push(items[i as usize].clone());
                i += step;
            }
        }
        result
    }

    async fn get_iterable(&self, value: &Value) -> Result<Vec<Value>> {
        match value {
            Value::List(l) => Ok(l.read().await.clone()),
            Value::Tuple(t) => Ok(t.as_ref().clone()),
            Value::String(s) => Ok(s.chars().map(|c| Value::String(Arc::new(c.to_string()))).collect()),
            Value::Dict(d) => {
                let map = d.read().await;
                Ok(map.keys().map(|k| Value::String(Arc::new(k.clone()))).collect())
            }
            _ => Err(BlueprintError::TypeError {
                expected: "iterable".into(),
                actual: value.type_name().into(),
            }),
        }
    }

    fn value_to_dict_key(&self, value: &Value) -> Result<String> {
        match value {
            Value::String(s) => Ok(s.as_ref().clone()),
            Value::Int(i) => Ok(i.to_string()),
            Value::Bool(b) => Ok(b.to_string()),
            Value::None => Ok("None".to_string()),
            _ => Err(BlueprintError::TypeError {
                expected: "hashable".into(),
                actual: value.type_name().into(),
            }),
        }
    }

    #[async_recursion::async_recursion]
    async fn assign_target(&self, target: &AstAssignTarget, value: Value, scope: Arc<Scope>) -> Result<()> {
        match &target.node {
            AssignTargetP::Identifier(ident) => {
                scope.set(ident.node.ident.as_str(), value).await;
                Ok(())
            }
            AssignTargetP::Tuple(targets) => {
                let values = self.get_iterable(&value).await?;
                if values.len() != targets.len() {
                    return Err(BlueprintError::ValueError {
                        message: format!(
                            "cannot unpack {} values into {} targets",
                            values.len(),
                            targets.len()
                        ),
                    });
                }
                for (t, v) in targets.iter().zip(values) {
                    self.assign_target(t, v, scope.clone()).await?;
                }
                Ok(())
            }
            AssignTargetP::Index(pair) => {
                let (target_expr, index_expr) = pair.as_ref();
                let target_val = self.eval_expr(target_expr, scope.clone()).await?;
                let index_val = self.eval_expr(index_expr, scope).await?;

                match target_val {
                    Value::List(l) => {
                        let idx = index_val.as_int()?;
                        let mut items = l.write().await;
                        let len = items.len() as i64;
                        let actual_idx = if idx < 0 { len + idx } else { idx };
                        if actual_idx < 0 || actual_idx >= len {
                            return Err(BlueprintError::IndexError {
                                message: format!("list index {} out of range", idx),
                            });
                        }
                        items[actual_idx as usize] = value;
                        Ok(())
                    }
                    Value::Dict(d) => {
                        let key = self.value_to_dict_key(&index_val)?;
                        let mut map = d.write().await;
                        map.insert(key, value);
                        Ok(())
                    }
                    _ => Err(BlueprintError::TypeError {
                        expected: "list or dict".into(),
                        actual: target_val.type_name().into(),
                    }),
                }
            }
            AssignTargetP::Dot(_, attr) => {
                Err(BlueprintError::Unsupported {
                    message: format!("attribute assignment to .{} is not supported", attr.node),
                })
            }
        }
    }

    async fn eval_assign_target_value(&self, target: &AstAssignTarget, scope: Arc<Scope>) -> Result<Value> {
        match &target.node {
            AssignTargetP::Identifier(ident) => {
                let name = ident.node.ident.as_str();
                scope.get(name).await.ok_or_else(|| BlueprintError::NameError {
                    name: name.to_string(),
                })
            }
            AssignTargetP::Index(pair) => {
                let (target_expr, index_expr) = pair.as_ref();
                let target_val = self.eval_expr(target_expr, scope.clone()).await?;
                let index_val = self.eval_expr(index_expr, scope).await?;
                self.eval_index(target_val, index_val).await
            }
            AssignTargetP::Dot(target_expr, attr) => {
                let target_val = self.eval_expr(target_expr, scope).await?;
                target_val.get_attr(attr.node.as_str()).ok_or_else(|| BlueprintError::AttributeError {
                    type_name: target_val.type_name().into(),
                    attr: attr.node.to_string(),
                })
            }
            AssignTargetP::Tuple(_) => Err(BlueprintError::Unsupported {
                message: "augmented assignment to tuple".into(),
            }),
        }
    }

    async fn apply_assign_op(&self, op: AssignOp, left: Value, right: Value) -> Result<Value> {
        match op {
            AssignOp::Add => self.eval_add(left, right).await,
            AssignOp::Subtract => self.eval_sub(left, right),
            AssignOp::Multiply => self.eval_mul(left, right),
            AssignOp::Divide => self.eval_div(left, right),
            AssignOp::FloorDivide => self.eval_floor_div(left, right),
            AssignOp::Percent => self.eval_mod(left, right),
            AssignOp::BitAnd => self.eval_bit_and(left, right),
            AssignOp::BitOr => self.eval_bit_or(left, right),
            AssignOp::BitXor => self.eval_bit_xor(left, right),
            AssignOp::LeftShift => self.eval_left_shift(left, right),
            AssignOp::RightShift => self.eval_right_shift(left, right),
        }
    }

    async fn eval_call_args(
        &self,
        args: &[blueprint_parser::AstArgument],
        scope: Arc<Scope>,
    ) -> Result<(Vec<Value>, HashMap<String, Value>)> {
        let mut positional = Vec::new();
        let mut kwargs = HashMap::new();

        for arg in args {
            match &arg.node {
                ArgumentP::Positional(expr) => {
                    positional.push(self.eval_expr(expr, scope.clone()).await?);
                }
                ArgumentP::Named(name, expr) => {
                    let value = self.eval_expr(expr, scope.clone()).await?;
                    kwargs.insert(name.node.clone(), value);
                }
                ArgumentP::Args(expr) => {
                    let value = self.eval_expr(expr, scope.clone()).await?;
                    let items = self.get_iterable(&value).await?;
                    positional.extend(items);
                }
                ArgumentP::KwArgs(expr) => {
                    let value = self.eval_expr(expr, scope.clone()).await?;
                    if let Value::Dict(d) = value {
                        let map = d.read().await;
                        for (k, v) in map.iter() {
                            kwargs.insert(k.clone(), v.clone());
                        }
                    } else {
                        return Err(BlueprintError::TypeError {
                            expected: "dict".into(),
                            actual: value.type_name().into(),
                        });
                    }
                }
            }
        }

        Ok((positional, kwargs))
    }

    async fn call_function(
        &self,
        func: Value,
        args: Vec<Value>,
        kwargs: HashMap<String, Value>,
        scope: Arc<Scope>,
    ) -> Result<Value> {
        match func {
            Value::NativeFunction(f) => f.call(args, kwargs).await,
            Value::Function(f) => self.call_user_function(&f, args, kwargs, scope).await,
            Value::Lambda(f) => self.call_lambda(&f, args, kwargs, scope).await,
            _ => Err(BlueprintError::NotCallable {
                type_name: func.type_name().into(),
            }),
        }
    }

    async fn call_user_function(
        &self,
        func: &blueprint_core::UserFunction,
        args: Vec<Value>,
        kwargs: HashMap<String, Value>,
        _parent_scope: Arc<Scope>,
    ) -> Result<Value> {
        let closure_scope = func.closure.as_ref().and_then(|c| c.downcast_ref::<Arc<Scope>>().cloned());
        let base_scope = closure_scope.unwrap_or_else(Scope::new_global);
        let call_scope = Scope::new_child(base_scope, ScopeKind::Function);

        self.bind_parameters(&func.params, args, kwargs, &call_scope).await?;

        let body = func.body.downcast_ref::<blueprint_parser::AstStmt>().ok_or_else(|| {
            BlueprintError::InternalError {
                message: "Invalid function body".into(),
            }
        })?;

        match self.eval_stmt(body, call_scope).await {
            Ok(_) => Ok(Value::None),
            Err(BlueprintError::Return { value }) => Ok((*value).clone()),
            Err(e) => Err(e),
        }
    }

    async fn call_lambda(
        &self,
        func: &blueprint_core::LambdaFunction,
        args: Vec<Value>,
        kwargs: HashMap<String, Value>,
        _parent_scope: Arc<Scope>,
    ) -> Result<Value> {
        let closure_scope = func.closure.as_ref().and_then(|c| c.downcast_ref::<Arc<Scope>>().cloned());
        let base_scope = closure_scope.unwrap_or_else(Scope::new_global);
        let call_scope = Scope::new_child(base_scope, ScopeKind::Function);

        self.bind_parameters(&func.params, args, kwargs, &call_scope).await?;

        let body = func.body.downcast_ref::<AstExpr>().ok_or_else(|| {
            BlueprintError::InternalError {
                message: "Invalid lambda body".into(),
            }
        })?;

        self.eval_expr(body, call_scope).await
    }

    pub async fn call_lambda_public(
        &self,
        func: &blueprint_core::LambdaFunction,
        args: Vec<Value>,
        kwargs: HashMap<String, Value>,
    ) -> Result<Value> {
        let scope = Scope::new_global();
        self.call_lambda(func, args, kwargs, scope).await
    }

    pub async fn call_function_public(
        &self,
        func: &blueprint_core::UserFunction,
        args: Vec<Value>,
        kwargs: HashMap<String, Value>,
    ) -> Result<Value> {
        let scope = Scope::new_global();
        self.call_user_function(func, args, kwargs, scope).await
    }

    async fn bind_parameters(
        &self,
        params: &[blueprint_core::Parameter],
        args: Vec<Value>,
        mut kwargs: HashMap<String, Value>,
        scope: &Arc<Scope>,
    ) -> Result<()> {
        let mut arg_idx = 0;

        for param in params {
            match param.kind {
                blueprint_core::ParameterKind::Positional => {
                    let value = if arg_idx < args.len() {
                        let v = args[arg_idx].clone();
                        arg_idx += 1;
                        v
                    } else if let Some(v) = kwargs.remove(&param.name) {
                        v
                    } else if let Some(ref default) = param.default {
                        default.clone()
                    } else {
                        return Err(BlueprintError::ArgumentError {
                            message: format!("missing required argument: {}", param.name),
                        });
                    };
                    scope.define(&param.name, value).await;
                }
                blueprint_core::ParameterKind::Args => {
                    let remaining: Vec<Value> = args[arg_idx..].to_vec();
                    scope
                        .define(&param.name, Value::List(Arc::new(tokio::sync::RwLock::new(remaining))))
                        .await;
                    arg_idx = args.len();
                }
                blueprint_core::ParameterKind::Kwargs => {
                    let remaining = std::mem::take(&mut kwargs);
                    scope
                        .define(&param.name, Value::Dict(Arc::new(tokio::sync::RwLock::new(remaining))))
                        .await;
                }
            }
        }

        if arg_idx < args.len() {
            return Err(BlueprintError::ArgumentError {
                message: format!(
                    "too many positional arguments: expected {}, got {}",
                    arg_idx,
                    args.len()
                ),
            });
        }

        if !kwargs.is_empty() {
            let unknown: Vec<_> = kwargs.keys().collect();
            return Err(BlueprintError::ArgumentError {
                message: format!("unexpected keyword arguments: {:?}", unknown),
            });
        }

        Ok(())
    }

    fn create_user_function(
        &self,
        def: &starlark_syntax::syntax::ast::DefP<starlark_syntax::syntax::ast::AstNoPayload>,
        scope: Arc<Scope>,
    ) -> Result<Value> {
        let params = self.convert_params(&def.params)?;

        let func = blueprint_core::UserFunction {
            name: def.name.node.ident.clone(),
            params,
            body: Box::new((*def.body).clone()),
            closure: Some(Arc::new(scope) as Arc<dyn std::any::Any + Send + Sync>),
        };

        Ok(Value::Function(Arc::new(func)))
    }

    fn create_lambda_function(
        &self,
        lambda: &starlark_syntax::syntax::ast::LambdaP<starlark_syntax::syntax::ast::AstNoPayload>,
        scope: Arc<Scope>,
    ) -> Result<Value> {
        let params = self.convert_params(&lambda.params)?;

        let func = blueprint_core::LambdaFunction {
            params,
            body: Box::new((*lambda.body).clone()),
            closure: Some(Arc::new(scope) as Arc<dyn std::any::Any + Send + Sync>),
        };

        Ok(Value::Lambda(Arc::new(func)))
    }

    fn convert_params(&self, params: &[AstParameter]) -> Result<Vec<blueprint_core::Parameter>> {
        let mut result = Vec::new();

        for param in params {
            match &param.node {
                ParameterP::Normal(ident, _type_ann, default) => {
                    let default_val = if let Some(def) = default {
                        Some(self.eval_const_expr(def)?)
                    } else {
                        None
                    };
                    result.push(blueprint_core::Parameter {
                        name: ident.node.ident.clone(),
                        default: default_val,
                        kind: blueprint_core::ParameterKind::Positional,
                    });
                }
                ParameterP::Args(ident, _type_ann) => {
                    result.push(blueprint_core::Parameter {
                        name: ident.node.ident.clone(),
                        default: None,
                        kind: blueprint_core::ParameterKind::Args,
                    });
                }
                ParameterP::KwArgs(ident, _type_ann) => {
                    result.push(blueprint_core::Parameter {
                        name: ident.node.ident.clone(),
                        default: None,
                        kind: blueprint_core::ParameterKind::Kwargs,
                    });
                }
                ParameterP::NoArgs | ParameterP::Slash => {}
            }
        }

        Ok(result)
    }

    fn eval_const_expr(&self, expr: &AstExpr) -> Result<Value> {
        match &expr.node {
            ExprP::Literal(lit) => self.eval_literal(lit),
            ExprP::List(items) => {
                let mut values = Vec::new();
                for item in items {
                    values.push(self.eval_const_expr(item)?);
                }
                Ok(Value::List(Arc::new(tokio::sync::RwLock::new(values))))
            }
            ExprP::Dict(pairs) => {
                let mut map = HashMap::new();
                for (k, v) in pairs {
                    let key = self.eval_const_expr(k)?;
                    let key_str = self.value_to_dict_key(&key)?;
                    let val = self.eval_const_expr(v)?;
                    map.insert(key_str, val);
                }
                Ok(Value::Dict(Arc::new(tokio::sync::RwLock::new(map))))
            }
            ExprP::Tuple(items) => {
                let mut values = Vec::new();
                for item in items {
                    values.push(self.eval_const_expr(item)?);
                }
                Ok(Value::Tuple(Arc::new(values)))
            }
            ExprP::Identifier(ident) => {
                let name = ident.node.ident.as_str();
                match name {
                    "None" => Ok(Value::None),
                    "True" => Ok(Value::Bool(true)),
                    "False" => Ok(Value::Bool(false)),
                    _ => Err(BlueprintError::ValueError {
                        message: format!("non-constant default: {}", name),
                    }),
                }
            }
            ExprP::Minus(inner) => {
                let val = self.eval_const_expr(inner)?;
                self.eval_unary_minus(val)
            }
            _ => Err(BlueprintError::ValueError {
                message: "non-constant expression in default".into(),
            }),
        }
    }

    async fn eval_list_comprehension(
        &self,
        body: &AstExpr,
        first: &ForClause,
        clauses: &[Clause],
        scope: Arc<Scope>,
    ) -> Result<Value> {
        let mut results = Vec::new();
        self.eval_comprehension_clauses(body, first, clauses, scope, &mut results)
            .await?;
        Ok(Value::List(Arc::new(tokio::sync::RwLock::new(results))))
    }

    #[async_recursion::async_recursion]
    async fn eval_comprehension_clauses(
        &self,
        body: &AstExpr,
        for_clause: &ForClause,
        remaining: &[Clause],
        scope: Arc<Scope>,
        results: &mut Vec<Value>,
    ) -> Result<()> {
        let ForClause { var, over, .. } = for_clause;
        let iterable = self.eval_expr(over, scope.clone()).await?;
        let items = self.get_iterable(&iterable).await?;

        for item in items {
            let iter_scope = Scope::new_child(scope.clone(), ScopeKind::Block);
            self.assign_target(var, item, iter_scope.clone()).await?;

            if remaining.is_empty() {
                let value = self.eval_expr(body, iter_scope).await?;
                results.push(value);
            } else {
                match &remaining[0] {
                    Clause::For(next_for) => {
                        self.eval_comprehension_clauses(body, next_for, &remaining[1..], iter_scope, results)
                            .await?;
                    }
                    Clause::If(cond) => {
                        let cond_val = self.eval_expr(cond, iter_scope.clone()).await?;
                        if cond_val.is_truthy() {
                            if remaining.len() == 1 {
                                let value = self.eval_expr(body, iter_scope).await?;
                                results.push(value);
                            } else {
                                match &remaining[1] {
                                    Clause::For(next_for) => {
                                        self.eval_comprehension_clauses(
                                            body,
                                            next_for,
                                            &remaining[2..],
                                            iter_scope,
                                            results,
                                        )
                                        .await?;
                                    }
                                    Clause::If(_) => {
                                        return Err(BlueprintError::InternalError {
                                            message: "consecutive if clauses".into(),
                                        });
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        Ok(())
    }

    async fn eval_dict_comprehension(
        &self,
        key_expr: &AstExpr,
        val_expr: &AstExpr,
        first: &ForClause,
        clauses: &[Clause],
        scope: Arc<Scope>,
    ) -> Result<Value> {
        let mut results = HashMap::new();
        self.eval_dict_comprehension_clauses(key_expr, val_expr, first, clauses, scope, &mut results)
            .await?;
        Ok(Value::Dict(Arc::new(tokio::sync::RwLock::new(results))))
    }

    #[async_recursion::async_recursion]
    async fn eval_dict_comprehension_clauses(
        &self,
        key_expr: &AstExpr,
        val_expr: &AstExpr,
        for_clause: &ForClause,
        remaining: &[Clause],
        scope: Arc<Scope>,
        results: &mut HashMap<String, Value>,
    ) -> Result<()> {
        let ForClause { var, over, .. } = for_clause;
        let iterable = self.eval_expr(over, scope.clone()).await?;
        let items = self.get_iterable(&iterable).await?;

        for item in items {
            let iter_scope = Scope::new_child(scope.clone(), ScopeKind::Block);
            self.assign_target(var, item, iter_scope.clone()).await?;

            if remaining.is_empty() {
                let key = self.eval_expr(key_expr, iter_scope.clone()).await?;
                let key_str = self.value_to_dict_key(&key)?;
                let val = self.eval_expr(val_expr, iter_scope).await?;
                results.insert(key_str, val);
            } else {
                match &remaining[0] {
                    Clause::For(next_for) => {
                        self.eval_dict_comprehension_clauses(
                            key_expr,
                            val_expr,
                            next_for,
                            &remaining[1..],
                            iter_scope,
                            results,
                        )
                        .await?;
                    }
                    Clause::If(cond) => {
                        let cond_val = self.eval_expr(cond, iter_scope.clone()).await?;
                        if cond_val.is_truthy() {
                            if remaining.len() == 1 {
                                let key = self.eval_expr(key_expr, iter_scope.clone()).await?;
                                let key_str = self.value_to_dict_key(&key)?;
                                let val = self.eval_expr(val_expr, iter_scope).await?;
                                results.insert(key_str, val);
                            } else if let Clause::For(next_for) = &remaining[1] {
                                self.eval_dict_comprehension_clauses(
                                    key_expr,
                                    val_expr,
                                    next_for,
                                    &remaining[2..],
                                    iter_scope,
                                    results,
                                )
                                .await?;
                            }
                        }
                    }
                }
            }
        }

        Ok(())
    }

    fn register_builtins(&mut self) {
        crate::natives::register_all(self);
    }
}

impl Default for Evaluator {
    fn default() -> Self {
        Self::new()
    }
}
