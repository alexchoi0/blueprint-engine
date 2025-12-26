mod assignment;
mod comprehension;
mod expr;
mod functions;
mod ops;
mod pattern;
mod stmt;
mod types;

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, OnceLock};

use indexmap::IndexMap;

use blueprint_engine_core::{
    fetch_package, find_workspace_root_from, get_packages_dir_from, BlueprintError, NativeFunction,
    PackageSpec, Result, Value,
};
use blueprint_engine_parser::{AstExpr, AstParameter, AstStmt, ParameterP, StmtP};
use blueprint_starlark_syntax::codemap::CodeMap;
use blueprint_starlark_syntax::syntax::ast::{ArgumentP, ExprP};
use tokio::sync::RwLock;

use crate::modules::ModuleRegistry;
use crate::scope::Scope;

pub struct FrozenModule {
    exports: HashMap<String, Value>,
}

static MODULE_CACHE: OnceLock<RwLock<HashMap<String, Arc<FrozenModule>>>> = OnceLock::new();
static STDLIB_REGISTRY: OnceLock<Arc<ModuleRegistry>> = OnceLock::new();

fn get_module_cache() -> &'static RwLock<HashMap<String, Arc<FrozenModule>>> {
    MODULE_CACHE.get_or_init(|| RwLock::new(HashMap::new()))
}

fn get_stdlib_registry() -> Arc<ModuleRegistry> {
    STDLIB_REGISTRY
        .get_or_init(|| Arc::new(crate::modules::build_registry()))
        .clone()
}

pub struct Evaluator {
    pub(crate) builtins: HashMap<String, Arc<NativeFunction>>,
    pub(crate) stdlib: Arc<ModuleRegistry>,
    pub(crate) codemap: Option<CodeMap>,
    pub(crate) current_file: Option<PathBuf>,
    pub(crate) local_cache: Option<Arc<RwLock<HashMap<String, Arc<FrozenModule>>>>>,
}

impl Evaluator {
    pub fn new() -> Self {
        let mut evaluator = Self {
            builtins: HashMap::new(),
            stdlib: get_stdlib_registry(),
            codemap: None,
            current_file: None,
            local_cache: None,
        };
        evaluator.register_builtins();
        evaluator
    }

    pub fn new_isolated() -> Self {
        let mut evaluator = Self {
            builtins: HashMap::new(),
            stdlib: get_stdlib_registry(),
            codemap: None,
            current_file: None,
            local_cache: Some(Arc::new(RwLock::new(HashMap::new()))),
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

    fn get_cache(&self) -> &RwLock<HashMap<String, Arc<FrozenModule>>> {
        match &self.local_cache {
            Some(cache) => cache.as_ref(),
            None => get_module_cache(),
        }
    }

    pub fn register_native(&mut self, func: NativeFunction) {
        self.builtins.insert(func.name.clone(), Arc::new(func));
    }

    fn register_builtins(&mut self) {
        crate::modules::register_builtins(self);
    }

    pub fn value_to_dict_key(&self, value: &Value) -> Result<String> {
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

    pub fn create_user_function(
        &self,
        def: &blueprint_starlark_syntax::syntax::ast::DefP<
            blueprint_starlark_syntax::syntax::ast::AstNoPayload,
        >,
        scope: Arc<Scope>,
    ) -> Result<Value> {
        let params = self.convert_params(&def.params)?;

        let func = blueprint_engine_core::UserFunction {
            name: def.name.node.ident.clone(),
            params,
            body: Box::new((*def.body).clone()),
            closure: Some(Arc::new(scope) as Arc<dyn std::any::Any + Send + Sync>),
        };

        Ok(Value::Function(Arc::new(func)))
    }

    pub fn create_lambda_function(
        &self,
        lambda: &blueprint_starlark_syntax::syntax::ast::LambdaP<
            blueprint_starlark_syntax::syntax::ast::AstNoPayload,
        >,
        scope: Arc<Scope>,
    ) -> Result<Value> {
        let params = self.convert_params(&lambda.params)?;

        let func = blueprint_engine_core::LambdaFunction {
            params,
            body: Box::new((*lambda.body).clone()),
            closure: Some(Arc::new(scope) as Arc<dyn std::any::Any + Send + Sync>),
        };

        Ok(Value::Lambda(Arc::new(func)))
    }

    pub fn convert_params(
        &self,
        params: &[AstParameter],
    ) -> Result<Vec<blueprint_engine_core::Parameter>> {
        let mut result = Vec::new();

        for param in params {
            match &param.node {
                ParameterP::Normal(ident, _type_ann, default) => {
                    let default_val = if let Some(def) = default {
                        Some(self.eval_const_expr(def)?)
                    } else {
                        None
                    };
                    result.push(blueprint_engine_core::Parameter {
                        name: ident.node.ident.clone(),
                        default: default_val,
                        kind: blueprint_engine_core::ParameterKind::Positional,
                    });
                }
                ParameterP::Args(ident, _type_ann) => {
                    result.push(blueprint_engine_core::Parameter {
                        name: ident.node.ident.clone(),
                        default: None,
                        kind: blueprint_engine_core::ParameterKind::Args,
                    });
                }
                ParameterP::KwArgs(ident, _type_ann) => {
                    result.push(blueprint_engine_core::Parameter {
                        name: ident.node.ident.clone(),
                        default: None,
                        kind: blueprint_engine_core::ParameterKind::Kwargs,
                    });
                }
                ParameterP::NoArgs | ParameterP::Slash => {}
            }
        }

        Ok(result)
    }

    pub fn eval_const_expr(&self, expr: &AstExpr) -> Result<Value> {
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
                let mut map = IndexMap::new();
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
                ops::eval_unary_minus(val)
            }
            _ => Err(BlueprintError::ValueError {
                message: "non-constant expression in default".into(),
            }),
        }
    }

    pub async fn eval_load(
        &self,
        load: &blueprint_starlark_syntax::syntax::ast::LoadP<
            blueprint_starlark_syntax::syntax::ast::AstNoPayload,
        >,
        scope: Arc<Scope>,
    ) -> Result<Value> {
        let module_path = &load.module.node;

        // @bp/ prefix is reserved for Rust stdlib only
        if let Some(stdlib_module) = module_path.strip_prefix("@bp/") {
            if self.stdlib.has_module(stdlib_module) {
                return self.bind_stdlib_module(load, stdlib_module, scope).await;
            }
            return Err(BlueprintError::ImportError {
                message: format!("Module '@bp/{}' not found in stdlib", stdlib_module),
            });
        }

        let resolved_path = self.resolve_module_path(module_path)?;
        let canonical_path = std::fs::canonicalize(&resolved_path)
            .unwrap_or_else(|_| resolved_path.clone())
            .to_string_lossy()
            .to_string();

        let cache = self.get_cache();

        {
            let cache_read = cache.read().await;
            if let Some(frozen) = cache_read.get(&canonical_path) {
                return self
                    .bind_load_args(load, &frozen.exports, scope, module_path)
                    .await;
            }
        }

        let source = tokio::fs::read_to_string(&resolved_path)
            .await
            .map_err(|e| BlueprintError::IoError {
                path: resolved_path.to_string_lossy().to_string(),
                message: e.to_string(),
            })?;

        let filename = resolved_path.to_string_lossy().to_string();
        let module = blueprint_engine_parser::parse(&filename, &source)?;

        let module_scope = Scope::new_global();
        module_scope
            .define("__file__", Value::String(Arc::new(canonical_path.clone())))
            .await;

        let mut module_evaluator = Evaluator {
            builtins: self.builtins.clone(),
            stdlib: self.stdlib.clone(),
            codemap: None,
            current_file: Some(resolved_path.clone()),
            local_cache: self.local_cache.clone(),
        };
        module_evaluator.eval(&module, module_scope.clone()).await?;

        let exports = module_scope.exports().await;
        let frozen = Arc::new(FrozenModule { exports });

        {
            let mut cache_write = cache.write().await;
            cache_write.insert(canonical_path, frozen.clone());
        }

        self.bind_load_args(load, &frozen.exports, scope, module_path)
            .await
    }

    async fn bind_stdlib_module(
        &self,
        load: &blueprint_starlark_syntax::syntax::ast::LoadP<
            blueprint_starlark_syntax::syntax::ast::AstNoPayload,
        >,
        module_name: &str,
        scope: Arc<Scope>,
    ) -> Result<Value> {
        let module_funcs = self
            .stdlib
            .get_module(module_name)
            .ok_or_else(|| BlueprintError::ImportError {
                message: format!("Module '@bp/{}' not found", module_name),
            })?;

        self.bind_native_functions(load, &module_funcs, scope, module_name, &format!("@bp/{}", module_name))
            .await
    }

    async fn bind_native_functions(
        &self,
        load: &blueprint_starlark_syntax::syntax::ast::LoadP<
            blueprint_starlark_syntax::syntax::ast::AstNoPayload,
        >,
        module_funcs: &HashMap<String, Arc<NativeFunction>>,
        scope: Arc<Scope>,
        default_name: &str,
        display_name: &str,
    ) -> Result<Value> {
        if load.args.is_empty() {
            let mut dict = IndexMap::new();
            for (func_name, func) in module_funcs {
                dict.insert(func_name.clone(), Value::NativeFunction(func.clone()));
            }
            scope
                .define(default_name, Value::Dict(Arc::new(RwLock::new(dict))))
                .await;
            return Ok(Value::None);
        }

        if load.args.len() == 1 && load.args[0].their.node == "__module__" {
            let alias_name = load.args[0].local.node.ident.as_str();
            let mut dict = IndexMap::new();
            for (func_name, func) in module_funcs {
                dict.insert(func_name.clone(), Value::NativeFunction(func.clone()));
            }
            scope
                .define(alias_name, Value::Dict(Arc::new(RwLock::new(dict))))
                .await;
            return Ok(Value::None);
        }

        if load.args.len() == 1 && load.args[0].their.node == "*" {
            for (func_name, func) in module_funcs {
                scope
                    .define(func_name, Value::NativeFunction(func.clone()))
                    .await;
            }
            return Ok(Value::None);
        }

        for arg in &load.args {
            let local_name = arg.local.node.ident.as_str();
            let their_name = &arg.their.node;

            let func = module_funcs
                .get(their_name)
                .ok_or_else(|| BlueprintError::ImportError {
                    message: format!(
                        "Function '{}' not found in module '{}'",
                        their_name, display_name
                    ),
                })?;

            scope
                .define(local_name, Value::NativeFunction(func.clone()))
                .await;
        }

        Ok(Value::None)
    }

    async fn bind_load_args(
        &self,
        load: &blueprint_starlark_syntax::syntax::ast::LoadP<
            blueprint_starlark_syntax::syntax::ast::AstNoPayload,
        >,
        exports: &HashMap<String, Value>,
        scope: Arc<Scope>,
        module_path: &str,
    ) -> Result<Value> {
        for arg in &load.args {
            let local_name = arg.local.node.ident.as_str();
            let their_name = &arg.their.node;

            let value = exports.get(their_name).ok_or_else(|| {
                if their_name.starts_with('_') {
                    BlueprintError::ImportError {
                        message: format!(
                            "'{}' is private and cannot be imported from '{}'",
                            their_name, module_path
                        ),
                    }
                } else {
                    BlueprintError::ImportError {
                        message: format!("'{}' not found in module '{}'", their_name, module_path),
                    }
                }
            })?;

            let copied_value = value.deep_copy().await;
            scope.define(local_name, copied_value).await;
        }

        Ok(Value::None)
    }

    fn resolve_module_path(&self, module_path: &str) -> Result<PathBuf> {
        // @bp/ is handled in eval_load, so any @ prefix here is a package
        if module_path.starts_with('@') {
            return self.resolve_package_path(module_path);
        }

        if module_path.starts_with("./") || module_path.starts_with("../") {
            let current_dir = if let Some(ref current_file) = self.current_file {
                current_file
                    .parent()
                    .map(|p| p.to_path_buf())
                    .unwrap_or_else(|| PathBuf::from("."))
            } else {
                PathBuf::from(".")
            };
            return Ok(current_dir.join(module_path));
        }

        if let Some(workspace_root) = self.find_workspace_root() {
            let resolved = workspace_root.join(module_path);
            if resolved.exists() {
                return Ok(resolved);
            }
        }

        let current_dir = if let Some(ref current_file) = self.current_file {
            current_file
                .parent()
                .map(|p| p.to_path_buf())
                .unwrap_or_else(|| PathBuf::from("."))
        } else {
            PathBuf::from(".")
        };

        Ok(current_dir.join(module_path))
    }

    fn resolve_package_path(&self, module_path: &str) -> Result<PathBuf> {
        let spec = PackageSpec::parse(module_path)?;

        let start_dir = self
            .current_file
            .as_ref()
            .and_then(|f| f.parent().map(|p| p.to_path_buf()));
        let packages_dir = get_packages_dir_from(start_dir);
        let package_dir = packages_dir.join(&spec.user).join(spec.dir_name());
        let lib_path = package_dir.join("lib.bp");

        if lib_path.exists() {
            return Ok(lib_path);
        }

        eprintln!("Installing package {}...", spec.display_name());
        fetch_package(&spec, &package_dir)?;
        eprintln!("Installed {}", spec.display_name());

        if lib_path.exists() {
            Ok(lib_path)
        } else {
            Err(BlueprintError::IoError {
                path: module_path.to_string(),
                message: "Package does not contain lib.bp".into(),
            })
        }
    }

    fn find_workspace_root(&self) -> Option<PathBuf> {
        let start_dir = self
            .current_file
            .as_ref()
            .and_then(|f| f.parent().map(|p| p.to_path_buf()))
            .or_else(|| std::env::current_dir().ok())?;
        find_workspace_root_from(start_dir)
    }

    pub fn contains_yield(stmt: &AstStmt) -> bool {
        Self::stmt_contains_yield(&stmt.node)
    }

    fn stmt_contains_yield(
        stmt: &StmtP<blueprint_starlark_syntax::syntax::ast::AstNoPayload>,
    ) -> bool {
        match stmt {
            StmtP::Yield(_) => true,
            StmtP::Statements(stmts) => stmts.iter().any(|s| Self::contains_yield(s)),
            StmtP::If(_, body) => Self::contains_yield(body),
            StmtP::IfElse(_, bodies) => {
                Self::contains_yield(&bodies.0) || Self::contains_yield(&bodies.1)
            }
            StmtP::For(for_stmt) => Self::contains_yield(&for_stmt.body),
            StmtP::Expression(expr) => Self::expr_contains_yield(expr),
            StmtP::Assign(assign) => Self::expr_contains_yield(&assign.rhs),
            StmtP::AssignModify(_, _, expr) => Self::expr_contains_yield(expr),
            StmtP::Return(Some(expr)) => Self::expr_contains_yield(expr),
            StmtP::Def(def) => Self::contains_yield(&def.body),
            StmtP::Match(m) => m.cases.iter().any(|c| Self::contains_yield(&c.node.body)),
            _ => false,
        }
    }

    fn expr_contains_yield(expr: &AstExpr) -> bool {
        match &expr.node {
            ExprP::Call(callee, args) => {
                Self::expr_contains_yield(callee)
                    || args.args.iter().any(|arg| {
                        Self::expr_contains_yield(match &arg.node {
                            ArgumentP::Positional(e) => e,
                            ArgumentP::Named(_, e) => e,
                            ArgumentP::Args(e) => e,
                            ArgumentP::KwArgs(e) => e,
                        })
                    })
            }
            ExprP::Tuple(items) | ExprP::List(items) | ExprP::Set(items) => {
                items.iter().any(|i| Self::expr_contains_yield(i))
            }
            ExprP::Dict(pairs) => pairs
                .iter()
                .any(|(k, v)| Self::expr_contains_yield(k) || Self::expr_contains_yield(v)),
            ExprP::If(cond_else) => {
                let (cond, then_expr, else_expr) = cond_else.as_ref();
                Self::expr_contains_yield(cond)
                    || Self::expr_contains_yield(then_expr)
                    || Self::expr_contains_yield(else_expr)
            }
            ExprP::Op(lhs, _, rhs) => {
                Self::expr_contains_yield(lhs) || Self::expr_contains_yield(rhs)
            }
            ExprP::Not(e) | ExprP::Minus(e) | ExprP::Plus(e) => Self::expr_contains_yield(e),
            ExprP::Index(pair) => {
                Self::expr_contains_yield(&pair.0) || Self::expr_contains_yield(&pair.1)
            }
            ExprP::Dot(e, _) => Self::expr_contains_yield(e),
            ExprP::ListComprehension(body, first, clauses) => {
                Self::expr_contains_yield(body) || Self::for_clause_contains_yield(first, clauses)
            }
            ExprP::DictComprehension(kv, first, clauses) => {
                Self::expr_contains_yield(&kv.0)
                    || Self::expr_contains_yield(&kv.1)
                    || Self::for_clause_contains_yield(first, clauses)
            }
            ExprP::SetComprehension(body, first, clauses) => {
                Self::expr_contains_yield(body) || Self::for_clause_contains_yield(first, clauses)
            }
            ExprP::Lambda(lambda) => Self::expr_contains_yield(&lambda.body),
            _ => false,
        }
    }

    fn for_clause_contains_yield(
        first: &blueprint_engine_parser::ForClause,
        clauses: &[blueprint_engine_parser::Clause],
    ) -> bool {
        use blueprint_engine_parser::Clause;

        Self::expr_contains_yield(&first.over)
            || clauses.iter().any(|c| match c {
                Clause::For(f) => Self::expr_contains_yield(&f.over),
                Clause::If(e) => Self::expr_contains_yield(e),
            })
    }

    pub fn get_span_location(
        &self,
        span: &blueprint_starlark_syntax::codemap::Span,
    ) -> (usize, usize) {
        if let Some(ref codemap) = self.codemap {
            let full_span = codemap.full_span();
            if span.begin() <= full_span.end() && span.end() <= full_span.end() {
                let pos = codemap.resolve_span(*span);
                (pos.begin.line, pos.begin.column)
            } else {
                (0, 0)
            }
        } else {
            (0, 0)
        }
    }
}

impl Default for Evaluator {
    fn default() -> Self {
        Self::new()
    }
}
