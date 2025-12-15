use std::collections::{HashMap, HashSet};
use std::rc::Rc;
use std::cell::RefCell;
use std::path::PathBuf;

use starlark_syntax::syntax::module::AstModule;
use starlark_syntax::syntax::ast::{
    AstStmt, StmtP, AstExpr, ExprP, AstLiteral, BinOp, AssignOp,
    ArgumentP, ParameterP, ForClauseP, ClauseP, AssignTargetP,
};
use starlark_syntax::syntax::Dialect;
use starlark_syntax::lexer::TokenInt;

use blueprint_common::{Schema, SchemaOp, SchemaOpId, SchemaSubPlan, SchemaSubPlanEntry, SchemaValue, compute_source_hash};
use super::value::{Value, Function, Parameter, FunctionBody, BuiltinFn, HashableValue};
use super::scope::Scope;
use super::builtins;

#[derive(Clone)]
pub struct CompiledModule {
    pub path: String,
    pub exports: HashMap<String, Value>,
    pub source_hash: Option<String>,
    pub is_builtin: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StringMethod {
    Upper, Lower, Strip, Lstrip, Rstrip,
    Capitalize, Title, Swapcase, Istitle,
    Isalpha, Isdigit, Isalnum, Isspace, Isupper, Islower,
    Split, Rsplit, Splitlines, Join, Replace,
    Find, Rfind, Index, Rindex, Count,
    Startswith, Endswith, Format, Removeprefix, Removesuffix, Elems,
    Partition, Rpartition,
}

impl StringMethod {
    pub fn from_str(s: &str) -> Option<Self> {
        Some(match s {
            "upper" => Self::Upper,
            "lower" => Self::Lower,
            "strip" => Self::Strip,
            "lstrip" => Self::Lstrip,
            "rstrip" => Self::Rstrip,
            "capitalize" => Self::Capitalize,
            "title" => Self::Title,
            "swapcase" => Self::Swapcase,
            "isalpha" => Self::Isalpha,
            "isdigit" => Self::Isdigit,
            "isalnum" => Self::Isalnum,
            "isspace" => Self::Isspace,
            "isupper" => Self::Isupper,
            "islower" => Self::Islower,
            "split" => Self::Split,
            "rsplit" => Self::Rsplit,
            "splitlines" => Self::Splitlines,
            "join" => Self::Join,
            "replace" => Self::Replace,
            "find" => Self::Find,
            "rfind" => Self::Rfind,
            "index" => Self::Index,
            "rindex" => Self::Rindex,
            "count" => Self::Count,
            "startswith" => Self::Startswith,
            "endswith" => Self::Endswith,
            "format" => Self::Format,
            "removeprefix" => Self::Removeprefix,
            "removesuffix" => Self::Removesuffix,
            "elems" => Self::Elems,
            "partition" => Self::Partition,
            "rpartition" => Self::Rpartition,
            "istitle" => Self::Istitle,
            _ => return None,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ListMethod {
    Append, Extend, Insert, Pop, Remove, Clear,
    Index, Count,
}

impl ListMethod {
    pub fn from_str(s: &str) -> Option<Self> {
        Some(match s {
            "append" => Self::Append,
            "extend" => Self::Extend,
            "insert" => Self::Insert,
            "pop" => Self::Pop,
            "remove" => Self::Remove,
            "clear" => Self::Clear,
            "index" => Self::Index,
            "count" => Self::Count,
            _ => return None,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DictMethod {
    Keys, Values, Items, Get, Pop, Clear, Update, Setdefault, Popitem,
}

impl DictMethod {
    pub fn from_str(s: &str) -> Option<Self> {
        Some(match s {
            "keys" => Self::Keys,
            "values" => Self::Values,
            "items" => Self::Items,
            "get" => Self::Get,
            "pop" => Self::Pop,
            "clear" => Self::Clear,
            "update" => Self::Update,
            "setdefault" => Self::Setdefault,
            "popitem" => Self::Popitem,
            _ => return None,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BytesMethod {
    Elems,
}

impl BytesMethod {
    pub fn from_str(s: &str) -> Option<Self> {
        Some(match s {
            "elems" => Self::Elems,
            _ => return None,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SetMethod {
    Add, Clear, Difference, DifferenceUpdate, Discard,
    Intersection, IntersectionUpdate, Isdisjoint, Issubset, Issuperset,
    Pop, Remove, SymmetricDifference, SymmetricDifferenceUpdate, Union, Update,
}

impl SetMethod {
    pub fn from_str(s: &str) -> Option<Self> {
        Some(match s {
            "add" => Self::Add,
            "clear" => Self::Clear,
            "difference" => Self::Difference,
            "difference_update" => Self::DifferenceUpdate,
            "discard" => Self::Discard,
            "intersection" => Self::Intersection,
            "intersection_update" => Self::IntersectionUpdate,
            "isdisjoint" => Self::Isdisjoint,
            "issubset" => Self::Issubset,
            "issuperset" => Self::Issuperset,
            "pop" => Self::Pop,
            "remove" => Self::Remove,
            "symmetric_difference" => Self::SymmetricDifference,
            "symmetric_difference_update" => Self::SymmetricDifferenceUpdate,
            "union" => Self::Union,
            "update" => Self::Update,
            _ => return None,
        })
    }
}

pub struct SchemaGenerator {
    schema: Schema,
    scope: Scope,
    module_registry: HashMap<String, CompiledModule>,
    module_stack: Vec<String>,
    current_file: Option<PathBuf>,
    last_expression_value: Option<Value>,
    in_function: bool,
    iterating_lists: HashSet<*const RefCell<Vec<Value>>>,
    iterating_dicts: HashSet<*const RefCell<HashMap<String, Value>>>,
    subplan_entries: Option<Vec<SchemaSubPlanEntry>>,
    subplan_next_id: u64,
    subplan_params: HashSet<String>,
}

impl SchemaGenerator {
    pub fn new() -> Self {
        let mut compiler = Self {
            schema: Schema::new(),
            scope: Scope::new(),
            module_registry: HashMap::new(),
            module_stack: Vec::new(),
            current_file: None,
            last_expression_value: None,
            in_function: false,
            iterating_lists: HashSet::new(),
            iterating_dicts: HashSet::new(),
            subplan_entries: None,
            subplan_next_id: 0,
            subplan_params: HashSet::new(),
        };

        compiler.scope.set("True", Value::Bool(true));
        compiler.scope.set("False", Value::Bool(false));
        compiler.scope.set("None", Value::None);

        compiler.register_builtin_modules();
        builtins::register_base_builtins(&mut compiler);

        compiler
    }

    fn register_builtin_modules(&mut self) {
        self.module_registry.insert("@bp/io".to_string(), CompiledModule {
            path: "@bp/io".to_string(),
            exports: builtins::create_io_exports(),
            source_hash: None,
            is_builtin: true,
        });
        self.module_registry.insert("@bp/http".to_string(), CompiledModule {
            path: "@bp/http".to_string(),
            exports: builtins::create_http_exports(),
            source_hash: None,
            is_builtin: true,
        });
        self.module_registry.insert("@bp/exec".to_string(), CompiledModule {
            path: "@bp/exec".to_string(),
            exports: builtins::create_exec_exports(),
            source_hash: None,
            is_builtin: true,
        });
        self.module_registry.insert("@bp/json".to_string(), CompiledModule {
            path: "@bp/json".to_string(),
            exports: builtins::create_json_exports(),
            source_hash: None,
            is_builtin: true,
        });
    }

    fn new_for_module(registry: HashMap<String, CompiledModule>) -> Self {
        let mut compiler = Self {
            schema: Schema::new(),
            scope: Scope::new(),
            module_registry: registry,
            module_stack: Vec::new(),
            current_file: None,
            last_expression_value: None,
            in_function: false,
            iterating_lists: HashSet::new(),
            iterating_dicts: HashSet::new(),
            subplan_entries: None,
            subplan_next_id: 0,
            subplan_params: HashSet::new(),
        };

        compiler.scope.set("True", Value::Bool(true));
        compiler.scope.set("False", Value::Bool(false));
        compiler.scope.set("None", Value::None);

        builtins::register_base_builtins(&mut compiler);

        compiler
    }

    pub fn register_builtin(&mut self, name: &str, func: fn(&mut SchemaGenerator, Vec<Value>, HashMap<String, Value>) -> Result<Value, String>) {
        let rc_func: BuiltinFn = Rc::new(func);
        self.scope.set(name.to_string(), Value::BuiltinFunction(rc_func));
    }

    pub fn set_global(&mut self, name: &str, value: Value) {
        self.scope.set(name.to_string(), value);
    }

    pub fn add_schema_op(&mut self, op: SchemaOp) -> SchemaOpId {
        if let Some(ref mut entries) = self.subplan_entries {
            let local_id = self.subplan_next_id;
            self.subplan_next_id += 1;
            entries.push(SchemaSubPlanEntry {
                local_id,
                op,
                guard: None,
            });
            SchemaOpId(local_id)
        } else {
            self.schema.add_op(op, None)
        }
    }

    fn compile_loop_body(&mut self, var_name: &str, body: &AstStmt) -> Result<SchemaSubPlan, String> {
        let old_entries = self.subplan_entries.take();
        let old_next_id = self.subplan_next_id;
        let old_params = std::mem::take(&mut self.subplan_params);
        let child_scope = self.scope.child();
        let old_scope = std::mem::replace(&mut self.scope, child_scope);

        self.subplan_entries = Some(Vec::new());
        self.subplan_next_id = 0;
        self.subplan_params.insert(var_name.to_string());

        self.scope.set(var_name, Value::ParamRef(var_name.to_string()));

        let result = self.exec_loop_body_for_subplan(body);

        self.scope = old_scope;

        let entries = self.subplan_entries.take().unwrap_or_default();
        let output = if entries.is_empty() { 0 } else { entries.last().map(|e| e.local_id).unwrap_or(0) };

        self.subplan_entries = old_entries;
        self.subplan_next_id = old_next_id;
        self.subplan_params = old_params;

        result?;

        Ok(SchemaSubPlan {
            params: vec![var_name.to_string()],
            entries,
            output,
        })
    }

    fn can_parallelize_loop(&self, subplan: &SchemaSubPlan, loop_var: &str) -> bool {
        for entry in &subplan.entries {
            if !self.is_op_parallelizable(&entry.op, loop_var) {
                return false;
            }
        }
        true
    }

    fn is_op_parallelizable(&self, op: &SchemaOp, loop_var: &str) -> bool {
        match op {
            SchemaOp::BpPrint { .. } => false,
            SchemaOp::Break | SchemaOp::Continue => false,

            SchemaOp::TcpSend { .. } | SchemaOp::TcpRecv { .. } | SchemaOp::TcpClose { .. } |
            SchemaOp::TcpConnect { .. } | SchemaOp::TcpListen { .. } | SchemaOp::TcpAccept { .. } => false,

            SchemaOp::UdpSendTo { .. } | SchemaOp::UdpRecvFrom { .. } | SchemaOp::UdpClose { .. } |
            SchemaOp::UdpBind { .. } => false,

            SchemaOp::UnixSend { .. } | SchemaOp::UnixRecv { .. } | SchemaOp::UnixClose { .. } |
            SchemaOp::UnixConnect { .. } | SchemaOp::UnixListen { .. } | SchemaOp::UnixAccept { .. } => false,

            SchemaOp::IoAppendFile { path, .. } => self.value_depends_on_loop_var(path, loop_var),

            SchemaOp::IoWriteFile { path, .. } => self.value_depends_on_loop_var(path, loop_var),
            SchemaOp::IoDeleteFile { path } => self.value_depends_on_loop_var(path, loop_var),
            SchemaOp::IoMkdir { path, .. } => self.value_depends_on_loop_var(path, loop_var),
            SchemaOp::IoRmdir { path, .. } => self.value_depends_on_loop_var(path, loop_var),
            SchemaOp::IoCopyFile { src, dst } => {
                self.value_depends_on_loop_var(src, loop_var) ||
                self.value_depends_on_loop_var(dst, loop_var)
            }
            SchemaOp::IoMoveFile { src, dst } => {
                self.value_depends_on_loop_var(src, loop_var) ||
                self.value_depends_on_loop_var(dst, loop_var)
            }

            SchemaOp::ForEach { body, parallel, .. } => {
                if !parallel {
                    return false;
                }
                for entry in &body.entries {
                    if !self.is_op_parallelizable(&entry.op, loop_var) {
                        return false;
                    }
                }
                true
            }

            SchemaOp::IfBlock { then_body, else_body, .. } => {
                for entry in &then_body.entries {
                    if !self.is_op_parallelizable(&entry.op, loop_var) {
                        return false;
                    }
                }
                if let Some(else_body) = else_body {
                    for entry in &else_body.entries {
                        if !self.is_op_parallelizable(&entry.op, loop_var) {
                            return false;
                        }
                    }
                }
                true
            }

            SchemaOp::Map { body, .. } => {
                for entry in &body.entries {
                    if !self.is_op_parallelizable(&entry.op, loop_var) {
                        return false;
                    }
                }
                true
            }

            SchemaOp::Filter { predicate, .. } => {
                for entry in &predicate.entries {
                    if !self.is_op_parallelizable(&entry.op, loop_var) {
                        return false;
                    }
                }
                true
            }

            SchemaOp::IoReadFile { .. } |
            SchemaOp::IoFileExists { .. } |
            SchemaOp::IoIsDir { .. } |
            SchemaOp::IoIsFile { .. } |
            SchemaOp::IoListDir { .. } |
            SchemaOp::IoFileSize { .. } => true,

            SchemaOp::HttpRequest { .. } => true,

            SchemaOp::ExecRun { .. } |
            SchemaOp::ExecShell { .. } |
            SchemaOp::ExecEnv { .. } => true,

            SchemaOp::JsonEncode { .. } |
            SchemaOp::JsonDecode { .. } => true,

            SchemaOp::BpSleep { .. } |
            SchemaOp::BpNow => true,

            SchemaOp::OpsAll { .. } |
            SchemaOp::OpsAny { .. } |
            SchemaOp::OpsAtLeast { .. } |
            SchemaOp::OpsAtMost { .. } |
            SchemaOp::OpsAfter { .. } => true,

            SchemaOp::Add { .. } |
            SchemaOp::Sub { .. } |
            SchemaOp::Mul { .. } |
            SchemaOp::Div { .. } |
            SchemaOp::FloorDiv { .. } |
            SchemaOp::Mod { .. } |
            SchemaOp::Neg { .. } |
            SchemaOp::Eq { .. } |
            SchemaOp::Ne { .. } |
            SchemaOp::Lt { .. } |
            SchemaOp::Le { .. } |
            SchemaOp::Gt { .. } |
            SchemaOp::Ge { .. } |
            SchemaOp::Not { .. } |
            SchemaOp::Concat { .. } |
            SchemaOp::Contains { .. } |
            SchemaOp::Len { .. } |
            SchemaOp::Index { .. } |
            SchemaOp::SetIndex { .. } |
            SchemaOp::Min { .. } |
            SchemaOp::Max { .. } |
            SchemaOp::Sum { .. } |
            SchemaOp::Abs { .. } |
            SchemaOp::Sorted { .. } |
            SchemaOp::Reversed { .. } |
            SchemaOp::ToBool { .. } |
            SchemaOp::ToInt { .. } |
            SchemaOp::ToFloat { .. } |
            SchemaOp::ToStr { .. } |
            SchemaOp::If { .. } |
            SchemaOp::FrozenValue { .. } => true,
        }
    }

    fn value_depends_on_loop_var(&self, value: &SchemaValue, loop_var: &str) -> bool {
        match value {
            SchemaValue::ParamRef(name) => name == loop_var,
            SchemaValue::OpRef { .. } => true,
            SchemaValue::List(items) => items.iter().any(|v| self.value_depends_on_loop_var(v, loop_var)),
            SchemaValue::Literal(_) | SchemaValue::EnvRef(_) | SchemaValue::ConfigRef(_) => false,
        }
    }

    fn exec_loop_body_for_subplan(&mut self, stmt: &AstStmt) -> Result<(), String> {
        match &stmt.node {
            StmtP::Statements(stmts) => {
                for s in stmts {
                    self.exec_loop_body_for_subplan(s)?;
                }
                Ok(())
            }
            StmtP::IfElse(cond, branches) => {
                let cond_value = self.eval_expr(cond)?;
                if cond_value.contains_dynamic() || cond_value.is_op_ref() {
                    let then_subplan = self.compile_nested_subplan(&branches.0)?;
                    let else_subplan = if matches!(branches.1.node, StmtP::Pass) {
                        None
                    } else {
                        Some(self.compile_nested_subplan(&branches.1)?)
                    };

                    let ifblock_op = SchemaOp::IfBlock {
                        condition: cond_value.to_schema_value(),
                        then_body: then_subplan,
                        else_body: else_subplan,
                    };

                    self.add_schema_op(ifblock_op);
                    Ok(())
                } else {
                    if cond_value.is_truthy() {
                        self.exec_loop_body_for_subplan(&branches.0)
                    } else {
                        self.exec_loop_body_for_subplan(&branches.1)
                    }
                }
            }
            StmtP::Expression(expr) => {
                self.eval_expr(expr)?;
                Ok(())
            }
            StmtP::Assign(assign) => {
                let value = self.eval_expr(&assign.rhs)?;
                self.assign_target(&assign.lhs.node, value)?;
                Ok(())
            }
            StmtP::AssignModify(target, op, expr) => {
                let current = self.eval_target_value(target)?;
                let rhs = self.eval_expr(expr)?;
                let new_value = self.apply_assign_op(&current, op, &rhs)?;
                self.assign_target(&target.node, new_value)?;
                Ok(())
            }
            StmtP::Pass => Ok(()),
            StmtP::Break => {
                self.add_schema_op(SchemaOp::Break);
                Ok(())
            }
            StmtP::Continue => {
                self.add_schema_op(SchemaOp::Continue);
                Ok(())
            }
            _ => Err(format!("statement type not supported in runtime loop body"))
        }
    }

    fn compile_nested_subplan(&mut self, body: &AstStmt) -> Result<SchemaSubPlan, String> {
        let old_entries = self.subplan_entries.take();
        let old_next_id = self.subplan_next_id;

        self.subplan_entries = Some(Vec::new());
        self.subplan_next_id = 0;

        let result = self.exec_loop_body_for_subplan(body);

        let entries = self.subplan_entries.take().unwrap_or_default();
        let output = if entries.is_empty() { 0 } else { entries.last().map(|e| e.local_id).unwrap_or(0) };

        self.subplan_entries = old_entries;
        self.subplan_next_id = old_next_id;

        result?;

        Ok(SchemaSubPlan {
            params: self.subplan_params.iter().cloned().collect(),
            entries,
            output,
        })
    }

    pub fn generate_subplan_from_function(&mut self, func: &Rc<Function>, param_name: &str) -> Result<SchemaSubPlan, String> {
        let old_entries = self.subplan_entries.take();
        let old_next_id = self.subplan_next_id;
        let old_params = std::mem::take(&mut self.subplan_params);
        let call_scope = func.closure.child();
        let old_scope = std::mem::replace(&mut self.scope, call_scope);

        self.subplan_entries = Some(Vec::new());
        self.subplan_next_id = 0;
        self.subplan_params.insert(param_name.to_string());

        if let Some(first_param) = func.params.first() {
            self.scope.set(&first_param.name, Value::ParamRef(param_name.to_string()));
        }

        let result = match &func.body {
            FunctionBody::Ast(stmt) => {
                self.exec_loop_body_for_subplan(stmt)
            }
            FunctionBody::Lambda(expr) => {
                let value = self.eval_expr(expr)?;
                if let Some(ref mut entries) = self.subplan_entries {
                    let local_id = self.subplan_next_id;
                    self.subplan_next_id += 1;

                    let op = match value {
                        Value::OpRef(id) => {
                            if let Some(entry) = self.schema.get(id) {
                                entry.op.clone()
                            } else {
                                return Err("invalid op reference in lambda".to_string());
                            }
                        }
                        Value::ParamRef(name) => SchemaOp::Index {
                            base: SchemaValue::ParamRef(name),
                            index: SchemaValue::literal_int(0),
                        },
                        other => {
                            let schema_value = other.to_schema_value();
                            SchemaOp::If {
                                condition: SchemaValue::literal_bool(true),
                                then_value: schema_value.clone(),
                                else_value: schema_value,
                            }
                        }
                    };

                    entries.push(SchemaSubPlanEntry {
                        local_id,
                        op,
                        guard: None,
                    });
                }
                Ok(())
            }
        };

        self.scope = old_scope;

        let entries = self.subplan_entries.take().unwrap_or_default();
        let output = if entries.is_empty() { 0 } else { entries.last().map(|e| e.local_id).unwrap_or(0) };

        self.subplan_entries = old_entries;
        self.subplan_next_id = old_next_id;
        self.subplan_params = old_params;

        result?;

        Ok(SchemaSubPlan {
            params: vec![param_name.to_string()],
            entries,
            output,
        })
    }

    fn eval_target_value(&mut self, target: &starlark_syntax::syntax::ast::AstAssignTargetP<starlark_syntax::syntax::ast::AstNoPayload>) -> Result<Value, String> {
        match &target.node {
            AssignTargetP::Identifier(ident) => {
                self.scope.get(&ident.node.ident)
                    .ok_or_else(|| format!("name '{}' is not defined", ident.node.ident))
            }
            AssignTargetP::Index(base_idx) => {
                let (base_expr, index_expr) = &**base_idx;
                let base = self.eval_expr(base_expr)?;
                let index = self.eval_expr(index_expr)?;
                self.eval_index(&base, &index)
            }
            _ => Err("unsupported assignment target for modify".to_string())
        }
    }

    pub fn generate(source: &str, filename: &str) -> Result<Schema, String> {
        let mut gen = Self::new();
        gen.generate_from_source(source, filename)
    }

    pub fn generate_from_source(&mut self, source: &str, filename: &str) -> Result<Schema, String> {
        self.current_file = Some(PathBuf::from(filename));
        let ast = AstModule::parse(filename, source.to_string(), &Dialect::Extended)
            .map_err(|e| e.to_string())?;

        self.generate_from_ast(&ast)?;
        Ok(std::mem::take(&mut self.schema))
    }

    pub fn generate_for_eval(source: &str, filename: &str) -> Result<Schema, String> {
        let mut gen = Self::new();
        gen.generate_from_source_for_eval(source, filename)
    }

    pub fn generate_from_source_for_eval(&mut self, source: &str, filename: &str) -> Result<Schema, String> {
        self.current_file = Some(PathBuf::from(filename));
        let ast = AstModule::parse(filename, source.to_string(), &Dialect::Extended)
            .map_err(|e| e.to_string())?;

        self.generate_from_ast(&ast)?;

        let result_value = self.last_expression_value.clone().unwrap_or(Value::None);
        self.schema.add_op(
            SchemaOp::FrozenValue {
                name: "_result".to_string(),
                value: result_value.to_schema_value(),
            },
            None,
        );

        Ok(std::mem::take(&mut self.schema))
    }

    pub fn generate_from_ast(&mut self, ast: &AstModule) -> Result<(), String> {
        use starlark_syntax::syntax::module::AstModuleFields;
        self.exec_stmt(ast.statement())
    }

    pub fn register_module(&mut self, module_path: &str, exports: HashMap<String, Value>) {
        self.module_registry.insert(module_path.to_string(), CompiledModule {
            path: module_path.to_string(),
            exports,
            source_hash: None,
            is_builtin: false,
        });
    }

    fn exec_stmt(&mut self, stmt: &AstStmt) -> Result<(), String> {
        match &stmt.node {
            StmtP::Pass => Ok(()),

            StmtP::Break => Err("break outside of loop".to_string()),

            StmtP::Continue => Err("continue outside of loop".to_string()),

            StmtP::Return(_) => Err("return outside of function".to_string()),

            StmtP::Expression(expr) => {
                let value = self.eval_expr(expr)?;
                self.last_expression_value = Some(value);
                Ok(())
            }

            StmtP::Assign(assign) => {
                let value = self.eval_expr(&assign.rhs)?;
                self.assign_target(&assign.lhs.node, value)?;
                Ok(())
            }

            StmtP::AssignModify(target, op, expr) => {
                let current = self.eval_assign_target_value(&target.node)?;
                let rhs = self.eval_expr(expr)?;
                let new_value = self.apply_assign_op(&current, op, &rhs)?;
                self.assign_target(&target.node, new_value)?;
                Ok(())
            }

            StmtP::Statements(stmts) => {
                for s in stmts {
                    self.exec_stmt(s)?;
                }
                Ok(())
            }

            StmtP::If(cond, body) => {
                if !self.in_function {
                    return Err("if statement not allowed at top level".to_string());
                }
                let cond_value = self.eval_expr(cond)?;
                if cond_value.is_truthy() {
                    self.exec_stmt(body)?;
                }
                Ok(())
            }

            StmtP::IfElse(cond, branches) => {
                if !self.in_function {
                    return Err("if statement not allowed at top level".to_string());
                }
                let cond_value = self.eval_expr(cond)?;
                if cond_value.is_truthy() {
                    self.exec_stmt(&branches.0)?;
                } else {
                    self.exec_stmt(&branches.1)?;
                }
                Ok(())
            }

            StmtP::For(for_stmt) => {
                if !self.in_function {
                    return Err("for statement not allowed at top level".to_string());
                }
                let iterable = self.eval_expr(&for_stmt.over)?;
                let items = self.extract_iterable(&iterable)?;

                let list_ptr = if let Value::List(ref l) = iterable {
                    let ptr = Rc::as_ptr(l);
                    self.iterating_lists.insert(ptr);
                    Some(ptr)
                } else {
                    None
                };
                let dict_ptr = if let Value::Dict(ref d) = iterable {
                    let ptr = Rc::as_ptr(d);
                    self.iterating_dicts.insert(ptr);
                    Some(ptr)
                } else {
                    None
                };

                let result = (|| {
                    for item in items {
                        self.assign_target(&for_stmt.var.node, item)?;
                        match self.exec_stmt(&for_stmt.body) {
                            Ok(()) => {}
                            Err(e) if e == "break" => break,
                            Err(e) if e == "continue" => continue,
                            Err(e) => return Err(e),
                        }
                    }
                    Ok(())
                })();

                if let Some(ptr) = list_ptr {
                    self.iterating_lists.remove(&ptr);
                }
                if let Some(ptr) = dict_ptr {
                    self.iterating_dicts.remove(&ptr);
                }

                result
            }

            StmtP::Def(def) => {
                let name = def.name.node.ident.clone();
                let params = self.extract_parameters(&def.params)?;
                let body = FunctionBody::Ast(Box::new((*def.body).clone()));

                let func = Function {
                    name: name.clone(),
                    params,
                    body,
                    closure: self.scope.clone(),
                };

                self.scope.set(name, Value::Function(Rc::new(func)));
                Ok(())
            }

            StmtP::Load(load) => {
                let module_path = &load.module.node;
                self.handle_load(module_path, &load.args)?;
                Ok(())
            }
        }
    }

    fn handle_load(&mut self, module_path: &str, args: &[starlark_syntax::syntax::ast::LoadArgP<starlark_syntax::syntax::ast::AstNoPayload>]) -> Result<(), String> {
        if !self.module_registry.contains_key(module_path) {
            self.compile_and_register_module(module_path)?;
        }

        let exports = self.module_registry.get(module_path)
            .ok_or_else(|| format!("Module not found: {}", module_path))?
            .exports.clone();

        for arg in args {
            let their_name = &arg.their.node;
            let local_name = &arg.local.node.ident;

            if their_name.starts_with('_') {
                return Err(format!("Cannot import private '{}' from '{}'", their_name, module_path));
            }

            let value = exports.get(their_name)
                .ok_or_else(|| format!("'{}' not found in module '{}'", their_name, module_path))?;

            self.scope.set(local_name.clone(), value.clone());
        }

        Ok(())
    }

    fn compile_and_register_module(&mut self, path: &str) -> Result<(), String> {
        if self.module_stack.contains(&path.to_string()) {
            let cycle = self.module_stack.join(" -> ");
            return Err(format!("Circular import: {} -> {}", cycle, path));
        }

        let resolved = self.resolve_module_path(path)?;
        let source = std::fs::read_to_string(&resolved)
            .map_err(|e| format!("Cannot read '{}': {}", path, e))?;

        self.module_stack.push(path.to_string());

        let mut child = Self::new_for_module(self.module_registry.clone());
        child.module_stack = self.module_stack.clone();
        child.current_file = Some(resolved.clone());
        child.generate_from_source(&source, resolved.to_str().unwrap_or(path))?;

        self.module_stack.pop();

        let exports = child.scope.get_all_globals()
            .into_iter()
            .filter(|(name, _)| !name.starts_with('_'))
            .collect();

        self.module_registry.insert(path.to_string(), CompiledModule {
            path: path.to_string(),
            exports,
            source_hash: Some(compute_source_hash(&source)),
            is_builtin: false,
        });

        for (k, v) in child.module_registry {
            self.module_registry.entry(k).or_insert(v);
        }

        Ok(())
    }

    fn resolve_module_path(&self, path: &str) -> Result<PathBuf, String> {
        if path.starts_with("@bp/") {
            return Err(format!("Built-in module '{}' should be pre-registered", path));
        }

        if let Some(ref current) = self.current_file {
            if let Some(dir) = current.parent() {
                let candidate = dir.join(path);
                if candidate.exists() {
                    return Ok(candidate);
                }
            }
        }

        Err(format!("Module not found: {}", path))
    }

    fn eval_expr(&mut self, expr: &AstExpr) -> Result<Value, String> {
        match &expr.node {
            ExprP::Literal(lit) => self.eval_literal(lit),

            ExprP::Identifier(ident) => {
                self.scope.get(&ident.node.ident)
                    .ok_or_else(|| format!("name '{}' is not defined", ident.node.ident))
            }

            ExprP::Tuple(exprs) => {
                let values: Result<Vec<Value>, String> = exprs.iter()
                    .map(|e| self.eval_expr(e))
                    .collect();
                Ok(Value::Tuple(values?))
            }

            ExprP::List(exprs) => {
                let values: Result<Vec<Value>, String> = exprs.iter()
                    .map(|e| self.eval_expr(e))
                    .collect();
                Ok(Value::List(Rc::new(RefCell::new(values?))))
            }

            ExprP::Dict(pairs) => {
                let mut map = HashMap::new();
                for (key_expr, val_expr) in pairs {
                    let key = self.eval_expr(key_expr)?;
                    let key_str = match key {
                        Value::String(s) => s,
                        _ => return Err("dict keys must be strings".to_string()),
                    };
                    let val = self.eval_expr(val_expr)?;
                    map.insert(key_str, val);
                }
                Ok(Value::Dict(Rc::new(RefCell::new(map))))
            }

            ExprP::Not(expr) => {
                let val = self.eval_expr(expr)?;
                Ok(Value::Bool(!val.is_truthy()))
            }

            ExprP::Minus(expr) => {
                let val = self.eval_expr(expr)?;
                match val {
                    Value::Int(n) => Ok(Value::Int(-n)),
                    Value::Float(f) => Ok(Value::Float(-f)),
                    _ => Err(format!("bad operand type for unary -: '{}'", val.type_name())),
                }
            }

            ExprP::Plus(expr) => {
                let val = self.eval_expr(expr)?;
                match val {
                    Value::Int(_) | Value::Float(_) => Ok(val),
                    _ => Err(format!("bad operand type for unary +: '{}'", val.type_name())),
                }
            }

            ExprP::BitNot(expr) => {
                let val = self.eval_expr(expr)?;
                match val {
                    Value::Int(n) => Ok(Value::Int(!n)),
                    _ => Err(format!("bad operand type for ~: '{}'", val.type_name())),
                }
            }

            ExprP::Op(lhs, op, rhs) => {
                self.eval_binop(lhs, *op, rhs)
            }

            ExprP::If(cond_then_else) => {
                let (cond, then_val, else_val) = &**cond_then_else;
                let cond_value = self.eval_expr(cond)?;
                if cond_value.is_truthy() {
                    self.eval_expr(then_val)
                } else {
                    self.eval_expr(else_val)
                }
            }

            ExprP::Index(base_index) => {
                let (base_expr, index_expr) = &**base_index;
                let base = self.eval_expr(base_expr)?;
                let index = self.eval_expr(index_expr)?;
                self.eval_index(&base, &index)
            }

            ExprP::Slice(base, start, stop, step) => {
                let base_val = self.eval_expr(base)?;
                let start_val = start.as_ref().map(|e| self.eval_expr(e)).transpose()?;
                let stop_val = stop.as_ref().map(|e| self.eval_expr(e)).transpose()?;
                let step_val = step.as_ref().map(|e| self.eval_expr(e)).transpose()?;
                self.eval_slice(&base_val, start_val, stop_val, step_val)
            }

            ExprP::Dot(base, attr) => {
                let base_val = self.eval_expr(base)?;
                self.eval_dot(&base_val, &attr.node)
            }

            ExprP::Call(func_expr, call_args) => {
                let func = self.eval_expr(func_expr)?;
                let (args, kwargs) = self.eval_call_args(&call_args.args)?;
                self.call_value(&func, args, kwargs)
            }

            ExprP::Lambda(lambda) => {
                let params = self.extract_parameters(&lambda.params)?;
                let body = FunctionBody::Lambda(Box::new((*lambda.body).clone()));

                let func = Function {
                    name: "<lambda>".to_string(),
                    params,
                    body,
                    closure: self.scope.clone(),
                };

                Ok(Value::Function(Rc::new(func)))
            }

            ExprP::ListComprehension(expr, for_clause, clauses) => {
                self.eval_list_comprehension(expr, for_clause, clauses)
            }

            ExprP::DictComprehension(key_val, for_clause, clauses) => {
                self.eval_dict_comprehension(&key_val.0, &key_val.1, for_clause, clauses)
            }

            ExprP::Index2(base_i0_i1) => {
                let (base, i0, i1) = &**base_i0_i1;
                let _base_val = self.eval_expr(base)?;
                let _i0_val = self.eval_expr(i0)?;
                let _i1_val = self.eval_expr(i1)?;
                Err("two-argument indexing not supported".to_string())
            }

            ExprP::FString(fstring) => {
                let format_str = &fstring.format.node;
                let mut result = String::new();
                let mut expr_iter = fstring.expressions.iter();

                let mut chars = format_str.chars().peekable();
                while let Some(c) = chars.next() {
                    if c == '{' {
                        if chars.peek() == Some(&'{') {
                            chars.next();
                            result.push('{');
                        } else {
                            while chars.peek() != Some(&'}') && chars.peek().is_some() {
                                chars.next();
                            }
                            chars.next();

                            if let Some(expr) = expr_iter.next() {
                                let val = self.eval_expr(expr)?;
                                result.push_str(&val.to_string_repr());
                            }
                        }
                    } else if c == '}' {
                        if chars.peek() == Some(&'}') {
                            chars.next();
                            result.push('}');
                        } else {
                            result.push(c);
                        }
                    } else {
                        result.push(c);
                    }
                }

                Ok(Value::String(result))
            }
        }
    }

    fn eval_literal(&self, lit: &AstLiteral) -> Result<Value, String> {
        match lit {
            AstLiteral::Int(i) => {
                match &i.node {
                    TokenInt::I32(n) => Ok(Value::Int(*n as i64)),
                    TokenInt::BigInt(big) => {
                        use std::convert::TryInto;
                        big.try_into()
                            .map(Value::Int)
                            .map_err(|_| format!("integer literal too large: {}", big))
                    }
                }
            }
            AstLiteral::Float(f) => Ok(Value::Float(f.node)),
            AstLiteral::String(s) => Ok(Value::String(s.node.clone())),
            AstLiteral::Ellipsis => Err("Ellipsis not supported".to_string()),
        }
    }

    fn eval_binop(&mut self, lhs: &AstExpr, op: BinOp, rhs: &AstExpr) -> Result<Value, String> {
        match op {
            BinOp::And => {
                let left = self.eval_expr(lhs)?;
                if !left.is_truthy() {
                    return Ok(left);
                }
                self.eval_expr(rhs)
            }
            BinOp::Or => {
                let left = self.eval_expr(lhs)?;
                if left.is_truthy() {
                    return Ok(left);
                }
                self.eval_expr(rhs)
            }
            _ => {
                let left = self.eval_expr(lhs)?;
                let right = self.eval_expr(rhs)?;
                self.apply_binop(&left, op, &right)
            }
        }
    }

    fn apply_binop(&self, left: &Value, op: BinOp, right: &Value) -> Result<Value, String> {
        match op {
            BinOp::Add => self.add_values(left, right),
            BinOp::Subtract => self.sub_values(left, right),
            BinOp::Multiply => self.mul_values(left, right),
            BinOp::Divide => self.div_values(left, right),
            BinOp::FloorDivide => self.floordiv_values(left, right),
            BinOp::Percent => self.mod_values(left, right),
            BinOp::Equal => Ok(Value::Bool(left == right)),
            BinOp::NotEqual => Ok(Value::Bool(left != right)),
            BinOp::Less => self.compare_values(left, right, |ord| ord == std::cmp::Ordering::Less),
            BinOp::LessOrEqual => self.compare_values(left, right, |ord| ord != std::cmp::Ordering::Greater),
            BinOp::Greater => self.compare_values(left, right, |ord| ord == std::cmp::Ordering::Greater),
            BinOp::GreaterOrEqual => self.compare_values(left, right, |ord| ord != std::cmp::Ordering::Less),
            BinOp::In => self.check_contains(right, left),
            BinOp::NotIn => self.check_contains(right, left).map(|v| {
                if let Value::Bool(b) = v { Value::Bool(!b) } else { v }
            }),
            BinOp::BitAnd => self.bitand_values(left, right),
            BinOp::BitOr => self.bitor_values(left, right),
            BinOp::BitXor => self.bitxor_values(left, right),
            BinOp::LeftShift => self.leftshift_values(left, right),
            BinOp::RightShift => self.rightshift_values(left, right),
            BinOp::And | BinOp::Or => unreachable!("handled above"),
        }
    }

    fn add_values(&self, left: &Value, right: &Value) -> Result<Value, String> {
        match (left, right) {
            (Value::Int(a), Value::Int(b)) => Ok(Value::Int(a + b)),
            (Value::Float(a), Value::Float(b)) => Ok(Value::Float(a + b)),
            (Value::Int(a), Value::Float(b)) => Ok(Value::Float(*a as f64 + b)),
            (Value::Float(a), Value::Int(b)) => Ok(Value::Float(a + *b as f64)),
            (Value::String(a), Value::String(b)) => Ok(Value::String(format!("{}{}", a, b))),
            (Value::List(a), Value::List(b)) => {
                let mut result = a.borrow().clone();
                result.extend(b.borrow().clone());
                Ok(Value::List(Rc::new(RefCell::new(result))))
            }
            (Value::Tuple(a), Value::Tuple(b)) => {
                let mut result = a.clone();
                result.extend(b.clone());
                Ok(Value::Tuple(result))
            }
            _ => Err(format!("unsupported operand type(s) for +: '{}' and '{}'", left.type_name(), right.type_name())),
        }
    }

    fn sub_values(&self, left: &Value, right: &Value) -> Result<Value, String> {
        match (left, right) {
            (Value::Int(a), Value::Int(b)) => Ok(Value::Int(a - b)),
            (Value::Float(a), Value::Float(b)) => Ok(Value::Float(a - b)),
            (Value::Int(a), Value::Float(b)) => Ok(Value::Float(*a as f64 - b)),
            (Value::Float(a), Value::Int(b)) => Ok(Value::Float(a - *b as f64)),
            (Value::Set(a), Value::Set(b)) => {
                let result: HashSet<HashableValue> = a.borrow()
                    .difference(&*b.borrow())
                    .cloned()
                    .collect();
                Ok(Value::Set(Rc::new(RefCell::new(result))))
            }
            _ => Err(format!("unsupported operand type(s) for -: '{}' and '{}'", left.type_name(), right.type_name())),
        }
    }

    fn mul_values(&self, left: &Value, right: &Value) -> Result<Value, String> {
        match (left, right) {
            (Value::Int(a), Value::Int(b)) => Ok(Value::Int(a * b)),
            (Value::Float(a), Value::Float(b)) => Ok(Value::Float(a * b)),
            (Value::Int(a), Value::Float(b)) => Ok(Value::Float(*a as f64 * b)),
            (Value::Float(a), Value::Int(b)) => Ok(Value::Float(a * *b as f64)),
            (Value::String(s), Value::Int(n)) | (Value::Int(n), Value::String(s)) => {
                if *n < 0 {
                    Ok(Value::String(String::new()))
                } else {
                    Ok(Value::String(s.repeat(*n as usize)))
                }
            }
            (Value::List(l), Value::Int(n)) | (Value::Int(n), Value::List(l)) => {
                if *n < 0 {
                    Ok(Value::List(Rc::new(RefCell::new(Vec::new()))))
                } else {
                    let items = l.borrow();
                    let mut result = Vec::new();
                    for _ in 0..*n {
                        result.extend(items.clone());
                    }
                    Ok(Value::List(Rc::new(RefCell::new(result))))
                }
            }
            (Value::Tuple(t), Value::Int(n)) | (Value::Int(n), Value::Tuple(t)) => {
                if *n < 0 {
                    Ok(Value::Tuple(Vec::new()))
                } else {
                    let mut result = Vec::new();
                    for _ in 0..*n {
                        result.extend(t.clone());
                    }
                    Ok(Value::Tuple(result))
                }
            }
            _ => Err(format!("unsupported operand type(s) for *: '{}' and '{}'", left.type_name(), right.type_name())),
        }
    }

    fn div_values(&self, left: &Value, right: &Value) -> Result<Value, String> {
        match (left, right) {
            (Value::Int(a), Value::Int(b)) => {
                if *b == 0 {
                    return Err("division by zero".to_string());
                }
                Ok(Value::Float(*a as f64 / *b as f64))
            }
            (Value::Float(a), Value::Float(b)) => {
                if *b == 0.0 {
                    return Err("division by zero".to_string());
                }
                Ok(Value::Float(a / b))
            }
            (Value::Int(a), Value::Float(b)) => {
                if *b == 0.0 {
                    return Err("division by zero".to_string());
                }
                Ok(Value::Float(*a as f64 / b))
            }
            (Value::Float(a), Value::Int(b)) => {
                if *b == 0 {
                    return Err("division by zero".to_string());
                }
                Ok(Value::Float(a / *b as f64))
            }
            _ => Err(format!("unsupported operand type(s) for /: '{}' and '{}'", left.type_name(), right.type_name())),
        }
    }

    fn floordiv_values(&self, left: &Value, right: &Value) -> Result<Value, String> {
        match (left, right) {
            (Value::Int(a), Value::Int(b)) => {
                if *b == 0 {
                    return Err("integer division by zero".to_string());
                }
                Ok(Value::Int((*a as f64 / *b as f64).floor() as i64))
            }
            (Value::Float(a), Value::Float(b)) => {
                if *b == 0.0 {
                    return Err("float floor division by zero".to_string());
                }
                Ok(Value::Float((a / b).floor()))
            }
            (Value::Int(a), Value::Float(b)) => {
                if *b == 0.0 {
                    return Err("float floor division by zero".to_string());
                }
                Ok(Value::Float((*a as f64 / b).floor()))
            }
            (Value::Float(a), Value::Int(b)) => {
                if *b == 0 {
                    return Err("float floor division by zero".to_string());
                }
                Ok(Value::Float((a / *b as f64).floor()))
            }
            _ => Err(format!("unsupported operand type(s) for //: '{}' and '{}'", left.type_name(), right.type_name())),
        }
    }

    fn mod_values(&self, left: &Value, right: &Value) -> Result<Value, String> {
        match (left, right) {
            (Value::Int(a), Value::Int(b)) => {
                if *b == 0 {
                    return Err("integer modulo by zero".to_string());
                }
                let floor_div = (*a as f64 / *b as f64).floor() as i64;
                Ok(Value::Int(a - floor_div * b))
            }
            (Value::Float(a), Value::Float(b)) => {
                if *b == 0.0 {
                    return Err("float modulo by zero".to_string());
                }
                Ok(Value::Float(a % b))
            }
            (Value::String(s), right) => {
                self.format_string(s, right)
            }
            _ => Err(format!("unsupported operand type(s) for %: '{}' and '{}'", left.type_name(), right.type_name())),
        }
    }

    fn format_string(&self, format_str: &str, args: &Value) -> Result<Value, String> {
        let format_args = match args {
            Value::Tuple(t) => t.clone(),
            other => vec![other.clone()],
        };

        let mut result = String::new();
        let mut arg_index = 0;
        let mut chars = format_str.chars().peekable();

        while let Some(c) = chars.next() {
            if c == '%' {
                if chars.peek() == Some(&'%') {
                    chars.next();
                    result.push('%');
                } else {
                    while chars.peek().map(|c| c.is_ascii_digit() || *c == '-' || *c == '+' || *c == ' ' || *c == '0' || *c == '.').unwrap_or(false) {
                        chars.next();
                    }

                    let format_char = chars.next().ok_or("incomplete format")?;

                    if arg_index >= format_args.len() {
                        return Err("not enough arguments for format string".to_string());
                    }

                    let arg = &format_args[arg_index];
                    arg_index += 1;

                    match format_char {
                        's' => result.push_str(&arg.to_string_repr()),
                        'r' => result.push_str(&arg.to_repr()),
                        'd' | 'i' => {
                            match arg {
                                Value::Int(n) => result.push_str(&n.to_string()),
                                _ => return Err(format!("%{} format requires an integer", format_char)),
                            }
                        }
                        'x' => {
                            match arg {
                                Value::Int(n) => result.push_str(&format!("{:x}", n)),
                                _ => return Err("%x format requires an integer".to_string()),
                            }
                        }
                        'X' => {
                            match arg {
                                Value::Int(n) => result.push_str(&format!("{:X}", n)),
                                _ => return Err("%X format requires an integer".to_string()),
                            }
                        }
                        'o' => {
                            match arg {
                                Value::Int(n) => result.push_str(&format!("{:o}", n)),
                                _ => return Err("%o format requires an integer".to_string()),
                            }
                        }
                        'f' | 'g' => {
                            match arg {
                                Value::Int(n) => result.push_str(&format!("{:?}", *n as f64)),
                                Value::Float(f) => result.push_str(&format!("{:?}", f)),
                                _ => return Err(format!("%{} format requires a number", format_char)),
                            }
                        }
                        'e' => {
                            match arg {
                                Value::Int(n) => result.push_str(&format!("{:e}", *n as f64)),
                                Value::Float(f) => result.push_str(&format!("{:e}", f)),
                                _ => return Err("%e format requires a number".to_string()),
                            }
                        }
                        _ => return Err(format!("unsupported format character: {}", format_char)),
                    }
                }
            } else {
                result.push(c);
            }
        }

        Ok(Value::String(result))
    }

    fn compare_values<F>(&self, left: &Value, right: &Value, f: F) -> Result<Value, String>
    where
        F: Fn(std::cmp::Ordering) -> bool,
    {
        match left.partial_cmp(right) {
            Some(ord) => Ok(Value::Bool(f(ord))),
            None => Err(format!("'<' not supported between '{}' and '{}'", left.type_name(), right.type_name())),
        }
    }

    fn check_contains(&self, container: &Value, item: &Value) -> Result<Value, String> {
        match container {
            Value::String(s) => {
                match item {
                    Value::String(needle) => Ok(Value::Bool(s.contains(needle.as_str()))),
                    _ => Err(format!("'in <string>' requires string as left operand, not {}", item.type_name())),
                }
            }
            Value::List(l) => Ok(Value::Bool(l.borrow().contains(item))),
            Value::Tuple(t) => Ok(Value::Bool(t.contains(item))),
            Value::Dict(d) => {
                match item {
                    Value::String(key) => Ok(Value::Bool(d.borrow().contains_key(key))),
                    _ => Err("dict keys must be strings".to_string()),
                }
            }
            Value::Set(s) => {
                let h = HashableValue::from_value(item)?;
                Ok(Value::Bool(s.borrow().contains(&h)))
            }
            _ => Err(format!("argument of type '{}' is not iterable", container.type_name())),
        }
    }

    fn bitand_values(&self, left: &Value, right: &Value) -> Result<Value, String> {
        match (left, right) {
            (Value::Int(a), Value::Int(b)) => Ok(Value::Int(a & b)),
            (Value::Set(a), Value::Set(b)) => {
                let result: HashSet<HashableValue> = a.borrow()
                    .intersection(&*b.borrow())
                    .cloned()
                    .collect();
                Ok(Value::Set(Rc::new(RefCell::new(result))))
            }
            _ => Err(format!("unsupported operand type(s) for &: '{}' and '{}'", left.type_name(), right.type_name())),
        }
    }

    fn bitor_values(&self, left: &Value, right: &Value) -> Result<Value, String> {
        match (left, right) {
            (Value::Int(a), Value::Int(b)) => Ok(Value::Int(a | b)),
            (Value::Set(a), Value::Set(b)) => {
                let result: HashSet<HashableValue> = a.borrow()
                    .union(&*b.borrow())
                    .cloned()
                    .collect();
                Ok(Value::Set(Rc::new(RefCell::new(result))))
            }
            _ => Err(format!("unsupported operand type(s) for |: '{}' and '{}'", left.type_name(), right.type_name())),
        }
    }

    fn bitxor_values(&self, left: &Value, right: &Value) -> Result<Value, String> {
        match (left, right) {
            (Value::Int(a), Value::Int(b)) => Ok(Value::Int(a ^ b)),
            (Value::Set(a), Value::Set(b)) => {
                let result: HashSet<HashableValue> = a.borrow()
                    .symmetric_difference(&*b.borrow())
                    .cloned()
                    .collect();
                Ok(Value::Set(Rc::new(RefCell::new(result))))
            }
            _ => Err(format!("unsupported operand type(s) for ^: '{}' and '{}'", left.type_name(), right.type_name())),
        }
    }

    fn leftshift_values(&self, left: &Value, right: &Value) -> Result<Value, String> {
        match (left, right) {
            (Value::Int(a), Value::Int(b)) => {
                if *b < 0 {
                    return Err("negative shift count".to_string());
                }
                Ok(Value::Int(a << b))
            }
            _ => Err(format!("unsupported operand type(s) for <<: '{}' and '{}'", left.type_name(), right.type_name())),
        }
    }

    fn rightshift_values(&self, left: &Value, right: &Value) -> Result<Value, String> {
        match (left, right) {
            (Value::Int(a), Value::Int(b)) => {
                if *b < 0 {
                    return Err("negative shift count".to_string());
                }
                Ok(Value::Int(a >> b))
            }
            _ => Err(format!("unsupported operand type(s) for >>: '{}' and '{}'", left.type_name(), right.type_name())),
        }
    }

    fn apply_assign_op(&self, current: &Value, op: &AssignOp, rhs: &Value) -> Result<Value, String> {
        match op {
            AssignOp::Add => self.add_values(current, rhs),
            AssignOp::Subtract => self.sub_values(current, rhs),
            AssignOp::Multiply => self.mul_values(current, rhs),
            AssignOp::Divide => self.div_values(current, rhs),
            AssignOp::FloorDivide => self.floordiv_values(current, rhs),
            AssignOp::Percent => self.mod_values(current, rhs),
            AssignOp::BitAnd => self.bitand_values(current, rhs),
            AssignOp::BitOr => self.bitor_values(current, rhs),
            AssignOp::BitXor => self.bitxor_values(current, rhs),
            AssignOp::LeftShift => self.leftshift_values(current, rhs),
            AssignOp::RightShift => self.rightshift_values(current, rhs),
        }
    }

    fn eval_index(&self, base: &Value, index: &Value) -> Result<Value, String> {
        match base {
            Value::String(s) => {
                let i = self.normalize_index(index, s.len())?;
                s.chars().nth(i)
                    .map(|c| Value::String(c.to_string()))
                    .ok_or_else(|| "string index out of range".to_string())
            }
            Value::List(l) => {
                let items = l.borrow();
                let i = self.normalize_index(index, items.len())?;
                items.get(i).cloned()
                    .ok_or_else(|| "list index out of range".to_string())
            }
            Value::Tuple(t) => {
                let i = self.normalize_index(index, t.len())?;
                t.get(i).cloned()
                    .ok_or_else(|| "tuple index out of range".to_string())
            }
            Value::Dict(d) => {
                match index {
                    Value::String(key) => {
                        d.borrow().get(key).cloned()
                            .ok_or_else(|| format!("KeyError: '{}'", key))
                    }
                    _ => Err("dict keys must be strings".to_string()),
                }
            }
            _ => Err(format!("'{}' object is not subscriptable", base.type_name())),
        }
    }

    fn normalize_index(&self, index: &Value, len: usize) -> Result<usize, String> {
        match index {
            Value::Int(i) => {
                let idx = if *i < 0 {
                    (len as i64 + *i) as usize
                } else {
                    *i as usize
                };
                if idx >= len {
                    Err("index out of range".to_string())
                } else {
                    Ok(idx)
                }
            }
            _ => Err(format!("indices must be integers, not {}", index.type_name())),
        }
    }

    fn eval_slice(&self, base: &Value, start: Option<Value>, stop: Option<Value>, step: Option<Value>) -> Result<Value, String> {
        let len = match base {
            Value::String(s) => s.len(),
            Value::List(l) => l.borrow().len(),
            Value::Tuple(t) => t.len(),
            _ => return Err(format!("'{}' object is not subscriptable", base.type_name())),
        };

        let step = match step {
            Some(Value::Int(s)) => {
                if s == 0 {
                    return Err("slice step cannot be zero".to_string());
                }
                s
            }
            Some(_) => return Err("slice indices must be integers".to_string()),
            None => 1,
        };

        let start = match start {
            Some(Value::Int(s)) => {
                let s = if s < 0 { (len as i64 + s).max(0) } else { s.min(len as i64) };
                s as usize
            }
            Some(_) => return Err("slice indices must be integers".to_string()),
            None => if step > 0 { 0 } else { len.saturating_sub(1) },
        };

        let stop = match stop {
            Some(Value::Int(s)) => {
                let s = if s < 0 { (len as i64 + s).max(0) } else { s.min(len as i64) };
                s as usize
            }
            Some(_) => return Err("slice indices must be integers".to_string()),
            None => if step > 0 { len } else { 0_usize.wrapping_sub(1) },
        };

        let indices: Vec<usize> = if step > 0 {
            (start..stop).step_by(step as usize).collect()
        } else {
            let mut v = Vec::new();
            let mut i = start as i64;
            while i > stop as i64 {
                v.push(i as usize);
                i += step;
            }
            v
        };

        match base {
            Value::String(s) => {
                let chars: Vec<char> = s.chars().collect();
                let result: String = indices.iter()
                    .filter_map(|&i| chars.get(i))
                    .collect();
                Ok(Value::String(result))
            }
            Value::List(l) => {
                let items = l.borrow();
                let result: Vec<Value> = indices.iter()
                    .filter_map(|&i| items.get(i).cloned())
                    .collect();
                Ok(Value::List(Rc::new(RefCell::new(result))))
            }
            Value::Tuple(t) => {
                let result: Vec<Value> = indices.iter()
                    .filter_map(|&i| t.get(i).cloned())
                    .collect();
                Ok(Value::Tuple(result))
            }
            _ => unreachable!(),
        }
    }

    fn eval_dot(&self, base: &Value, attr: &str) -> Result<Value, String> {
        match base {
            Value::String(s) => self.string_method(s, attr),
            Value::Bytes(b) => self.bytes_method(b, attr),
            Value::List(l) => self.list_method(l, attr),
            Value::Dict(d) => {
                if let Some(value) = d.borrow().get(attr) {
                    Ok(value.clone())
                } else {
                    self.dict_method(d, attr)
                }
            }
            Value::Set(s) => self.set_method(s, attr),
            Value::Struct(fields) => {
                fields.get(attr)
                    .cloned()
                    .ok_or_else(|| format!("struct has no field '{}'", attr))
            }
            _ => Err(format!("'{}' object has no attribute '{}'", base.type_name(), attr)),
        }
    }

    fn string_method(&self, s: &str, method: &str) -> Result<Value, String> {
        let method_enum = StringMethod::from_str(method)
            .ok_or_else(|| format!("'str' object has no attribute '{}'", method))?;

        let s_clone = s.to_string();
        Ok(Value::BuiltinFunction(Rc::new(move |compiler, args, kwargs| {
            Self::call_string_method(compiler, &s_clone, method_enum, args, kwargs)
        })))
    }

    pub fn string_method_value(&self, s: &str, method: &str) -> Result<Value, String> {
        self.string_method(s, method)
    }

    pub fn list_method_value(&self, l: &Rc<RefCell<Vec<Value>>>, method: &str) -> Result<Value, String> {
        self.list_method(l, method)
    }

    pub fn dict_method_value(&self, d: &Rc<RefCell<HashMap<String, Value>>>, method: &str) -> Result<Value, String> {
        self.dict_method(d, method)
    }

    fn call_string_method(_compiler: &mut SchemaGenerator, s: &str, method: StringMethod, args: Vec<Value>, _kwargs: HashMap<String, Value>) -> Result<Value, String> {
        match method {
            StringMethod::Upper => Ok(Value::String(s.to_uppercase())),
            StringMethod::Lower => Ok(Value::String(s.to_lowercase())),
            StringMethod::Strip => Ok(Value::String(s.trim().to_string())),
            StringMethod::Lstrip => {
                let result = if let Some(Value::String(chars)) = args.first() {
                    s.trim_start_matches(|c| chars.contains(c)).to_string()
                } else {
                    s.trim_start().to_string()
                };
                Ok(Value::String(result))
            }
            StringMethod::Rstrip => {
                let result = if let Some(Value::String(chars)) = args.first() {
                    s.trim_end_matches(|c| chars.contains(c)).to_string()
                } else {
                    s.trim_end().to_string()
                };
                Ok(Value::String(result))
            }
            StringMethod::Capitalize => {
                let mut c = s.chars();
                let result = match c.next() {
                    None => String::new(),
                    Some(first) => first.to_uppercase().collect::<String>() + &c.as_str().to_lowercase(),
                };
                Ok(Value::String(result))
            }
            StringMethod::Title => {
                let result = s.split_whitespace()
                    .map(|word| {
                        let mut c = word.chars();
                        match c.next() {
                            None => String::new(),
                            Some(first) => first.to_uppercase().collect::<String>() + &c.as_str().to_lowercase(),
                        }
                    })
                    .collect::<Vec<_>>()
                    .join(" ");
                Ok(Value::String(result))
            }
            StringMethod::Swapcase => {
                let result: String = s.chars().map(|c| {
                    if c.is_uppercase() {
                        c.to_lowercase().collect::<String>()
                    } else {
                        c.to_uppercase().collect::<String>()
                    }
                }).collect();
                Ok(Value::String(result))
            }
            StringMethod::Isalpha => Ok(Value::Bool(!s.is_empty() && s.chars().all(|c| c.is_alphabetic()))),
            StringMethod::Isdigit => Ok(Value::Bool(!s.is_empty() && s.chars().all(|c| c.is_ascii_digit()))),
            StringMethod::Isalnum => Ok(Value::Bool(!s.is_empty() && s.chars().all(|c| c.is_alphanumeric()))),
            StringMethod::Isspace => Ok(Value::Bool(!s.is_empty() && s.chars().all(|c| c.is_whitespace()))),
            StringMethod::Isupper => Ok(Value::Bool(s.chars().any(|c| c.is_uppercase()) && !s.chars().any(|c| c.is_lowercase()))),
            StringMethod::Islower => Ok(Value::Bool(s.chars().any(|c| c.is_lowercase()) && !s.chars().any(|c| c.is_uppercase()))),
            StringMethod::Split => {
                let sep = args.first().and_then(|v| {
                    if let Value::String(s) = v { Some(s.clone()) } else { None }
                });
                let maxsplit = args.get(1).and_then(|v| {
                    if let Value::Int(n) = v { Some(*n as usize) } else { None }
                });
                let parts: Vec<Value> = match (sep, maxsplit) {
                    (Some(ref sep), Some(n)) => s.splitn(n + 1, sep.as_str()).map(|p| Value::String(p.to_string())).collect(),
                    (Some(ref sep), None) => s.split(sep.as_str()).map(|p| Value::String(p.to_string())).collect(),
                    (None, _) => s.split_whitespace().map(|p| Value::String(p.to_string())).collect(),
                };
                Ok(Value::List(Rc::new(RefCell::new(parts))))
            }
            StringMethod::Rsplit => {
                let sep = args.first().and_then(|v| {
                    if let Value::String(s) = v { Some(s.clone()) } else { None }
                });
                let maxsplit = args.get(1).and_then(|v| {
                    if let Value::Int(n) = v { Some(*n as usize) } else { None }
                });
                let parts: Vec<Value> = match (sep, maxsplit) {
                    (Some(ref sep), Some(n)) => s.rsplitn(n + 1, sep.as_str()).map(|p| Value::String(p.to_string())).collect(),
                    (Some(ref sep), None) => s.rsplit(sep.as_str()).map(|p| Value::String(p.to_string())).collect(),
                    (None, _) => s.split_whitespace().rev().map(|p| Value::String(p.to_string())).collect(),
                };
                Ok(Value::List(Rc::new(RefCell::new(parts))))
            }
            StringMethod::Splitlines => {
                let parts: Vec<Value> = s.lines().map(|l| Value::String(l.to_string())).collect();
                Ok(Value::List(Rc::new(RefCell::new(parts))))
            }
            StringMethod::Join => {
                if args.len() != 1 {
                    return Err("join() takes exactly 1 argument".to_string());
                }
                let items = match &args[0] {
                    Value::List(l) => l.borrow().clone(),
                    Value::Tuple(t) => t.clone(),
                    _ => return Err("join() argument must be iterable".to_string()),
                };
                let strings: Result<Vec<String>, String> = items.iter().map(|v| {
                    match v {
                        Value::String(s) => Ok(s.clone()),
                        _ => Err("join() sequence item must be str".to_string()),
                    }
                }).collect();
                Ok(Value::String(strings?.join(s)))
            }
            StringMethod::Replace => {
                if args.len() < 2 {
                    return Err("replace() takes at least 2 arguments".to_string());
                }
                let old = match &args[0] {
                    Value::String(s) => s.as_str(),
                    _ => return Err("replace() argument 1 must be str".to_string()),
                };
                let new = match &args[1] {
                    Value::String(s) => s.as_str(),
                    _ => return Err("replace() argument 2 must be str".to_string()),
                };
                let count = args.get(2).and_then(|v| {
                    if let Value::Int(n) = v { Some(*n as usize) } else { None }
                });
                let result = match count {
                    Some(n) => s.replacen(old, new, n),
                    None => s.replace(old, new),
                };
                Ok(Value::String(result))
            }
            StringMethod::Find => {
                if args.is_empty() {
                    return Err("find() takes at least 1 argument".to_string());
                }
                let needle = match &args[0] {
                    Value::String(s) => s.as_str(),
                    _ => return Err("find() argument must be str".to_string()),
                };
                Ok(Value::Int(s.find(needle).map(|i| i as i64).unwrap_or(-1)))
            }
            StringMethod::Rfind => {
                if args.is_empty() {
                    return Err("rfind() takes at least 1 argument".to_string());
                }
                let needle = match &args[0] {
                    Value::String(s) => s.as_str(),
                    _ => return Err("rfind() argument must be str".to_string()),
                };
                Ok(Value::Int(s.rfind(needle).map(|i| i as i64).unwrap_or(-1)))
            }
            StringMethod::Index => {
                if args.is_empty() {
                    return Err("index() takes at least 1 argument".to_string());
                }
                let needle = match &args[0] {
                    Value::String(s) => s.as_str(),
                    _ => return Err("index() argument must be str".to_string()),
                };
                s.find(needle)
                    .map(|i| Value::Int(i as i64))
                    .ok_or_else(|| "substring not found".to_string())
            }
            StringMethod::Rindex => {
                if args.is_empty() {
                    return Err("rindex() takes at least 1 argument".to_string());
                }
                let needle = match &args[0] {
                    Value::String(s) => s.as_str(),
                    _ => return Err("rindex() argument must be str".to_string()),
                };
                s.rfind(needle)
                    .map(|i| Value::Int(i as i64))
                    .ok_or_else(|| "substring not found".to_string())
            }
            StringMethod::Count => {
                if args.is_empty() {
                    return Err("count() takes at least 1 argument".to_string());
                }
                let needle = match &args[0] {
                    Value::String(n) => n.clone(),
                    _ => return Err("count() argument must be str".to_string()),
                };
                let start = args.get(1).and_then(|v| {
                    if let Value::Int(n) = v { Some(*n as usize) } else { None }
                }).unwrap_or(0);
                let end = args.get(2).and_then(|v| {
                    if let Value::Int(n) = v { Some(*n as usize) } else { None }
                }).unwrap_or(s.len());
                let substring = &s[start.min(s.len())..end.min(s.len())];
                Ok(Value::Int(substring.matches(needle.as_str()).count() as i64))
            }
            StringMethod::Startswith => {
                if args.is_empty() {
                    return Err("startswith() takes at least 1 argument".to_string());
                }
                let prefix = match &args[0] {
                    Value::String(s) => s.as_str(),
                    _ => return Err("startswith() argument must be str".to_string()),
                };
                Ok(Value::Bool(s.starts_with(prefix)))
            }
            StringMethod::Endswith => {
                if args.is_empty() {
                    return Err("endswith() takes at least 1 argument".to_string());
                }
                let suffix = match &args[0] {
                    Value::String(s) => s.as_str(),
                    _ => return Err("endswith() argument must be str".to_string()),
                };
                Ok(Value::Bool(s.ends_with(suffix)))
            }
            StringMethod::Format => {
                let result = s.to_string();
                let mut auto_index = 0;
                let mut new_result = String::new();
                let mut chars = result.chars().peekable();
                while let Some(c) = chars.next() {
                    if c == '{' {
                        if chars.peek() == Some(&'{') {
                            chars.next();
                            new_result.push('{');
                        } else {
                            let mut placeholder = String::new();
                            while let Some(&pc) = chars.peek() {
                                if pc == '}' { break; }
                                placeholder.push(chars.next().unwrap());
                            }
                            chars.next();
                            let value = if placeholder.is_empty() {
                                let v = args.get(auto_index).map(|v| v.to_string_repr()).unwrap_or_default();
                                auto_index += 1;
                                v
                            } else if let Ok(idx) = placeholder.parse::<usize>() {
                                args.get(idx).map(|v| v.to_string_repr()).unwrap_or_default()
                            } else {
                                _kwargs.get(&placeholder).map(|v| v.to_string_repr()).unwrap_or_default()
                            };
                            new_result.push_str(&value);
                        }
                    } else if c == '}' {
                        if chars.peek() == Some(&'}') {
                            chars.next();
                            new_result.push('}');
                        } else {
                            new_result.push(c);
                        }
                    } else {
                        new_result.push(c);
                    }
                }
                Ok(Value::String(new_result))
            }
            StringMethod::Removeprefix => {
                if args.is_empty() {
                    return Err("removeprefix() takes exactly 1 argument".to_string());
                }
                let prefix = match &args[0] {
                    Value::String(p) => p.as_str(),
                    _ => return Err("removeprefix() argument must be str".to_string()),
                };
                let result = s.strip_prefix(prefix).unwrap_or(s);
                Ok(Value::String(result.to_string()))
            }
            StringMethod::Removesuffix => {
                if args.is_empty() {
                    return Err("removesuffix() takes exactly 1 argument".to_string());
                }
                let suffix = match &args[0] {
                    Value::String(p) => p.as_str(),
                    _ => return Err("removesuffix() argument must be str".to_string()),
                };
                let result = s.strip_suffix(suffix).unwrap_or(s);
                Ok(Value::String(result.to_string()))
            }
            StringMethod::Elems => {
                let chars: Vec<Value> = s.chars().map(|c| Value::String(c.to_string())).collect();
                Ok(Value::List(Rc::new(RefCell::new(chars))))
            }
            StringMethod::Partition => {
                if args.is_empty() {
                    return Err("partition() takes exactly 1 argument".to_string());
                }
                let sep = match &args[0] {
                    Value::String(sep) => sep.as_str(),
                    _ => return Err("partition() argument must be str".to_string()),
                };
                let (before, after) = match s.find(sep) {
                    Some(idx) => (&s[..idx], &s[idx + sep.len()..]),
                    None => (s, ""),
                };
                let sep_found = if s.contains(sep) { sep } else { "" };
                Ok(Value::Tuple(vec![
                    Value::String(before.to_string()),
                    Value::String(sep_found.to_string()),
                    Value::String(after.to_string()),
                ]))
            }
            StringMethod::Rpartition => {
                if args.is_empty() {
                    return Err("rpartition() takes exactly 1 argument".to_string());
                }
                let sep = match &args[0] {
                    Value::String(sep) => sep.as_str(),
                    _ => return Err("rpartition() argument must be str".to_string()),
                };
                let (before, after) = match s.rfind(sep) {
                    Some(idx) => (&s[..idx], &s[idx + sep.len()..]),
                    None => ("", s),
                };
                let sep_found = if s.contains(sep) { sep } else { "" };
                Ok(Value::Tuple(vec![
                    Value::String(before.to_string()),
                    Value::String(sep_found.to_string()),
                    Value::String(after.to_string()),
                ]))
            }
            StringMethod::Istitle => {
                let result = !s.is_empty() && s.split_whitespace().all(|word| {
                    let mut chars = word.chars();
                    match chars.next() {
                        Some(first) => first.is_uppercase() && chars.all(|c| !c.is_uppercase()),
                        None => true,
                    }
                });
                Ok(Value::Bool(result))
            }
        }
    }

    fn list_method(&self, l: &Rc<RefCell<Vec<Value>>>, method: &str) -> Result<Value, String> {
        let method_enum = ListMethod::from_str(method)
            .ok_or_else(|| format!("'list' object has no attribute '{}'", method))?;

        let l_clone = Rc::clone(l);
        Ok(Value::BuiltinFunction(Rc::new(move |compiler, args, kwargs| {
            let is_mutating = matches!(
                method_enum,
                ListMethod::Append | ListMethod::Extend | ListMethod::Insert |
                ListMethod::Pop | ListMethod::Remove | ListMethod::Clear
            );
            if is_mutating {
                let ptr = Rc::as_ptr(&l_clone);
                if compiler.iterating_lists.contains(&ptr) {
                    return Err("cannot mutate list during iteration".to_string());
                }
            }
            Self::call_list_method(&l_clone, method_enum, args, kwargs)
        })))
    }

    fn call_list_method(l: &Rc<RefCell<Vec<Value>>>, method: ListMethod, args: Vec<Value>, _kwargs: HashMap<String, Value>) -> Result<Value, String> {
        match method {
            ListMethod::Append => {
                if args.len() != 1 {
                    return Err("append() takes exactly 1 argument".to_string());
                }
                l.borrow_mut().push(args[0].clone());
                Ok(Value::None)
            }
            ListMethod::Extend => {
                if args.len() != 1 {
                    return Err("extend() takes exactly 1 argument".to_string());
                }
                let items = match &args[0] {
                    Value::List(other) => other.borrow().clone(),
                    Value::Tuple(t) => t.clone(),
                    _ => return Err("extend() argument must be iterable".to_string()),
                };
                l.borrow_mut().extend(items);
                Ok(Value::None)
            }
            ListMethod::Insert => {
                if args.len() != 2 {
                    return Err("insert() takes exactly 2 arguments".to_string());
                }
                let index = match &args[0] {
                    Value::Int(i) => *i as usize,
                    _ => return Err("insert() index must be int".to_string()),
                };
                let mut list = l.borrow_mut();
                let index = index.min(list.len());
                list.insert(index, args[1].clone());
                Ok(Value::None)
            }
            ListMethod::Pop => {
                let mut list = l.borrow_mut();
                if list.is_empty() {
                    return Err("pop from empty list".to_string());
                }
                let index = args.first()
                    .map(|v| match v {
                        Value::Int(i) => {
                            let len = list.len() as i64;
                            if *i < 0 { (len + i) as usize } else { *i as usize }
                        }
                        _ => list.len() - 1,
                    })
                    .unwrap_or(list.len() - 1);
                if index >= list.len() {
                    return Err("pop index out of range".to_string());
                }
                Ok(list.remove(index))
            }
            ListMethod::Remove => {
                if args.len() != 1 {
                    return Err("remove() takes exactly 1 argument".to_string());
                }
                let mut list = l.borrow_mut();
                if let Some(pos) = list.iter().position(|x| x == &args[0]) {
                    list.remove(pos);
                    Ok(Value::None)
                } else {
                    Err("list.remove(x): x not in list".to_string())
                }
            }
            ListMethod::Clear => {
                l.borrow_mut().clear();
                Ok(Value::None)
            }
            ListMethod::Index => {
                if args.is_empty() {
                    return Err("index() takes at least 1 argument".to_string());
                }
                let list = l.borrow();
                list.iter()
                    .position(|x| x == &args[0])
                    .map(|i| Value::Int(i as i64))
                    .ok_or_else(|| "value not in list".to_string())
            }
            ListMethod::Count => {
                if args.len() != 1 {
                    return Err("count() takes exactly 1 argument".to_string());
                }
                let list = l.borrow();
                let count = list.iter().filter(|x| *x == &args[0]).count();
                Ok(Value::Int(count as i64))
            }
        }
    }

    fn dict_method(&self, d: &Rc<RefCell<HashMap<String, Value>>>, method: &str) -> Result<Value, String> {
        let method_enum = DictMethod::from_str(method)
            .ok_or_else(|| format!("'dict' object has no attribute '{}'", method))?;

        let d_clone = Rc::clone(d);
        Ok(Value::BuiltinFunction(Rc::new(move |compiler, args, kwargs| {
            let is_mutating = matches!(
                method_enum,
                DictMethod::Pop | DictMethod::Clear | DictMethod::Update | DictMethod::Setdefault | DictMethod::Popitem
            );
            if is_mutating {
                let ptr = Rc::as_ptr(&d_clone);
                if compiler.iterating_dicts.contains(&ptr) {
                    return Err("cannot mutate dict during iteration".to_string());
                }
            }
            Self::call_dict_method(&d_clone, method_enum, args, kwargs)
        })))
    }

    fn call_dict_method(d: &Rc<RefCell<HashMap<String, Value>>>, method: DictMethod, args: Vec<Value>, kwargs: HashMap<String, Value>) -> Result<Value, String> {
        match method {
            DictMethod::Keys => {
                let keys: Vec<Value> = d.borrow().keys()
                    .map(|k| Value::String(k.clone()))
                    .collect();
                Ok(Value::List(Rc::new(RefCell::new(keys))))
            }
            DictMethod::Values => {
                let values: Vec<Value> = d.borrow().values().cloned().collect();
                Ok(Value::List(Rc::new(RefCell::new(values))))
            }
            DictMethod::Items => {
                let items: Vec<Value> = d.borrow().iter()
                    .map(|(k, v)| Value::Tuple(vec![Value::String(k.clone()), v.clone()]))
                    .collect();
                Ok(Value::List(Rc::new(RefCell::new(items))))
            }
            DictMethod::Get => {
                if args.is_empty() {
                    return Err("get() takes at least 1 argument".to_string());
                }
                let key = match &args[0] {
                    Value::String(s) => s.clone(),
                    _ => return Err("dict keys must be strings".to_string()),
                };
                let default = args.get(1).cloned().unwrap_or(Value::None);
                Ok(d.borrow().get(&key).cloned().unwrap_or(default))
            }
            DictMethod::Pop => {
                if args.is_empty() {
                    return Err("pop() takes at least 1 argument".to_string());
                }
                let key = match &args[0] {
                    Value::String(s) => s.clone(),
                    _ => return Err("dict keys must be strings".to_string()),
                };
                let default = args.get(1).cloned();
                match d.borrow_mut().remove(&key) {
                    Some(v) => Ok(v),
                    None => default.ok_or_else(|| format!("KeyError: '{}'", key)),
                }
            }
            DictMethod::Clear => {
                d.borrow_mut().clear();
                Ok(Value::None)
            }
            DictMethod::Update => {
                let mut dict = d.borrow_mut();
                if !args.is_empty() {
                    match &args[0] {
                        Value::Dict(other) => {
                            for (k, v) in other.borrow().iter() {
                                dict.insert(k.clone(), v.clone());
                            }
                        }
                        Value::List(list) => {
                            for item in list.borrow().iter() {
                                match item {
                                    Value::Tuple(pair) if pair.len() == 2 => {
                                        let key = match &pair[0] {
                                            Value::String(s) => s.clone(),
                                            _ => return Err("update(): dict keys must be strings".to_string()),
                                        };
                                        dict.insert(key, pair[1].clone());
                                    }
                                    _ => return Err("update(): list items must be (key, value) pairs".to_string()),
                                }
                            }
                        }
                        _ => return Err("update() argument must be dict or list of pairs".to_string()),
                    }
                }
                for (k, v) in kwargs {
                    dict.insert(k, v);
                }
                Ok(Value::None)
            }
            DictMethod::Setdefault => {
                if args.is_empty() {
                    return Err("setdefault() takes at least 1 argument".to_string());
                }
                let key = match &args[0] {
                    Value::String(s) => s.clone(),
                    _ => return Err("dict keys must be strings".to_string()),
                };
                let default = args.get(1).cloned().unwrap_or(Value::None);
                let mut dict = d.borrow_mut();
                let value = dict.entry(key).or_insert(default).clone();
                Ok(value)
            }
            DictMethod::Popitem => {
                let mut dict = d.borrow_mut();
                if dict.is_empty() {
                    return Err("popitem(): dictionary is empty".to_string());
                }
                let key = dict.keys().next().unwrap().clone();
                let value = dict.remove(&key).unwrap();
                Ok(Value::Tuple(vec![Value::String(key), value]))
            }
        }
    }

    fn bytes_method(&self, b: &[u8], method: &str) -> Result<Value, String> {
        let method_enum = BytesMethod::from_str(method)
            .ok_or_else(|| format!("'bytes' object has no attribute '{}'", method))?;

        let b_clone = b.to_vec();
        Ok(Value::BuiltinFunction(Rc::new(move |_compiler, args, _kwargs| {
            Self::call_bytes_method(&b_clone, method_enum, args)
        })))
    }

    fn call_bytes_method(b: &[u8], method: BytesMethod, _args: Vec<Value>) -> Result<Value, String> {
        match method {
            BytesMethod::Elems => {
                let elems: Vec<Value> = b.iter()
                    .map(|byte| Value::Int(*byte as i64))
                    .collect();
                Ok(Value::List(Rc::new(RefCell::new(elems))))
            }
        }
    }

    fn set_method(&self, s: &Rc<RefCell<HashSet<HashableValue>>>, method: &str) -> Result<Value, String> {
        let method_enum = SetMethod::from_str(method)
            .ok_or_else(|| format!("'set' object has no attribute '{}'", method))?;

        let s_clone = Rc::clone(s);
        Ok(Value::BuiltinFunction(Rc::new(move |_compiler, args, _kwargs| {
            Self::call_set_method(&s_clone, method_enum, args)
        })))
    }

    fn call_set_method(s: &Rc<RefCell<HashSet<HashableValue>>>, method: SetMethod, args: Vec<Value>) -> Result<Value, String> {
        match method {
            SetMethod::Add => {
                if args.len() != 1 {
                    return Err("add() takes exactly 1 argument".to_string());
                }
                let h = HashableValue::from_value(&args[0])?;
                s.borrow_mut().insert(h);
                Ok(Value::None)
            }
            SetMethod::Clear => {
                s.borrow_mut().clear();
                Ok(Value::None)
            }
            SetMethod::Difference => {
                let mut result = s.borrow().clone();
                for arg in &args {
                    match arg {
                        Value::Set(other) => {
                            for h in other.borrow().iter() {
                                result.remove(h);
                            }
                        }
                        _ => return Err("difference() argument must be a set".to_string()),
                    }
                }
                Ok(Value::Set(Rc::new(RefCell::new(result))))
            }
            SetMethod::DifferenceUpdate => {
                for arg in &args {
                    match arg {
                        Value::Set(other) => {
                            let mut set = s.borrow_mut();
                            for h in other.borrow().iter() {
                                set.remove(h);
                            }
                        }
                        _ => return Err("difference_update() argument must be a set".to_string()),
                    }
                }
                Ok(Value::None)
            }
            SetMethod::Discard => {
                if args.len() != 1 {
                    return Err("discard() takes exactly 1 argument".to_string());
                }
                let h = HashableValue::from_value(&args[0])?;
                s.borrow_mut().remove(&h);
                Ok(Value::None)
            }
            SetMethod::Intersection => {
                let mut result = s.borrow().clone();
                for arg in &args {
                    match arg {
                        Value::Set(other) => {
                            result = result.intersection(&*other.borrow()).cloned().collect();
                        }
                        _ => return Err("intersection() argument must be a set".to_string()),
                    }
                }
                Ok(Value::Set(Rc::new(RefCell::new(result))))
            }
            SetMethod::IntersectionUpdate => {
                for arg in &args {
                    match arg {
                        Value::Set(other) => {
                            let current = s.borrow().clone();
                            let new_set = current.intersection(&*other.borrow()).cloned().collect();
                            *s.borrow_mut() = new_set;
                        }
                        _ => return Err("intersection_update() argument must be a set".to_string()),
                    }
                }
                Ok(Value::None)
            }
            SetMethod::Isdisjoint => {
                if args.len() != 1 {
                    return Err("isdisjoint() takes exactly 1 argument".to_string());
                }
                match &args[0] {
                    Value::Set(other) => {
                        Ok(Value::Bool(s.borrow().is_disjoint(&*other.borrow())))
                    }
                    _ => Err("isdisjoint() argument must be a set".to_string()),
                }
            }
            SetMethod::Issubset => {
                if args.len() != 1 {
                    return Err("issubset() takes exactly 1 argument".to_string());
                }
                match &args[0] {
                    Value::Set(other) => {
                        Ok(Value::Bool(s.borrow().is_subset(&*other.borrow())))
                    }
                    _ => Err("issubset() argument must be a set".to_string()),
                }
            }
            SetMethod::Issuperset => {
                if args.len() != 1 {
                    return Err("issuperset() takes exactly 1 argument".to_string());
                }
                match &args[0] {
                    Value::Set(other) => {
                        Ok(Value::Bool(s.borrow().is_superset(&*other.borrow())))
                    }
                    _ => Err("issuperset() argument must be a set".to_string()),
                }
            }
            SetMethod::Pop => {
                let mut set = s.borrow_mut();
                if set.is_empty() {
                    return Err("pop from an empty set".to_string());
                }
                let item = set.iter().next().cloned().unwrap();
                set.remove(&item);
                Ok(item.to_value())
            }
            SetMethod::Remove => {
                if args.len() != 1 {
                    return Err("remove() takes exactly 1 argument".to_string());
                }
                let h = HashableValue::from_value(&args[0])?;
                if !s.borrow_mut().remove(&h) {
                    return Err("KeyError: element not in set".to_string());
                }
                Ok(Value::None)
            }
            SetMethod::SymmetricDifference => {
                if args.len() != 1 {
                    return Err("symmetric_difference() takes exactly 1 argument".to_string());
                }
                match &args[0] {
                    Value::Set(other) => {
                        let result: HashSet<HashableValue> = s.borrow()
                            .symmetric_difference(&*other.borrow())
                            .cloned()
                            .collect();
                        Ok(Value::Set(Rc::new(RefCell::new(result))))
                    }
                    _ => Err("symmetric_difference() argument must be a set".to_string()),
                }
            }
            SetMethod::SymmetricDifferenceUpdate => {
                if args.len() != 1 {
                    return Err("symmetric_difference_update() takes exactly 1 argument".to_string());
                }
                match &args[0] {
                    Value::Set(other) => {
                        let current = s.borrow().clone();
                        let new_set = current.symmetric_difference(&*other.borrow()).cloned().collect();
                        *s.borrow_mut() = new_set;
                        Ok(Value::None)
                    }
                    _ => Err("symmetric_difference_update() argument must be a set".to_string()),
                }
            }
            SetMethod::Union => {
                let mut result = s.borrow().clone();
                for arg in &args {
                    match arg {
                        Value::Set(other) => {
                            for h in other.borrow().iter() {
                                result.insert(h.clone());
                            }
                        }
                        _ => return Err("union() argument must be a set".to_string()),
                    }
                }
                Ok(Value::Set(Rc::new(RefCell::new(result))))
            }
            SetMethod::Update => {
                for arg in &args {
                    match arg {
                        Value::Set(other) => {
                            let mut set = s.borrow_mut();
                            for h in other.borrow().iter() {
                                set.insert(h.clone());
                            }
                        }
                        _ => return Err("update() argument must be a set".to_string()),
                    }
                }
                Ok(Value::None)
            }
        }
    }

    fn eval_call_args(&mut self, args: &[starlark_syntax::syntax::ast::AstArgumentP<starlark_syntax::syntax::ast::AstNoPayload>]) -> Result<(Vec<Value>, HashMap<String, Value>), String> {
        let mut positional = Vec::new();
        let mut kwargs = HashMap::new();

        for arg in args {
            match &arg.node {
                ArgumentP::Positional(expr) => {
                    positional.push(self.eval_expr(expr)?);
                }
                ArgumentP::Named(name, expr) => {
                    kwargs.insert(name.node.clone(), self.eval_expr(expr)?);
                }
                ArgumentP::Args(expr) => {
                    let val = self.eval_expr(expr)?;
                    match val {
                        Value::List(l) => positional.extend(l.borrow().clone()),
                        Value::Tuple(t) => positional.extend(t),
                        _ => return Err("argument after * must be iterable".to_string()),
                    }
                }
                ArgumentP::KwArgs(expr) => {
                    let val = self.eval_expr(expr)?;
                    match val {
                        Value::Dict(d) => {
                            for (k, v) in d.borrow().iter() {
                                kwargs.insert(k.clone(), v.clone());
                            }
                        }
                        _ => return Err("argument after ** must be dict".to_string()),
                    }
                }
            }
        }

        Ok((positional, kwargs))
    }

    pub fn call_value(&mut self, func: &Value, args: Vec<Value>, kwargs: HashMap<String, Value>) -> Result<Value, String> {
        match func {
            Value::BuiltinFunction(f) => f(self, args, kwargs),
            Value::Function(f) => self.call_function(f, args, kwargs),
            Value::Partial { func, bound_args, bound_kwargs } => {
                let mut combined_args = bound_args.clone();
                combined_args.extend(args);
                let mut combined_kwargs = bound_kwargs.clone();
                combined_kwargs.extend(kwargs);
                self.call_function(func, combined_args, combined_kwargs)
            }
            _ => Err(format!("'{}' object is not callable", func.type_name())),
        }
    }

    fn call_function(&mut self, func: &Rc<Function>, args: Vec<Value>, kwargs: HashMap<String, Value>) -> Result<Value, String> {
        let call_scope = func.closure.child();

        let mut args_remaining: Vec<Value> = args.into_iter().collect();
        let mut used_kwargs = std::collections::HashSet::new();
        let mut args_consumed = false;

        for param in &func.params {
            if param.is_args {
                let rest: Vec<Value> = args_remaining.drain(..).collect();
                call_scope.set(param.name.clone(), Value::List(Rc::new(RefCell::new(rest))));
                args_consumed = true;
            } else if param.is_kwargs {
                let mut rest_kwargs = HashMap::new();
                for (k, v) in &kwargs {
                    if !used_kwargs.contains(k) {
                        rest_kwargs.insert(k.clone(), v.clone());
                    }
                }
                call_scope.set(param.name.clone(), Value::Dict(Rc::new(RefCell::new(rest_kwargs))));
            } else {
                let value = if let Some(kwarg) = kwargs.get(&param.name) {
                    used_kwargs.insert(param.name.clone());
                    kwarg.clone()
                } else if !args_consumed && !args_remaining.is_empty() {
                    args_remaining.remove(0)
                } else if let Some(default) = &param.default {
                    default.clone()
                } else {
                    return Err(format!("{}() missing required argument: '{}'", func.name, param.name));
                };
                call_scope.set(param.name.clone(), value);
            }
        }

        let old_scope = std::mem::replace(&mut self.scope, call_scope);

        let result = match &func.body {
            FunctionBody::Ast(stmt) => {
                match self.exec_function_body(stmt) {
                    Ok(()) => Ok(Value::None),
                    Err(ReturnValue::Return(v)) => Ok(v),
                    Err(ReturnValue::Error(e)) => Err(e),
                }
            }
            FunctionBody::Lambda(expr) => self.eval_expr(expr),
        };

        self.scope = old_scope;
        result
    }

    fn exec_function_body(&mut self, stmt: &AstStmt) -> Result<(), ReturnValue> {
        match &stmt.node {
            StmtP::Return(expr) => {
                let value = match expr {
                    Some(e) => self.eval_expr(e).map_err(ReturnValue::Error)?,
                    None => Value::None,
                };
                Err(ReturnValue::Return(value))
            }
            StmtP::Statements(stmts) => {
                for s in stmts {
                    self.exec_function_body(s)?;
                }
                Ok(())
            }
            StmtP::If(cond, body) => {
                let cond_value = self.eval_expr(cond).map_err(ReturnValue::Error)?;
                if cond_value.is_truthy() {
                    self.exec_function_body(body)?;
                }
                Ok(())
            }
            StmtP::IfElse(cond, branches) => {
                let cond_value = self.eval_expr(cond).map_err(ReturnValue::Error)?;
                if cond_value.is_truthy() {
                    self.exec_function_body(&branches.0)?;
                } else {
                    self.exec_function_body(&branches.1)?;
                }
                Ok(())
            }
            StmtP::For(for_stmt) => {
                let iterable = self.eval_expr(&for_stmt.over).map_err(ReturnValue::Error)?;

                if iterable.contains_dynamic() {
                    let var_name = match &for_stmt.var.node {
                        AssignTargetP::Identifier(ident) => ident.node.ident.clone(),
                        _ => return Err(ReturnValue::Error(
                            "runtime for loop only supports simple variable binding".to_string()
                        )),
                    };

                    let subplan = self.compile_loop_body(&var_name, &for_stmt.body)
                        .map_err(ReturnValue::Error)?;

                    let can_parallel = self.can_parallelize_loop(&subplan, &var_name);

                    let foreach_op = SchemaOp::ForEach {
                        items: iterable.to_schema_value(),
                        item_name: var_name.clone(),
                        body: subplan,
                        parallel: can_parallel,
                    };

                    let op_id = self.add_schema_op(foreach_op);
                    self.last_expression_value = Some(Value::OpRef(op_id));
                    return Ok(());
                }

                let items = self.extract_iterable(&iterable).map_err(ReturnValue::Error)?;

                let list_ptr = if let Value::List(ref l) = iterable {
                    let ptr = Rc::as_ptr(l);
                    self.iterating_lists.insert(ptr);
                    Some(ptr)
                } else {
                    None
                };
                let dict_ptr = if let Value::Dict(ref d) = iterable {
                    let ptr = Rc::as_ptr(d);
                    self.iterating_dicts.insert(ptr);
                    Some(ptr)
                } else {
                    None
                };

                let result = (|| {
                    for item in items {
                        self.assign_target(&for_stmt.var.node, item).map_err(ReturnValue::Error)?;
                        match self.exec_function_body(&for_stmt.body) {
                            Ok(()) => {}
                            Err(ReturnValue::Return(v)) => return Err(ReturnValue::Return(v)),
                            Err(ReturnValue::Error(e)) if e == "break" => break,
                            Err(ReturnValue::Error(e)) if e == "continue" => continue,
                            Err(e) => return Err(e),
                        }
                    }
                    Ok(())
                })();

                if let Some(ptr) = list_ptr {
                    self.iterating_lists.remove(&ptr);
                }
                if let Some(ptr) = dict_ptr {
                    self.iterating_dicts.remove(&ptr);
                }

                result
            }
            StmtP::Break => Err(ReturnValue::Error("break".to_string())),
            StmtP::Continue => Err(ReturnValue::Error("continue".to_string())),
            _ => self.exec_stmt(stmt).map_err(ReturnValue::Error),
        }
    }

    fn extract_parameters(&self, params: &[starlark_syntax::syntax::ast::AstParameterP<starlark_syntax::syntax::ast::AstNoPayload>]) -> Result<Vec<Parameter>, String> {
        let mut result = Vec::new();

        for param in params {
            match &param.node {
                ParameterP::Normal(name, _type, default) => {
                    let default_value = match default {
                        Some(expr) => {
                            let mut temp_compiler = SchemaGenerator::new();
                            Some(temp_compiler.eval_expr(expr)?)
                        }
                        None => None,
                    };
                    result.push(Parameter {
                        name: name.node.ident.clone(),
                        default: default_value,
                        is_args: false,
                        is_kwargs: false,
                    });
                }
                ParameterP::Args(name, _type) => {
                    result.push(Parameter {
                        name: name.node.ident.clone(),
                        default: None,
                        is_args: true,
                        is_kwargs: false,
                    });
                }
                ParameterP::KwArgs(name, _type) => {
                    result.push(Parameter {
                        name: name.node.ident.clone(),
                        default: None,
                        is_args: false,
                        is_kwargs: true,
                    });
                }
                ParameterP::NoArgs | ParameterP::Slash => {}
            }
        }

        Ok(result)
    }

    fn assign_target(&mut self, target: &AssignTargetP<starlark_syntax::syntax::ast::AstNoPayload>, value: Value) -> Result<(), String> {
        match target {
            AssignTargetP::Identifier(ident) => {
                self.scope.set(ident.node.ident.clone(), value);
                Ok(())
            }
            AssignTargetP::Tuple(targets) => {
                let items = self.extract_iterable(&value)?;
                if items.len() != targets.len() {
                    return Err(format!(
                        "cannot unpack {} values into {} variables",
                        items.len(),
                        targets.len()
                    ));
                }
                for (target, item) in targets.iter().zip(items) {
                    self.assign_target(&target.node, item)?;
                }
                Ok(())
            }
            AssignTargetP::Index(base_index) => {
                let (base_expr, index_expr) = &**base_index;
                let base = self.eval_expr(base_expr)?;
                let index = self.eval_expr(index_expr)?;
                let new_value = self.assign_index(base, index, value)?;
                if let Some(new_ref) = new_value {
                    if let ExprP::Identifier(ident) = &base_expr.node {
                        self.scope.set(ident.node.ident.clone(), new_ref);
                    }
                }
                Ok(())
            }
            AssignTargetP::Dot(base_expr, attr) => {
                let _base = self.eval_expr(base_expr)?;
                Err(format!("cannot assign to attribute '{}'", attr.node))
            }
        }
    }

    fn assign_index(&mut self, base: Value, index: Value, value: Value) -> Result<Option<Value>, String> {
        match base {
            Value::List(l) => {
                let ptr = Rc::as_ptr(&l);
                if self.iterating_lists.contains(&ptr) {
                    return Err("cannot mutate list during iteration".to_string());
                }
                let i = match index {
                    Value::Int(i) => {
                        let len = l.borrow().len() as i64;
                        if i < 0 { (len + i) as usize } else { i as usize }
                    }
                    _ => return Err("list indices must be integers".to_string()),
                };
                let mut list = l.borrow_mut();
                if i >= list.len() {
                    return Err("list assignment index out of range".to_string());
                }
                list[i] = value;
                Ok(None)
            }
            Value::Dict(d) => {
                let ptr = Rc::as_ptr(&d);
                if self.iterating_dicts.contains(&ptr) {
                    return Err("cannot mutate dict during iteration".to_string());
                }
                let key = match index {
                    Value::String(s) => s,
                    _ => return Err("dict keys must be strings".to_string()),
                };
                d.borrow_mut().insert(key, value);
                Ok(None)
            }
            Value::OpRef(op_id) => {
                let base_sv = SchemaValue::OpRef { id: op_id, path: Vec::new() };
                let index_sv = index.to_schema_value();
                let value_sv = value.to_schema_value();
                let new_op_id = self.schema.add_op(SchemaOp::SetIndex {
                    base: base_sv,
                    index: index_sv,
                    value: value_sv,
                }, None);
                Ok(Some(Value::OpRef(new_op_id)))
            }
            _ => Err(format!("'{}' object does not support item assignment", base.type_name())),
        }
    }

    fn eval_assign_target_value(&mut self, target: &AssignTargetP<starlark_syntax::syntax::ast::AstNoPayload>) -> Result<Value, String> {
        match target {
            AssignTargetP::Identifier(ident) => {
                self.scope.get(&ident.node.ident)
                    .ok_or_else(|| format!("name '{}' is not defined", ident.node.ident))
            }
            AssignTargetP::Index(base_index) => {
                let (base_expr, index_expr) = &**base_index;
                let base = self.eval_expr(base_expr)?;
                let index = self.eval_expr(index_expr)?;
                self.eval_index(&base, &index)
            }
            AssignTargetP::Dot(base_expr, attr) => {
                let base = self.eval_expr(base_expr)?;
                self.eval_dot(&base, &attr.node)
            }
            AssignTargetP::Tuple(_) => {
                Err("cannot use tuple as augmented assignment target".to_string())
            }
        }
    }

    fn extract_iterable(&self, value: &Value) -> Result<Vec<Value>, String> {
        match value {
            Value::List(l) => Ok(l.borrow().clone()),
            Value::Tuple(t) => Ok(t.clone()),
            Value::String(s) => Ok(s.chars().map(|c| Value::String(c.to_string())).collect()),
            Value::Bytes(b) => Ok(b.iter().map(|byte| Value::Int(*byte as i64)).collect()),
            Value::Dict(d) => Ok(d.borrow().keys().map(|k| Value::String(k.clone())).collect()),
            Value::Set(s) => Ok(s.borrow().iter().map(|h| h.to_value()).collect()),
            _ => Err(format!("'{}' object is not iterable", value.type_name())),
        }
    }

    fn eval_list_comprehension(
        &mut self,
        expr: &AstExpr,
        for_clause: &ForClauseP<starlark_syntax::syntax::ast::AstNoPayload>,
        clauses: &[ClauseP<starlark_syntax::syntax::ast::AstNoPayload>],
    ) -> Result<Value, String> {
        let mut result = Vec::new();
        self.eval_comprehension_clauses(
            expr,
            for_clause,
            clauses,
            &mut |compiler, e| {
                result.push(compiler.eval_expr(e)?);
                Ok(())
            },
        )?;
        Ok(Value::List(Rc::new(RefCell::new(result))))
    }

    fn eval_dict_comprehension(
        &mut self,
        key_expr: &AstExpr,
        val_expr: &AstExpr,
        for_clause: &ForClauseP<starlark_syntax::syntax::ast::AstNoPayload>,
        clauses: &[ClauseP<starlark_syntax::syntax::ast::AstNoPayload>],
    ) -> Result<Value, String> {
        let mut result = HashMap::new();

        let iterable = self.eval_expr(&for_clause.over)?;
        let items = self.extract_iterable(&iterable)?;

        for item in items {
            self.assign_target(&for_clause.var.node, item)?;

            let mut should_include = true;
            for clause in clauses {
                match clause {
                    ClauseP::If(cond) => {
                        if !self.eval_expr(cond)?.is_truthy() {
                            should_include = false;
                            break;
                        }
                    }
                    ClauseP::For(_) => {
                        return Err("nested for in dict comprehension not yet supported".to_string());
                    }
                }
            }

            if should_include {
                let key = self.eval_expr(key_expr)?;
                let key_str = match key {
                    Value::String(s) => s,
                    _ => return Err("dict keys must be strings".to_string()),
                };
                let val = self.eval_expr(val_expr)?;
                result.insert(key_str, val);
            }
        }

        Ok(Value::Dict(Rc::new(RefCell::new(result))))
    }

    fn eval_comprehension_clauses<F>(
        &mut self,
        expr: &AstExpr,
        for_clause: &ForClauseP<starlark_syntax::syntax::ast::AstNoPayload>,
        clauses: &[ClauseP<starlark_syntax::syntax::ast::AstNoPayload>],
        collector: &mut F,
    ) -> Result<(), String>
    where
        F: FnMut(&mut Self, &AstExpr) -> Result<(), String>,
    {
        let iterable = self.eval_expr(&for_clause.over)?;
        let items = self.extract_iterable(&iterable)?;

        for item in items {
            self.assign_target(&for_clause.var.node, item)?;
            self.eval_remaining_clauses(expr, clauses, collector)?;
        }

        Ok(())
    }

    fn eval_remaining_clauses<F>(
        &mut self,
        expr: &AstExpr,
        clauses: &[ClauseP<starlark_syntax::syntax::ast::AstNoPayload>],
        collector: &mut F,
    ) -> Result<(), String>
    where
        F: FnMut(&mut Self, &AstExpr) -> Result<(), String>,
    {
        if clauses.is_empty() {
            collector(self, expr)?;
            return Ok(());
        }

        match &clauses[0] {
            ClauseP::If(cond) => {
                if self.eval_expr(cond)?.is_truthy() {
                    self.eval_remaining_clauses(expr, &clauses[1..], collector)?;
                }
            }
            ClauseP::For(for_clause) => {
                let iterable = self.eval_expr(&for_clause.over)?;
                let items = self.extract_iterable(&iterable)?;

                for item in items {
                    self.assign_target(&for_clause.var.node, item)?;
                    self.eval_remaining_clauses(expr, &clauses[1..], collector)?;
                }
            }
        }

        Ok(())
    }
}

impl Default for SchemaGenerator {
    fn default() -> Self {
        Self::new()
    }
}

enum ReturnValue {
    Return(Value),
    Error(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_expression() {
        let schema = SchemaGenerator::generate("x = 1 + 2", "test.star").unwrap();
        assert!(schema.is_empty());
    }

    #[test]
    fn test_function_definition() {
        let schema = SchemaGenerator::generate(
            r#"
def add(a, b):
    return a + b

result = add(1, 2)
"#,
            "test.star",
        )
        .unwrap();
        assert!(schema.is_empty());
    }

    #[test]
    fn test_list_operations() {
        let schema = SchemaGenerator::generate(
            r#"
x = [1, 2, 3]
x.append(4)
y = [i * 2 for i in x]
"#,
            "test.star",
        )
        .unwrap();
        assert!(schema.is_empty());
    }

    #[test]
    fn test_builtin_io() {
        let schema = SchemaGenerator::generate(
            r#"
load("@bp/io", "read_file", "write_file")

content = read_file("input.txt")
write_file("output.txt", content)
"#,
            "test.star",
        )
        .unwrap();

        assert_eq!(schema.len(), 2);
    }

    #[test]
    fn test_http_builtin() {
        let schema = SchemaGenerator::generate(
            r#"
load("@bp/http", "http_request")

response = http_request("GET", "https://example.com")
"#,
            "test.star",
        )
        .unwrap();

        assert_eq!(schema.len(), 1);
    }

    #[test]
    fn test_conditional() {
        let schema = SchemaGenerator::generate(
            r#"
def check():
    x = 5
    if x > 3:
        y = "big"
    else:
        y = "small"
    return y
result = check()
"#,
            "test.star",
        )
        .unwrap();
        assert!(schema.is_empty());
    }

    #[test]
    fn test_for_loop() {
        let schema = SchemaGenerator::generate(
            r#"
def sum_range():
    total = 0
    for i in range(5):
        total += i
    return total
result = sum_range()
"#,
            "test.star",
        )
        .unwrap();
        assert!(schema.is_empty());
    }

    #[test]
    fn test_string_methods() {
        let schema = SchemaGenerator::generate(
            r#"
s = "hello world"
upper = s.upper()
parts = s.split(" ")
"#,
            "test.star",
        )
        .unwrap();
        assert!(schema.is_empty());
    }

    #[test]
    fn test_dict_operations() {
        let schema = SchemaGenerator::generate(
            r#"
d = {"a": 1, "b": 2}
d["c"] = 3
keys = d.keys()
"#,
            "test.star",
        )
        .unwrap();
        assert!(schema.is_empty());
    }

    #[test]
    fn test_lambda() {
        let schema = SchemaGenerator::generate(
            r#"
double = lambda x: x * 2
result = double(5)
"#,
            "test.star",
        )
        .unwrap();
        assert!(schema.is_empty());
    }

    #[test]
    fn test_user_module_loading() {
        let temp_dir = std::env::temp_dir().join("blueprint_test_user_module");
        std::fs::create_dir_all(&temp_dir).unwrap();

        std::fs::write(
            temp_dir.join("utils.star"),
            r#"
def add(a, b):
    return a + b

def multiply(a, b):
    return a * b
"#,
        )
        .unwrap();

        let main_path = temp_dir.join("main.star");
        std::fs::write(
            &main_path,
            r#"
load("utils.star", "add", "multiply")
result = add(2, 3) + multiply(2, 3)
"#,
        )
        .unwrap();

        let mut compiler = SchemaGenerator::new();
        compiler.current_file = Some(main_path.clone());
        let source = std::fs::read_to_string(&main_path).unwrap();
        compiler.generate_from_source(&source, main_path.to_str().unwrap()).unwrap();

        let result = compiler.scope.get("result").unwrap();
        assert_eq!(result, Value::Int(11));

        std::fs::remove_dir_all(&temp_dir).ok();
    }

    #[test]
    fn test_circular_import_detection() {
        let temp_dir = std::env::temp_dir().join("blueprint_test_circular");
        std::fs::create_dir_all(&temp_dir).unwrap();

        std::fs::write(
            temp_dir.join("a.star"),
            r#"
load("b.star", "b_func")
def a_func():
    return 1
"#,
        )
        .unwrap();

        std::fs::write(
            temp_dir.join("b.star"),
            r#"
load("a.star", "a_func")
def b_func():
    return 2
"#,
        )
        .unwrap();

        let main_path = temp_dir.join("a.star");
        let mut compiler = SchemaGenerator::new();
        compiler.current_file = Some(main_path.clone());
        let source = std::fs::read_to_string(&main_path).unwrap();
        let result = compiler.generate_from_source(&source, main_path.to_str().unwrap());

        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Circular import"));

        std::fs::remove_dir_all(&temp_dir).ok();
    }

    #[test]
    fn test_private_symbol_rejection() {
        let temp_dir = std::env::temp_dir().join("blueprint_test_private");
        std::fs::create_dir_all(&temp_dir).unwrap();

        std::fs::write(
            temp_dir.join("utils.star"),
            r#"
def _private_helper():
    return 42

def public_func():
    return _private_helper()
"#,
        )
        .unwrap();

        let main_path = temp_dir.join("main.star");
        std::fs::write(
            &main_path,
            r#"
load("utils.star", "_private_helper")
"#,
        )
        .unwrap();

        let mut compiler = SchemaGenerator::new();
        compiler.current_file = Some(main_path.clone());
        let source = std::fs::read_to_string(&main_path).unwrap();
        let result = compiler.generate_from_source(&source, main_path.to_str().unwrap());

        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Cannot import private"));

        std::fs::remove_dir_all(&temp_dir).ok();
    }

    #[test]
    fn test_module_not_found() {
        let temp_dir = std::env::temp_dir().join("blueprint_test_notfound");
        std::fs::create_dir_all(&temp_dir).unwrap();

        let main_path = temp_dir.join("main.star");
        std::fs::write(
            &main_path,
            r#"
load("nonexistent.star", "func")
"#,
        )
        .unwrap();

        let mut compiler = SchemaGenerator::new();
        compiler.current_file = Some(main_path.clone());
        let source = std::fs::read_to_string(&main_path).unwrap();
        let result = compiler.generate_from_source(&source, main_path.to_str().unwrap());

        assert!(result.is_err());
        assert!(result.unwrap_err().contains("Module not found"));

        std::fs::remove_dir_all(&temp_dir).ok();
    }

    #[test]
    fn test_export_non_private_only() {
        let temp_dir = std::env::temp_dir().join("blueprint_test_export");
        std::fs::create_dir_all(&temp_dir).unwrap();

        std::fs::write(
            temp_dir.join("lib.star"),
            r#"
_SECRET = 42
PUBLIC = 100
def _helper():
    return _SECRET
def exported():
    return _helper() + PUBLIC
"#,
        )
        .unwrap();

        let main_path = temp_dir.join("main.star");
        std::fs::write(
            &main_path,
            r#"
load("lib.star", "PUBLIC", "exported")
result = exported() + PUBLIC
"#,
        )
        .unwrap();

        let mut compiler = SchemaGenerator::new();
        compiler.current_file = Some(main_path.clone());
        let source = std::fs::read_to_string(&main_path).unwrap();
        compiler.generate_from_source(&source, main_path.to_str().unwrap()).unwrap();

        let result = compiler.scope.get("result").unwrap();
        assert_eq!(result, Value::Int(242));

        std::fs::remove_dir_all(&temp_dir).ok();
    }

    #[test]
    fn test_reject_top_level_if() {
        let result = SchemaGenerator::generate("if True:\n    x = 1", "test.star");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("not allowed at top level"));
    }

    #[test]
    fn test_reject_top_level_if_else() {
        let result = SchemaGenerator::generate("if True:\n    x = 1\nelse:\n    x = 2", "test.star");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("not allowed at top level"));
    }

    #[test]
    fn test_reject_top_level_for() {
        let result = SchemaGenerator::generate("for i in [1,2]:\n    x = i", "test.star");
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("not allowed at top level"));
    }

    #[test]
    fn test_allow_if_inside_function() {
        let result = SchemaGenerator::generate(
            r#"
def foo():
    if True:
        return 1
    return 0
result = foo()
"#,
            "test.star",
        );
        assert!(result.is_ok());
    }

    #[test]
    fn test_allow_for_inside_function() {
        let result = SchemaGenerator::generate(
            r#"
def sum_list():
    total = 0
    for i in [1, 2, 3]:
        total = total + i
    return total
result = sum_list()
"#,
            "test.star",
        );
        assert!(result.is_ok());
    }

    #[test]
    fn test_runtime_for_loop_with_op_result() {
        let result = SchemaGenerator::generate(
            r#"
load("@bp/io", "read_file")

def process_lines():
    content = read_file("data.txt")
    lines = [content]
    for line in lines:
        print(line)
    return None
process_lines()
"#,
            "test.star",
        );
        if result.is_err() {
            panic!("Compilation failed: {}", result.unwrap_err());
        }
        let schema = result.unwrap();
        let ops: Vec<_> = schema.entries.iter().map(|e| e.op.name()).collect();
        assert!(!schema.is_empty(), "Schema is empty!");
        assert!(ops.contains(&"io.read_file"), "Missing io.read_file op");
        assert!(ops.contains(&"foreach"), "Missing foreach op");
    }

    #[test]
    fn test_runtime_for_loop_simple_body() {
        let result = SchemaGenerator::generate(
            r#"
load("@bp/io", "read_file", "write_file")

def copy_files():
    files = [read_file("a.txt"), read_file("b.txt")]
    for content in files:
        write_file("out.txt", content)
copy_files()
"#,
            "test.star",
        );
        if result.is_err() {
            panic!("Compilation failed: {}", result.unwrap_err());
        }
        let schema = result.unwrap();
        let ops: Vec<_> = schema.entries.iter().map(|e| e.op.name()).collect();
        assert_eq!(ops.iter().filter(|&n| *n == "io.read_file").count(), 2, "Expected 2 read_file ops");
        assert!(ops.contains(&"foreach"), "Missing foreach op");
    }

    fn get_foreach_parallel_flag(schema: &Schema) -> Option<bool> {
        for entry in &schema.entries {
            if let SchemaOp::ForEach { parallel, .. } = &entry.op {
                return Some(*parallel);
            }
        }
        None
    }

    #[test]
    fn test_foreach_parallel_with_read_file() {
        let result = SchemaGenerator::generate(
            r#"
load("@bp/io", "read_file", "list_dir")

def main():
    files = list_dir("/tmp")
    for f in files:
        content = read_file(f)
main()
"#,
            "test.star",
        );
        assert!(result.is_ok(), "Compilation failed: {:?}", result.err());
        let schema = result.unwrap();
        let parallel = get_foreach_parallel_flag(&schema);
        assert_eq!(parallel, Some(true), "Loop with read_file (path depends on loop var) should be parallel");
    }

    #[test]
    fn test_foreach_sequential_with_print() {
        let result = SchemaGenerator::generate(
            r#"
load("@bp/io", "list_dir")

def main():
    files = list_dir("/tmp")
    for f in files:
        print(f)
main()
"#,
            "test.star",
        );
        assert!(result.is_ok(), "Compilation failed: {:?}", result.err());
        let schema = result.unwrap();
        let parallel = get_foreach_parallel_flag(&schema);
        assert_eq!(parallel, Some(false), "Loop with print should be sequential");
    }

    #[test]
    fn test_foreach_parallel_with_http() {
        let result = SchemaGenerator::generate(
            r#"
load("@bp/io", "list_dir")
load("@bp/http", "http_request")

def main():
    urls = list_dir("/tmp/urls")
    for url in urls:
        http_request("GET", url)
main()
"#,
            "test.star",
        );
        assert!(result.is_ok(), "Compilation failed: {:?}", result.err());
        let schema = result.unwrap();
        let parallel = get_foreach_parallel_flag(&schema);
        assert_eq!(parallel, Some(true), "Loop with HTTP requests should be parallel");
    }

    #[test]
    fn test_foreach_parallel_file_write_with_loop_var() {
        let result = SchemaGenerator::generate(
            r#"
load("@bp/io", "list_dir", "write_file")

def main():
    files = list_dir("/tmp")
    for f in files:
        write_file(f, "content")
main()
"#,
            "test.star",
        );
        assert!(result.is_ok(), "Compilation failed: {:?}", result.err());
        let schema = result.unwrap();
        let parallel = get_foreach_parallel_flag(&schema);
        assert_eq!(parallel, Some(true), "Loop writing to different files (via loop var) should be parallel");
    }

    #[test]
    fn test_foreach_sequential_file_write_static_path() {
        let result = SchemaGenerator::generate(
            r#"
load("@bp/io", "list_dir", "write_file")

def main():
    files = list_dir("/tmp")
    for f in files:
        write_file("output.txt", f)
main()
"#,
            "test.star",
        );
        assert!(result.is_ok(), "Compilation failed: {:?}", result.err());
        let schema = result.unwrap();
        let parallel = get_foreach_parallel_flag(&schema);
        assert_eq!(parallel, Some(false), "Loop writing to same file should be sequential");
    }

    #[test]
    fn test_foreach_sequential_append_same_file() {
        let result = SchemaGenerator::generate(
            r#"
load("@bp/io", "list_dir", "append_file")

def main():
    items = list_dir("/tmp")
    for item in items:
        append_file("log.txt", item)
main()
"#,
            "test.star",
        );
        assert!(result.is_ok(), "Compilation failed: {:?}", result.err());
        let schema = result.unwrap();
        let parallel = get_foreach_parallel_flag(&schema);
        assert_eq!(parallel, Some(false), "Loop appending to same file should be sequential");
    }

    fn get_map_op(schema: &Schema) -> Option<&SchemaOp> {
        for entry in &schema.entries {
            if let SchemaOp::Map { .. } = &entry.op {
                return Some(&entry.op);
            }
        }
        None
    }

    fn get_filter_op(schema: &Schema) -> Option<&SchemaOp> {
        for entry in &schema.entries {
            if let SchemaOp::Filter { .. } = &entry.op {
                return Some(&entry.op);
            }
        }
        None
    }

    #[test]
    fn test_map_with_dynamic_iterable() {
        let result = SchemaGenerator::generate(
            r#"
load("@bp/io", "list_dir", "read_file")

def main():
    files = list_dir("/tmp")
    contents = map(lambda path: read_file(path), files)
main()
"#,
            "test.star",
        );
        assert!(result.is_ok(), "Compilation failed: {:?}", result.err());
        let schema = result.unwrap();
        let map_op = get_map_op(&schema);
        assert!(map_op.is_some(), "Expected Map op to be created for dynamic iterable");
    }

    #[test]
    fn test_filter_with_dynamic_iterable() {
        let result = SchemaGenerator::generate(
            r#"
load("@bp/io", "list_dir", "file_exists")

def main():
    files = list_dir("/tmp")
    valid_files = filter(lambda path: file_exists(path), files)
main()
"#,
            "test.star",
        );
        assert!(result.is_ok(), "Compilation failed: {:?}", result.err());
        let schema = result.unwrap();
        let filter_op = get_filter_op(&schema);
        assert!(filter_op.is_some(), "Expected Filter op to be created for dynamic iterable");
    }

    #[test]
    fn test_map_with_static_iterable_no_deferred_op() {
        let result = SchemaGenerator::generate(
            r#"
def double(x):
    return x * 2

def main():
    numbers = [1, 2, 3]
    doubled = map(double, numbers)
main()
"#,
            "test.star",
        );
        assert!(result.is_ok(), "Compilation failed: {:?}", result.err());
        let schema = result.unwrap();
        let map_op = get_map_op(&schema);
        assert!(map_op.is_none(), "Map with static iterable should NOT create deferred Map op");
    }

    #[test]
    fn test_filter_with_static_iterable_no_deferred_op() {
        let result = SchemaGenerator::generate(
            r#"
def is_even(x):
    return x % 2 == 0

def main():
    numbers = [1, 2, 3, 4]
    evens = filter(is_even, numbers)
main()
"#,
            "test.star",
        );
        assert!(result.is_ok(), "Compilation failed: {:?}", result.err());
        let schema = result.unwrap();
        let filter_op = get_filter_op(&schema);
        assert!(filter_op.is_none(), "Filter with static iterable should NOT create deferred Filter op");
    }

    #[test]
    fn test_map_with_lambda_calling_builtin() {
        let result = SchemaGenerator::generate(
            r#"
load("@bp/io", "list_dir", "file_exists")

def main():
    files = list_dir("/tmp")
    exists = map(lambda x: file_exists(x), files)
main()
"#,
            "test.star",
        );
        assert!(result.is_ok(), "Compilation failed: {:?}", result.err());
        let schema = result.unwrap();
        let map_op = get_map_op(&schema);
        assert!(map_op.is_some(), "Expected Map op to be created for lambda with dynamic iterable");
    }
}
