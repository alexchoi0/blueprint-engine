use std::collections::HashSet;
use std::path::{Path, PathBuf};

use blueprint_core::SourceLocation;
use blueprint_parser::{
    AstExpr, AstParameter, AstStmt, AssignTargetP, Clause, ExprP, ForClause, ParameterP, StmtP,
};
use starlark_syntax::codemap::CodeMap;
use starlark_syntax::syntax::ast::ArgumentP;

pub struct CheckerError {
    pub message: String,
    pub location: SourceLocation,
}

pub struct Checker {
    codemap: Option<CodeMap>,
    current_file: Option<PathBuf>,
    builtins: HashSet<String>,
    errors: Vec<CheckerError>,
}

impl Checker {
    pub fn new() -> Self {
        let mut builtins = HashSet::new();
        for name in [
            "True", "False", "None",
            "print", "len", "range", "str", "int", "float", "bool", "list", "dict", "tuple",
            "type", "isinstance", "hasattr", "getattr", "setattr",
            "min", "max", "sum", "abs", "round", "sorted", "reversed", "enumerate", "zip", "map", "filter",
            "any", "all", "input", "open", "exit",
            "http", "json", "time", "crypto", "jwt", "ws", "task", "fs",
            "parallel", "sleep", "env", "run", "glob", "assert", "redact", "hash",
        ] {
            builtins.insert(name.to_string());
        }

        Self {
            codemap: None,
            current_file: None,
            builtins,
            errors: Vec::new(),
        }
    }

    pub fn with_file(mut self, path: impl AsRef<Path>) -> Self {
        self.current_file = Some(path.as_ref().to_path_buf());
        self
    }

    pub fn check(&mut self, module: &blueprint_parser::ParsedModule) -> Vec<CheckerError> {
        self.codemap = Some(module.codemap.clone());
        self.errors.clear();

        let mut scope = CheckScope::new();
        self.check_stmt(module.statements(), &mut scope);

        std::mem::take(&mut self.errors)
    }

    fn check_stmt(&mut self, stmt: &AstStmt, scope: &mut CheckScope) {
        match &stmt.node {
            StmtP::Statements(stmts) => {
                for s in stmts {
                    self.check_stmt(s, scope);
                }
            }

            StmtP::Expression(expr) => {
                self.check_expr(expr, scope);
            }

            StmtP::Assign(assign) => {
                self.check_expr(&assign.rhs, scope);
                self.define_target(&assign.lhs, scope);
            }

            StmtP::AssignModify(lhs, _op, rhs) => {
                self.check_assign_target(lhs, scope);
                self.check_expr(rhs, scope);
            }

            StmtP::If(cond, then_block) => {
                self.check_expr(cond, scope);
                let mut child_scope = scope.child();
                self.check_stmt(then_block, &mut child_scope);
            }

            StmtP::IfElse(cond, branches) => {
                let (then_block, else_block) = branches.as_ref();
                self.check_expr(cond, scope);
                let mut then_scope = scope.child();
                self.check_stmt(then_block, &mut then_scope);
                let mut else_scope = scope.child();
                self.check_stmt(else_block, &mut else_scope);
            }

            StmtP::For(for_stmt) => {
                self.check_expr(&for_stmt.over, scope);
                let mut loop_scope = scope.child();
                self.define_target(&for_stmt.var, &mut loop_scope);
                self.check_stmt(&for_stmt.body, &mut loop_scope);
            }

            StmtP::Def(def) => {
                let func_name = def.name.node.ident.as_str();
                scope.define(func_name.to_string());

                let mut func_scope = scope.child();
                for param in &def.params {
                    if let Some(name) = self.get_param_name(param) {
                        func_scope.define(name);
                    }
                    if let Some(default) = self.get_param_default(param) {
                        self.check_expr(default, scope);
                    }
                }
                self.check_stmt(&def.body, &mut func_scope);
            }

            StmtP::Load(load) => {
                let module_path = &load.module.node;

                if let Err(msg) = self.check_module_exists(module_path) {
                    self.errors.push(CheckerError {
                        message: msg,
                        location: self.get_location(&stmt.span),
                    });
                }

                for arg in &load.args {
                    let local_name = arg.local.node.ident.as_str();
                    let their_name = &arg.their.node;

                    if their_name.starts_with('_') {
                        self.errors.push(CheckerError {
                            message: format!(
                                "'{}' is private and cannot be imported from '{}'",
                                their_name, module_path
                            ),
                            location: self.get_location(&stmt.span),
                        });
                    }

                    scope.define_frozen(local_name.to_string());
                }
            }

            StmtP::Return(expr) => {
                if let Some(e) = expr {
                    self.check_expr(e, scope);
                }
            }

            StmtP::Yield(expr) => {
                if let Some(e) = expr {
                    self.check_expr(e, scope);
                }
            }

            StmtP::Struct(struct_def) => {
                let struct_name = struct_def.name.node.ident.as_str();
                scope.define(struct_name.to_string());

                for field in &struct_def.fields {
                    if let Some(ref default_expr) = field.node.default {
                        self.check_expr(default_expr, scope);
                    }
                }
            }

            StmtP::Break | StmtP::Continue | StmtP::Pass => {}
        }
    }

    fn check_expr(&mut self, expr: &AstExpr, scope: &CheckScope) {
        match &expr.node {
            ExprP::Identifier(ident) => {
                let name = ident.node.ident.as_str();
                if !scope.is_defined(name) && !self.builtins.contains(name) {
                    self.errors.push(CheckerError {
                        message: format!("undefined variable '{}'", name),
                        location: self.get_location(&expr.span),
                    });
                }
            }

            ExprP::Literal(_) => {}

            ExprP::Tuple(items) | ExprP::List(items) => {
                for item in items {
                    self.check_expr(item, scope);
                }
            }

            ExprP::Dict(pairs) => {
                for (key, value) in pairs {
                    self.check_expr(key, scope);
                    self.check_expr(value, scope);
                }
            }

            ExprP::Call(callee, args) => {
                self.check_expr(callee, scope);
                for arg in &args.args {
                    match &arg.node {
                        ArgumentP::Positional(e) | ArgumentP::Named(_, e) |
                        ArgumentP::Args(e) | ArgumentP::KwArgs(e) => {
                            self.check_expr(e, scope);
                        }
                    }
                }
            }

            ExprP::Index(pair) => {
                let (target, index) = pair.as_ref();
                self.check_expr(target, scope);
                self.check_expr(index, scope);
            }

            ExprP::Index2(triple) => {
                let (target, start, end) = triple.as_ref();
                self.check_expr(target, scope);
                self.check_expr(start, scope);
                self.check_expr(end, scope);
            }

            ExprP::Slice(arr, start, stop, step) => {
                self.check_expr(arr, scope);
                if let Some(s) = start {
                    self.check_expr(s, scope);
                }
                if let Some(s) = stop {
                    self.check_expr(s, scope);
                }
                if let Some(s) = step {
                    self.check_expr(s, scope);
                }
            }

            ExprP::Dot(target, _attr) => {
                self.check_expr(target, scope);
            }

            ExprP::Not(inner) | ExprP::Minus(inner) | ExprP::Plus(inner) => {
                self.check_expr(inner, scope);
            }

            ExprP::Op(lhs, _op, rhs) => {
                self.check_expr(lhs, scope);
                self.check_expr(rhs, scope);
            }

            ExprP::If(triple) => {
                let (cond, then_expr, else_expr) = triple.as_ref();
                self.check_expr(cond, scope);
                self.check_expr(then_expr, scope);
                self.check_expr(else_expr, scope);
            }

            ExprP::Lambda(lambda) => {
                let mut lambda_scope = scope.child();
                for param in &lambda.params {
                    if let Some(name) = self.get_param_name(param) {
                        lambda_scope.define(name);
                    }
                    if let Some(default) = self.get_param_default(param) {
                        self.check_expr(default, scope);
                    }
                }
                self.check_expr(&lambda.body, &lambda_scope);
            }

            ExprP::ListComprehension(body, first, clauses) => {
                let mut comp_scope = scope.child();
                self.check_for_clause(first, &mut comp_scope, scope);
                for clause in clauses {
                    self.check_clause(clause, &mut comp_scope);
                }
                self.check_expr(body, &comp_scope);
            }

            ExprP::DictComprehension(pair, first, clauses) => {
                let (key_expr, val_expr) = pair.as_ref();
                let mut comp_scope = scope.child();
                self.check_for_clause(first, &mut comp_scope, scope);
                for clause in clauses {
                    self.check_clause(clause, &mut comp_scope);
                }
                self.check_expr(key_expr, &comp_scope);
                self.check_expr(val_expr, &comp_scope);
            }

            ExprP::FString(fstring) => {
                for expr in &fstring.expressions {
                    self.check_expr(expr, scope);
                }
            }

            _ => {}
        }
    }

    fn check_for_clause(&mut self, clause: &ForClause, comp_scope: &mut CheckScope, parent_scope: &CheckScope) {
        self.check_expr(&clause.over, parent_scope);
        self.define_target(&clause.var, comp_scope);
    }

    fn check_clause(&mut self, clause: &Clause, scope: &mut CheckScope) {
        match clause {
            Clause::For(for_clause) => {
                self.check_expr(&for_clause.over, scope);
                self.define_target(&for_clause.var, scope);
            }
            Clause::If(cond) => {
                self.check_expr(cond, scope);
            }
        }
    }

    fn define_target(&mut self, target: &starlark_syntax::syntax::ast::AstAssignTarget, scope: &mut CheckScope) {
        match &target.node {
            AssignTargetP::Identifier(ident) => {
                let name = &ident.node.ident;
                if scope.is_frozen(name) {
                    self.errors.push(CheckerError {
                        message: format!("cannot reassign imported variable '{}'", name),
                        location: self.get_location(&target.span),
                    });
                }
                scope.define(name.clone());
            }
            AssignTargetP::Tuple(targets) => {
                for t in targets {
                    self.define_target(t, scope);
                }
            }
            AssignTargetP::Index(pair) => {
                let (target_expr, index_expr) = pair.as_ref();
                self.check_expr(target_expr, scope);
                self.check_expr(index_expr, scope);
            }
            AssignTargetP::Dot(target_expr, attr) => {
                self.check_expr(target_expr, scope);
                self.errors.push(CheckerError {
                    message: format!("cannot assign to field '.{}': structs are immutable", attr.node),
                    location: self.get_location(&target.span),
                });
            }
        }
    }

    fn check_assign_target(&mut self, target: &starlark_syntax::syntax::ast::AstAssignTarget, scope: &CheckScope) {
        match &target.node {
            AssignTargetP::Identifier(ident) => {
                let name = ident.node.ident.as_str();
                if !scope.is_defined(name) && !self.builtins.contains(name) {
                    self.errors.push(CheckerError {
                        message: format!("undefined variable '{}'", name),
                        location: self.get_location(&target.span),
                    });
                } else if scope.is_frozen(name) {
                    self.errors.push(CheckerError {
                        message: format!("cannot reassign imported variable '{}'", name),
                        location: self.get_location(&target.span),
                    });
                }
            }
            AssignTargetP::Index(pair) => {
                let (target_expr, index_expr) = pair.as_ref();
                self.check_expr(target_expr, scope);
                self.check_expr(index_expr, scope);
            }
            AssignTargetP::Dot(target_expr, attr) => {
                self.check_expr(target_expr, scope);
                self.errors.push(CheckerError {
                    message: format!("cannot assign to field '.{}': structs are immutable", attr.node),
                    location: self.get_location(&target.span),
                });
            }
            AssignTargetP::Tuple(targets) => {
                for t in targets {
                    self.check_assign_target(t, scope);
                }
            }
        }
    }

    fn get_param_name(&self, param: &AstParameter) -> Option<String> {
        match &param.node {
            ParameterP::Normal(ident, _, _) => Some(ident.node.ident.clone()),
            ParameterP::Args(ident, _) => Some(ident.node.ident.clone()),
            ParameterP::KwArgs(ident, _) => Some(ident.node.ident.clone()),
            ParameterP::NoArgs | ParameterP::Slash => None,
        }
    }

    fn get_param_default<'a>(&self, param: &'a AstParameter) -> Option<&'a AstExpr> {
        match &param.node {
            ParameterP::Normal(_, _, Some(default)) => Some(default.as_ref()),
            _ => None,
        }
    }

    fn check_module_exists(&self, module_path: &str) -> Result<(), String> {
        if module_path.starts_with("@bp/") {
            return Ok(());
        }

        if module_path.starts_with('@') {
            return Ok(());
        }

        if module_path.starts_with("./") || module_path.starts_with("../") {
            let current_dir = self.current_file
                .as_ref()
                .and_then(|f| f.parent().map(|p| p.to_path_buf()))
                .unwrap_or_else(|| PathBuf::from("."));

            let resolved = current_dir.join(module_path);
            if !resolved.exists() {
                return Err(format!("module '{}' not found", module_path));
            }
            return Ok(());
        }

        if let Some(ref current_file) = self.current_file {
            if let Some(current_dir) = current_file.parent() {
                let resolved = current_dir.join(module_path);
                if resolved.exists() {
                    return Ok(());
                }
            }
        }

        Err(format!("module '{}' not found", module_path))
    }

    fn get_location(&self, span: &starlark_syntax::codemap::Span) -> SourceLocation {
        if let Some(ref codemap) = self.codemap {
            blueprint_parser::get_location(codemap, *span)
        } else {
            SourceLocation {
                file: self.current_file.as_ref().map(|p| p.to_string_lossy().to_string()),
                line: 0,
                column: 0,
                span: None,
            }
        }
    }
}

impl Default for Checker {
    fn default() -> Self {
        Self::new()
    }
}

struct CheckScope {
    defined: HashSet<String>,
    frozen: HashSet<String>,
    parent: Option<Box<CheckScope>>,
}

impl CheckScope {
    fn new() -> Self {
        Self {
            defined: HashSet::new(),
            frozen: HashSet::new(),
            parent: None,
        }
    }

    fn child(&self) -> Self {
        Self {
            defined: HashSet::new(),
            frozen: HashSet::new(),
            parent: Some(Box::new(Self {
                defined: self.defined.clone(),
                frozen: self.frozen.clone(),
                parent: self.parent.clone(),
            })),
        }
    }

    fn define(&mut self, name: String) {
        self.defined.insert(name);
    }

    fn define_frozen(&mut self, name: String) {
        self.defined.insert(name.clone());
        self.frozen.insert(name);
    }

    fn is_defined(&self, name: &str) -> bool {
        if self.defined.contains(name) {
            return true;
        }
        if let Some(ref parent) = self.parent {
            return parent.is_defined(name);
        }
        false
    }

    fn is_frozen(&self, name: &str) -> bool {
        if self.frozen.contains(name) {
            return true;
        }
        if let Some(ref parent) = self.parent {
            return parent.is_frozen(name);
        }
        false
    }
}

impl Clone for CheckScope {
    fn clone(&self) -> Self {
        Self {
            defined: self.defined.clone(),
            frozen: self.frozen.clone(),
            parent: self.parent.clone(),
        }
    }
}
