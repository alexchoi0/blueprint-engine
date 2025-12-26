use std::collections::HashMap;
use std::path::{Path, PathBuf};

use blueprint_engine_parser::{AstExpr, AstStmt, ExprP, StmtP};
use blueprint_starlark_syntax::syntax::ast::ArgumentP;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NodeKind {
    Entry,
    Exit,
    Statement,
    Condition,
    ForLoop,
    Match,
    Yield,
    Export,
}

#[derive(Debug, Clone)]
pub struct CfgNode {
    pub id: usize,
    pub kind: NodeKind,
    pub label: String,
    pub file: PathBuf,
    pub function: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EdgeKind {
    Sequential,
    TrueBranch,
    FalseBranch,
    LoopBack,
    LoopDone,
    LoopBreak,
    Call,
    Exports,
}

#[derive(Debug, Clone)]
pub struct CfgEdge {
    pub from: usize,
    pub to: usize,
    pub kind: EdgeKind,
}

#[derive(Debug, Default)]
pub struct ControlFlowGraph {
    pub nodes: Vec<CfgNode>,
    pub edges: Vec<CfgEdge>,
    node_counter: usize,
}

impl ControlFlowGraph {
    pub fn new() -> Self {
        Self::default()
    }

    fn add_node(&mut self, kind: NodeKind, label: String, file: &Path, function: Option<&str>) -> usize {
        let id = self.node_counter;
        self.node_counter += 1;
        self.nodes.push(CfgNode {
            id,
            kind,
            label,
            file: file.to_path_buf(),
            function: function.map(|s| s.to_string()),
        });
        id
    }

    fn add_edge(&mut self, from: usize, to: usize, kind: EdgeKind) {
        self.edges.push(CfgEdge { from, to, kind });
    }

    pub fn to_dot(&self) -> String {
        let mut dot = String::new();
        dot.push_str("digraph ControlFlowGraph {\n");
        dot.push_str("    rankdir=TB;\n");
        dot.push_str("    node [fontname=\"Helvetica\", fontsize=10];\n");
        dot.push_str("    edge [fontname=\"Helvetica\", fontsize=9];\n");
        dot.push_str("\n");

        // Group nodes by function
        let mut functions: HashMap<(PathBuf, Option<String>), Vec<&CfgNode>> = HashMap::new();
        for node in &self.nodes {
            functions
                .entry((node.file.clone(), node.function.clone()))
                .or_default()
                .push(node);
        }

        // Create subgraphs for each function
        for ((file, func), nodes) in &functions {
            let file_stem = file.file_stem()
                .map(|s| s.to_string_lossy().to_string())
                .unwrap_or_else(|| "unknown".to_string());

            let subgraph_name = match func {
                Some(f) => format!("{}::{}", file_stem, f),
                None => format!("{}::<module>", file_stem),
            };

            dot.push_str(&format!("    subgraph \"cluster_{}\" {{\n", subgraph_name));
            dot.push_str(&format!("        label=\"{}\";\n", subgraph_name));
            dot.push_str("        style=rounded;\n");
            dot.push_str("        color=gray;\n");

            for node in nodes {
                let (shape, style, color) = match node.kind {
                    NodeKind::Entry => ("ellipse", "filled", "lightgreen"),
                    NodeKind::Exit => ("ellipse", "filled", "lightcoral"),
                    NodeKind::Statement => ("box", "rounded", "white"),
                    NodeKind::Condition => ("diamond", "filled", "lightyellow"),
                    NodeKind::ForLoop => ("hexagon", "filled", "lightblue"),
                    NodeKind::Match => ("octagon", "filled", "plum"),
                    NodeKind::Yield => ("parallelogram", "filled", "orange"),
                    NodeKind::Export => ("cds", "filled", "lightcyan"),
                };

                let escaped_label = node.label
                    .replace('\\', "\\\\")
                    .replace('"', "\\\"")
                    .replace('\n', "\\n");

                dot.push_str(&format!(
                    "        n{} [label=\"{}\" shape={} style=\"{}\" fillcolor=\"{}\"];\n",
                    node.id, escaped_label, shape, style, color
                ));
            }

            dot.push_str("    }\n\n");
        }

        // Add edges
        for edge in &self.edges {
            let (style, color, label) = match edge.kind {
                EdgeKind::Sequential => ("solid", "black", ""),
                EdgeKind::TrueBranch => ("solid", "green", "T"),
                EdgeKind::FalseBranch => ("solid", "red", "F"),
                EdgeKind::LoopBack => ("dashed", "blue", "loop"),
                EdgeKind::LoopDone => ("solid", "purple", "done"),
                EdgeKind::LoopBreak => ("bold", "red", "break"),
                EdgeKind::Call => ("dotted", "orange", "call"),
                EdgeKind::Exports => ("bold", "cyan", ""),
            };

            if label.is_empty() {
                dot.push_str(&format!(
                    "    n{} -> n{} [style={} color={}];\n",
                    edge.from, edge.to, style, color
                ));
            } else {
                dot.push_str(&format!(
                    "    n{} -> n{} [style={} color={} label=\"{}\"];\n",
                    edge.from, edge.to, style, color, label
                ));
            }
        }

        dot.push_str("}\n");
        dot
    }
}

pub struct CfgBuilder {
    graph: ControlFlowGraph,
    current_file: PathBuf,
    current_function: Option<String>,
    function_entries: HashMap<String, usize>,
    loop_stack: Vec<LoopContext>,
    module_exports: Vec<String>,
    current_function_exit: Option<usize>,
    pending_false_branches: Vec<usize>,
}

struct LoopContext {
    header: usize,
    break_targets: Vec<usize>,
}

impl CfgBuilder {
    pub fn new() -> Self {
        Self {
            graph: ControlFlowGraph::new(),
            current_file: PathBuf::new(),
            current_function: None,
            function_entries: HashMap::new(),
            loop_stack: Vec::new(),
            module_exports: Vec::new(),
            current_function_exit: None,
            pending_false_branches: Vec::new(),
        }
    }

    pub fn analyze_file(&mut self, path: &Path, module: &blueprint_engine_parser::ParsedModule) {
        self.current_file = path.to_path_buf();
        self.current_function = None;
        self.function_entries.clear();
        self.module_exports.clear();

        // First pass: collect function definitions and exports
        self.collect_functions(module.statements());
        self.collect_exports(module.statements());

        // Second pass: build CFG for module-level code
        let entry = self.graph.add_node(
            NodeKind::Entry,
            "module entry".to_string(),
            &self.current_file,
            None,
        );

        let exit = self.graph.add_node(
            NodeKind::Exit,
            "module exit".to_string(),
            &self.current_file,
            None,
        );

        let last_nodes = self.analyze_stmt(module.statements(), vec![entry]);
        for last in last_nodes {
            self.add_predecessor_edge(last, exit);
        }

        // Create export nodes for public symbols
        if !self.module_exports.is_empty() {
            let exports_label = self.module_exports.join(", ");
            let export_node = self.graph.add_node(
                NodeKind::Export,
                format!("exports: {}", exports_label),
                &self.current_file,
                None,
            );
            self.graph.add_edge(exit, export_node, EdgeKind::Exports);
        }
    }

    fn collect_exports(&mut self, stmt: &AstStmt) {
        match &stmt.node {
            StmtP::Statements(stmts) => {
                for s in stmts {
                    self.collect_exports(s);
                }
            }
            StmtP::Assign(assign) => {
                // Only collect top-level assignments (not inside functions)
                if self.current_function.is_none() {
                    if let Some(name) = self.get_assign_name(&assign.lhs) {
                        if !name.starts_with('_') {
                            self.module_exports.push(name);
                        }
                    }
                }
            }
            StmtP::Def(def) => {
                let name = def.name.node.ident.as_str();
                if !name.starts_with('_') {
                    self.module_exports.push(name.to_string());
                }
            }
            StmtP::Struct(struct_def) => {
                let name = struct_def.name.node.ident.as_str();
                if !name.starts_with('_') {
                    self.module_exports.push(name.to_string());
                }
            }
            _ => {}
        }
    }

    fn get_assign_name(&self, target: &blueprint_starlark_syntax::syntax::ast::AstAssignTarget) -> Option<String> {
        use blueprint_starlark_syntax::syntax::ast::AssignTargetP;
        match &target.node {
            AssignTargetP::Identifier(ident) => Some(ident.node.ident.as_str().to_string()),
            _ => None,
        }
    }

    fn collect_functions(&mut self, stmt: &AstStmt) {
        match &stmt.node {
            StmtP::Statements(stmts) => {
                for s in stmts {
                    self.collect_functions(s);
                }
            }
            StmtP::Def(def) => {
                let name = def.name.node.ident.as_str().to_string();

                let prev_func = self.current_function.take();
                let prev_exit = self.current_function_exit.take();
                self.current_function = Some(name.clone());

                let entry = self.graph.add_node(
                    NodeKind::Entry,
                    format!("{}()", name),
                    &self.current_file,
                    Some(&name),
                );
                self.function_entries.insert(name.clone(), entry);

                let exit = self.graph.add_node(
                    NodeKind::Exit,
                    "return".to_string(),
                    &self.current_file,
                    Some(&name),
                );
                self.current_function_exit = Some(exit);

                let last_nodes = self.analyze_stmt(&def.body, vec![entry]);
                for last in last_nodes {
                    self.add_predecessor_edge(last, exit);
                }

                self.current_function = prev_func;
                self.current_function_exit = prev_exit;
            }
            _ => {}
        }
    }

    fn analyze_stmt(&mut self, stmt: &AstStmt, predecessors: Vec<usize>) -> Vec<usize> {
        if predecessors.is_empty() {
            return vec![];
        }

        match &stmt.node {
            StmtP::Statements(stmts) => {
                let mut current = predecessors;
                for s in stmts {
                    current = self.analyze_stmt(s, current);
                }
                current
            }

            StmtP::Expression(expr) => {
                let label = self.expr_to_string(expr);
                let node = self.graph.add_node(
                    NodeKind::Statement,
                    label,
                    &self.current_file,
                    self.current_function.as_deref(),
                );

                for pred in &predecessors {
                    self.add_predecessor_edge(*pred, node);
                }

                // Add call edges for function calls in expression
                self.add_call_edges(expr, node);

                vec![node]
            }

            StmtP::Assign(assign) => {
                let lhs = self.assign_target_to_string(&assign.lhs);
                let rhs = self.expr_to_string(&assign.rhs);
                let label = format!("{} = {}", lhs, rhs);

                let node = self.graph.add_node(
                    NodeKind::Statement,
                    label,
                    &self.current_file,
                    self.current_function.as_deref(),
                );

                for pred in &predecessors {
                    self.add_predecessor_edge(*pred, node);
                }

                self.add_call_edges(&assign.rhs, node);

                vec![node]
            }

            StmtP::AssignModify(lhs, op, rhs) => {
                let lhs_str = self.assign_target_to_string(lhs);
                let rhs_str = self.expr_to_string(rhs);
                let op_str = format!("{:?}", op).to_lowercase();
                let label = format!("{} {}= {}", lhs_str, op_str, rhs_str);

                let node = self.graph.add_node(
                    NodeKind::Statement,
                    label,
                    &self.current_file,
                    self.current_function.as_deref(),
                );

                for pred in &predecessors {
                    self.add_predecessor_edge(*pred, node);
                }

                vec![node]
            }

            StmtP::If(cond, then_block) => {
                let cond_str = self.expr_to_string(cond);
                let cond_node = self.graph.add_node(
                    NodeKind::Condition,
                    format!("if {}", cond_str),
                    &self.current_file,
                    self.current_function.as_deref(),
                );

                for pred in &predecessors {
                    self.add_predecessor_edge(*pred, cond_node);
                }

                // True branch
                let then_exits = self.analyze_stmt(then_block, vec![cond_node]);

                // Create merge point
                let mut exits = then_exits;
                exits.push(cond_node); // False branch goes directly to merge

                // Update edges: true branch
                if let Some(first_then) = self.find_first_successor(cond_node) {
                    self.update_edge_kind(cond_node, first_then, EdgeKind::TrueBranch);
                }

                // Mark cond_node for false branch edge to next statement
                self.pending_false_branches.push(cond_node);

                exits
            }

            StmtP::IfElse(cond, branches) => {
                let (then_block, else_block) = branches.as_ref();

                let cond_str = self.expr_to_string(cond);
                let cond_node = self.graph.add_node(
                    NodeKind::Condition,
                    format!("if {}", cond_str),
                    &self.current_file,
                    self.current_function.as_deref(),
                );

                for pred in &predecessors {
                    self.add_predecessor_edge(*pred, cond_node);
                }

                // True branch
                let then_exits = self.analyze_stmt(then_block, vec![cond_node]);

                // False branch
                let else_exits = self.analyze_stmt(else_block, vec![cond_node]);

                // Update edge kinds
                if let Some(first_then) = self.find_first_successor_after(cond_node, 0) {
                    self.update_edge_kind(cond_node, first_then, EdgeKind::TrueBranch);
                }
                if let Some(first_else) = self.find_first_successor_after(cond_node, 1) {
                    self.update_edge_kind(cond_node, first_else, EdgeKind::FalseBranch);
                }

                // Merge exits
                let mut exits = then_exits;
                exits.extend(else_exits);
                exits
            }

            StmtP::For(for_stmt) => {
                let var = self.assign_target_to_string(&for_stmt.var);
                let iter = self.expr_to_string(&for_stmt.over);

                let loop_node = self.graph.add_node(
                    NodeKind::ForLoop,
                    format!("for {} in {}", var, iter),
                    &self.current_file,
                    self.current_function.as_deref(),
                );

                for pred in &predecessors {
                    self.add_predecessor_edge(*pred, loop_node);
                }

                // Push loop context for break/continue
                self.loop_stack.push(LoopContext {
                    header: loop_node,
                    break_targets: Vec::new(),
                });

                // Loop body
                let body_exits = self.analyze_stmt(&for_stmt.body, vec![loop_node]);

                // Pop loop context and collect break targets
                let loop_ctx = self.loop_stack.pop().unwrap();

                // Back edge from body end to loop header
                for exit in &body_exits {
                    self.graph.add_edge(*exit, loop_node, EdgeKind::LoopBack);
                }

                // Create a merge node for loop exit
                let loop_exit = self.graph.add_node(
                    NodeKind::Statement,
                    "loop exit".to_string(),
                    &self.current_file,
                    self.current_function.as_deref(),
                );

                // Normal loop completion (iterator exhausted)
                self.graph.add_edge(loop_node, loop_exit, EdgeKind::LoopDone);

                // Break statements exit the loop
                for break_node in loop_ctx.break_targets {
                    self.graph.add_edge(break_node, loop_exit, EdgeKind::LoopBreak);
                }

                vec![loop_exit]
            }

            StmtP::Match(match_stmt) => {
                let subject = self.expr_to_string(&match_stmt.subject);
                let match_node = self.graph.add_node(
                    NodeKind::Match,
                    format!("match {}", subject),
                    &self.current_file,
                    self.current_function.as_deref(),
                );

                for pred in &predecessors {
                    self.add_predecessor_edge(*pred, match_node);
                }

                let mut all_exits = vec![];
                for case in &match_stmt.cases {
                    let pattern = self.expr_to_string(&case.node.pattern);
                    let case_node = self.graph.add_node(
                        NodeKind::Condition,
                        format!("case {}", pattern),
                        &self.current_file,
                        self.current_function.as_deref(),
                    );
                    self.graph.add_edge(match_node, case_node, EdgeKind::Sequential);

                    let case_exits = self.analyze_stmt(&case.node.body, vec![case_node]);
                    all_exits.extend(case_exits);
                }

                all_exits
            }

            StmtP::Return(expr) => {
                let label = match expr {
                    Some(e) => format!("return {}", self.expr_to_string(e)),
                    None => "return".to_string(),
                };

                let node = self.graph.add_node(
                    NodeKind::Statement,
                    label,
                    &self.current_file,
                    self.current_function.as_deref(),
                );

                for pred in &predecessors {
                    self.add_predecessor_edge(*pred, node);
                }

                // Add call edges for function calls in return expression
                if let Some(e) = expr {
                    self.add_call_edges(e, node);
                }

                // Connect return to function exit node
                if let Some(exit) = self.current_function_exit {
                    self.graph.add_edge(node, exit, EdgeKind::Sequential);
                }

                // Return doesn't have successors in normal flow
                vec![]
            }

            StmtP::Yield(expr) => {
                let label = match expr {
                    Some(e) => format!("yield {}", self.expr_to_string(e)),
                    None => "yield".to_string(),
                };

                let node = self.graph.add_node(
                    NodeKind::Yield,
                    label,
                    &self.current_file,
                    self.current_function.as_deref(),
                );

                for pred in &predecessors {
                    self.add_predecessor_edge(*pred, node);
                }

                // Add call edges for function calls in yield expression
                if let Some(e) = expr {
                    self.add_call_edges(e, node);
                }

                vec![node]
            }

            StmtP::Load(load) => {
                let module = &load.module.node;
                let symbols: Vec<String> = load.args.iter()
                    .map(|a| a.local.node.ident.as_str().to_string())
                    .collect();

                let label = format!("load({}, {})", module, symbols.join(", "));
                let node = self.graph.add_node(
                    NodeKind::Statement,
                    label,
                    &self.current_file,
                    self.current_function.as_deref(),
                );

                for pred in &predecessors {
                    self.add_predecessor_edge(*pred, node);
                }

                vec![node]
            }

            StmtP::Def(_) => {
                // Function definitions are handled separately in collect_functions
                predecessors
            }

            StmtP::Struct(struct_def) => {
                let name = struct_def.name.node.ident.as_str();
                let fields: Vec<String> = struct_def.fields.iter()
                    .map(|f| f.node.name.node.ident.as_str().to_string())
                    .collect();

                let label = format!("struct {}({})", name, fields.join(", "));
                let node = self.graph.add_node(
                    NodeKind::Statement,
                    label,
                    &self.current_file,
                    self.current_function.as_deref(),
                );

                for pred in &predecessors {
                    self.add_predecessor_edge(*pred, node);
                }

                vec![node]
            }

            StmtP::Break => {
                let node = self.graph.add_node(
                    NodeKind::Statement,
                    "break".to_string(),
                    &self.current_file,
                    self.current_function.as_deref(),
                );

                for pred in &predecessors {
                    self.add_predecessor_edge(*pred, node);
                }

                // Record break target for the enclosing loop
                if let Some(loop_ctx) = self.loop_stack.last_mut() {
                    loop_ctx.break_targets.push(node);
                }

                vec![]
            }

            StmtP::Continue => {
                let node = self.graph.add_node(
                    NodeKind::Statement,
                    "continue".to_string(),
                    &self.current_file,
                    self.current_function.as_deref(),
                );

                for pred in &predecessors {
                    self.add_predecessor_edge(*pred, node);
                }

                // Connect continue back to loop header
                if let Some(loop_ctx) = self.loop_stack.last() {
                    self.graph.add_edge(node, loop_ctx.header, EdgeKind::LoopBack);
                }

                vec![]
            }

            StmtP::Pass => {
                let node = self.graph.add_node(
                    NodeKind::Statement,
                    "pass".to_string(),
                    &self.current_file,
                    self.current_function.as_deref(),
                );

                for pred in &predecessors {
                    self.add_predecessor_edge(*pred, node);
                }

                vec![node]
            }
        }
    }

    fn add_call_edges(&mut self, expr: &AstExpr, from_node: usize) {
        match &expr.node {
            ExprP::Call(callee, args) => {
                if let ExprP::Identifier(ident) = &callee.node {
                    let name = ident.node.ident.as_str();
                    if let Some(&entry) = self.function_entries.get(name) {
                        self.graph.add_edge(from_node, entry, EdgeKind::Call);
                    }
                }

                // Recurse into arguments
                for arg in &args.args {
                    match &arg.node {
                        ArgumentP::Positional(e) | ArgumentP::Named(_, e)
                        | ArgumentP::Args(e) | ArgumentP::KwArgs(e) => {
                            self.add_call_edges(e, from_node);
                        }
                    }
                }
            }
            ExprP::Op(lhs, _, rhs) => {
                self.add_call_edges(lhs, from_node);
                self.add_call_edges(rhs, from_node);
            }
            ExprP::If(triple) => {
                let (cond, then_expr, else_expr) = triple.as_ref();
                self.add_call_edges(cond, from_node);
                self.add_call_edges(then_expr, from_node);
                self.add_call_edges(else_expr, from_node);
            }
            ExprP::List(items) | ExprP::Tuple(items) => {
                for item in items {
                    self.add_call_edges(item, from_node);
                }
            }
            ExprP::Dict(pairs) => {
                for (k, v) in pairs {
                    self.add_call_edges(k, from_node);
                    self.add_call_edges(v, from_node);
                }
            }
            _ => {}
        }
    }

    fn find_first_successor(&self, node: usize) -> Option<usize> {
        self.graph.edges.iter()
            .find(|e| e.from == node)
            .map(|e| e.to)
    }

    fn find_first_successor_after(&self, node: usize, skip: usize) -> Option<usize> {
        self.graph.edges.iter()
            .filter(|e| e.from == node)
            .nth(skip)
            .map(|e| e.to)
    }

    fn update_edge_kind(&mut self, from: usize, to: usize, kind: EdgeKind) {
        for edge in &mut self.graph.edges {
            if edge.from == from && edge.to == to {
                edge.kind = kind;
                break;
            }
        }
    }

    fn add_predecessor_edge(&mut self, pred: usize, to: usize) {
        let kind = if self.pending_false_branches.contains(&pred) {
            self.pending_false_branches.retain(|&x| x != pred);
            EdgeKind::FalseBranch
        } else {
            EdgeKind::Sequential
        };
        self.graph.add_edge(pred, to, kind);
    }

    fn expr_to_string(&self, expr: &AstExpr) -> String {
        match &expr.node {
            ExprP::Identifier(ident) => ident.node.ident.as_str().to_string(),
            ExprP::Literal(lit) => {
                use blueprint_starlark_syntax::syntax::ast::AstLiteral;
                match lit {
                    AstLiteral::Int(i) => format!("{}", i.node),
                    AstLiteral::Float(f) => format!("{}", f.node),
                    AstLiteral::String(s) => format!("\"{}\"", s.node),
                    AstLiteral::ByteString(s) => format!("b\"{}\"", String::from_utf8_lossy(&s.node)),
                    AstLiteral::Ellipsis => "...".to_string(),
                }
            }
            ExprP::Call(callee, args) => {
                let callee_str = self.expr_to_string(callee);
                let args_str: Vec<String> = args.args.iter()
                    .map(|a| match &a.node {
                        ArgumentP::Positional(e) => self.expr_to_string(e),
                        ArgumentP::Named(name, e) => format!("{}={}", name.node, self.expr_to_string(e)),
                        ArgumentP::Args(e) => format!("*{}", self.expr_to_string(e)),
                        ArgumentP::KwArgs(e) => format!("**{}", self.expr_to_string(e)),
                    })
                    .collect();
                format!("{}({})", callee_str, args_str.join(", "))
            }
            ExprP::Dot(target, attr) => {
                format!("{}.{}", self.expr_to_string(target), attr.node)
            }
            ExprP::Index(pair) => {
                let (target, index) = pair.as_ref();
                format!("{}[{}]", self.expr_to_string(target), self.expr_to_string(index))
            }
            ExprP::Op(lhs, op, rhs) => {
                use blueprint_starlark_syntax::syntax::ast::BinOp;
                let op_str = match op {
                    BinOp::Add => "+",
                    BinOp::Subtract => "-",
                    BinOp::Multiply => "*",
                    BinOp::Divide => "/",
                    BinOp::FloorDivide => "//",
                    BinOp::Percent => "%",
                    BinOp::Equal => "==",
                    BinOp::NotEqual => "!=",
                    BinOp::Less => "<",
                    BinOp::Greater => ">",
                    BinOp::LessOrEqual => "<=",
                    BinOp::GreaterOrEqual => ">=",
                    BinOp::In => "in",
                    BinOp::NotIn => "not in",
                    BinOp::And => "and",
                    BinOp::Or => "or",
                    BinOp::BitAnd => "&",
                    BinOp::BitOr => "|",
                    BinOp::BitXor => "^",
                    BinOp::LeftShift => "<<",
                    BinOp::RightShift => ">>",
                };
                format!("{} {} {}", self.expr_to_string(lhs), op_str, self.expr_to_string(rhs))
            }
            ExprP::Not(inner) => format!("not {}", self.expr_to_string(inner)),
            ExprP::Minus(inner) => format!("-{}", self.expr_to_string(inner)),
            ExprP::Plus(inner) => format!("+{}", self.expr_to_string(inner)),
            ExprP::List(items) => {
                let items_str: Vec<String> = items.iter().map(|e| self.expr_to_string(e)).collect();
                format!("[{}]", items_str.join(", "))
            }
            ExprP::Tuple(items) => {
                let items_str: Vec<String> = items.iter().map(|e| self.expr_to_string(e)).collect();
                format!("({})", items_str.join(", "))
            }
            ExprP::Dict(pairs) => {
                let pairs_str: Vec<String> = pairs.iter()
                    .map(|(k, v)| format!("{}: {}", self.expr_to_string(k), self.expr_to_string(v)))
                    .collect();
                format!("{{{}}}", pairs_str.join(", "))
            }
            ExprP::If(triple) => {
                let (cond, then_expr, else_expr) = triple.as_ref();
                format!("{} if {} else {}",
                    self.expr_to_string(then_expr),
                    self.expr_to_string(cond),
                    self.expr_to_string(else_expr))
            }
            ExprP::Lambda(lambda) => {
                format!("lambda: {}", self.expr_to_string(&lambda.body))
            }
            ExprP::ListComprehension(body, first, _) => {
                format!("[{} for {} in {}]",
                    self.expr_to_string(body),
                    self.assign_target_to_string(&first.var),
                    self.expr_to_string(&first.over))
            }
            ExprP::FString(fstring) => {
                format!("f\"...{}...\"", fstring.expressions.len())
            }
            _ => "...".to_string(),
        }
    }

    fn assign_target_to_string(&self, target: &blueprint_starlark_syntax::syntax::ast::AstAssignTarget) -> String {
        use blueprint_starlark_syntax::syntax::ast::AssignTargetP;
        match &target.node {
            AssignTargetP::Identifier(ident) => ident.node.ident.as_str().to_string(),
            AssignTargetP::Tuple(targets) => {
                let items: Vec<String> = targets.iter()
                    .map(|t| self.assign_target_to_string(t))
                    .collect();
                format!("({})", items.join(", "))
            }
            AssignTargetP::Index(pair) => {
                let (target, index) = pair.as_ref();
                format!("{}[{}]", self.expr_to_string(target), self.expr_to_string(index))
            }
            AssignTargetP::Dot(target, attr) => {
                format!("{}.{}", self.expr_to_string(target), attr.node)
            }
        }
    }

    pub fn build(self) -> ControlFlowGraph {
        self.graph
    }
}

impl Default for CfgBuilder {
    fn default() -> Self {
        Self::new()
    }
}

pub fn analyze_files(files: &[PathBuf]) -> ControlFlowGraph {
    let mut builder = CfgBuilder::new();

    for file in files {
        let content = match std::fs::read_to_string(file) {
            Ok(c) => c,
            Err(_) => continue,
        };

        let filename = file.to_string_lossy().to_string();
        let module = match blueprint_engine_parser::parse(&filename, &content) {
            Ok(m) => m,
            Err(_) => continue,
        };

        builder.analyze_file(file, &module);
    }

    builder.build()
}
