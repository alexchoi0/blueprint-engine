use std::path::{Path, PathBuf};
use std::sync::Arc;

use blueprint_core::{
    BlueprintError, PackageSpec, Result, Value, fetch_package, find_workspace_root, get_packages_dir,
};
use blueprint_eval::{Evaluator, Scope, triggers};
use blueprint_parser::parse;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;

pub async fn run_scripts(
    scripts: Vec<PathBuf>,
    jobs: usize,
    verbose: bool,
    script_args: Vec<String>,
) -> Result<()> {
    let scripts = expand_globs(scripts)?;

    if scripts.is_empty() {
        eprintln!("No scripts found");
        return Ok(());
    }

    if verbose {
        eprintln!("Running {} script(s)", scripts.len());
    }

    let semaphore = if jobs > 0 {
        Some(Arc::new(Semaphore::new(jobs)))
    } else {
        None
    };

    let script_args = Arc::new(script_args);
    let mut join_set: JoinSet<std::result::Result<(PathBuf, Option<BlueprintError>), (PathBuf, BlueprintError)>> =
        JoinSet::new();

    for script_path in scripts {
        let semaphore = semaphore.clone();
        let script_args = script_args.clone();

        join_set.spawn(async move {
            let _permit = if let Some(sem) = &semaphore {
                Some(sem.acquire().await.unwrap())
            } else {
                None
            };

            match run_single_script(&script_path, (*script_args).clone(), verbose).await {
                Ok(()) => Ok((script_path, None)),
                Err(e) => {
                    if matches!(e.inner_error(), BlueprintError::Exit { .. }) {
                        Ok((script_path, Some(e)))
                    } else {
                        Err((script_path, e))
                    }
                }
            }
        });
    }

    let mut errors: Vec<(PathBuf, BlueprintError)> = vec![];
    let mut exit_error: Option<BlueprintError> = None;
    let mut success_count = 0;

    while let Some(result) = join_set.join_next().await {
        match result {
            Ok(Ok((path, maybe_exit))) => {
                if let Some(exit_err) = maybe_exit {
                    exit_error = Some(exit_err);
                } else {
                    success_count += 1;
                    if verbose {
                        eprintln!("[OK] {}", path.display());
                    }
                }
            }
            Ok(Err((path, error))) => {
                eprintln!("[FAIL] {}", path.display());
                errors.push((path, error));
            }
            Err(join_error) => {
                eprintln!("[PANIC] Task panicked: {}", join_error);
            }
        }
    }

    if let Some(exit_err) = exit_error {
        return Err(exit_err);
    }

    if verbose {
        eprintln!("\nResults: {} succeeded, {} failed", success_count, errors.len());
    }

    if !errors.is_empty() {
        eprintln!("\nErrors:");
        for (path, error) in &errors {
            report_error(path, error);
        }
        return Err(BlueprintError::Silent);
    }

    Ok(())
}

async fn run_single_script(
    path: &Path,
    script_args: Vec<String>,
    verbose: bool,
) -> Result<()> {
    let source = tokio::fs::read_to_string(path)
        .await
        .map_err(|e| BlueprintError::IoError {
            path: path.to_string_lossy().to_string(),
            message: e.to_string(),
        })?;

    let filename = path.to_string_lossy().to_string();
    let module = parse(&filename, &source)?;

    let mut evaluator = Evaluator::new();
    evaluator.set_file(path);
    let scope = Scope::new_global();

    let argv: Vec<Value> = std::iter::once(Value::String(Arc::new(filename.clone())))
        .chain(script_args.into_iter().map(|s| Value::String(Arc::new(s))))
        .collect();

    scope
        .define("argv", Value::List(Arc::new(tokio::sync::RwLock::new(argv))))
        .await;

    let abs_path = std::fs::canonicalize(path)
        .unwrap_or_else(|_| path.to_path_buf())
        .to_string_lossy()
        .to_string();
    scope
        .define("__file__", Value::String(Arc::new(abs_path)))
        .await;

    if verbose {
        scope.define("__verbose__", Value::Bool(true)).await;
    }

    evaluator.eval(&module, scope).await?;

    if triggers::has_active_triggers().await {
        if verbose {
            eprintln!("Active triggers detected, waiting for shutdown...");
        }
        triggers::wait_for_shutdown().await;
    }

    Ok(())
}

pub async fn check_scripts(scripts: Vec<PathBuf>, verbose: bool) -> Result<()> {
    let scripts = expand_globs(scripts)?;

    if scripts.is_empty() {
        eprintln!("No scripts found");
        return Ok(());
    }

    let mut errors: Vec<(PathBuf, BlueprintError)> = vec![];

    for path in &scripts {
        if verbose {
            eprintln!("Checking {}...", path.display());
        }

        let source = match tokio::fs::read_to_string(path).await {
            Ok(s) => s,
            Err(e) => {
                errors.push((
                    path.clone(),
                    BlueprintError::IoError {
                        path: path.to_string_lossy().to_string(),
                        message: e.to_string(),
                    },
                ));
                continue;
            }
        };

        let filename = path.to_string_lossy().to_string();
        if let Err(e) = parse(&filename, &source) {
            errors.push((path.clone(), e));
        }
    }

    if errors.is_empty() {
        eprintln!("All {} script(s) OK", scripts.len());
        Ok(())
    } else {
        for (path, error) in &errors {
            report_error(path, error);
        }
        Err(BlueprintError::InternalError {
            message: format!("{} script(s) have errors", errors.len()),
        })
    }
}

pub async fn run_inline(code: &str, verbose: bool, script_args: Vec<String>) -> Result<()> {
    let module = parse("<inline>", code)?;

    let mut evaluator = Evaluator::new();
    let scope = Scope::new_global();

    let argv: Vec<Value> = std::iter::once(Value::String(Arc::new("<inline>".to_string())))
        .chain(script_args.into_iter().map(|s| Value::String(Arc::new(s))))
        .collect();

    scope
        .define("argv", Value::List(Arc::new(tokio::sync::RwLock::new(argv))))
        .await;

    scope
        .define("__file__", Value::String(Arc::new("<inline>".to_string())))
        .await;

    if verbose {
        scope.define("__verbose__", Value::Bool(true)).await;
    }

    evaluator.eval(&module, scope).await?;

    if triggers::has_active_triggers().await {
        if verbose {
            eprintln!("Active triggers detected, waiting for shutdown...");
        }
        triggers::wait_for_shutdown().await;
    }

    Ok(())
}

pub async fn eval_expression(expression: &str, port: Option<u16>) -> Result<()> {
    if let Some(p) = port {
        eval_remote(expression, p).await
    } else {
        eval_local(expression).await
    }
}

async fn eval_local(expression: &str) -> Result<()> {
    let wrapped = format!("__result__ = {}", expression);
    let module = parse("<eval>", &wrapped)?;

    let mut evaluator = Evaluator::new();
    let scope = Scope::new_global();

    evaluator.eval(&module, scope.clone()).await?;

    if let Some(result) = scope.get("__result__").await {
        if !result.is_none() {
            println!("{}", result.repr());
        }
    }

    Ok(())
}

async fn eval_remote(code: &str, port: u16) -> Result<()> {
    let trimmed = code.trim();
    if trimmed == "exit" || trimmed == "quit" || trimmed == "shutdown" {
        let client = reqwest::Client::new();
        client
            .post(format!("http://127.0.0.1:{}/shutdown", port))
            .send()
            .await
            .ok();
        println!("REPL server shutdown");
        return Ok(());
    }

    #[derive(serde::Deserialize)]
    struct EvalResponse {
        success: bool,
        result: Option<String>,
        error: Option<String>,
    }

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("http://127.0.0.1:{}/eval", port))
        .json(&serde_json::json!({"code": code}))
        .send()
        .await
        .map_err(|e| BlueprintError::HttpError {
            url: format!("http://127.0.0.1:{}/eval", port),
            message: e.to_string(),
        })?;

    let eval_resp: EvalResponse = resp.json().await.map_err(|e| BlueprintError::HttpError {
        url: format!("http://127.0.0.1:{}/eval", port),
        message: e.to_string(),
    })?;

    if eval_resp.success {
        if let Some(result) = eval_resp.result {
            println!("{}", result);
        }
        Ok(())
    } else {
        Err(BlueprintError::InternalError {
            message: eval_resp.error.unwrap_or_else(|| "unknown error".to_string()),
        })
    }
}

pub async fn repl(port: Option<u16>) -> Result<()> {
    if let Some(p) = port {
        repl_server(p).await
    } else {
        repl_interactive().await
    }
}

use rustyline::validate::{ValidationContext, ValidationResult, Validator};
use rustyline::highlight::Highlighter;
use rustyline::hint::Hinter;
use rustyline::completion::Completer;
use rustyline::{Helper, ConditionalEventHandler, Event, RepeatCount, Cmd};

#[derive(Clone)]
struct ReplHelper;

struct EnterHandler;

impl ConditionalEventHandler for EnterHandler {
    fn handle(&self, evt: &Event, _n: RepeatCount, _positive: bool, ctx: &rustyline::EventContext) -> Option<Cmd> {
        if let Some(k) = evt.get(0) {
            if let rustyline::KeyEvent { 0: rustyline::KeyCode::Enter, .. } = k {
                let input = ctx.line();
                if needs_more_input(input) {
                    return Some(Cmd::Insert(1, "\n... ".to_string()));
                } else {
                    return Some(Cmd::AcceptLine);
                }
            }
        }
        None
    }
}

impl Completer for ReplHelper {
    type Candidate = String;
}

impl Hinter for ReplHelper {
    type Hint = String;
}

impl Highlighter for ReplHelper {}

const CONTINUATION_PREFIX: &str = "... ";

fn strip_continuation_prefixes(input: &str) -> String {
    input
        .lines()
        .enumerate()
        .map(|(i, line)| {
            if i > 0 && line.starts_with(CONTINUATION_PREFIX) {
                &line[CONTINUATION_PREFIX.len()..]
            } else {
                line
            }
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn needs_more_input(input: &str) -> bool {
    if input.ends_with("... ") || input.ends_with("...\n") {
        return false;
    }

    let clean = strip_continuation_prefixes(input);

    if clean.trim().is_empty() {
        return false;
    }

    let mut bracket_depth = 0i32;
    let mut in_string = false;
    let mut string_char = ' ';
    let mut prev_char = ' ';

    for ch in clean.chars() {
        if in_string {
            if ch == string_char && prev_char != '\\' {
                in_string = false;
            }
        } else {
            match ch {
                '"' | '\'' => {
                    in_string = true;
                    string_char = ch;
                }
                '(' | '[' | '{' => bracket_depth += 1,
                ')' | ']' | '}' => bracket_depth -= 1,
                _ => {}
            }
        }
        prev_char = ch;
    }

    if bracket_depth > 0 || in_string {
        return true;
    }

    let lines: Vec<&str> = clean.lines().collect();

    if let Some(last_line) = lines.last() {
        let last_trimmed = last_line.trim();
        if last_trimmed.ends_with(':') {
            return true;
        }
    }

    let has_block_starter = lines.iter().any(|l| l.trim().ends_with(':'));
    if has_block_starter {
        if let Some(last_line) = lines.last() {
            if last_line.trim().is_empty() {
                return false;
            }
            if last_line.starts_with(|c: char| c.is_whitespace()) {
                return true;
            }
        }
    }

    false
}

impl Validator for ReplHelper {
    fn validate(&self, ctx: &mut ValidationContext) -> rustyline::Result<ValidationResult> {
        let input = ctx.input();

        if input.trim().is_empty() {
            return Ok(ValidationResult::Valid(None));
        }

        if needs_more_input(input) {
            Ok(ValidationResult::Incomplete)
        } else {
            Ok(ValidationResult::Valid(None))
        }
    }
}

impl Helper for ReplHelper {}

async fn repl_interactive() -> Result<()> {
    use rustyline::error::ReadlineError;
    use rustyline::{Config, Editor, EditMode, KeyEvent, EventHandler};

    println!("Blueprint REPL (type 'exit' or Ctrl+D to quit)");
    println!();

    let mut evaluator = Evaluator::new();
    let scope = Scope::new_global();

    let config = Config::builder()
        .auto_add_history(true)
        .bracketed_paste(true)
        .tab_stop(4)
        .edit_mode(EditMode::Emacs)
        .build();

    let mut rl: Editor<ReplHelper, _> = Editor::with_config(config).map_err(|e| {
        BlueprintError::InternalError {
            message: format!("Failed to create REPL: {}", e),
        }
    })?;

    rl.set_helper(Some(ReplHelper));
    rl.bind_sequence(KeyEvent::from('\t'), EventHandler::Simple(Cmd::Insert(1, "    ".to_string())));
    rl.bind_sequence(KeyEvent::from('\r'), EventHandler::Conditional(Box::new(EnterHandler)));

    loop {
        match rl.readline(">>> ") {
            Ok(line) => {
                let trimmed = line.trim();

                if trimmed == "exit" || trimmed == "quit" {
                    break;
                }

                if trimmed.is_empty() {
                    continue;
                }

                let clean_code = strip_continuation_prefixes(&line);
                if let Some(exit_err) = execute_repl_code(&mut evaluator, &scope, &clean_code).await {
                    println!();
                    return Err(exit_err);
                }
            }
            Err(ReadlineError::Interrupted) => {
                println!("^C");
                continue;
            }
            Err(ReadlineError::Eof) => {
                break;
            }
            Err(err) => {
                eprintln!("Error: {:?}", err);
                break;
            }
        }
    }

    println!();
    Ok(())
}

async fn execute_repl_code(evaluator: &mut Evaluator, scope: &Arc<Scope>, code: &str) -> Option<BlueprintError> {
    let result = eval_code_in_scope(evaluator, scope, code).await;
    match result {
        Ok(Some(value)) => {
            println!("{}", value);
            None
        }
        Ok(None) => None,
        Err(e) => {
            if matches!(e.inner_error(), BlueprintError::Exit { .. }) {
                Some(e)
            } else {
                eprintln!("error: {}", e.format_with_stack());
                None
            }
        }
    }
}

async fn eval_code_in_scope(
    evaluator: &mut Evaluator,
    scope: &Arc<Scope>,
    code: &str,
) -> Result<Option<String>> {
    let is_expr = !code.contains('=')
        && !code.starts_with("def ")
        && !code.starts_with("if ")
        && !code.starts_with("for ")
        && !code.starts_with("print(")
        && !code.starts_with("load(");

    let wrapped = if is_expr {
        format!("__repl_result__ = {}", code)
    } else {
        code.to_string()
    };

    let module = parse("<repl>", &wrapped)?;
    evaluator.eval(&module, scope.clone()).await?;

    if is_expr {
        if let Some(result) = scope.get("__repl_result__").await {
            if !result.is_none() {
                return Ok(Some(result.repr()));
            }
        }
    }

    Ok(None)
}

async fn repl_server(port: u16) -> Result<()> {
    use axum::{extract::State, routing::post, Json, Router};
    use std::net::SocketAddr;
    use tokio::sync::Mutex;

    let evaluator = Arc::new(Mutex::new(Evaluator::new()));
    let scope = Scope::new_global();

    #[derive(serde::Deserialize)]
    struct EvalRequest {
        code: String,
    }

    #[derive(serde::Serialize)]
    struct EvalResponse {
        success: bool,
        result: Option<String>,
        error: Option<String>,
    }

    let state = (evaluator, scope);

    let app = Router::new()
        .route(
            "/eval",
            post(
                |State((eval, scope)): State<(Arc<Mutex<Evaluator>>, Arc<Scope>)>,
                 Json(req): Json<EvalRequest>| async move {
                    let mut evaluator = eval.lock().await;
                    match eval_code_in_scope(&mut evaluator, &scope, &req.code).await {
                        Ok(result) => Json(EvalResponse {
                            success: true,
                            result,
                            error: None,
                        }),
                        Err(e) => Json(EvalResponse {
                            success: false,
                            result: None,
                            error: Some(e.to_string()),
                        }),
                    }
                },
            ),
        )
        .route(
            "/shutdown",
            post(|| async {
                tokio::spawn(async {
                    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
                    std::process::exit(0);
                });
                "shutting down"
            }),
        )
        .with_state(state);

    let addr = SocketAddr::from(([127, 0, 0, 1], port));
    println!("REPL server listening on http://{}", addr);

    let listener = tokio::net::TcpListener::bind(addr).await.map_err(|e| {
        BlueprintError::IoError {
            path: format!("127.0.0.1:{}", port),
            message: e.to_string(),
        }
    })?;

    axum::serve(listener, app).await.map_err(|e| {
        BlueprintError::IoError {
            path: format!("127.0.0.1:{}", port),
            message: e.to_string(),
        }
    })?;

    Ok(())
}

pub async fn install_package(package: &str) -> Result<()> {
    let spec = PackageSpec::parse(package)?;
    let packages_dir = get_packages_dir();
    let package_dir = packages_dir.join(&spec.user).join(spec.dir_name());

    if package_dir.exists() {
        println!("Package {} is already installed", spec.display_name());
        return Ok(());
    }

    println!("Installing {}...", spec.display_name());
    fetch_package(&spec, &package_dir)?;
    println!("Installed {}", spec.display_name());

    Ok(())
}

pub async fn uninstall_package(package: &str) -> Result<()> {
    let spec = PackageSpec::parse(package)?;
    let packages_dir = get_packages_dir();
    let user_dir = packages_dir.join(&spec.user);

    if spec.version != "main" {
        let package_dir = user_dir.join(spec.dir_name());
        if package_dir.exists() {
            std::fs::remove_dir_all(&package_dir).map_err(|e| BlueprintError::IoError {
                path: package_dir.to_string_lossy().to_string(),
                message: e.to_string(),
            })?;
            println!("Uninstalled {}", spec.display_name());
        } else {
            println!("Package {} is not installed", spec.display_name());
        }
    } else {
        if !user_dir.exists() {
            println!("No packages from @{}/{} are installed", spec.user, spec.repo);
            return Ok(());
        }
        let mut found = false;
        for entry in std::fs::read_dir(&user_dir).map_err(|e| BlueprintError::IoError {
            path: user_dir.to_string_lossy().to_string(),
            message: e.to_string(),
        })? {
            let entry = entry.map_err(|e| BlueprintError::IoError {
                path: user_dir.to_string_lossy().to_string(),
                message: e.to_string(),
            })?;
            let name = entry.file_name().to_string_lossy().to_string();
            if name.starts_with(&format!("{}#", spec.repo)) {
                std::fs::remove_dir_all(entry.path()).map_err(|e| BlueprintError::IoError {
                    path: entry.path().to_string_lossy().to_string(),
                    message: e.to_string(),
                })?;
                println!("Uninstalled @{}/{}", spec.user, name);
                found = true;
            }
        }
        if !found {
            println!("No packages from @{}/{} are installed", spec.user, spec.repo);
        }
    }

    Ok(())
}

pub async fn list_packages() -> Result<()> {
    let packages_dir = get_packages_dir();

    if let Some(workspace) = find_workspace_root() {
        println!("Packages in workspace: {}", workspace.display());
    }

    if !packages_dir.exists() {
        println!("No packages installed");
        return Ok(());
    }

    let mut packages = Vec::new();

    for user_entry in std::fs::read_dir(&packages_dir).map_err(|e| BlueprintError::IoError {
        path: packages_dir.to_string_lossy().to_string(),
        message: e.to_string(),
    })? {
        let user_entry = user_entry.map_err(|e| BlueprintError::IoError {
            path: packages_dir.to_string_lossy().to_string(),
            message: e.to_string(),
        })?;

        if !user_entry.path().is_dir() {
            continue;
        }

        let user = user_entry.file_name().to_string_lossy().to_string();

        for pkg_entry in std::fs::read_dir(user_entry.path()).map_err(|e| BlueprintError::IoError {
            path: user_entry.path().to_string_lossy().to_string(),
            message: e.to_string(),
        })? {
            let pkg_entry = pkg_entry.map_err(|e| BlueprintError::IoError {
                path: user_entry.path().to_string_lossy().to_string(),
                message: e.to_string(),
            })?;

            if pkg_entry.path().is_dir() {
                let name = pkg_entry.file_name().to_string_lossy().to_string();
                packages.push(format!("@{}/{}", user, name));
            }
        }
    }

    if packages.is_empty() {
        println!("No packages installed");
    } else {
        packages.sort();
        for pkg in packages {
            println!("{}", pkg);
        }
    }

    Ok(())
}

fn expand_globs(patterns: Vec<PathBuf>) -> Result<Vec<PathBuf>> {
    let mut result = vec![];

    for pattern in patterns {
        let pattern_str = pattern.to_string_lossy();

        if pattern_str.contains('*') || pattern_str.contains('?') {
            for entry in glob::glob(&pattern_str).map_err(|e| BlueprintError::GlobError {
                message: e.to_string(),
            })? {
                match entry {
                    Ok(path) => result.push(path),
                    Err(e) => {
                        return Err(BlueprintError::GlobError {
                            message: e.to_string(),
                        })
                    }
                }
            }
        } else {
            result.push(pattern);
        }
    }

    Ok(result)
}

fn report_error(path: &Path, error: &BlueprintError) {
    eprintln!("\n--- {} ---", path.display());
    eprintln!("{}", error.format_with_stack());
}

pub async fn init_workspace() -> Result<()> {
    let current_dir = std::env::current_dir().map_err(|e| BlueprintError::IoError {
        path: ".".into(),
        message: e.to_string(),
    })?;
    crate::workspace::init_workspace(&current_dir)
}

pub async fn sync_workspace() -> Result<()> {
    let current_dir = std::env::current_dir().map_err(|e| BlueprintError::IoError {
        path: ".".into(),
        message: e.to_string(),
    })?;

    let workspace = crate::workspace::Workspace::find(&current_dir).ok_or_else(|| {
        BlueprintError::IoError {
            path: current_dir.to_string_lossy().to_string(),
            message: "No BP.toml found in current directory or any parent".into(),
        }
    })?;

    if workspace.config.dependencies.is_empty() {
        println!("No dependencies to install");
        return Ok(());
    }

    println!("Installing dependencies from BP.toml...");
    workspace.install_all()?;
    println!("Done!");
    Ok(())
}
