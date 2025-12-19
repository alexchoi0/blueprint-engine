use std::path::{Path, PathBuf};
use std::sync::Arc;

use blueprint_core::{BlueprintError, Result, Value};
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
    let mut join_set: JoinSet<std::result::Result<PathBuf, (PathBuf, BlueprintError)>> =
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
                Ok(()) => Ok(script_path),
                Err(e) => Err((script_path, e)),
            }
        });
    }

    let mut errors: Vec<(PathBuf, BlueprintError)> = vec![];
    let mut success_count = 0;

    while let Some(result) = join_set.join_next().await {
        match result {
            Ok(Ok(path)) => {
                success_count += 1;
                if verbose {
                    eprintln!("[OK] {}", path.display());
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

    if verbose {
        eprintln!("\nResults: {} succeeded, {} failed", success_count, errors.len());
    }

    if !errors.is_empty() {
        eprintln!("\nErrors:");
        for (path, error) in &errors {
            report_error(path, error);
        }
        return Err(BlueprintError::InternalError {
            message: format!("{} script(s) failed", errors.len()),
        });
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

async fn repl_interactive() -> Result<()> {
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

    println!("Blueprint REPL (type 'exit' or Ctrl+D to quit)");

    let mut evaluator = Evaluator::new();
    let scope = Scope::new_global();

    let stdin = tokio::io::stdin();
    let mut reader = BufReader::new(stdin);
    let mut stdout = tokio::io::stdout();

    loop {
        stdout.write_all(b">>> ").await.ok();
        stdout.flush().await.ok();

        let mut line = String::new();
        match reader.read_line(&mut line).await {
            Ok(0) => break,
            Ok(_) => {}
            Err(_) => break,
        }

        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        if trimmed == "exit" || trimmed == "quit" {
            break;
        }

        let result = eval_code_in_scope(&mut evaluator, &scope, trimmed).await;
        match result {
            Ok(Some(value)) => println!("{}", value),
            Ok(None) => {}
            Err(e) => eprintln!("error: {}", e),
        }
    }

    println!();
    Ok(())
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
    eprintln!("error: {} in {}", error, path.display());
}
