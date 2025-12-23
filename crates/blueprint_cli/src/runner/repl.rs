use std::sync::Arc;

use blueprint_engine_core::{BlueprintError, Result};
use blueprint_engine_eval::{Evaluator, Scope};
use blueprint_engine_parser::parse;

use rustyline::completion::Completer;
use rustyline::highlight::Highlighter;
use rustyline::hint::Hinter;
use rustyline::validate::{ValidationContext, ValidationResult, Validator};
use rustyline::{Cmd, ConditionalEventHandler, Event, Helper, RepeatCount};

#[derive(Clone)]
pub struct ReplHelper;

pub struct EnterHandler;

impl ConditionalEventHandler for EnterHandler {
    fn handle(
        &self,
        evt: &Event,
        _n: RepeatCount,
        _positive: bool,
        ctx: &rustyline::EventContext,
    ) -> Option<Cmd> {
        if let Some(k) = evt.get(0) {
            if let rustyline::KeyEvent {
                0: rustyline::KeyCode::Enter,
                ..
            } = k
            {
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

pub fn strip_continuation_prefixes(input: &str) -> String {
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

pub async fn repl(port: Option<u16>) -> Result<()> {
    if let Some(p) = port {
        repl_server(p).await
    } else {
        repl_interactive().await
    }
}

async fn repl_interactive() -> Result<()> {
    use rustyline::error::ReadlineError;
    use rustyline::{Config, EditMode, Editor, EventHandler, KeyEvent};

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
    rl.bind_sequence(
        KeyEvent::from('\t'),
        EventHandler::Simple(Cmd::Insert(1, "    ".to_string())),
    );
    rl.bind_sequence(
        KeyEvent::from('\r'),
        EventHandler::Conditional(Box::new(EnterHandler)),
    );

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
                if let Some(exit_err) =
                    execute_repl_code(&mut evaluator, &scope, &clean_code).await
                {
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

async fn execute_repl_code(
    evaluator: &mut Evaluator,
    scope: &Arc<Scope>,
    code: &str,
) -> Option<BlueprintError> {
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

pub async fn eval_code_in_scope(
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

    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .map_err(|e| BlueprintError::IoError {
            path: format!("127.0.0.1:{}", port),
            message: e.to_string(),
        })?;

    axum::serve(listener, app)
        .await
        .map_err(|e| BlueprintError::IoError {
            path: format!("127.0.0.1:{}", port),
            message: e.to_string(),
        })?;

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

    let eval_resp: EvalResponse = resp
        .json()
        .await
        .map_err(|e| BlueprintError::HttpError {
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
