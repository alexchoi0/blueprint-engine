mod package;
mod publish;
mod repl;

pub use package::{
    init_workspace, install_package, list_packages, sync_workspace, uninstall_package,
};
pub use publish::{login, logout, publish, whoami};
pub use repl::{eval_expression, repl};

use std::path::{Path, PathBuf};
use std::sync::Arc;

use blueprint_engine_core::{
    with_permissions_async, BlueprintError, Permissions, Policy, Result, Value,
};
use blueprint_engine_eval::{triggers, Checker, Evaluator, Scope};
use blueprint_engine_parser::parse;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;

use crate::workspace::Workspace;

#[derive(Clone, Default)]
pub struct PermissionFlags {
    pub sandbox: bool,
    pub allow_all: bool,
    pub ask: bool,
    pub allow: Vec<String>,
    pub deny: Vec<String>,
}

impl PermissionFlags {
    pub fn resolve(&self, workspace_perms: Option<Permissions>) -> Option<Arc<Permissions>> {
        if self.allow_all {
            return None;
        }

        if self.sandbox {
            return Some(Arc::new(Permissions::none()));
        }

        if self.ask {
            return Some(Arc::new(Permissions::ask_all()));
        }

        let has_cli_flags = !self.allow.is_empty() || !self.deny.is_empty();

        if has_cli_flags {
            let perms = Permissions {
                policy: Policy::Deny,
                allow: self.allow.clone(),
                ask: vec![],
                deny: self.deny.clone(),
            };
            return Some(Arc::new(perms));
        }

        workspace_perms.map(Arc::new)
    }
}

fn load_workspace_permissions(script_path: Option<&Path>) -> Option<Permissions> {
    let start_dir = script_path
        .and_then(|p| p.parent())
        .map(|p| p.to_path_buf())
        .or_else(|| std::env::current_dir().ok())?;

    Workspace::find(&start_dir).map(|ws| ws.config.permissions)
}

pub async fn run_scripts(
    scripts: Vec<PathBuf>,
    jobs: usize,
    verbose: bool,
    script_args: Vec<String>,
    perm_flags: PermissionFlags,
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
    let perm_flags = Arc::new(perm_flags);
    let mut join_set: JoinSet<
        std::result::Result<(PathBuf, Option<BlueprintError>), (PathBuf, BlueprintError)>,
    > = JoinSet::new();

    for script_path in scripts {
        let semaphore = semaphore.clone();
        let script_args = script_args.clone();
        let perm_flags = perm_flags.clone();

        join_set.spawn(async move {
            let _permit = if let Some(sem) = &semaphore {
                Some(sem.acquire().await.unwrap())
            } else {
                None
            };

            match run_single_script(&script_path, (*script_args).clone(), verbose, &perm_flags)
                .await
            {
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
        eprintln!(
            "\nResults: {} succeeded, {} failed",
            success_count,
            errors.len()
        );
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
    perm_flags: &PermissionFlags,
) -> Result<()> {
    let source = tokio::fs::read_to_string(path)
        .await
        .map_err(|e| BlueprintError::IoError {
            path: path.to_string_lossy().to_string(),
            message: e.to_string(),
        })?;

    let filename = path.to_string_lossy().to_string();
    let module = parse(&filename, &source)?;

    let mut checker = Checker::new().with_file(path);
    let errors = checker.check(&module);
    if !errors.is_empty() {
        let mut message = String::new();
        for error in &errors {
            if !message.is_empty() {
                message.push('\n');
            }
            message.push_str(&format!("{}: {}", error.location, error.message));
        }
        return Err(BlueprintError::ValueError { message });
    }

    let workspace_perms = load_workspace_permissions(Some(path));
    let permissions = perm_flags.resolve(workspace_perms);

    let run_script = async {
        let mut evaluator = Evaluator::new();
        evaluator.set_file(path);
        let scope = Scope::new_global();

        let argv: Vec<Value> = std::iter::once(Value::String(Arc::new(filename.clone())))
            .chain(script_args.into_iter().map(|s| Value::String(Arc::new(s))))
            .collect();

        scope
            .define(
                "argv",
                Value::List(Arc::new(tokio::sync::RwLock::new(argv))),
            )
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
    };

    if let Some(perms) = permissions {
        with_permissions_async(perms, || run_script).await
    } else {
        run_script.await
    }
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

pub async fn run_inline(
    code: &str,
    verbose: bool,
    script_args: Vec<String>,
    perm_flags: PermissionFlags,
) -> Result<()> {
    let module = parse("<inline>", code)?;

    let workspace_perms = load_workspace_permissions(None);
    let permissions = perm_flags.resolve(workspace_perms);

    let run_script = async {
        let mut evaluator = Evaluator::new();
        let scope = Scope::new_global();

        let argv: Vec<Value> = std::iter::once(Value::String(Arc::new("<inline>".to_string())))
            .chain(script_args.into_iter().map(|s| Value::String(Arc::new(s))))
            .collect();

        scope
            .define(
                "argv",
                Value::List(Arc::new(tokio::sync::RwLock::new(argv))),
            )
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
    };

    if let Some(perms) = permissions {
        with_permissions_async(perms, || run_script).await
    } else {
        run_script.await
    }
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

pub async fn generate_dot(pattern: &str, output: Option<&Path>) -> Result<()> {
    let files = expand_globs(vec![PathBuf::from(pattern)])?;

    if files.is_empty() {
        eprintln!("No files found matching pattern: {}", pattern);
        return Ok(());
    }

    eprintln!("Analyzing {} file(s)...", files.len());

    let graph = crate::callgraph::analyze_files(&files);
    let dot = graph.to_dot();

    if let Some(output_path) = output {
        tokio::fs::write(output_path, &dot)
            .await
            .map_err(|e| BlueprintError::IoError {
                path: output_path.to_string_lossy().to_string(),
                message: e.to_string(),
            })?;
        eprintln!("Written to {}", output_path.display());
    } else {
        println!("{}", dot);
    }

    Ok(())
}
