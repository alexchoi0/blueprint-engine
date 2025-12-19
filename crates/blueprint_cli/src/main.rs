mod args;
mod runner;
mod workspace;

use blueprint_core::BlueprintError;
use clap::Parser;
use tokio::runtime::Builder;

use args::{Cli, Commands};

fn main() {
    let cli = Cli::parse();

    let runtime = Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("Failed to build Tokio runtime");

    let result = runtime.block_on(async {
        match cli.command {
            Commands::Run {
                scripts,
                exec,
                jobs,
                verbose,
                script_args,
            } => {
                if let Some(code) = exec {
                    runner::run_inline(&code, verbose, script_args).await
                } else {
                    runner::run_scripts(scripts, jobs, verbose, script_args).await
                }
            }
            Commands::Check { scripts, verbose } => runner::check_scripts(scripts, verbose).await,
            Commands::Eval { expression, port } => runner::eval_expression(&expression, port).await,
            Commands::Repl { port } => runner::repl(port).await,
            Commands::Install { package } => runner::install_package(&package).await,
            Commands::Uninstall { package } => runner::uninstall_package(&package).await,
            Commands::List => runner::list_packages().await,
            Commands::Init => runner::init_workspace().await,
            Commands::Sync => runner::sync_workspace().await,
        }
    });

    if let Err(e) = result {
        let exit_code = extract_exit_code(&e);
        if exit_code == 0 {
            std::process::exit(0);
        }
        if !matches!(e.inner_error(), BlueprintError::Exit { .. } | BlueprintError::Silent) {
            eprintln!("error: {}", e);
        }
        std::process::exit(exit_code);
    }
}

fn extract_exit_code(e: &BlueprintError) -> i32 {
    match e.inner_error() {
        BlueprintError::Exit { code } => *code,
        _ => 1,
    }
}
