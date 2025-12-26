mod args;
mod callgraph;
mod runner;
mod workspace;

use blueprint_engine_core::BlueprintError;
use clap::Parser;
use tokio::runtime::Builder;

use args::{Cli, Commands, GenerateCommands};
use runner::PermissionFlags;

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
                sandbox,
                allow_all,
                ask,
                allow,
                deny,
                script_args,
            } => {
                let perm_flags = PermissionFlags {
                    sandbox,
                    allow_all,
                    ask,
                    allow,
                    deny,
                };
                if let Some(code) = exec {
                    runner::run_inline(&code, verbose, script_args, perm_flags).await
                } else {
                    runner::run_scripts(scripts, jobs, verbose, script_args, perm_flags).await
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
            Commands::Login { registry } => runner::login(registry.as_deref()).await,
            Commands::Logout => runner::logout().await,
            Commands::Publish {
                path,
                registry,
                token,
                yes,
            } => runner::publish(path, registry.as_deref(), token.as_deref(), yes).await,
            Commands::Whoami => runner::whoami().await,
            Commands::Generate { command } => match command {
                GenerateCommands::Dot { pattern, output } => {
                    runner::generate_dot(&pattern, output.as_deref()).await
                }
            },
        }
    });

    if let Err(e) = result {
        let exit_code = extract_exit_code(&e);
        if exit_code == 0 {
            std::process::exit(0);
        }
        if !matches!(
            e.inner_error(),
            BlueprintError::Exit { .. } | BlueprintError::Silent
        ) {
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
