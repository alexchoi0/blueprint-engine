mod args;
mod runner;

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
        }
    });

    if let Err(e) = result {
        eprintln!("error: {}", e);
        std::process::exit(1);
    }
}
