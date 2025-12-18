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
                jobs,
                verbose,
                script_args,
            } => runner::run_scripts(scripts, jobs, verbose, script_args).await,
            Commands::Check { scripts, verbose } => runner::check_scripts(scripts, verbose).await,
            Commands::Eval { expression } => runner::eval_expression(&expression).await,
        }
    });

    if let Err(e) = result {
        eprintln!("error: {}", e);
        std::process::exit(1);
    }
}
