use clap::{Parser, Subcommand};
use std::path::PathBuf;

#[derive(Parser)]
#[command(name = "blueprint3")]
#[command(author, version, about = "High-performance Starlark script executor with implicit async I/O")]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand)]
pub enum Commands {
    #[command(about = "Run one or more Starlark scripts")]
    Run {
        #[arg(required_unless_present = "exec", num_args = 1..)]
        scripts: Vec<PathBuf>,

        #[arg(short = 'e', long = "exec", help = "Execute inline code instead of files")]
        exec: Option<String>,

        #[arg(short = 'j', long, default_value = "0", help = "Max concurrent scripts (0 = unlimited)")]
        jobs: usize,

        #[arg(short, long, help = "Verbose output")]
        verbose: bool,

        #[arg(last = true, help = "Arguments passed to scripts")]
        script_args: Vec<String>,
    },

    #[command(about = "Check scripts for syntax errors (dry run)")]
    Check {
        #[arg(required = true, num_args = 1..)]
        scripts: Vec<PathBuf>,

        #[arg(short, long, help = "Verbose output")]
        verbose: bool,
    },

    #[command(about = "Evaluate a Starlark expression")]
    Eval {
        #[arg(help = "Expression to evaluate")]
        expression: String,

        #[arg(short, long, help = "Connect to REPL server on specified port")]
        port: Option<u16>,
    },

    #[command(about = "Start interactive REPL session")]
    Repl {
        #[arg(short, long, help = "Start REPL server on specified port")]
        port: Option<u16>,
    },
}
