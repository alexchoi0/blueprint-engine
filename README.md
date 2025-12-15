# Blueprint

[![CI](https://github.com/anthropics/blueprint/actions/workflows/ci.yml/badge.svg)](https://github.com/anthropics/blueprint/actions/workflows/ci.yml)

A two-phase execution engine for Starlark scripts with controlled system access and approval workflows.

## Overview

Blueprint separates script planning from execution, enabling safe and auditable automation:

1. **Planning Phase**: Scripts are parsed and compiled into an operational plan without executing side effects
2. **Execution Phase**: Plans execute in parallel while respecting dependencies and approval policies

This architecture allows you to inspect exactly what a script will do before it runs, making Blueprint ideal for automation tasks that require oversight.

## Features

- **Starlark Language**: Python-like syntax that's easy to read and write
- **Two-Phase Execution**: Plan first, execute later with full visibility
- **Parallel Execution**: Independent operations run concurrently
- **Approval System**: Policy-based approval for sensitive operations
- **Builtin Modules**: File I/O, HTTP, JSON, and shell execution

## Installation

```bash
cargo install --path .
```

## Quick Start

Create a script `hello.star`:

```python
load("@bp/io", "write_file")

write_file("/tmp/hello.txt", "Hello, Blueprint!")
```

Check what the script will do:

```bash
blueprint schema hello.star
```

Run the script:

```bash
blueprint run hello.star
```

## CLI Commands

| Command | Description |
|---------|-------------|
| `check <script>` | Validate script syntax |
| `schema <script>` | Show what operations the script will perform |
| `compile <script>` | Compile to a binary plan file |
| `run <script>` | Parse, compile, and execute in one step |
| `exec <plan>` | Execute a pre-compiled plan |
| `inspect <file>` | Examine compiled plans or schemas |

## Standard Library

Blueprint provides builtin modules that are loaded directly - no stdlib files needed:

### `@bp/io` - File Operations

```python
load("@bp/io", "read_file", "write_file", "append_file", "delete_file")
load("@bp/io", "file_exists", "is_file", "is_dir", "file_size")
load("@bp/io", "mkdir", "rmdir", "list_dir", "copy_file", "move_file")

content = read_file("input.txt")
write_file("output.txt", content)
mkdir("/tmp/mydir", recursive=True)
```

### `@bp/http` - HTTP Requests

```python
load("@bp/http", "http_request")

response = http_request("GET", "https://api.example.com/users")
response = http_request("POST", "https://api.example.com/data", body='{"key": "value"}')
```

### `@bp/json` - JSON Encoding/Decoding

```python
load("@bp/json", "json_encode", "json_decode")

data = {"name": "Blueprint", "version": "0.1.0"}
json_str = json_encode(data)
parsed = json_decode('{"key": "value"}')
```

### `@bp/exec` - Shell Execution

```python
load("@bp/exec", "exec_shell", "exec_run", "env_get")

result = exec_shell("echo 'Hello' && date")
result = exec_run("ls", ["-la", "/tmp"])
home = env_get("HOME")
```

## Architecture

Blueprint is organized into several crates:

| Crate | Description |
|-------|-------------|
| `blueprint_common` | Shared types (Op, Plan, Schema) |
| `blueprint_generator` | Starlark → Schema → Plan compilation |
| `blueprint_interpreter` | Async plan execution |
| `blueprint_approval` | Policy-based approval system |
| `blueprint_storage` | SQLite state persistence |

## License

Licensed under either of:

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or http://www.apache.org/licenses/LICENSE-2.0)
- MIT license ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)

at your option.

## Copyright

Copyright 2025 Alex Choi
