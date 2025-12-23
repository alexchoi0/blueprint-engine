# Blueprint Engine Architecture

## Implicit Async Starlark Runtime

Blueprint Engine is a high-performance Starlark script executor with implicit async I/O.
Scripts use standard Starlark syntax—no async/await keywords—while the runtime
automatically yields to Tokio at I/O boundaries, enabling thousands of concurrent scripts.

---

## System Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                            BLUEPRINT ENGINE                                 │
│                     Implicit Async Starlark Runtime                         │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐   │
│  │  script1.bp │    │  script2.bp │    │  script3.bp │    │  scriptN.bp │   │
│  └──────┬──────┘    └──────┬──────┘    └──────┬──────┘    └──────┬──────┘   │
│         │                  │                  │                  │          │
│         ▼                  ▼                  ▼                  ▼          │
│  ┌──────────────────────────────────────────────────────────────────────┐   │
│  │                        STARLARK PARSER                               │   │
│  │                    (reuse starlark-rust AST)                         │   │
│  └──────────────────────────────────────────────────────────────────────┘   │
│         │                  │                  │                  │          │
│         ▼                  ▼                  ▼                  ▼          │
│  ┌──────────────────────────────────────────────────────────────────────┐   │
│  │                      ASYNC EVALUATOR                                 │   │
│  │                                                                      │   │
│  │   ┌─────────┐  ┌─────────┐  ┌─────────┐  ┌─────────┐                 │   │
│  │   │ Task 1  │  │ Task 2  │  │ Task 3  │  │ Task N  │   Lightweight   │   │
│  │   │ (async) │  │ (async) │  │ (async) │  │ (async) │   Tokio Tasks   │   │
│  │   └────┬────┘  └────┬────┘  └────┬────┘  └────┬────┘                 │   │
│  │        │            │            │            │                      │   │
│  │        └────────────┴─────┬──────┴────────────┘                      │   │
│  │                           │                                          │   │
│  │                           ▼                                          │   │
│  │              ┌─────────────────────────┐                             │   │
│  │              │    YIELD POINTS         │                             │   │
│  │              │  (at every native I/O)  │                             │   │
│  │              └─────────────────────────┘                             │   │
│  └──────────────────────────────────────────────────────────────────────┘   │
│                              │                                              │
│                              ▼                                              │
│  ┌──────────────────────────────────────────────────────────────────────┐   │
│  │                     NATIVE FUNCTIONS                                 │   │
│  │                                                                      │   │
│  │  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐                │   │
│  │  │  File I/O    │  │  HTTP        │  │  Process     │                │   │
│  │  │  read_file   │  │ http_request │  │  run         │                │   │
│  │  │  write_file  │  │  download    │  │  shell       │                │   │
│  │  │  glob        │  │              │  │              │                │   │
│  │  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘                │   │
│  │         │                 │                 │                        │   │
│  │         │    All async    │    under the    │    hood                │   │
│  │         │                 │                 │                        │   │
│  │         ▼                 ▼                 ▼                        │   │
│  │  ┌──────────────────────────────────────────────────────────────┐    │   │
│  │  │                    TOKIO RUNTIME                             │    │   │
│  │  │              (multi-threaded work-stealing)                  │    │   │
│  │  │                                                              │    │   │
│  │  │   Worker 1        Worker 2        Worker 3        Worker 4   │    │   │
│  │  │   ┌──────┐        ┌──────┐        ┌──────┐        ┌──────┐   │    │   │
│  │  │   │ ████ │        │ ████ │        │ ████ │        │ ████ │   │    │   │
│  │  │   │ ████ │        │ ████ │        │ ████ │        │ ████ │   │    │   │
│  │  │   └──────┘        └──────┘        └──────┘        └──────┘   │    │   │
│  │  │                                                              │    │   │
│  │  │   Tasks migrate between workers at yield points              │    │   │
│  │  └──────────────────────────────────────────────────────────────┘    │   │
│  └──────────────────────────────────────────────────────────────────────┘   │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Crate Structure

```
blueprint/
├── Cargo.toml                   # Workspace
├── ARCHITECTURE.md              # This file
│
├── crates/
│   ├── blueprint-engine-core/   # Shared types, errors
│   │   └── src/
│   │       ├── lib.rs
│   │       ├── error.rs         # BlueprintError
│   │       └── value.rs         # Runtime values
│   │
│   ├── blueprint-engine-parser/ # Starlark parsing
│   │   └── src/
│   │       └── lib.rs           # Parse to AST (wraps starlark-rust)
│   │
│   ├── blueprint-engine-eval/   # Async evaluator (THE CORE)
│   │   └── src/
│   │       ├── lib.rs
│   │       ├── eval.rs          # Main async eval loop
│   │       ├── scope.rs         # Variable scopes
│   │       └── natives/
│   │           ├── mod.rs
│   │           ├── file.rs      # read_file, write_file, glob, exists
│   │           ├── http.rs      # http_request, download
│   │           ├── process.rs   # run, shell, env
│   │           ├── parallel.rs  # parallel() function
│   │           ├── console.rs   # print, input
│   │           ├── time.rs      # sleep, now
│   │           ├── json.rs      # json_encode, json_decode
│   │           ├── crypto.rs    # sha256, hmac_sha256
│   │           ├── jwt.rs       # jwt_encode, jwt_decode
│   │           ├── approval.rs  # ask_for_approval
│   │           ├── redact.rs    # redact_pii, redact_secrets
│   │           ├── task.rs      # task() with timeout
│   │           └── triggers.rs  # http_server, cron, interval
│   │
│   └── blueprint_cli/           # CLI interface
│       └── src/
│           ├── main.rs
│           ├── args.rs          # CLI argument parsing
│           └── runner.rs        # Script execution
│
└── stdlib/                      # Standard library modules
    ├── aws.bp                   # @bp/aws - S3 operations
    ├── gcp.bp                   # @bp/gcp - GCS operations
    └── llm.bp                   # @bp/llm - LLM/agent functions
```

---

## CLI Commands

```bash
# Run scripts
bp run script.bp                    # Run a script
bp run script.bp -- arg1 arg2       # With arguments
bp run *.bp                         # Run multiple scripts
bp run -j 10 *.bp                   # Limit concurrency
bp run -e 'print("hello")'          # Inline code execution

# REPL
bp repl                             # Interactive REPL
bp repl --port 8888                 # Start REPL server

# Evaluate (connects to REPL server if --port specified)
bp eval "1 + 2"                     # Evaluate expression
bp eval "x = 10" --port 8888        # Eval against REPL server
bp eval "exit" --port 8888          # Shutdown REPL server

# Package management
bp install @user/repo               # Install from GitHub (main branch)
bp install @user/repo#v1.0          # Install specific version/tag
bp uninstall @user/repo             # Uninstall package
bp list                             # List installed packages

# Other
bp check script.bp                  # Syntax check only
```

---

## Package Manager

Packages are GitHub repositories that auto-install on first use:

```starlark
# Auto-installs @user/repo from GitHub if not present
load("@user/repo", "func")

# Specific version/tag
load("@user/repo#v1.0", "func")

# Built-in stdlib
load("@bp/aws", "s3_upload", "s3_download")
load("@bp/gcp", "gcs_upload", "gcs_download")
load("@bp/llm", "agent")
```

Package location: `~/.blueprint/packages/@user/repo#version/`

Entry point: `lib.bp` in repository root

---

## Triggers System

Triggers allow scripts to run as daemons:

```starlark
# HTTP server
server = http_server(8080, {
    "GET /health": lambda req: {"status": "ok"},
    "POST /webhook": handle_webhook,
})

# Cron job (standard cron syntax)
job = cron("0 * * * *", lambda: print("every hour"))

# Interval timer
timer = interval(60, lambda: print("every 60 seconds"))

# Control functions
print(running(server))  # True/False
print(triggers())       # List all active triggers
stop(server)            # Stop specific trigger
stop_all()              # Stop all triggers
```

Script stays alive while triggers are active, exits when all stopped.

---

## Native Functions Reference

### File Operations

| Function | Signature | Description |
|----------|-----------|-------------|
| `read_file` | `read_file(path) -> str` | Read file contents |
| `write_file` | `write_file(path, content) -> None` | Write to file |
| `append_file` | `append_file(path, content) -> None` | Append to file |
| `exists` | `exists(path) -> bool` | Check if path exists |
| `is_file` | `is_file(path) -> bool` | Check if path is file |
| `is_dir` | `is_dir(path) -> bool` | Check if path is directory |
| `glob` | `glob(pattern) -> list` | Find files matching pattern |
| `mkdir` | `mkdir(path) -> None` | Create directory |
| `rm` | `rm(path) -> None` | Remove file or directory |
| `cp` | `cp(src, dst) -> None` | Copy file |
| `mv` | `mv(src, dst) -> None` | Move file |

### HTTP Operations

| Function | Signature | Description |
|----------|-----------|-------------|
| `http_request` | `http_request(method, url, body=, headers=, timeout=) -> Response` | HTTP request |
| `download` | `download(url, path) -> None` | Download file |

Response object: `.status`, `.body`, `.headers`

### Process Operations

| Function | Signature | Description |
|----------|-----------|-------------|
| `run` | `run(args) -> Result` | Run command (list or string) |
| `shell` | `shell(cmd, cwd=, env=) -> Result` | Run shell command |
| `env` | `env(name, default="") -> str` | Get env var |
| `set_env` | `set_env(name, value) -> None` | Set env var |

Result object: `.code`, `.stdout`, `.stderr`

### Concurrency

| Function | Signature | Description |
|----------|-----------|-------------|
| `parallel` | `parallel(fns) -> list` | Run functions in parallel |
| `sleep` | `sleep(seconds) -> None` | Async sleep |
| `task` | `task(fn, timeout=) -> any` | Run with timeout |

### Triggers

| Function | Signature | Description |
|----------|-----------|-------------|
| `http_server` | `http_server(port, routes, host=) -> handle` | Start HTTP server |
| `cron` | `cron(schedule, handler) -> handle` | Cron job |
| `interval` | `interval(seconds, handler) -> handle` | Interval timer |
| `stop` | `stop(handle) -> None` | Stop trigger(s) |
| `stop_all` | `stop_all() -> None` | Stop all triggers |
| `running` | `running(handle) -> bool` | Check if running |
| `triggers` | `triggers() -> list` | List active triggers |

### JSON

| Function | Signature | Description |
|----------|-----------|-------------|
| `json_encode` | `json_encode(value) -> str` | Encode to JSON |
| `json_decode` | `json_decode(str) -> value` | Decode from JSON |

### Crypto

| Function | Signature | Description |
|----------|-----------|-------------|
| `sha256` | `sha256(data) -> str` | SHA-256 hash (hex) |
| `hmac_sha256` | `hmac_sha256(key, data, key_hex=) -> str` | HMAC-SHA256 |

### JWT

| Function | Signature | Description |
|----------|-----------|-------------|
| `jwt_encode` | `jwt_encode(payload, secret, algorithm=) -> str` | Create JWT |
| `jwt_decode` | `jwt_decode(token, secret, algorithm=) -> dict` | Decode/verify JWT |

### Console

| Function | Signature | Description |
|----------|-----------|-------------|
| `print` | `print(*args, sep=, end=) -> None` | Print to stdout |
| `eprint` | `eprint(*args) -> None` | Print to stderr |
| `input` | `input(prompt="") -> str` | Read from stdin |

### Time

| Function | Signature | Description |
|----------|-----------|-------------|
| `now` | `now() -> float` | Current Unix timestamp |
| `sleep` | `sleep(seconds) -> None` | Async sleep |

### Assertions

| Function | Signature | Description |
|----------|-----------|-------------|
| `fail` | `fail(msg) -> Never` | Fail with error |
| `assert_true` | `assert_true(cond, msg=) -> None` | Assert condition |
| `assert_eq` | `assert_eq(a, b, msg=) -> None` | Assert equality |
| `assert_ne` | `assert_ne(a, b, msg=) -> None` | Assert inequality |
| `assert_contains` | `assert_contains(haystack, needle, msg=) -> None` | Assert contains |

### Security

| Function | Signature | Description |
|----------|-----------|-------------|
| `redact_pii` | `redact_pii(text) -> str` | Redact PII |
| `redact_secrets` | `redact_secrets(text) -> str` | Redact secrets |
| `ask_for_approval` | `ask_for_approval(prompt) -> bool` | Interactive approval |

---

## Standard Library

### @bp/aws

```starlark
load("@bp/aws", "s3_upload", "s3_download", "s3_list", "s3_delete")

# Configure credentials
creds = aws_auth(
    access_key_id = env("AWS_ACCESS_KEY_ID"),
    secret_access_key = env("AWS_SECRET_ACCESS_KEY"),
    region = "us-east-1"
)

# S3 operations
s3_upload(creds, "my-bucket", "key.txt", "content")
content = s3_download(creds, "my-bucket", "key.txt")
files = s3_list(creds, "my-bucket", prefix="logs/")
s3_delete(creds, "my-bucket", "key.txt")
```

### @bp/gcp

```starlark
load("@bp/gcp", "gcs_upload", "gcs_download", "gcs_list", "gcs_delete")

# Configure credentials (service account JSON)
creds = gcp_auth(service_account_json = read_file("sa.json"))

# GCS operations
gcs_upload(creds, "my-bucket", "key.txt", "content")
content = gcs_download(creds, "my-bucket", "key.txt")
files = gcs_list(creds, "my-bucket", prefix="logs/")
gcs_delete(creds, "my-bucket", "key.txt")
```

### @bp/llm

```starlark
load("@bp/llm", "agent")

# Simple agent
response = agent("What is 2 + 2?")

# Agent with tools
response = agent(
    prompt = "Get the weather in NYC",
    tools = [get_weather],
    model = "claude-sonnet-4-20250514"
)
```

---

## Execution Model

### Implicit Async

Scripts look synchronous but run asynchronously:

```starlark
# This looks blocking but yields at I/O boundaries
content = read_file("data.txt")    # yields while reading
response = http_request("GET", url) # yields while fetching
result = shell("make build")        # yields while running
```

### Parallel Execution

```
TIME ──────────────────────────────────────────────────────────────────────────►

Core 1   ║ S1: eval ║ S1: read_file ║ S5: eval ║ S5: http_get ║ S9: eval  ║
         ║   (cpu)  ║   (yields)    ║   (cpu)  ║   (yields)   ║   (cpu)   ║

Core 2   ║ S2: eval ║ S2: http_get  ║ S6: eval ║ S6: read_file║ S10: eval ║
         ║   (cpu)  ║   (yields)    ║   (cpu)  ║   (yields)   ║   (cpu)   ║

Core 3   ║ S3: eval ║ S3: write_file║ S7: eval ║ S7: run()    ║ S11: eval ║
         ║   (cpu)  ║   (yields)    ║   (cpu)  ║   (yields)   ║   (cpu)   ║

Core 4   ║ S4: eval ║ S4: http_post ║ S8: eval ║ S8: glob()   ║ S12: eval ║
         ║   (cpu)  ║   (yields)    ║   (cpu)  ║   (yields)   ║   (cpu)   ║
```

### Explicit Parallel

```starlark
# Sequential - 3x time
a = http_request("GET", url1)
b = http_request("GET", url2)
c = http_request("GET", url3)

# Parallel - 1x time
results = parallel([
    lambda: http_request("GET", url1),
    lambda: http_request("GET", url2),
    lambda: http_request("GET", url3),
])
```

---

## Example Scripts

### HTTP API Client

```starlark
response = http_request("GET", "https://api.github.com/users/octocat")

if response.status == 200:
    data = json_decode(response.body)
    print("User:", data["login"])
    print("Repos:", data["public_repos"])
else:
    fail("Error: " + str(response.status))
```

### Parallel Downloads

```starlark
urls = [
    "https://example.com/file1.txt",
    "https://example.com/file2.txt",
    "https://example.com/file3.txt",
]

def fetch(url):
    return http_request("GET", url).body

results = parallel([lambda u=u: fetch(u) for u in urls])

for i, content in enumerate(results):
    write_file("output_{}.txt".format(i), content)
```

### Webhook Server

```starlark
def handle_webhook(req):
    data = json_decode(req["body"])
    print("Received:", data)

    # Process webhook...
    shell("./process.sh " + data["id"])

    return {"status": "ok"}

server = http_server(8080, {
    "POST /webhook": handle_webhook,
    "GET /health": lambda r: "healthy",
})

print("Listening on :8080")
```

### Cron Job

```starlark
def cleanup():
    files = glob("/tmp/old-*")
    for f in files:
        rm(f)
    print("Cleaned up", len(files), "files")

job = cron("0 0 * * *", cleanup)  # Daily at midnight
print("Cleanup job scheduled")
```

### CI/CD Script

```starlark
print("Building...")
result = shell("cargo build --release")
if result.code != 0:
    fail("Build failed: " + result.stderr)

print("Testing...")
result = shell("cargo test")
if result.code != 0:
    fail("Tests failed: " + result.stderr)

print("Deploying...")
result = shell("docker push myapp:latest")
if result.code != 0:
    fail("Deploy failed: " + result.stderr)

# Notify Slack
http_request("POST", env("SLACK_WEBHOOK"),
    json_encode({"text": "Deployed successfully!"}),
    headers={"Content-Type": "application/json"})
```

---

## Installation

```bash
# From source
cargo install --git https://github.com/alexchoi0/blueprint --bin bp

# Verify
bp --version
```
