# Blueprint

A high-performance Starlark script executor with implicit async I/O, built on Tokio.

Blueprint lets you write simple, synchronous-looking scripts while the runtime automatically handles async I/O operations under the hood. Perfect for automation, deployment scripts, webhooks, and data processing tasks.

## Features

- **Implicit Async I/O** - Write sync code, get async performance
- **Generators** - Lazy iteration with `yield` for memory-efficient streaming
- **Lazy map/filter** - First-class generator support: `map()`, `filter()`, `iter()` return generators
- **WebSocket Support** - Native client and server with `ws_connect()` and `ws_server()`
- **Triggers** - HTTP servers, cron jobs, and interval timers for daemon mode
- **Workspace System** - `BP.toml` for project configuration and dependency management
- **Package Manager** - Auto-install packages from GitHub with `load("@user/repo", ...)`
- **REPL Server** - Persistent eval sessions for integration with other tools
- **Concurrent Execution** - Run multiple scripts in parallel
- **Parallel Function** - Run tasks concurrently within a script with `parallel()`
- **Module System** - Import functions and values with `load()`
- **Native Functions** - File I/O, HTTP, process execution, JSON, crypto, JWT, and more
- **Full Starlark Support** - Functions, lambdas, comprehensions, f-strings
- **Fast** - Built on Rust and Tokio for maximum performance

## Installation

```bash
# From source
cargo install --git https://github.com/alexchoi0/blueprint --bin bp

# Or build locally
git clone https://github.com/alexchoi0/blueprint.git
cd blueprint
cargo build --release
cp target/release/bp /usr/local/bin/
```

## Quick Start

```bash
# Run a script
bp run script.bp

# Inline code execution
bp run -e 'print("hello")'

# Evaluate an expression
bp eval "1 + 2"

# Interactive REPL
bp repl

# Run multiple scripts concurrently
bp run "scripts/*.bp" -j 4
```

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

# Workspace
bp init                             # Create BP.toml in current directory
bp sync                             # Install dependencies from BP.toml

# Other
bp check script.bp                  # Syntax check only
```

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

### Webhook Server

```starlark
def handle_webhook(req):
    data = json_decode(req["body"])
    print("Received:", data)
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

### Generator Functions

```starlark
def squares(n):
    for i in range(n):
        yield i * i

for sq in squares(10):
    print(sq)  # Prints 0, 1, 4, 9, 16, ...
```

### Lazy Map/Filter

```starlark
# map() and filter() return generators for lazy evaluation
doubled = map(lambda x: x * 2, [1, 2, 3, 4, 5])
evens = filter(lambda x: x % 2 == 0, range(10))

# Chain operations lazily
result = filter(lambda x: x > 5, map(lambda x: x * 2, range(10)))

# Materialize when needed
print(list(result))  # [6, 8, 10, 12, 14, 16, 18]

# Convert list to generator with iter()
for item in iter([1, 2, 3]):
    print(item)
```

### WebSocket Client

```starlark
ws = ws_connect("wss://echo.websocket.org")

ws.send("Hello WebSocket!")
response = ws.recv()
print("Received:", response)

ws.close()
```

### WebSocket Server

```starlark
def handle_client(ws):
    print("Client connected")
    for msg in ws.messages:
        print("Received:", msg)
        ws.send("Echo: " + msg)
    print("Client disconnected")

server = ws_server(8765, handle_client)
print("WebSocket server running on port 8765")
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

Package location: `~/.blueprint/packages/@user/repo#version/` (or `.blueprint/packages/` in workspace)

Entry point: `lib.bp` in repository root

## Workspace (BP.toml)

Blueprint supports project workspaces with `BP.toml` for dependency management:

```bash
# Initialize a new workspace
bp init

# Install all dependencies
bp sync
```

### BP.toml Format

```toml
[workspace]
name = "my-project"
version = "0.1.0"
description = "My Blueprint project"

[dependencies]
# Simple format: "user/repo" = "version"
"user/mylib" = "v1.0.0"
"another/lib" = "main"

# Detailed format
"user/package" = { git = "https://github.com/user/package.git", tag = "v1.0.0" }

# Local path dependency
"local/lib" = { path = "./libs/mylib" }
```

### Path Resolution

When a `BP.toml` exists, `load()` paths are resolved relative to the workspace root:

```
my-project/
├── BP.toml
├── lib/
│   └── utils.bp
└── src/
    └── app.bp
```

```starlark
# In src/app.bp - paths resolve from workspace root
load("lib/utils.bp", "helper")

# Relative paths still work from current file
load("./sibling.bp", "func")
load("../other.bp", "func")
```

Packages are installed to `.blueprint/packages/` within the workspace directory.

## Triggers

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

## REPL Server

Start a REPL server for persistent sessions:

```bash
# Terminal 1: Start REPL server
bp repl --port 8888

# Terminal 2: Evaluate commands (state persists)
bp eval "x = 10" --port 8888
bp eval "y = 5" --port 8888
bp eval "x + y" --port 8888   # returns 15
bp eval "exit" --port 8888    # shutdown server
```

## Native Functions

### File Operations
```starlark
content = read_file("path/to/file")
write_file("path/to/file", "content")
append_file("path/to/file", "more content")
exists("path")              # True/False
is_file("path")             # True/False
is_dir("path")              # True/False
mkdir("new/directory")
rm("file_or_dir")
cp("src", "dst")
mv("src", "dst")
files = glob("**/*.bp")
```

### Process Execution
```starlark
result = run(["echo", "hello"])
print(result.stdout)        # "hello\n"
print(result.code)          # 0

result = shell("echo hello && pwd")
result = shell("cmd", cwd="/some/dir", env={"KEY": "value"})
```

### Environment Variables
```starlark
home = env("HOME")
path = env("MY_VAR", "default_value")
set_env("MY_VAR", "new_value")
```

### HTTP Requests
```starlark
resp = http_request("GET", "https://api.example.com/data")
print(resp.status)          # 200
print(resp.body)            # response body
print(resp.headers)         # {"content-type": "..."}

resp = http_request("POST", url, body='{"key": "value"}',
                    headers={"Content-Type": "application/json"})

download("https://example.com/file.zip", "local/file.zip")

# Streaming large responses
for chunk in http_request("GET", "https://example.com/large-file", stream=True):
    process(chunk)
```

### WebSocket
```starlark
# Client
ws = ws_connect("wss://example.com/ws", headers={"Auth": "token"})
ws.send("hello")
msg = ws.recv()              # Receive single message
for msg in ws.messages:      # Iterate all messages
    print(msg)
ws.close()

# Server
def handler(ws):
    for msg in ws.messages:
        ws.send("echo: " + msg)

server = ws_server(8765, handler, host="0.0.0.0", path="/ws")
stop(server)                 # Stop server
```

### JSON
```starlark
data = {"name": "Blueprint", "version": 1}
json_str = json_encode(data)
parsed = json_decode('{"key": "value"}')
```

### Crypto
```starlark
hash = sha256("data")                    # hex string
sig = hmac_sha256("key", "data")         # hex string
sig = hmac_sha256(key, data, key_hex=True)  # binary key
```

### JWT
```starlark
token = jwt_encode({"sub": "user"}, "secret")
payload = jwt_decode(token, "secret")
```

### Time
```starlark
start = now()               # Unix timestamp as float
sleep(0.5)                  # Sleep for 500ms
elapsed = now() - start
```

### Console
```starlark
print("Hello", "World")     # Print to stdout
eprint("Error!")            # Print to stderr
name = input("Name: ")      # Read from stdin
```

### Parallel Execution
```starlark
results = parallel([
    lambda: http_request("GET", "https://api1.com/data"),
    lambda: http_request("GET", "https://api2.com/data"),
    lambda: http_request("GET", "https://api3.com/data"),
])
```

### Lazy Iteration
```starlark
# map() and filter() return generators (lazy evaluation)
doubled = map(lambda x: x * 2, data)
evens = filter(lambda x: x % 2 == 0, data)

# Chain operations without intermediate lists
result = filter(pred, map(transform, large_data))

# Materialize with list()
items = list(result)

# iter() converts collections to generators
gen = iter([1, 2, 3])

# enumerate() returns generator when given generator
for i, item in enumerate(gen):
    print(i, item)

# Generators don't support indexing
gen[0]  # Error: use list() to materialize first
```

### Assertions
```starlark
fail("Something went wrong")
assert_true(x > 0, "x must be positive")
assert_eq(a, b, "values must match")
assert_ne(a, b)
assert_contains(list, item)
```

### Security
```starlark
clean = redact_pii(text)           # Redact PII
clean = redact_secrets(text)       # Redact secrets
approved = ask_for_approval("Deploy?")  # Interactive approval
```

## Module System

```starlark
# lib/utils.bp
PI = 3.14159

def greet(name):
    return "Hello, " + name + "!"
```

```starlark
# main.bp
load("lib/utils.bp", "greet", "PI")

print(greet("World"))    # Hello, World!
print(PI)                # 3.14159

# Aliasing
load("lib/utils.bp", say_hello="greet")
say_hello("Blueprint")
```

## Standard Library

### @bp/aws
```starlark
load("@bp/aws", "s3_upload", "s3_download", "s3_list", "s3_delete")

creds = aws_auth(
    access_key_id = env("AWS_ACCESS_KEY_ID"),
    secret_access_key = env("AWS_SECRET_ACCESS_KEY"),
    region = "us-east-1"
)

s3_upload(creds, "bucket", "key.txt", "content")
content = s3_download(creds, "bucket", "key.txt")
```

### @bp/gcp
```starlark
load("@bp/gcp", "gcs_upload", "gcs_download", "gcs_list", "gcs_delete")

creds = gcp_auth(service_account_json = read_file("sa.json"))

gcs_upload(creds, "bucket", "key.txt", "content")
content = gcs_download(creds, "bucket", "key.txt")
```

### @bp/llm
```starlark
load("@bp/llm", "agent")

# Non-streaming
response = agent("What is 2 + 2?")
response = agent(prompt="Get weather", tools=[get_weather], model="claude-sonnet-4-20250514")

# Streaming - iterate over tokens as they arrive
stream = agent("Write a poem about Rust", stream=True)
for chunk in stream:
    print(chunk, end="")
print()
print("Full response:", stream.content)
```

## Script Globals

Scripts have access to:
- `argv` - List of command-line arguments (first element is script path)
- `__file__` - Absolute path to the current script

```starlark
print("Script:", __file__)
print("Args:", argv[1:])
```

```bash
bp run script.bp -- arg1 arg2
```

## Architecture

See [ARCHITECTURE.md](ARCHITECTURE.md) for detailed documentation.

## License

MIT
