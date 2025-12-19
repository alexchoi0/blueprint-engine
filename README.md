# Blueprint

A high-performance Starlark script executor with implicit async I/O, built on Tokio.

Blueprint lets you write simple, synchronous-looking scripts while the runtime automatically handles async I/O operations under the hood. Perfect for automation, deployment scripts, and data processing tasks.

## Features

- **Implicit Async I/O** - Write sync code, get async performance
- **Concurrent Execution** - Run multiple scripts in parallel with `bp run "*.star"`
- **Parallel Function** - Run tasks concurrently within a script with `parallel()`
- **Module System** - Import functions and values from other files with `load()`
- **Frozen Module Cache** - Loaded modules execute once and are shared across parallel tasks
- **Native Functions** - File I/O, HTTP requests, process execution, JSON, and more
- **Full Starlark Support** - Functions, lambdas, comprehensions, f-strings
- **Fast** - Built on Rust and Tokio for maximum performance

## Installation

```bash
git clone https://github.com/alexchoi0/blueprint.git
cd blueprint
cargo build --release
cp target/release/bp /usr/local/bin/
```

## Quick Start

```bash
# Evaluate an expression
bp eval "1 + 2"
# => 3

# Run a script
bp run script.star

# Run multiple scripts concurrently
bp run "scripts/*.star"

# Limit concurrency
bp run "scripts/*.star" -j 4

# Check syntax without running
bp check script.star
```

## Example Script

```python
# deploy.star
print("Starting deployment...")

config = json.decode(read_file("config.json"))
print(f"Deploying {config['app']} to {config['env']}")

result = shell("npm run build")
if result.code != 0:
    fail("Build failed: " + result.stderr)

for file in glob("dist/*"):
    print(f"Uploading {file}...")

print("Deployment complete!")
```

Run it:
```bash
bp run deploy.star
```

## Native Functions

### File Operations
```python
content = read_file("path/to/file")
write_file("path/to/file", "content")
append_file("path/to/file", "more content")
exists("path/to/file")      # True/False
is_file("path")             # True/False
is_dir("path")              # True/False
mkdir("new/directory")
rm("file_or_dir")
cp("src", "dst")
mv("src", "dst")
files = glob("**/*.star")
```

### Process Execution
```python
result = run(["echo", "hello"])
print(result.stdout)        # "hello\n"
print(result.code)          # 0

result = shell("echo hello && pwd")
print(result.stdout)

# With options
result = run(["cmd"], cwd="/some/dir", env={"KEY": "value"})
```

### Environment Variables
```python
home = env("HOME")
path = env("MY_VAR", "default_value")
set_env("MY_VAR", "new_value")
```

### HTTP Requests
```python
resp = http_request("GET", "https://api.example.com/data")
print(resp.status)          # 200
print(resp.body)            # response body
print(resp.headers)         # {"content-type": "..."}

resp = http_request("POST", "https://api.example.com/data",
                    body='{"key": "value"}',
                    headers={"Content-Type": "application/json"})

resp = http_request("PUT", url, body=data)
resp = http_request("DELETE", url)
resp = http_request("PATCH", url, body=patch_data)

download("https://example.com/file.zip", "local/file.zip")
```

### JSON
```python
data = {"name": "Blueprint", "version": 1}
json_str = json.encode(data)
json_pretty = json.encode(data, indent=2)

parsed = json.decode('{"key": "value"}')
```

### Time
```python
start = now()               # Unix timestamp as float
sleep(0.5)                  # Sleep for 500ms
elapsed = now() - start
```

### Console
```python
print("Hello", "World")     # Print to stdout
eprint("Error!")            # Print to stderr
name = input("Name: ")      # Read from stdin
```

### Parallel Execution
```python
# Run multiple I/O operations concurrently
results = parallel([
    lambda: http_request("GET", "https://api1.com/data"),
    lambda: http_request("GET", "https://api2.com/data"),
    lambda: http_request("GET", "https://api3.com/data"),
])

# Results are returned in order
resp1, resp2, resp3 = results[0], results[1], results[2]

# Works with any functions
def fetch_user(id):
    return http_request("GET", f"https://api.com/users/{id}")

users = parallel([
    lambda: fetch_user(1),
    lambda: fetch_user(2),
    lambda: fetch_user(3),
])
```

### Builtins
```python
len([1, 2, 3])              # 3
range(5)                    # [0, 1, 2, 3, 4]
range(1, 5)                 # [1, 2, 3, 4]
sum([1, 2, 3])              # 6
min([3, 1, 2])              # 1
max([3, 1, 2])              # 3
sorted([3, 1, 2])           # [1, 2, 3]
reversed([1, 2, 3])         # [3, 2, 1]
enumerate(["a", "b"])       # [(0, "a"), (1, "b")]
zip([1, 2], ["a", "b"])     # [(1, "a"), (2, "b")]
str(123)                    # "123"
int("42")                   # 42
float("3.14")               # 3.14
bool(1)                     # True
list((1, 2, 3))             # [1, 2, 3]
type(42)                    # "int"
```

### String Methods
```python
s = "Hello, World!"
s.upper()                   # "HELLO, WORLD!"
s.lower()                   # "hello, world!"
s.strip()                   # Remove whitespace
s.split(",")                # ["Hello", " World!"]
s.replace("World", "BP")    # "Hello, BP!"
s.startswith("Hello")       # True
s.endswith("!")             # True
s.find("World")             # 7
", ".join(["a", "b", "c"])  # "a, b, c"
"Hi {}!".format("there")    # "Hi there!"
```

### Control Flow
```python
fail("Something went wrong")    # Exit with error
assert(x > 0, "x must be positive")
```

## Module System

Import functions and values from other `.star` files using `load()`:

```python
# lib/utils.star
PI = 3.14159

def greet(name):
    return "Hello, " + name + "!"

def add(a, b):
    return a + b
```

```python
# main.star
load("lib/utils.star", "greet", "add", "PI")

print(greet("World"))    # Hello, World!
print(add(2, 3))         # 5
print(PI)                # 3.14159
```

### Aliasing Imports

Rename symbols when importing:

```python
load("lib/utils.star", say_hello="greet", sum="add")

say_hello("Blueprint")   # Uses greet() as say_hello()
sum(1, 2)                # Uses add() as sum()
```

### Relative Paths

```python
load("utils.star", "func")           # Same directory
load("lib/utils.star", "func")       # Subdirectory
load("../common/utils.star", "func") # Parent directory
```

### Nested Modules

Modules can load other modules:

```python
# lib/advanced.star
load("math.star", "square")

def square_sum(a, b):
    return square(a) + square(b)
```

### Frozen Module Cache

Loaded modules are cached and execute only once, even when multiple scripts or parallel tasks load the same module. This is ideal for shared resources like database connections or HTTP clients:

```python
# infra.star - Executes ONCE, shared across all loaders
print("Initializing...")  # Prints once

servers = start_servers(port=8080)
db = connect_database("postgres://localhost/mydb")
```

```python
# worker_a.star
load("infra.star", "servers", "db")

def run():
    return servers.request("/api/users")
```

```python
# worker_b.star
load("infra.star", "servers", "db")

def run():
    return db.query("SELECT * FROM users")
```

```python
# main.star
load("worker_a.star", run_a="run")
load("worker_b.star", run_b="run")

# Both workers share the same servers/db instances
results = parallel([
    lambda: run_a(),
    lambda: run_b(),
])
```

When `main.star` runs:
1. `worker_a.star` loads `infra.star` → executes `start_servers()` and `connect_database()`
2. `worker_b.star` loads `infra.star` → returns cached exports (no re-execution)
3. Both workers share the same `servers` and `db` instances

## Script Globals

Scripts have access to:
- `argv` - List of command-line arguments (first element is script path)
- `__file__` - Absolute path to the current script

```python
print("Script:", __file__)
print("Args:", argv[1:])
```

```bash
bp run script.star -- arg1 arg2
```

## Concurrent Execution

Blueprint runs multiple scripts concurrently on a single Tokio runtime:

```bash
# Run all .star files in parallel
bp run "scripts/*.star"

# Limit to 4 concurrent scripts
bp run "scripts/*.star" -j 4

# Verbose output shows progress
bp run "scripts/*.star" -v
```

## Project Structure

```
blueprint/
├── Cargo.toml              # Workspace configuration
├── crates/
│   ├── blueprint_cli/      # CLI binary (bp)
│   ├── blueprint_core/     # Core types (Value, Error)
│   ├── blueprint_eval/     # Async evaluator
│   └── blueprint_parser/   # Starlark parser wrapper
└── examples/               # Example scripts
```

## Building

```bash
cargo build              # Debug build
cargo build --release    # Release build
cargo test               # Run tests
cargo run --bin bp -- run "examples/*.star"
```

## License

MIT
