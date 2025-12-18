# Blueprint3 Architecture

## Implicit Async Starlark Runtime

Blueprint3 is a high-performance Starlark script executor with implicit async I/O.
Scripts use standard Starlark syntax—no async/await keywords—while the runtime
automatically yields to Tokio at I/O boundaries, enabling thousands of concurrent scripts.

---

## System Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              BLUEPRINT3                                      │
│                     Implicit Async Starlark Runtime                          │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  ┌─────────────┐    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐   │
│  │  script1.⭐  │    │  script2.⭐  │    │  script3.⭐  │    │  scriptN.⭐  │   │
│  └──────┬──────┘    └──────┬──────┘    └──────┬──────┘    └──────┬──────┘   │
│         │                  │                  │                  │          │
│         ▼                  ▼                  ▼                  ▼          │
│  ┌──────────────────────────────────────────────────────────────────────┐   │
│  │                        STARLARK PARSER                                │   │
│  │                    (reuse starlark-rust AST)                          │   │
│  └──────────────────────────────────────────────────────────────────────┘   │
│         │                  │                  │                  │          │
│         ▼                  ▼                  ▼                  ▼          │
│  ┌──────────────────────────────────────────────────────────────────────┐   │
│  │                      ASYNC EVALUATOR                                  │   │
│  │                                                                       │   │
│  │   ┌─────────┐  ┌─────────┐  ┌─────────┐  ┌─────────┐                 │   │
│  │   │ Task 1  │  │ Task 2  │  │ Task 3  │  │ Task N  │   Lightweight   │   │
│  │   │ (async) │  │ (async) │  │ (async) │  │ (async) │   Tokio Tasks   │   │
│  │   └────┬────┘  └────┬────┘  └────┬────┘  └────┬────┘                 │   │
│  │        │            │            │            │                       │   │
│  │        └────────────┴─────┬──────┴────────────┘                       │   │
│  │                           │                                           │   │
│  │                           ▼                                           │   │
│  │              ┌─────────────────────────┐                              │   │
│  │              │    YIELD POINTS         │                              │   │
│  │              │  (at every native I/O)  │                              │   │
│  │              └─────────────────────────┘                              │   │
│  └──────────────────────────────────────────────────────────────────────┘   │
│                              │                                              │
│                              ▼                                              │
│  ┌──────────────────────────────────────────────────────────────────────┐   │
│  │                     NATIVE FUNCTIONS                                  │   │
│  │                                                                       │   │
│  │  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐                │   │
│  │  │  File I/O    │  │  HTTP        │  │  Process     │                │   │
│  │  │  read_file   │  │  http_get    │  │  run         │                │   │
│  │  │  write_file  │  │  http_post   │  │  shell       │                │   │
│  │  │  glob        │  │  http_put    │  │  exec        │                │   │
│  │  │  exists      │  │  http_delete │  │              │                │   │
│  │  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘                │   │
│  │         │                 │                 │                         │   │
│  │         │    All async    │    under the    │    hood                 │   │
│  │         │                 │                 │                         │   │
│  │         ▼                 ▼                 ▼                         │   │
│  │  ┌──────────────────────────────────────────────────────────────┐    │   │
│  │  │                    TOKIO RUNTIME                              │    │   │
│  │  │              (multi-threaded work-stealing)                   │    │   │
│  │  │                                                               │    │   │
│  │  │   Worker 1        Worker 2        Worker 3        Worker 4    │    │   │
│  │  │   ┌──────┐        ┌──────┐        ┌──────┐        ┌──────┐   │    │   │
│  │  │   │ ████ │        │ ████ │        │ ████ │        │ ████ │   │    │   │
│  │  │   │ ████ │        │ ████ │        │ ████ │        │ ████ │   │    │   │
│  │  │   └──────┘        └──────┘        └──────┘        └──────┘   │    │   │
│  │  │                                                               │    │   │
│  │  │   Tasks migrate between workers at yield points               │    │   │
│  │  └──────────────────────────────────────────────────────────────┘    │   │
│  └──────────────────────────────────────────────────────────────────────┘   │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Execution Model: 1000 Scripts on 4 Cores

```
TIME ──────────────────────────────────────────────────────────────────────────►

Core 1   ║ S1: eval ║ S1: read_file ║ S5: eval ║ S5: http_get ║ S9: eval  ║
         ║   (cpu)  ║   (yields)    ║   (cpu)  ║   (yields)   ║   (cpu)   ║
         ╠══════════╬═══════════════╬══════════╬══════════════╬═══════════╣

Core 2   ║ S2: eval ║ S2: http_get  ║ S6: eval ║ S6: read_file║ S10: eval ║
         ║   (cpu)  ║   (yields)    ║   (cpu)  ║   (yields)   ║   (cpu)   ║
         ╠══════════╬═══════════════╬══════════╬══════════════╬═══════════╣

Core 3   ║ S3: eval ║ S3: write_file║ S7: eval ║ S7: run()    ║ S11: eval ║
         ║   (cpu)  ║   (yields)    ║   (cpu)  ║   (yields)   ║   (cpu)   ║
         ╠══════════╬═══════════════╬══════════╬══════════════╬═══════════╣

Core 4   ║ S4: eval ║ S4: http_post ║ S8: eval ║ S8: glob()   ║ S12: eval ║
         ║   (cpu)  ║   (yields)    ║   (cpu)  ║   (yields)   ║   (cpu)   ║
         ╚══════════╩═══════════════╩══════════╩══════════════╩═══════════╝

                    ▲               ▲          ▲              ▲
                    │               │          │              │
              YIELD POINT     YIELD POINT   YIELD       YIELD POINT
              (I/O starts)    (I/O done)    POINT       (I/O done)

LEGEND:
  S1-S1000 = Script tasks (lightweight, ~few KB each)
  eval     = CPU-bound Starlark evaluation
  yields   = Task yields to Tokio, another task runs
  ════════ = Task boundaries
```

### How Yield Works

```
┌─────────────────────────────────────────────────────────────────┐
│                    SCRIPT EXECUTION FLOW                         │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│   User Script                    Under the Hood                  │
│   ───────────                    ──────────────                  │
│                                                                  │
│   content = read_file("x.txt")   1. Evaluator calls native fn   │
│                      │           2. Native fn returns Future     │
│                      │           3. Evaluator awaits Future      │
│                      │           4. Tokio sees .await            │
│                      │           5. Task YIELDS ◄── other tasks  │
│                      │              run while I/O pending        │
│                      │           6. I/O completes                │
│                      │           7. Task resumes                 │
│                      ▼           8. Value returned to script     │
│   print(content)                                                 │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

---

## Parallel Function

For explicit parallelism within a single script:

```python
# Sequential (slow)
a = http_get("https://api1.com/data")
b = http_get("https://api2.com/data")
c = http_get("https://api3.com/data")

# Parallel (fast) - all three requests run concurrently
results = parallel([
    lambda: http_get("https://api1.com/data"),
    lambda: http_get("https://api2.com/data"),
    lambda: http_get("https://api3.com/data"),
])
a, b, c = results[0], results[1], results[2]
```

### Parallel Execution Flow

```
┌─────────────────────────────────────────────────────────────────┐
│                    parallel() EXECUTION                          │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│   TIME ─────────────────────────────────────────────────►        │
│                                                                  │
│   Sequential:                                                    │
│   ├── http_get(api1) ──────┤                                    │
│                             ├── http_get(api2) ──────┤          │
│                                                       ├── http_get(api3) ──────┤
│   Total: ═══════════════════════════════════════════════════════ 3x time
│                                                                  │
│   Parallel:                                                      │
│   ├── http_get(api1) ──────┤                                    │
│   ├── http_get(api2) ──────┤  (concurrent)                      │
│   ├── http_get(api3) ──────┤                                    │
│   Total: ═══════════════════ 1x time                            │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

---

## Crate Structure

```
blueprint3/
├── Cargo.toml                 # Workspace
├── ARCHITECTURE.md            # This file
│
├── crates/
│   ├── blueprint_core/        # Shared types, errors
│   │   └── src/
│   │       ├── lib.rs
│   │       ├── error.rs       # BlueprintError
│   │       └── value.rs       # Runtime values
│   │
│   ├── blueprint_parser/      # Starlark parsing
│   │   └── src/
│   │       ├── lib.rs
│   │       ├── parse.rs       # Parse to AST
│   │       └── ast.rs         # AST types (from starlark-rust)
│   │
│   ├── blueprint_eval/        # Async evaluator (THE CORE)
│   │   └── src/
│   │       ├── lib.rs
│   │       ├── eval.rs        # Main async eval loop
│   │       ├── scope.rs       # Variable scopes
│   │       ├── natives/
│   │       │   ├── mod.rs
│   │       │   ├── file.rs    # read_file, write_file, glob, exists
│   │       │   ├── http.rs    # http_get, http_post, http_put, http_delete
│   │       │   ├── process.rs # run, shell, exec
│   │       │   ├── parallel.rs# parallel() function
│   │       │   ├── console.rs # print, input
│   │       │   └── time.rs    # sleep, now
│   │       └── builtins.rs    # len, str, int, list, dict, etc.
│   │
│   └── blueprint_cli/         # CLI interface
│       └── src/
│           ├── main.rs
│           └── args.rs        # CLI argument parsing
│
└── examples/
    ├── hello.star
    ├── http_fetch.star
    └── parallel_downloads.star
```

---

## Native Functions Reference

### File Operations

| Function | Signature | Description |
|----------|-----------|-------------|
| `read_file` | `read_file(path: str) -> str` | Read file contents |
| `write_file` | `write_file(path: str, content: str) -> None` | Write to file |
| `append_file` | `append_file(path: str, content: str) -> None` | Append to file |
| `exists` | `exists(path: str) -> bool` | Check if path exists |
| `is_file` | `is_file(path: str) -> bool` | Check if path is file |
| `is_dir` | `is_dir(path: str) -> bool` | Check if path is directory |
| `glob` | `glob(pattern: str) -> list[str]` | Find files matching pattern |
| `mkdir` | `mkdir(path: str) -> None` | Create directory |
| `rm` | `rm(path: str) -> None` | Remove file or directory |
| `cp` | `cp(src: str, dst: str) -> None` | Copy file |
| `mv` | `mv(src: str, dst: str) -> None` | Move file |

### HTTP Operations

| Function | Signature | Description |
|----------|-----------|-------------|
| `http_get` | `http_get(url: str, headers: dict = {}) -> Response` | HTTP GET |
| `http_post` | `http_post(url: str, body: str, headers: dict = {}) -> Response` | HTTP POST |
| `http_put` | `http_put(url: str, body: str, headers: dict = {}) -> Response` | HTTP PUT |
| `http_delete` | `http_delete(url: str, headers: dict = {}) -> Response` | HTTP DELETE |
| `download` | `download(url: str, path: str) -> None` | Download file |

### Process Operations

| Function | Signature | Description |
|----------|-----------|-------------|
| `run` | `run(args: list[str]) -> Result` | Run command |
| `shell` | `shell(cmd: str) -> Result` | Run shell command |
| `exec` | `exec(args: list[str]) -> None` | Replace process |
| `env` | `env(name: str, default: str = "") -> str` | Get env var |
| `set_env` | `set_env(name: str, value: str) -> None` | Set env var |

### Concurrency

| Function | Signature | Description |
|----------|-----------|-------------|
| `parallel` | `parallel(fns: list[fn]) -> list[any]` | Run functions in parallel |
| `sleep` | `sleep(seconds: float) -> None` | Async sleep |

### Console

| Function | Signature | Description |
|----------|-----------|-------------|
| `print` | `print(*args) -> None` | Print to stdout |
| `eprint` | `eprint(*args) -> None` | Print to stderr |
| `input` | `input(prompt: str = "") -> str` | Read from stdin |

### Time

| Function | Signature | Description |
|----------|-----------|-------------|
| `now` | `now() -> float` | Current Unix timestamp |
| `sleep` | `sleep(seconds: float) -> None` | Async sleep |

### Control Flow

| Function | Signature | Description |
|----------|-----------|-------------|
| `fail` | `fail(msg: str) -> Never` | Fail with error |
| `assert` | `assert(cond: bool, msg: str = "") -> None` | Assert condition |

---

## Example Scripts

### hello.star
```python
print("Hello, Blueprint3!")

name = input("What's your name? ")
print("Nice to meet you,", name)
```

### file_ops.star
```python
content = read_file("input.txt")

if "error" in content:
    print("Found error in file!")
    write_file("errors.log", content)
else:
    print("File looks good")
```

### http_fetch.star
```python
response = http_get("https://api.github.com/users/octocat")

if response.status == 200:
    data = json.decode(response.body)
    print("User:", data["login"])
    print("Repos:", data["public_repos"])
else:
    print("Error:", response.status)
```

### parallel_downloads.star
```python
urls = [
    "https://example.com/file1.txt",
    "https://example.com/file2.txt",
    "https://example.com/file3.txt",
]

def fetch(url):
    return http_get(url).body

results = parallel([lambda u=u: fetch(u) for u in urls])

for i, content in enumerate(results):
    write_file("output_{}.txt".format(i), content)

print("Downloaded", len(results), "files")
```

### deploy.star
```python
config = read_file("config.json")
settings = json.decode(config)

print("Deploying to", settings["environment"])

result = run(["docker", "build", "-t", settings["image"], "."])
if result.code != 0:
    fail("Docker build failed: " + result.stderr)

result = run(["docker", "push", settings["image"]])
if result.code != 0:
    fail("Docker push failed: " + result.stderr)

http_post(
    settings["webhook_url"],
    json.encode({"status": "deployed", "image": settings["image"]}),
    headers={"Content-Type": "application/json"}
)

print("Deployment complete!")
```

---

## CLI Usage

```bash
# Run a single script
blueprint3 run script.star

# Run multiple scripts in parallel
blueprint3 run *.star

# Limit concurrency
blueprint3 run -j 100 scripts/*.star

# Pass arguments to script
blueprint3 run script.star -- arg1 arg2

# Verbose output
blueprint3 run -v script.star

# Dry run (parse only)
blueprint3 check script.star
```

---

## Implementation Phases

### Phase 1: Core Infrastructure
- [ ] Workspace setup
- [ ] blueprint_core: Error types, Value enum
- [ ] blueprint_parser: Wrap starlark-rust parser

### Phase 2: Async Evaluator
- [ ] blueprint_eval: Basic expression evaluation
- [ ] Scope management
- [ ] Control flow (if/else, for, while)
- [ ] Function definitions and calls

### Phase 3: Native Functions
- [ ] File I/O (read_file, write_file, etc.)
- [ ] HTTP client (http_get, http_post, etc.)
- [ ] Process execution (run, shell)
- [ ] parallel() function

### Phase 4: CLI
- [ ] Argument parsing
- [ ] Script execution
- [ ] Concurrent multi-script execution
- [ ] Error reporting

### Phase 5: Polish
- [ ] Better error messages with source locations
- [ ] REPL mode
- [ ] Script caching
- [ ] Performance optimization
