# Blueprint Engine Refactoring Plan

## Current State Analysis

**Codebase Size**: ~13,600 LOC across 50 Rust files
**Test Coverage**: ~15-20% (needs improvement)
**Architecture**: Generally solid, but inconsistent patterns

### Key Issues Identified

| Priority | Issue | Impact | Effort |
|----------|-------|--------|--------|
| HIGH | Inconsistent native function registration | High | Medium |
| HIGH | Duplicate argument validation (50+ instances) | High | Low |
| HIGH | Missing test coverage | High | High |
| MEDIUM | Evaluator god object (67 methods) | Medium | High |
| MEDIUM | Duplicate module binding logic | Medium | Medium |
| MEDIUM | Magic string builtins in checker | Medium | Medium |
| MEDIUM | Blocking git operations in package.rs | Medium | Low |
| LOW | Parser crate too thin (103 LOC) | Low | Low |

---

## Phase 1: Quick Wins (Low Effort, High Impact)

### 1.1 Extract Argument Validation Helpers

**Problem**: 50+ instances of duplicate argument validation across native functions:
```rust
if args.len() != 1 {
    return Err(BlueprintError::ArgumentError {
        message: format!("func() takes exactly 1 argument ({} given)", args.len()),
    });
}
```

**Solution**: Add helpers to `blueprint-engine-core/src/validation.rs`:

```rust
pub fn require_args(name: &str, args: &[Value], count: usize) -> Result<()> {
    if args.len() != count {
        return Err(BlueprintError::ArgumentError {
            message: format!(
                "{}() takes exactly {} argument(s) ({} given)",
                name, count, args.len()
            ),
        });
    }
    Ok(())
}

pub fn require_args_range(name: &str, args: &[Value], min: usize, max: usize) -> Result<()> {
    if args.len() < min || args.len() > max {
        return Err(BlueprintError::ArgumentError {
            message: format!(
                "{}() takes {}-{} arguments ({} given)",
                name, min, max, args.len()
            ),
        });
    }
    Ok(())
}

pub fn require_string(name: &str, arg: &Value, arg_name: &str) -> Result<String> {
    arg.as_string().map_err(|_| BlueprintError::TypeError {
        expected: "string".into(),
        actual: arg.type_name().into(),
    })
}

pub fn require_int(name: &str, arg: &Value, arg_name: &str) -> Result<i64> {
    arg.as_int().map_err(|_| BlueprintError::TypeError {
        expected: "int".into(),
        actual: arg.type_name().into(),
    })
}
```

**Files to update**:
- `crates/blueprint-engine-core/src/lib.rs` - export validation module
- `crates/blueprint-engine-eval/src/natives/file.rs` - 15 instances
- `crates/blueprint-engine-eval/src/natives/http.rs` - 2 instances
- `crates/blueprint-engine-eval/src/natives/json.rs` - 3 instances
- `crates/blueprint-engine-eval/src/natives/builtins/types.rs` - 12 instances
- All other natives/*.rs files

**Estimated reduction**: ~300 lines of duplicate code

---

### 1.2 Fix Blocking Operations

**Problem**: `crates/blueprint-engine-core/src/package.rs` uses `std::process::Command` (blocking) inside async context.

**Solution**: Use `tokio::process::Command`:

```rust
// Before
let output = std::process::Command::new("git")
    .args(["clone", "--depth", "1", url, target])
    .output()?;

// After
let output = tokio::process::Command::new("git")
    .args(["clone", "--depth", "1", url, target])
    .output()
    .await?;
```

**Files to update**:
- `crates/blueprint-engine-core/src/package.rs` lines 81-112

---

### 1.3 Fix Unused Code Warnings

**Current warnings in blueprint-engine-eval**:
```
warning: unused import: `builtins::call_func`
warning: unused variable: `status` in triggers.rs:636
warning: methods `get_function` and `module_names` are never used
```

**Solution**: Remove or prefix with underscore as appropriate.

---

## Phase 2: Unify Native Function Registration

### 2.1 Current Problem

Three different registration patterns exist:

1. **Direct registration** (builtins/mod.rs):
   ```rust
   evaluator.register_native(NativeFunction::new("len", introspection::len));
   ```

2. **Registry pattern** (natives/mod.rs):
   ```rust
   registry.register_module("json", json::get_functions());
   ```

3. **Hybrid** (builtins registers process/time/triggers BOTH globally AND in registry)

### 2.2 Proposed Unified Pattern

**All natives use registry pattern**. Builtins become a special module that's auto-imported:

```rust
// natives/mod.rs
pub fn build_registry() -> NativeModuleRegistry {
    let mut registry = NativeModuleRegistry::new();

    // Core builtins - always available without load()
    registry.register_module("__builtins__", builtins::get_functions());

    // Standard library - require load("@bp/module", "*")
    registry.register_module("json", json::get_functions());
    registry.register_module("http", http::get_functions());
    registry.register_module("file", file::get_functions());
    // ... etc

    registry
}

// Evaluator automatically imports __builtins__ into every scope
impl Evaluator {
    fn register_builtins(&mut self) {
        let builtins = self.native_registry.get_module("__builtins__").unwrap();
        for (name, func) in builtins {
            self.natives.insert(name.clone(), func.clone());
        }
    }
}
```

**Files to update**:
- `crates/blueprint-engine-eval/src/natives/mod.rs`
- `crates/blueprint-engine-eval/src/natives/builtins/mod.rs`
- `crates/blueprint-engine-eval/src/eval/mod.rs`

### 2.3 Remove Double Registration

**Problem**: process/time/triggers registered both globally and in registry.

**Solution**: Only register in registry. If needed globally, use the unified pattern above.

---

## Phase 3: DRY Up Module Binding

### 3.1 Current Problem

Three nearly-identical functions in `eval/mod.rs` (lines 302-430):
- `bind_evaluator_module()`
- `bind_native_module()`
- `bind_load_args()`

All handle the same cases:
- Empty args → import as dict
- `*` → import all
- `__module__` → alias
- Named imports

### 3.2 Proposed Solution

Extract common trait and implementation:

```rust
trait ModuleExports {
    fn get(&self, name: &str) -> Option<Value>;
    fn iter(&self) -> impl Iterator<Item = (&str, Value)>;
}

impl ModuleExports for HashMap<String, Arc<NativeFunction>> {
    fn get(&self, name: &str) -> Option<Value> {
        self.get(name).map(|f| Value::NativeFunction(f.clone()))
    }
    fn iter(&self) -> impl Iterator<Item = (&str, Value)> {
        self.iter().map(|(k, v)| (k.as_str(), Value::NativeFunction(v.clone())))
    }
}

impl ModuleExports for HashMap<String, Value> {
    fn get(&self, name: &str) -> Option<Value> {
        self.get(name).cloned()
    }
    fn iter(&self) -> impl Iterator<Item = (&str, Value)> {
        self.iter().map(|(k, v)| (k.as_str(), v.clone()))
    }
}

async fn bind_module_exports<E: ModuleExports>(
    load: &LoadP,
    exports: &E,
    scope: Arc<Scope>,
    module_name: &str,
) -> Result<Value> {
    // Single implementation for all cases
}
```

**Estimated reduction**: ~100 lines of duplicate code

---

## Phase 4: Split Evaluator God Object

### 4.1 Current Problem

`Evaluator` struct has 67+ methods handling:
- Module loading and caching
- Expression evaluation
- Statement evaluation
- Function creation
- Constant expression evaluation
- Native function management
- File path resolution

### 4.2 Proposed Split

```
Evaluator (facade)
├── ModuleLoader
│   ├── resolve_module_path()
│   ├── resolve_stdlib_path()
│   ├── resolve_package_path()
│   ├── eval_load()
│   └── bind_module_exports()
│
├── ExprEvaluator
│   ├── eval_expr()
│   ├── eval_call()
│   ├── eval_literal()
│   └── eval_list_comprehension()
│
├── StmtEvaluator
│   ├── eval_stmt()
│   ├── eval_if()
│   ├── eval_for()
│   └── eval_def()
│
└── FunctionFactory
    ├── create_user_function()
    ├── create_lambda_function()
    └── convert_params()
```

**Implementation approach**: Start by extracting `ModuleLoader` as it's most self-contained.

---

## Phase 5: Replace Magic Strings

### 5.1 Checker Builtins

**Problem**: `checker.rs` lines 26-80 has hardcoded list of 50+ builtin names.

**Solution**: Generate from registry:

```rust
impl Checker {
    pub fn new(evaluator: &Evaluator) -> Self {
        let builtins: HashSet<String> = evaluator
            .native_registry
            .get_module("__builtins__")
            .map(|m| m.keys().cloned().collect())
            .unwrap_or_default();

        Self { builtins, ..Default::default() }
    }
}
```

### 5.2 Module Type Enum

**Problem**: String comparisons for module types:
```rust
if module_path.starts_with("@bp/") { ... }
if module_path.starts_with("@") { ... }
```

**Solution**:
```rust
enum ModuleRef {
    Native { module: String },           // @bp/json
    Package { spec: PackageSpec },        // @user/repo#version
    Relative { path: PathBuf },           // ./foo.bp
    Workspace { path: PathBuf },          // lib/utils.bp
}

impl ModuleRef {
    fn parse(path: &str) -> Result<Self> {
        if let Some(native) = path.strip_prefix("@bp/") {
            Ok(ModuleRef::Native { module: native.to_string() })
        } else if path.starts_with('@') {
            Ok(ModuleRef::Package { spec: PackageSpec::parse(path)? })
        } else if path.starts_with("./") || path.starts_with("../") {
            Ok(ModuleRef::Relative { path: path.into() })
        } else {
            Ok(ModuleRef::Workspace { path: path.into() })
        }
    }
}
```

---

## Phase 6: Improve Test Coverage

### 6.1 Current State

- `permissions.rs`: ~200 lines of tests (good)
- `scope.rs`: 3 basic tests
- `parser/lib.rs`: 4 tests
- Native functions: 0 tests
- Evaluator: 0 tests
- Error handling: 0 tests

### 6.2 Test Plan

**Priority 1: Native functions**
```rust
// tests/natives/test_json.rs
#[tokio::test]
async fn test_json_parse() {
    let result = json::parse(vec![Value::String("{\"a\": 1}".into())], HashMap::new()).await;
    assert!(result.is_ok());
    // ... verify structure
}

#[tokio::test]
async fn test_json_parse_invalid() {
    let result = json::parse(vec![Value::String("not json".into())], HashMap::new()).await;
    assert!(matches!(result, Err(BlueprintError::ValueError { .. })));
}
```

**Priority 2: Evaluator**
```rust
#[tokio::test]
async fn test_eval_simple_expression() {
    let evaluator = Evaluator::new();
    let module = parse("test", "1 + 2").unwrap();
    let scope = Scope::new_global();
    let result = evaluator.eval(&module, scope).await;
    assert_eq!(result, Ok(Value::Int(3)));
}
```

**Priority 3: Module loading**
```rust
#[tokio::test]
async fn test_load_native_module() {
    let evaluator = Evaluator::new();
    let module = parse("test", "load('@bp/json', 'parse')").unwrap();
    let scope = Scope::new_global();
    evaluator.eval(&module, scope.clone()).await.unwrap();
    assert!(scope.get("parse").await.is_some());
}
```

**Target**: 80%+ coverage

---

## Phase 7: Optional - Merge Parser Crate

### 7.1 Rationale

`blueprint-engine-parser` is only 103 lines and just wraps external parser:
```rust
pub fn parse(filename: &str, source: &str) -> Result<AstModule, BlueprintError> {
    // 10 lines of actual code
}
```

### 7.2 Decision

**Keep separate IF**:
- Planning to add preprocessing
- Need different parser backends
- Want to version parser separately

**Merge into core IF**:
- Parser will stay a thin wrapper
- Simplifies dependency graph
- One less crate to maintain

---

## Implementation Timeline

### Week 1: Quick Wins
- [ ] Extract argument validation helpers (1.1)
- [ ] Fix blocking operations (1.2)
- [ ] Fix unused code warnings (1.3)

### Week 2: Registration Unification
- [ ] Unify native function registration (2.1-2.3)

### Week 3: DRY Refactoring
- [ ] DRY up module binding (3.1-3.2)
- [ ] Replace magic strings (5.1-5.2)

### Week 4: Tests
- [ ] Add native function tests
- [ ] Add evaluator tests
- [ ] Add module loading tests

### Future: Major Refactoring
- [ ] Split Evaluator (Phase 4) - do when adding significant new features
- [ ] Merge parser crate - evaluate based on roadmap

---

## Success Metrics

| Metric | Before | Target |
|--------|--------|--------|
| Test coverage | 15-20% | 80%+ |
| Duplicate validation blocks | 50+ | 0 |
| Clippy warnings | 3 | 0 |
| Native registration patterns | 3 | 1 |
| Module binding implementations | 3 | 1 |
| Evaluator methods | 67 | 30 (after split) |

---

## Files Reference

**Most Changed Files**:
- `crates/blueprint-engine-core/src/lib.rs`
- `crates/blueprint-engine-core/src/validation.rs` (new)
- `crates/blueprint-engine-eval/src/natives/mod.rs`
- `crates/blueprint-engine-eval/src/natives/builtins/mod.rs`
- `crates/blueprint-engine-eval/src/eval/mod.rs`
- All `crates/blueprint-engine-eval/src/natives/*.rs` files

**New Test Files**:
- `crates/blueprint-engine-eval/tests/natives/*.rs`
- `crates/blueprint-engine-eval/tests/eval/*.rs`
- `crates/blueprint-engine-eval/tests/module/*.rs`
