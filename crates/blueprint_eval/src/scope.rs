use blueprint_core::{GeneratorMessage, Value};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{mpsc, RwLock};

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ScopeKind {
    Global,
    Function,
    Generator,
    Loop,
    Block,
}

pub struct Scope {
    variables: RwLock<HashMap<String, Value>>,
    parent: Option<Arc<Scope>>,
    kind: ScopeKind,
    yield_tx: Option<mpsc::Sender<GeneratorMessage>>,
}

impl std::fmt::Debug for Scope {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Scope")
            .field("kind", &self.kind)
            .field("has_parent", &self.parent.is_some())
            .finish()
    }
}

impl Scope {
    pub fn new_global() -> Arc<Self> {
        Arc::new(Self {
            variables: RwLock::new(HashMap::new()),
            parent: None,
            kind: ScopeKind::Global,
            yield_tx: None,
        })
    }

    pub fn new_child(parent: Arc<Scope>, kind: ScopeKind) -> Arc<Self> {
        Arc::new(Self {
            variables: RwLock::new(HashMap::new()),
            parent: Some(parent),
            kind,
            yield_tx: None,
        })
    }

    pub fn new_generator(parent: Arc<Scope>, yield_tx: mpsc::Sender<GeneratorMessage>) -> Arc<Self> {
        Arc::new(Self {
            variables: RwLock::new(HashMap::new()),
            parent: Some(parent),
            kind: ScopeKind::Generator,
            yield_tx: Some(yield_tx),
        })
    }

    pub fn get_yield_tx(&self) -> Option<mpsc::Sender<GeneratorMessage>> {
        if let Some(ref tx) = self.yield_tx {
            return Some(tx.clone());
        }
        if let Some(ref parent) = self.parent {
            return parent.get_yield_tx();
        }
        None
    }

    #[async_recursion::async_recursion]
    pub async fn get(&self, name: &str) -> Option<Value> {
        if let Some(value) = self.variables.read().await.get(name) {
            return Some(value.clone());
        }

        if let Some(ref parent) = self.parent {
            return parent.get(name).await;
        }

        None
    }

    pub fn get_blocking(&self, name: &str) -> Option<Value> {
        if let Some(value) = self.variables.blocking_read().get(name) {
            return Some(value.clone());
        }

        if let Some(ref parent) = self.parent {
            return parent.get_blocking(name);
        }

        None
    }

    pub async fn set(&self, name: &str, value: Value) {
        match self.kind {
            ScopeKind::Function | ScopeKind::Global | ScopeKind::Generator => {
                self.variables.write().await.insert(name.to_string(), value);
            }
            ScopeKind::Loop | ScopeKind::Block => {
                if self.exists_in_parents(name).await {
                    self.set_in_chain(name, value).await;
                } else {
                    self.variables.write().await.insert(name.to_string(), value);
                }
            }
        }
    }

    pub async fn define(&self, name: &str, value: Value) {
        self.variables.write().await.insert(name.to_string(), value);
    }

    #[async_recursion::async_recursion]
    async fn exists_in_parents(&self, name: &str) -> bool {
        if let Some(ref parent) = self.parent {
            if parent.variables.read().await.contains_key(name) {
                return true;
            }
            return parent.exists_in_parents(name).await;
        }
        false
    }

    #[async_recursion::async_recursion]
    async fn set_in_chain(&self, name: &str, value: Value) {
        if self.variables.read().await.contains_key(name) {
            self.variables.write().await.insert(name.to_string(), value);
            return;
        }
        if let Some(ref parent) = self.parent {
            parent.set_in_chain(name, value).await;
        }
    }

    pub fn kind(&self) -> ScopeKind {
        self.kind
    }

    pub fn parent(&self) -> Option<&Arc<Scope>> {
        self.parent.as_ref()
    }

    pub async fn all_variables(&self) -> HashMap<String, Value> {
        let mut vars = HashMap::new();
        self.collect_variables(&mut vars).await;
        vars
    }

    pub async fn exports(&self) -> HashMap<String, Value> {
        self.variables.read().await.clone()
    }

    #[async_recursion::async_recursion]
    async fn collect_variables(&self, vars: &mut HashMap<String, Value>) {
        if let Some(ref parent) = self.parent {
            parent.collect_variables(vars).await;
        }
        for (k, v) in self.variables.read().await.iter() {
            vars.insert(k.clone(), v.clone());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_global_scope() {
        let scope = Scope::new_global();
        scope.set("x", Value::Int(42)).await;
        assert_eq!(scope.get("x").await, Some(Value::Int(42)));
    }

    #[tokio::test]
    async fn test_child_scope() {
        let global = Scope::new_global();
        global.set("x", Value::Int(1)).await;

        let child = Scope::new_child(global.clone(), ScopeKind::Function);
        child.set("y", Value::Int(2)).await;

        assert_eq!(child.get("x").await, Some(Value::Int(1)));
        assert_eq!(child.get("y").await, Some(Value::Int(2)));
        assert_eq!(global.get("y").await, None);
    }

    #[tokio::test]
    async fn test_loop_scope_updates_parent() {
        let func = Scope::new_child(Scope::new_global(), ScopeKind::Function);
        func.set("i", Value::Int(0)).await;

        let loop_scope = Scope::new_child(func.clone(), ScopeKind::Loop);
        loop_scope.set("i", Value::Int(1)).await;

        assert_eq!(func.get("i").await, Some(Value::Int(1)));
    }
}
