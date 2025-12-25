use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use indexmap::IndexMap;
use tokio::sync::{mpsc, oneshot, Mutex, RwLock};

use super::Value;

pub struct StreamIterator {
    rx: Mutex<mpsc::Receiver<Option<String>>>,
    content: Mutex<String>,
    done: Mutex<bool>,
    result: Mutex<Option<IndexMap<String, Value>>>,
}

impl StreamIterator {
    pub fn new(rx: mpsc::Receiver<Option<String>>) -> Self {
        Self {
            rx: Mutex::new(rx),
            content: Mutex::new(String::new()),
            done: Mutex::new(false),
            result: Mutex::new(None),
        }
    }

    pub async fn next(&self) -> Option<Value> {
        let mut done = self.done.lock().await;
        if *done {
            return None;
        }

        let mut rx = self.rx.lock().await;
        match rx.recv().await {
            Some(Some(chunk)) => {
                let mut content = self.content.lock().await;
                content.push_str(&chunk);
                Some(Value::String(Arc::new(chunk)))
            }
            Some(None) | None => {
                *done = true;
                None
            }
        }
    }

    pub async fn set_result(&self, result: IndexMap<String, Value>) {
        let mut r = self.result.lock().await;
        *r = Some(result);
    }

    pub fn get_attr(&self, name: &str) -> Option<Value> {
        match name {
            "content" => {
                let content = self.content.try_lock().ok()?;
                Some(Value::String(Arc::new(content.clone())))
            }
            "done" => {
                let done = self.done.try_lock().ok()?;
                Some(Value::Bool(*done))
            }
            "result" => {
                let result = self.result.try_lock().ok()?;
                match result.as_ref() {
                    Some(map) => Some(Value::Dict(Arc::new(RwLock::new(map.clone())))),
                    None => Some(Value::None),
                }
            }
            _ => None,
        }
    }
}

pub enum GeneratorMessage {
    Yielded(Value, oneshot::Sender<()>),
    Complete,
}

pub struct Generator {
    rx: Mutex<mpsc::Receiver<GeneratorMessage>>,
    done: AtomicBool,
    pub name: String,
}

impl Generator {
    pub fn new(rx: mpsc::Receiver<GeneratorMessage>, name: String) -> Self {
        Self {
            rx: Mutex::new(rx),
            done: AtomicBool::new(false),
            name,
        }
    }

    pub async fn next(&self) -> Option<Value> {
        if self.done.load(Ordering::SeqCst) {
            return None;
        }

        let mut rx = self.rx.lock().await;
        match rx.recv().await {
            Some(GeneratorMessage::Yielded(value, resume_tx)) => {
                let _ = resume_tx.send(());
                Some(value)
            }
            Some(GeneratorMessage::Complete) | None => {
                self.done.store(true, Ordering::SeqCst);
                None
            }
        }
    }

    pub fn is_done(&self) -> bool {
        self.done.load(Ordering::SeqCst)
    }
}

impl fmt::Debug for Generator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "<generator {}>", self.name)
    }
}
