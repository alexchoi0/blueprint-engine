use std::collections::HashSet;
use std::io::{self, Write};
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::task_local;

use crate::{BlueprintError, PermissionCheck, Permissions, Result};

task_local! {
    static PERMISSIONS: Arc<Permissions>;
    static PROMPT_STATE: Arc<PromptState>;
}

pub struct PromptState {
    session_allowed: RwLock<HashSet<String>>,
    session_denied: RwLock<HashSet<String>>,
    interactive: bool,
}

impl PromptState {
    pub fn new(interactive: bool) -> Self {
        Self {
            session_allowed: RwLock::new(HashSet::new()),
            session_denied: RwLock::new(HashSet::new()),
            interactive,
        }
    }
}

impl Default for PromptState {
    fn default() -> Self {
        Self::new(true)
    }
}

pub fn with_permissions<F, R>(permissions: Arc<Permissions>, f: F) -> R
where
    F: FnOnce() -> R,
{
    let prompt_state = Arc::new(PromptState::default());
    PERMISSIONS.sync_scope(permissions, || {
        PROMPT_STATE.sync_scope(prompt_state, f)
    })
}

pub async fn with_permissions_async<F, Fut, R>(permissions: Arc<Permissions>, f: F) -> R
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = R>,
{
    let prompt_state = Arc::new(PromptState::default());
    PERMISSIONS
        .scope(permissions, async {
            PROMPT_STATE.scope(prompt_state, f()).await
        })
        .await
}

pub async fn with_permissions_and_prompt<F, Fut, R>(
    permissions: Arc<Permissions>,
    prompt_state: Arc<PromptState>,
    f: F,
) -> R
where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = R>,
{
    PERMISSIONS
        .scope(permissions, async {
            PROMPT_STATE.scope(prompt_state, f()).await
        })
        .await
}

pub fn get_permissions() -> Option<Arc<Permissions>> {
    PERMISSIONS.try_with(|p| p.clone()).ok()
}

fn get_prompt_state() -> Option<Arc<PromptState>> {
    PROMPT_STATE.try_with(|p| p.clone()).ok()
}

async fn handle_permission_check(
    check: PermissionCheck,
    operation: &str,
    resource: Option<&str>,
) -> Result<()> {
    match check {
        PermissionCheck::Allow => Ok(()),
        PermissionCheck::Deny => {
            let resource_str = resource.unwrap_or("");
            Err(BlueprintError::PermissionDenied {
                operation: operation.into(),
                resource: resource_str.into(),
                hint: format!(
                    "Add '{}:{}' to permissions.allow in BP.toml",
                    operation,
                    if resource_str.is_empty() { "*" } else { resource_str }
                ),
            })
        }
        PermissionCheck::Ask => {
            let key = match resource {
                Some(r) => format!("{}:{}", operation, r),
                None => operation.to_string(),
            };

            if let Some(state) = get_prompt_state() {
                if state.session_allowed.read().await.contains(&key) {
                    return Ok(());
                }
                if state.session_denied.read().await.contains(&key) {
                    return Err(BlueprintError::PermissionDenied {
                        operation: operation.into(),
                        resource: resource.unwrap_or("").into(),
                        hint: "Permission was denied earlier in this session".into(),
                    });
                }

                if state.interactive {
                    let allowed = prompt_user(operation, resource).await?;
                    if allowed {
                        state.session_allowed.write().await.insert(key);
                        return Ok(());
                    } else {
                        state.session_denied.write().await.insert(key);
                        return Err(BlueprintError::PermissionDenied {
                            operation: operation.into(),
                            resource: resource.unwrap_or("").into(),
                            hint: "Permission denied by user".into(),
                        });
                    }
                }
            }

            Err(BlueprintError::PermissionDenied {
                operation: operation.into(),
                resource: resource.unwrap_or("").into(),
                hint: format!(
                    "Add '{}:{}' to permissions.allow in BP.toml (or run interactively to be prompted)",
                    operation,
                    resource.unwrap_or("*")
                ),
            })
        }
    }
}

async fn prompt_user(operation: &str, resource: Option<&str>) -> Result<bool> {
    let resource_display = resource.unwrap_or("");

    eprintln!();
    eprintln!("┌─────────────────────────────────────────────────────────────────┐");
    eprintln!("│ Permission Request                                              │");
    eprintln!("├─────────────────────────────────────────────────────────────────┤");
    eprintln!("│ Operation: {:<52} │", operation);
    if !resource_display.is_empty() {
        let truncated = if resource_display.len() > 52 {
            format!("...{}", &resource_display[resource_display.len()-49..])
        } else {
            resource_display.to_string()
        };
        eprintln!("│ Resource:  {:<52} │", truncated);
    }
    eprintln!("├─────────────────────────────────────────────────────────────────┤");
    eprintln!("│ [y] Allow   [n] Deny   [Y] Allow all similar   [N] Deny all    │");
    eprintln!("└─────────────────────────────────────────────────────────────────┘");
    eprint!("Choice: ");
    io::stderr().flush().ok();

    let mut input = String::new();

    tokio::task::spawn_blocking(move || {
        io::stdin().read_line(&mut input).ok();
        input.trim().to_lowercase()
    })
    .await
    .map(|response| {
        matches!(response.as_str(), "y" | "yes" | "")
    })
    .map_err(|e| BlueprintError::IoError {
        path: "stdin".into(),
        message: e.to_string(),
    })
}

pub async fn check_fs_read(path: &str) -> Result<()> {
    match get_permissions() {
        None => Ok(()),
        Some(p) => {
            let check = p.check_fs_read(path);
            handle_permission_check(check, "fs.read", Some(path)).await
        }
    }
}

pub async fn check_fs_write(path: &str) -> Result<()> {
    match get_permissions() {
        None => Ok(()),
        Some(p) => {
            let check = p.check_fs_write(path);
            handle_permission_check(check, "fs.write", Some(path)).await
        }
    }
}

pub async fn check_fs_delete(path: &str) -> Result<()> {
    match get_permissions() {
        None => Ok(()),
        Some(p) => {
            let check = p.check_fs_delete(path);
            handle_permission_check(check, "fs.delete", Some(path)).await
        }
    }
}

pub async fn check_http(url: &str) -> Result<()> {
    match get_permissions() {
        None => Ok(()),
        Some(p) => {
            let check = p.check_http(url);
            handle_permission_check(check, "net.http", Some(url)).await
        }
    }
}

pub async fn check_ws(url: &str) -> Result<()> {
    match get_permissions() {
        None => Ok(()),
        Some(p) => {
            let check = p.check_ws(url);
            handle_permission_check(check, "net.ws", Some(url)).await
        }
    }
}

pub async fn check_process_run(binary: &str) -> Result<()> {
    match get_permissions() {
        None => Ok(()),
        Some(p) => {
            let check = p.check_process_run(binary);
            handle_permission_check(check, "process.run", Some(binary)).await
        }
    }
}

pub async fn check_process_shell() -> Result<()> {
    match get_permissions() {
        None => Ok(()),
        Some(p) => {
            let check = p.check_process_shell();
            handle_permission_check(check, "process.shell", None).await
        }
    }
}

pub async fn check_env_read(var: &str) -> Result<()> {
    match get_permissions() {
        None => Ok(()),
        Some(p) => {
            let check = p.check_env_read(var);
            handle_permission_check(check, "env.read", Some(var)).await
        }
    }
}

pub async fn check_env_write() -> Result<()> {
    match get_permissions() {
        None => Ok(()),
        Some(p) => {
            let check = p.check_env_write();
            handle_permission_check(check, "env.write", None).await
        }
    }
}
