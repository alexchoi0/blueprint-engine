use blueprint_engine_core::{BlueprintError, Result};
use reqwest::multipart::{Form, Part};
use serde::{Deserialize, Serialize};
use std::io::Write;
use std::path::{Path, PathBuf};

const DEFAULT_REGISTRY: &str = "https://blueprint.fleetnet.engineering";
const CREDENTIALS_FILE: &str = ".blueprint/credentials.toml";

#[derive(Debug, Serialize, Deserialize, Default)]
struct Credentials {
    #[serde(default)]
    tokens: std::collections::HashMap<String, String>,
}

impl Credentials {
    fn load() -> Self {
        let path = Self::path();
        if !path.exists() {
            return Self::default();
        }
        std::fs::read_to_string(&path)
            .ok()
            .and_then(|s| toml::from_str(&s).ok())
            .unwrap_or_default()
    }

    fn save(&self) -> Result<()> {
        let path = Self::path();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(|e| BlueprintError::IoError {
                path: parent.to_string_lossy().to_string(),
                message: e.to_string(),
            })?;
        }
        let content = toml::to_string_pretty(self).map_err(|e| BlueprintError::IoError {
            path: path.to_string_lossy().to_string(),
            message: e.to_string(),
        })?;
        std::fs::write(&path, content).map_err(|e| BlueprintError::IoError {
            path: path.to_string_lossy().to_string(),
            message: e.to_string(),
        })?;
        Ok(())
    }

    fn path() -> PathBuf {
        dirs::home_dir()
            .unwrap_or_else(|| PathBuf::from("."))
            .join(CREDENTIALS_FILE)
    }

    fn get_token(&self, registry: &str) -> Option<&String> {
        self.tokens.get(registry)
    }

    fn set_token(&mut self, registry: &str, token: &str) {
        self.tokens.insert(registry.to_string(), token.to_string());
    }

    fn remove_token(&mut self, registry: &str) {
        self.tokens.remove(registry);
    }
}

#[derive(Debug, Deserialize)]
struct LoginResponse {
    token: String,
    user: UserInfo,
}

#[derive(Debug, Deserialize)]
struct UserInfo {
    email: String,
    name: Option<String>,
}

#[derive(Debug, Deserialize)]
struct PublishResponse {
    version: String,
    checksum: String,
}

#[derive(Debug, Deserialize)]
struct ErrorResponse {
    error: String,
}

fn get_registry(registry: Option<&str>) -> String {
    registry
        .map(|s| s.to_string())
        .or_else(|| std::env::var("BP_REGISTRY").ok())
        .unwrap_or_else(|| DEFAULT_REGISTRY.to_string())
}

fn get_token(registry: &str, token: Option<&str>) -> Option<String> {
    token
        .map(|s| s.to_string())
        .or_else(|| std::env::var("BP_TOKEN").ok())
        .or_else(|| Credentials::load().get_token(registry).cloned())
}

pub async fn login(registry: Option<&str>) -> Result<()> {
    let registry = get_registry(registry);

    println!("Logging in to {}...", registry);

    print!("Email: ");
    std::io::stdout().flush().ok();
    let mut email = String::new();
    std::io::stdin().read_line(&mut email).map_err(|e| BlueprintError::IoError {
        path: "stdin".into(),
        message: e.to_string(),
    })?;
    let email = email.trim().to_string();

    print!("Password: ");
    std::io::stdout().flush().ok();
    let password = rpassword_read().unwrap_or_default();
    println!();

    let client = reqwest::Client::new();
    let response = client
        .post(format!("{}/api/v1/login", registry))
        .json(&serde_json::json!({
            "email": email,
            "password": password
        }))
        .send()
        .await
        .map_err(|e| BlueprintError::IoError {
            path: registry.clone(),
            message: e.to_string(),
        })?;

    if !response.status().is_success() {
        let error: ErrorResponse = response.json().await.map_err(|e| BlueprintError::IoError {
            path: registry.clone(),
            message: e.to_string(),
        })?;
        return Err(BlueprintError::IoError {
            path: registry,
            message: error.error,
        });
    }

    let login: LoginResponse = response.json().await.map_err(|e| BlueprintError::IoError {
        path: registry.clone(),
        message: e.to_string(),
    })?;

    let mut creds = Credentials::load();
    creds.set_token(&registry, &login.token);
    creds.save()?;

    println!("Logged in as {}", login.user.email);
    if let Some(name) = login.user.name {
        println!("Welcome, {}!", name);
    }

    Ok(())
}

pub async fn logout() -> Result<()> {
    let registry = get_registry(None);
    let mut creds = Credentials::load();
    creds.remove_token(&registry);
    creds.save()?;
    println!("Logged out from {}", registry);
    Ok(())
}

pub async fn whoami() -> Result<()> {
    let registry = get_registry(None);
    let token = get_token(&registry, None);

    match token {
        Some(t) => {
            println!("Logged in to: {}", registry);
            let display_len = 8.min(t.len());
            println!("Token: {}...", &t[..display_len]);
        }
        None => {
            println!("Not logged in to {}", registry);
        }
    }
    Ok(())
}

pub async fn publish(
    path: Option<PathBuf>,
    registry: Option<&str>,
    token: Option<&str>,
    skip_confirm: bool,
) -> Result<()> {
    let registry = get_registry(registry);
    let token = get_token(&registry, token).ok_or_else(|| BlueprintError::IoError {
        path: registry.clone(),
        message: "Not logged in. Run 'bp login' or set BP_TOKEN environment variable.".into(),
    })?;

    let package_dir = path.unwrap_or_else(|| PathBuf::from("."));
    let manifest_path = package_dir.join("blueprint.toml");

    if !manifest_path.exists() {
        return Err(BlueprintError::IoError {
            path: manifest_path.to_string_lossy().to_string(),
            message: "blueprint.toml not found. Is this a Blueprint package?".into(),
        });
    }

    let manifest_content = std::fs::read_to_string(&manifest_path).map_err(|e| BlueprintError::IoError {
        path: manifest_path.to_string_lossy().to_string(),
        message: e.to_string(),
    })?;

    let manifest: toml::Value = toml::from_str(&manifest_content).map_err(|e| BlueprintError::IoError {
        path: manifest_path.to_string_lossy().to_string(),
        message: format!("Invalid manifest: {}", e),
    })?;

    let package = manifest.get("package").ok_or_else(|| BlueprintError::IoError {
        path: manifest_path.to_string_lossy().to_string(),
        message: "Missing [package] section in blueprint.toml".into(),
    })?;

    let name = package
        .get("name")
        .and_then(|v| v.as_str())
        .ok_or_else(|| BlueprintError::IoError {
            path: manifest_path.to_string_lossy().to_string(),
            message: "Missing package.name in blueprint.toml".into(),
        })?;

    let version = package
        .get("version")
        .and_then(|v| v.as_str())
        .ok_or_else(|| BlueprintError::IoError {
            path: manifest_path.to_string_lossy().to_string(),
            message: "Missing package.version in blueprint.toml".into(),
        })?;

    if !skip_confirm {
        println!("Publishing {} v{} to {}", name, version, registry);
        print!("Continue? [y/N] ");
        std::io::stdout().flush().ok();
        let mut confirm = String::new();
        std::io::stdin().read_line(&mut confirm).ok();
        if !confirm.trim().eq_ignore_ascii_case("y") {
            println!("Aborted.");
            return Ok(());
        }
    }

    println!("Packaging {}...", name);
    let package_data = create_package_tarball(&package_dir)?;

    let namespace = extract_namespace(&token)?;

    println!("Uploading to {}/api/v1/packages/{}/{}...", registry, namespace, name);

    let form = Form::new()
        .part("manifest", Part::text(manifest_content.clone()).file_name("blueprint.toml"))
        .part(
            "package",
            Part::bytes(package_data)
                .file_name(format!("{}-{}.tar.gz", name, version))
                .mime_str("application/gzip")
                .map_err(|e| BlueprintError::IoError {
                    path: "multipart".into(),
                    message: e.to_string(),
                })?,
        );

    let client = reqwest::Client::new();
    let response = client
        .post(format!("{}/api/v1/packages/{}/{}", registry, namespace, name))
        .header("Authorization", format!("token {}", token))
        .multipart(form)
        .send()
        .await
        .map_err(|e| BlueprintError::IoError {
            path: registry.clone(),
            message: e.to_string(),
        })?;

    if !response.status().is_success() {
        let status = response.status();
        let error: ErrorResponse = response.json().await.unwrap_or(ErrorResponse {
            error: format!("HTTP {}", status),
        });
        return Err(BlueprintError::IoError {
            path: registry,
            message: format!("Failed to publish: {}", error.error),
        });
    }

    let result: PublishResponse = response.json().await.map_err(|e| BlueprintError::IoError {
        path: registry.clone(),
        message: e.to_string(),
    })?;

    println!("Published {} v{}", name, result.version);
    println!("Checksum: {}", result.checksum);

    Ok(())
}

fn extract_namespace(token: &str) -> Result<String> {
    if token.starts_with("bp_") {
        return Ok("user".to_string());
    }
    Ok("user".to_string())
}

fn create_package_tarball(dir: &Path) -> Result<Vec<u8>> {
    use std::io::Cursor;

    let mut archive_data = Vec::new();
    {
        let cursor = Cursor::new(&mut archive_data);
        let encoder = flate2::write::GzEncoder::new(cursor, flate2::Compression::default());
        let mut archive = tar::Builder::new(encoder);

        for entry in walkdir::WalkDir::new(dir)
            .into_iter()
            .filter_entry(|e| !is_hidden(e) && !is_excluded(e))
        {
            let entry = entry.map_err(|e| BlueprintError::IoError {
                path: dir.to_string_lossy().to_string(),
                message: e.to_string(),
            })?;

            let path = entry.path();
            if path.is_file() {
                let rel_path = path.strip_prefix(dir).unwrap_or(path);
                archive
                    .append_path_with_name(path, rel_path)
                    .map_err(|e| BlueprintError::IoError {
                        path: path.to_string_lossy().to_string(),
                        message: e.to_string(),
                    })?;
            }
        }

        archive.finish().map_err(|e| BlueprintError::IoError {
            path: dir.to_string_lossy().to_string(),
            message: e.to_string(),
        })?;
    }

    Ok(archive_data)
}

fn is_hidden(entry: &walkdir::DirEntry) -> bool {
    entry
        .file_name()
        .to_str()
        .map(|s| s.starts_with('.'))
        .unwrap_or(false)
}

fn is_excluded(entry: &walkdir::DirEntry) -> bool {
    let name = entry.file_name().to_string_lossy();
    matches!(
        name.as_ref(),
        "target" | "node_modules" | "__pycache__" | ".git" | "vendor"
    )
}

fn rpassword_read() -> Option<String> {
    use std::io::BufRead;

    #[cfg(unix)]
    {
        use std::os::unix::io::AsRawFd;
        let stdin = std::io::stdin();
        let fd = stdin.as_raw_fd();

        let mut old_termios = std::mem::MaybeUninit::uninit();
        unsafe {
            if libc::tcgetattr(fd, old_termios.as_mut_ptr()) != 0 {
                return None;
            }
            let mut new_termios = old_termios.assume_init();
            new_termios.c_lflag &= !libc::ECHO;
            libc::tcsetattr(fd, libc::TCSANOW, &new_termios);
        }

        let mut password = String::new();
        stdin.lock().read_line(&mut password).ok()?;

        unsafe {
            libc::tcsetattr(fd, libc::TCSANOW, old_termios.as_ptr());
        }

        Some(password.trim().to_string())
    }

    #[cfg(not(unix))]
    {
        let stdin = std::io::stdin();
        let mut password = String::new();
        stdin.lock().read_line(&mut password).ok()?;
        Some(password.trim().to_string())
    }
}
