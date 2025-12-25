use crate::{BlueprintError, Result};
use std::path::PathBuf;

#[derive(Debug, Clone)]
pub struct PackageSpec {
    pub user: String,
    pub repo: String,
    pub version: String,
}

impl PackageSpec {
    pub fn parse(package: &str) -> Result<Self> {
        let path = package.strip_prefix('@').unwrap_or(package);

        let (repo_path, version) = if let Some(idx) = path.find('#') {
            (&path[..idx], Some(&path[idx + 1..]))
        } else {
            (path, None)
        };

        let parts: Vec<&str> = repo_path.splitn(2, '/').collect();
        if parts.len() != 2 {
            return Err(BlueprintError::ArgumentError {
                message: "Invalid package format. Expected @user/repo or @user/repo#version".into(),
            });
        }

        Ok(Self {
            user: parts[0].to_string(),
            repo: parts[1].to_string(),
            version: version.unwrap_or("main").to_string(),
        })
    }

    pub fn display_name(&self) -> String {
        format!("@{}/{}#{}", self.user, self.repo, self.version)
    }

    pub fn dir_name(&self) -> String {
        format!("{}#{}", self.repo, self.version)
    }
}

pub fn find_workspace_root() -> Option<PathBuf> {
    find_workspace_root_from(std::env::current_dir().ok()?)
}

pub fn find_workspace_root_from(start: PathBuf) -> Option<PathBuf> {
    let mut current = start;
    loop {
        let bp_toml = current.join("BP.toml");
        if bp_toml.exists() {
            return Some(current);
        }
        if !current.pop() {
            break;
        }
    }
    None
}

pub fn get_packages_dir() -> PathBuf {
    if let Some(workspace) = find_workspace_root() {
        workspace.join(".blueprint").join("packages")
    } else {
        let home = std::env::var("HOME").unwrap_or_else(|_| ".".to_string());
        PathBuf::from(&home).join(".blueprint").join("packages")
    }
}

pub fn get_packages_dir_from(start: Option<PathBuf>) -> PathBuf {
    let workspace = start.and_then(find_workspace_root_from);
    if let Some(ws) = workspace {
        ws.join(".blueprint").join("packages")
    } else {
        let home = std::env::var("HOME").unwrap_or_else(|_| ".".to_string());
        PathBuf::from(&home).join(".blueprint").join("packages")
    }
}

const DEFAULT_REGISTRY: &str = "https://blueprint.fleetnet.engineering";

pub fn get_registry_url() -> String {
    std::env::var("BP_REGISTRY").unwrap_or_else(|_| DEFAULT_REGISTRY.to_string())
}

pub fn fetch_package(spec: &PackageSpec, dest: &PathBuf) -> Result<()> {
    let registry = get_registry_url();
    let download_url = format!(
        "{}/api/v1/packages/{}/{}/{}/download",
        registry, spec.user, spec.repo, spec.version
    );

    let output = std::process::Command::new("curl")
        .args(["-fsSL", "-o", "-", &download_url])
        .output()
        .map_err(|e| BlueprintError::IoError {
            path: download_url.clone(),
            message: e.to_string(),
        })?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(BlueprintError::IoError {
            path: download_url,
            message: format!("Failed to download package: {}", stderr.trim()),
        });
    }

    if let Some(parent) = dest.parent() {
        std::fs::create_dir_all(parent).map_err(|e| BlueprintError::IoError {
            path: parent.to_string_lossy().to_string(),
            message: e.to_string(),
        })?;
    }

    std::fs::create_dir_all(dest).map_err(|e| BlueprintError::IoError {
        path: dest.to_string_lossy().to_string(),
        message: e.to_string(),
    })?;

    let tar_output = std::process::Command::new("tar")
        .args(["-xzf", "-", "-C"])
        .arg(dest)
        .stdin(std::process::Stdio::piped())
        .spawn()
        .and_then(|mut child| {
            use std::io::Write;
            if let Some(stdin) = child.stdin.as_mut() {
                stdin.write_all(&output.stdout)?;
            }
            child.wait()
        })
        .map_err(|e| BlueprintError::IoError {
            path: dest.to_string_lossy().to_string(),
            message: format!("Failed to extract package: {}", e),
        })?;

    if !tar_output.success() {
        std::fs::remove_dir_all(dest).ok();
        return Err(BlueprintError::IoError {
            path: dest.to_string_lossy().to_string(),
            message: "Failed to extract package tarball".into(),
        });
    }

    Ok(())
}
