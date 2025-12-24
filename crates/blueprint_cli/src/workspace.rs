use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};

use blueprint_engine_core::{BlueprintError, Permissions, Result};

#[derive(Debug, Deserialize, Serialize, Default)]
pub struct BpToml {
    #[serde(default)]
    pub workspace: WorkspaceConfig,
    #[serde(default)]
    pub permissions: Permissions,
    #[serde(default)]
    pub dependencies: HashMap<String, Dependency>,
}

#[derive(Debug, Deserialize, Serialize, Default)]
pub struct WorkspaceConfig {
    pub name: Option<String>,
    pub version: Option<String>,
    pub description: Option<String>,
    pub authors: Option<Vec<String>>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(untagged)]
pub enum Dependency {
    Simple(String),
    Detailed(DetailedDependency),
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct DetailedDependency {
    pub git: Option<String>,
    pub version: Option<String>,
    pub branch: Option<String>,
    pub tag: Option<String>,
    pub path: Option<String>,
}

impl Dependency {
    pub fn version(&self) -> &str {
        match self {
            Dependency::Simple(v) => v,
            Dependency::Detailed(d) => {
                d.tag.as_deref()
                    .or(d.branch.as_deref())
                    .or(d.version.as_deref())
                    .unwrap_or("main")
            }
        }
    }

    pub fn git_url(&self, name: &str) -> Option<String> {
        match self {
            Dependency::Simple(_) => {
                let parts: Vec<&str> = name.splitn(2, '/').collect();
                if parts.len() == 2 {
                    Some(format!("https://github.com/{}/{}.git", parts[0], parts[1]))
                } else {
                    None
                }
            }
            Dependency::Detailed(d) => d.git.clone(),
        }
    }

    pub fn local_path(&self) -> Option<&str> {
        match self {
            Dependency::Simple(_) => None,
            Dependency::Detailed(d) => d.path.as_deref(),
        }
    }
}

pub struct Workspace {
    pub root: PathBuf,
    pub config: BpToml,
    pub packages_dir: PathBuf,
}

impl Workspace {
    pub fn find(start_dir: &Path) -> Option<Self> {
        let mut current = start_dir.to_path_buf();
        loop {
            let bp_toml = current.join("BP.toml");
            if bp_toml.exists() {
                if let Ok(workspace) = Self::load(&current) {
                    return Some(workspace);
                }
            }
            if !current.pop() {
                break;
            }
        }
        None
    }

    pub fn load(root: &Path) -> Result<Self> {
        let bp_toml_path = root.join("BP.toml");
        let content = std::fs::read_to_string(&bp_toml_path).map_err(|e| BlueprintError::IoError {
            path: bp_toml_path.to_string_lossy().to_string(),
            message: e.to_string(),
        })?;

        let config: BpToml = toml::from_str(&content).map_err(|e| BlueprintError::IoError {
            path: bp_toml_path.to_string_lossy().to_string(),
            message: format!("Failed to parse BP.toml: {}", e),
        })?;

        let packages_dir = root.join(".blueprint").join("packages");

        Ok(Self {
            root: root.to_path_buf(),
            config,
            packages_dir,
        })
    }

    pub fn ensure_packages_dir(&self) -> Result<()> {
        if !self.packages_dir.exists() {
            std::fs::create_dir_all(&self.packages_dir).map_err(|e| BlueprintError::IoError {
                path: self.packages_dir.to_string_lossy().to_string(),
                message: e.to_string(),
            })?;
        }
        Ok(())
    }

    pub fn package_path(&self, name: &str, version: &str) -> PathBuf {
        let parts: Vec<&str> = name.splitn(2, '/').collect();
        if parts.len() == 2 {
            self.packages_dir
                .join(parts[0])
                .join(format!("{}#{}", parts[1], version))
        } else {
            self.packages_dir.join(format!("{}#{}", name, version))
        }
    }

    #[allow(dead_code)]
    pub fn is_installed(&self, name: &str, version: &str) -> bool {
        self.package_path(name, version).exists()
    }

    pub fn install_dependency(&self, name: &str, dep: &Dependency) -> Result<()> {
        let version = dep.version();
        let pkg_path = self.package_path(name, version);

        if pkg_path.exists() {
            return Ok(());
        }

        if let Some(local_path) = dep.local_path() {
            let source = self.root.join(local_path);
            if !source.exists() {
                return Err(BlueprintError::IoError {
                    path: source.to_string_lossy().to_string(),
                    message: "Local dependency path does not exist".into(),
                });
            }
            return Ok(());
        }

        if let Some(git_url) = dep.git_url(name) {
            self.ensure_packages_dir()?;

            if let Some(parent) = pkg_path.parent() {
                std::fs::create_dir_all(parent).map_err(|e| BlueprintError::IoError {
                    path: parent.to_string_lossy().to_string(),
                    message: e.to_string(),
                })?;
            }

            println!("Installing {}#{}...", name, version);

            let output = std::process::Command::new("git")
                .args(["clone", "--depth", "1", "--branch", version, &git_url])
                .arg(&pkg_path)
                .output()
                .map_err(|e| BlueprintError::IoError {
                    path: git_url.clone(),
                    message: e.to_string(),
                })?;

            if !output.status.success() {
                std::fs::remove_dir_all(&pkg_path).ok();
                let stderr = String::from_utf8_lossy(&output.stderr);
                return Err(BlueprintError::IoError {
                    path: git_url,
                    message: format!("Failed to clone: {}", stderr.trim()),
                });
            }

            std::fs::remove_dir_all(pkg_path.join(".git")).ok();
            println!("Installed {}#{}", name, version);
        }

        Ok(())
    }

    pub fn install_all(&self) -> Result<()> {
        for (name, dep) in &self.config.dependencies {
            self.install_dependency(name, dep)?;
        }
        Ok(())
    }

    #[allow(dead_code)]
    pub fn resolve_package(&self, module_path: &str) -> Option<PathBuf> {
        let path = module_path.strip_prefix('@').unwrap_or(module_path);

        let (repo_path, explicit_version) = if let Some(idx) = path.find('#') {
            (&path[..idx], Some(&path[idx + 1..]))
        } else {
            (path, None)
        };

        if let Some(dep) = self.config.dependencies.get(repo_path) {
            if let Some(local_path) = dep.local_path() {
                let lib_path = self.root.join(local_path).join("lib.bp");
                if lib_path.exists() {
                    return Some(lib_path);
                }
            }

            let version = explicit_version.unwrap_or_else(|| dep.version());
            let pkg_path = self.package_path(repo_path, version);
            let lib_path = pkg_path.join("lib.bp");
            if lib_path.exists() {
                return Some(lib_path);
            }
        }

        None
    }
}

pub fn init_workspace(path: &Path) -> Result<()> {
    let bp_toml_path = path.join("BP.toml");

    if bp_toml_path.exists() {
        return Err(BlueprintError::IoError {
            path: bp_toml_path.to_string_lossy().to_string(),
            message: "BP.toml already exists".into(),
        });
    }

    let dir_name = path
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("my-project");

    let config = BpToml {
        workspace: WorkspaceConfig {
            name: Some(dir_name.to_string()),
            version: Some("0.1.0".to_string()),
            description: None,
            authors: None,
        },
        permissions: Permissions::default(),
        dependencies: HashMap::new(),
    };

    let content = toml::to_string_pretty(&config).map_err(|e| BlueprintError::IoError {
        path: bp_toml_path.to_string_lossy().to_string(),
        message: e.to_string(),
    })?;

    std::fs::write(&bp_toml_path, content).map_err(|e| BlueprintError::IoError {
        path: bp_toml_path.to_string_lossy().to_string(),
        message: e.to_string(),
    })?;

    std::fs::create_dir_all(path.join(".blueprint").join("packages")).ok();

    println!("Created BP.toml in {}", path.display());
    Ok(())
}
