use blueprint_engine_core::{
    BlueprintError, PackageSpec, Result, fetch_package, find_workspace_root, get_packages_dir,
};

pub async fn install_package(package: &str) -> Result<()> {
    let spec = PackageSpec::parse(package)?;
    let packages_dir = get_packages_dir();
    let package_dir = packages_dir.join(&spec.user).join(spec.dir_name());

    if package_dir.exists() {
        println!("Package {} is already installed", spec.display_name());
        return Ok(());
    }

    println!("Installing {}...", spec.display_name());
    fetch_package(&spec, &package_dir)?;
    println!("Installed {}", spec.display_name());

    Ok(())
}

pub async fn uninstall_package(package: &str) -> Result<()> {
    let spec = PackageSpec::parse(package)?;
    let packages_dir = get_packages_dir();
    let user_dir = packages_dir.join(&spec.user);

    if spec.version != "main" {
        let package_dir = user_dir.join(spec.dir_name());
        if package_dir.exists() {
            std::fs::remove_dir_all(&package_dir).map_err(|e| BlueprintError::IoError {
                path: package_dir.to_string_lossy().to_string(),
                message: e.to_string(),
            })?;
            println!("Uninstalled {}", spec.display_name());
        } else {
            println!("Package {} is not installed", spec.display_name());
        }
    } else {
        if !user_dir.exists() {
            println!("No packages from @{}/{} are installed", spec.user, spec.repo);
            return Ok(());
        }
        let mut found = false;
        for entry in std::fs::read_dir(&user_dir).map_err(|e| BlueprintError::IoError {
            path: user_dir.to_string_lossy().to_string(),
            message: e.to_string(),
        })? {
            let entry = entry.map_err(|e| BlueprintError::IoError {
                path: user_dir.to_string_lossy().to_string(),
                message: e.to_string(),
            })?;
            let name = entry.file_name().to_string_lossy().to_string();
            if name.starts_with(&format!("{}#", spec.repo)) {
                std::fs::remove_dir_all(entry.path()).map_err(|e| BlueprintError::IoError {
                    path: entry.path().to_string_lossy().to_string(),
                    message: e.to_string(),
                })?;
                println!("Uninstalled @{}/{}", spec.user, name);
                found = true;
            }
        }
        if !found {
            println!("No packages from @{}/{} are installed", spec.user, spec.repo);
        }
    }

    Ok(())
}

pub async fn list_packages() -> Result<()> {
    let packages_dir = get_packages_dir();

    if let Some(workspace) = find_workspace_root() {
        println!("Packages in workspace: {}", workspace.display());
    }

    if !packages_dir.exists() {
        println!("No packages installed");
        return Ok(());
    }

    let mut packages = Vec::new();

    for user_entry in std::fs::read_dir(&packages_dir).map_err(|e| BlueprintError::IoError {
        path: packages_dir.to_string_lossy().to_string(),
        message: e.to_string(),
    })? {
        let user_entry = user_entry.map_err(|e| BlueprintError::IoError {
            path: packages_dir.to_string_lossy().to_string(),
            message: e.to_string(),
        })?;

        if !user_entry.path().is_dir() {
            continue;
        }

        let user = user_entry.file_name().to_string_lossy().to_string();

        for pkg_entry in std::fs::read_dir(user_entry.path()).map_err(|e| BlueprintError::IoError {
            path: user_entry.path().to_string_lossy().to_string(),
            message: e.to_string(),
        })? {
            let pkg_entry = pkg_entry.map_err(|e| BlueprintError::IoError {
                path: user_entry.path().to_string_lossy().to_string(),
                message: e.to_string(),
            })?;

            if pkg_entry.path().is_dir() {
                let name = pkg_entry.file_name().to_string_lossy().to_string();
                packages.push(format!("@{}/{}", user, name));
            }
        }
    }

    if packages.is_empty() {
        println!("No packages installed");
    } else {
        packages.sort();
        for pkg in packages {
            println!("{}", pkg);
        }
    }

    Ok(())
}

pub async fn init_workspace() -> Result<()> {
    let current_dir = std::env::current_dir().map_err(|e| BlueprintError::IoError {
        path: ".".into(),
        message: e.to_string(),
    })?;
    crate::workspace::init_workspace(&current_dir)
}

pub async fn sync_workspace() -> Result<()> {
    let current_dir = std::env::current_dir().map_err(|e| BlueprintError::IoError {
        path: ".".into(),
        message: e.to_string(),
    })?;

    let workspace = crate::workspace::Workspace::find(&current_dir).ok_or_else(|| {
        BlueprintError::IoError {
            path: current_dir.to_string_lossy().to_string(),
            message: "No BP.toml found in current directory or any parent".into(),
        }
    })?;

    if workspace.config.dependencies.is_empty() {
        println!("No dependencies to install");
        return Ok(());
    }

    println!("Installing dependencies from BP.toml...");
    workspace.install_all()?;
    println!("Done!");
    Ok(())
}
