use serde::{Deserialize, Serialize};
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlueprintManifest {
    pub package: PackageSection,
    #[serde(default)]
    pub dependencies: HashMap<String, DependencySpec>,
    #[serde(default)]
    pub dev_dependencies: HashMap<String, DependencySpec>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PackageSection {
    pub name: String,
    pub version: String,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub authors: Vec<String>,
    #[serde(default)]
    pub license: Option<String>,
    #[serde(default)]
    pub repository: Option<String>,
    #[serde(default)]
    pub homepage: Option<String>,
    #[serde(default)]
    pub documentation: Option<String>,
    #[serde(default)]
    pub keywords: Vec<String>,
    #[serde(default)]
    pub categories: Vec<String>,
    #[serde(default)]
    pub readme: Option<String>,
    #[serde(default)]
    pub include: Vec<String>,
    #[serde(default)]
    pub exclude: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum DependencySpec {
    Simple(String),
    Detailed(DetailedDependency),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DetailedDependency {
    pub version: Option<String>,
    pub path: Option<String>,
    pub git: Option<String>,
    pub branch: Option<String>,
    pub tag: Option<String>,
    pub rev: Option<String>,
    #[serde(default)]
    pub optional: bool,
}

impl BlueprintManifest {
    pub fn parse(content: &str) -> Result<Self, toml::de::Error> {
        toml::from_str(content)
    }

    pub fn validate(&self) -> Result<(), ManifestError> {
        if self.package.name.is_empty() {
            return Err(ManifestError::MissingField("package.name".into()));
        }

        if !is_valid_package_name(&self.package.name) {
            return Err(ManifestError::InvalidPackageName(self.package.name.clone()));
        }

        if self.package.version.is_empty() {
            return Err(ManifestError::MissingField("package.version".into()));
        }

        semver::Version::parse(&self.package.version)
            .map_err(|_| ManifestError::InvalidVersion(self.package.version.clone()))?;

        for keyword in &self.package.keywords {
            if keyword.len() > 20 {
                return Err(ManifestError::KeywordTooLong(keyword.clone()));
            }
        }

        if self.package.keywords.len() > 5 {
            return Err(ManifestError::TooManyKeywords);
        }

        for (name, spec) in &self.dependencies {
            validate_dependency(name, spec)?;
        }

        for (name, spec) in &self.dev_dependencies {
            validate_dependency(name, spec)?;
        }

        Ok(())
    }
}

fn is_valid_package_name(name: &str) -> bool {
    if name.is_empty() || name.len() > 64 {
        return false;
    }

    let mut chars = name.chars().peekable();

    if let Some(first) = chars.peek() {
        if !first.is_ascii_lowercase() {
            return false;
        }
    }

    for c in chars {
        if !c.is_ascii_lowercase() && !c.is_ascii_digit() && c != '-' && c != '_' {
            return false;
        }
    }

    !name.starts_with('-') && !name.ends_with('-')
}

fn validate_dependency(name: &str, spec: &DependencySpec) -> Result<(), ManifestError> {
    if !is_valid_package_name(name) {
        return Err(ManifestError::InvalidDependencyName(name.to_string()));
    }

    match spec {
        DependencySpec::Simple(version) => {
            validate_version_req(version)?;
        }
        DependencySpec::Detailed(detailed) => {
            if let Some(version) = &detailed.version {
                validate_version_req(version)?;
            }

            let sources = [
                detailed.version.is_some() && detailed.path.is_none() && detailed.git.is_none(),
                detailed.path.is_some(),
                detailed.git.is_some(),
            ];

            let source_count = sources.iter().filter(|&&b| b).count();
            if source_count == 0 && detailed.version.is_none() && detailed.path.is_none() && detailed.git.is_none() {
                return Err(ManifestError::MissingDependencySource(name.to_string()));
            }
        }
    }

    Ok(())
}

fn validate_version_req(version: &str) -> Result<(), ManifestError> {
    semver::VersionReq::parse(version)
        .map_err(|_| ManifestError::InvalidVersionReq(version.to_string()))?;
    Ok(())
}

#[derive(Debug, thiserror::Error)]
pub enum ManifestError {
    #[error("Missing required field: {0}")]
    MissingField(String),

    #[error("Invalid package name: {0}. Names must start with a lowercase letter, contain only lowercase letters, digits, hyphens, and underscores, and be at most 64 characters")]
    InvalidPackageName(String),

    #[error("Invalid version: {0}. Must be valid semver")]
    InvalidVersion(String),

    #[error("Invalid version requirement: {0}")]
    InvalidVersionReq(String),

    #[error("Keyword too long: {0}. Keywords must be at most 20 characters")]
    KeywordTooLong(String),

    #[error("Too many keywords. Maximum is 5")]
    TooManyKeywords,

    #[error("Invalid dependency name: {0}")]
    InvalidDependencyName(String),

    #[error("Dependency {0} must specify version, path, or git source")]
    MissingDependencySource(String),

    #[error("TOML parse error: {0}")]
    ParseError(#[from] toml::de::Error),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_simple_manifest() {
        let content = r#"
[package]
name = "my-package"
version = "1.0.0"
description = "A test package"
authors = ["Test Author <test@example.com>"]
license = "MIT"
"#;

        let manifest = BlueprintManifest::parse(content).unwrap();
        assert_eq!(manifest.package.name, "my-package");
        assert_eq!(manifest.package.version, "1.0.0");
        assert!(manifest.validate().is_ok());
    }

    #[test]
    fn test_parse_with_dependencies() {
        let content = r#"
[package]
name = "my-package"
version = "1.0.0"

[dependencies]
web = "^1.0"
playwright = { version = "^0.5", optional = true }

[dev_dependencies]
surveyor = "^0.1"
"#;

        let manifest = BlueprintManifest::parse(content).unwrap();
        assert!(manifest.dependencies.contains_key("web"));
        assert!(manifest.dev_dependencies.contains_key("surveyor"));
        assert!(manifest.validate().is_ok());
    }

    #[test]
    fn test_invalid_package_name() {
        let content = r#"
[package]
name = "My-Package"
version = "1.0.0"
"#;

        let manifest = BlueprintManifest::parse(content).unwrap();
        assert!(manifest.validate().is_err());
    }
}
