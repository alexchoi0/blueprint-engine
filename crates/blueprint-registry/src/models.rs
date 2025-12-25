use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::sync::RwLock;
use uuid::Uuid;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Package {
    pub id: Uuid,
    pub namespace: String,
    pub name: String,
    pub description: Option<String>,
    pub repository: Option<String>,
    pub homepage: Option<String>,
    pub documentation: Option<String>,
    pub license: Option<String>,
    pub keywords: Vec<String>,
    pub categories: Vec<String>,
    pub owner_id: Uuid,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Version {
    pub id: Uuid,
    pub package_id: Uuid,
    pub version: String,
    pub checksum: String,
    pub size: i64,
    pub downloads: i64,
    pub yanked: bool,
    pub published_at: DateTime<Utc>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ApiToken {
    pub id: Uuid,
    pub user_id: Uuid,
    pub name: String,
    pub token_hash: String,
    pub token_prefix: String,
    pub created_at: DateTime<Utc>,
    pub last_used_at: Option<DateTime<Utc>>,
}

pub struct PackageStore {
    packages: RwLock<HashMap<Uuid, Package>>,
    versions: RwLock<HashMap<Uuid, Version>>,
    package_data: RwLock<HashMap<String, Vec<u8>>>,
    api_tokens: RwLock<HashMap<Uuid, ApiToken>>,
}

impl PackageStore {
    pub fn new() -> Self {
        Self {
            packages: RwLock::new(HashMap::new()),
            versions: RwLock::new(HashMap::new()),
            package_data: RwLock::new(HashMap::new()),
            api_tokens: RwLock::new(HashMap::new()),
        }
    }

    pub fn create_package(&self, package: Package) -> Package {
        let mut packages = self.packages.write().unwrap();
        packages.insert(package.id, package.clone());
        package
    }

    pub fn find_package(&self, namespace: &str, name: &str) -> Option<Package> {
        let packages = self.packages.read().unwrap();
        packages
            .values()
            .find(|p| p.namespace == namespace && p.name == name)
            .cloned()
    }

    pub fn find_package_by_id(&self, id: Uuid) -> Option<Package> {
        let packages = self.packages.read().unwrap();
        packages.get(&id).cloned()
    }

    pub fn list_packages(&self) -> Vec<Package> {
        let packages = self.packages.read().unwrap();
        packages.values().cloned().collect()
    }

    pub fn list_packages_by_owner(&self, owner_id: Uuid) -> Vec<Package> {
        let packages = self.packages.read().unwrap();
        packages
            .values()
            .filter(|p| p.owner_id == owner_id)
            .cloned()
            .collect()
    }

    pub fn create_version(&self, version: Version) -> Version {
        let mut versions = self.versions.write().unwrap();
        versions.insert(version.id, version.clone());
        version
    }

    pub fn find_version(&self, package_id: Uuid, version: &str) -> Option<Version> {
        let versions = self.versions.read().unwrap();
        versions
            .values()
            .find(|v| v.package_id == package_id && v.version == version)
            .cloned()
    }

    pub fn list_versions(&self, package_id: Uuid) -> Vec<Version> {
        let versions = self.versions.read().unwrap();
        versions
            .values()
            .filter(|v| v.package_id == package_id)
            .cloned()
            .collect()
    }

    pub fn update_version(&self, version: Version) {
        let mut versions = self.versions.write().unwrap();
        versions.insert(version.id, version);
    }

    pub fn store_package_data(&self, key: &str, data: Vec<u8>) {
        let mut package_data = self.package_data.write().unwrap();
        package_data.insert(key.to_string(), data);
    }

    pub fn get_package_data(&self, key: &str) -> Option<Vec<u8>> {
        let package_data = self.package_data.read().unwrap();
        package_data.get(key).cloned()
    }

    // API Token methods
    pub fn create_api_token(&self, user_id: Uuid, name: &str) -> (ApiToken, String) {
        let token = generate_api_token();
        let token_hash = hash_token(&token);
        let token_prefix = token.chars().take(8).collect();

        let api_token = ApiToken {
            id: Uuid::new_v4(),
            user_id,
            name: name.to_string(),
            token_hash,
            token_prefix,
            created_at: Utc::now(),
            last_used_at: None,
        };

        let mut tokens = self.api_tokens.write().unwrap();
        tokens.insert(api_token.id, api_token.clone());

        (api_token, token)
    }

    pub fn list_api_tokens(&self, user_id: Uuid) -> Vec<ApiToken> {
        let tokens = self.api_tokens.read().unwrap();
        tokens
            .values()
            .filter(|t| t.user_id == user_id)
            .cloned()
            .collect()
    }

    pub fn find_api_token_by_hash(&self, token_hash: &str) -> Option<ApiToken> {
        let tokens = self.api_tokens.read().unwrap();
        tokens
            .values()
            .find(|t| t.token_hash == token_hash)
            .cloned()
    }

    pub fn delete_api_token(&self, id: Uuid, user_id: Uuid) -> bool {
        let mut tokens = self.api_tokens.write().unwrap();
        if let Some(token) = tokens.get(&id) {
            if token.user_id == user_id {
                tokens.remove(&id);
                return true;
            }
        }
        false
    }

    pub fn update_api_token_last_used(&self, id: Uuid) {
        let mut tokens = self.api_tokens.write().unwrap();
        if let Some(token) = tokens.get_mut(&id) {
            token.last_used_at = Some(Utc::now());
        }
    }
}

impl Default for PackageStore {
    fn default() -> Self {
        Self::new()
    }
}

pub fn generate_api_token() -> String {
    format!("bp_{}", Uuid::new_v4().to_string().replace("-", ""))
}

pub fn hash_token(token: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(token.as_bytes());
    hex::encode(hasher.finalize())
}
