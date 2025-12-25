use std::sync::Arc;

use axum::{
    extract::{Multipart, Path, Query, State},
    routing::{get, post},
    Json, Router,
};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::auth::AuthUser;
use crate::error::{ApiError, ApiResult};
use crate::manifest::BlueprintManifest;
use crate::models::{Package, Version};
use crate::AppState;

pub fn routes() -> Router<Arc<AppState>> {
    Router::new()
        // Auth
        .route("/register", post(register))
        .route("/login", post(login))
        // Packages
        .route("/packages", get(list_packages))
        .route("/packages/{namespace}/{name}", get(get_package))
        .route("/packages/{namespace}/{name}", post(publish_package))
        .route("/packages/{namespace}/{name}/{version}", get(get_version))
        .route("/packages/{namespace}/{name}/{version}/download", get(download_package))
        .route("/packages/{namespace}/{name}/{version}/yank", post(yank_version))
        // Search
        .route("/search", get(search_packages))
}

// ============ Auth ============

#[derive(Deserialize)]
pub struct RegisterRequest {
    email: String,
    password: String,
    name: Option<String>,
}

#[derive(Serialize)]
pub struct AuthResponse {
    token: String,
    user: UserInfo,
}

#[derive(Serialize)]
pub struct UserInfo {
    id: Uuid,
    email: String,
    name: Option<String>,
}

async fn register(
    State(state): State<Arc<AppState>>,
    Json(req): Json<RegisterRequest>,
) -> ApiResult<Json<AuthResponse>> {
    if req.password.len() < 8 {
        return Err(ApiError::BadRequest("Password must be at least 8 characters".into()));
    }

    let (user, _session, token) = state
        .auth
        .signup(&req.email, &req.password, req.name)
        .await
        .map_err(|e| match e {
            tsa_auth::TsaError::UserAlreadyExists => ApiError::Conflict("Email already exists".into()),
            _ => ApiError::Internal(e.to_string()),
        })?;

    Ok(Json(AuthResponse {
        token,
        user: UserInfo {
            id: user.id,
            email: user.email,
            name: user.name,
        },
    }))
}

#[derive(Deserialize)]
pub struct LoginRequest {
    email: String,
    password: String,
}

async fn login(
    State(state): State<Arc<AppState>>,
    Json(req): Json<LoginRequest>,
) -> ApiResult<Json<AuthResponse>> {
    let (user, _session, token) = state
        .auth
        .signin(&req.email, &req.password, None, None)
        .await
        .map_err(|e| match e {
            tsa_auth::TsaError::InvalidCredentials => ApiError::Unauthorized("Invalid credentials".into()),
            _ => ApiError::Internal(e.to_string()),
        })?;

    Ok(Json(AuthResponse {
        token,
        user: UserInfo {
            id: user.id,
            email: user.email,
            name: user.name,
        },
    }))
}

// ============ Packages ============

#[derive(Deserialize)]
pub struct ListParams {
    page: Option<u64>,
    per_page: Option<u64>,
}

#[derive(Serialize)]
pub struct PackageListResponse {
    packages: Vec<PackageInfo>,
    total: u64,
    page: u64,
    per_page: u64,
}

#[derive(Serialize)]
pub struct PackageInfo {
    namespace: String,
    name: String,
    description: Option<String>,
    latest_version: Option<String>,
    downloads: i64,
    created_at: chrono::DateTime<Utc>,
}

async fn list_packages(
    State(state): State<Arc<AppState>>,
    Query(params): Query<ListParams>,
) -> ApiResult<Json<PackageListResponse>> {
    let page = params.page.unwrap_or(1);
    let per_page = params.per_page.unwrap_or(20).min(100);

    let packages = state.packages.list_packages();

    let mut package_infos = Vec::new();
    for pkg in &packages {
        let versions = state.packages.list_versions(pkg.id);
        let latest = versions.iter()
            .filter(|v| !v.yanked)
            .max_by_key(|v| &v.published_at);

        let total_downloads: i64 = versions.iter().map(|v| v.downloads).sum();

        package_infos.push(PackageInfo {
            namespace: pkg.namespace.clone(),
            name: pkg.name.clone(),
            description: pkg.description.clone(),
            latest_version: latest.map(|v| v.version.clone()),
            downloads: total_downloads,
            created_at: pkg.created_at,
        });
    }

    let total = package_infos.len() as u64;
    let start = ((page - 1) * per_page) as usize;
    let packages: Vec<_> = package_infos.into_iter().skip(start).take(per_page as usize).collect();

    Ok(Json(PackageListResponse {
        packages,
        total,
        page,
        per_page,
    }))
}

#[derive(Serialize)]
pub struct PackageDetail {
    namespace: String,
    name: String,
    description: Option<String>,
    repository: Option<String>,
    homepage: Option<String>,
    documentation: Option<String>,
    license: Option<String>,
    keywords: Vec<String>,
    categories: Vec<String>,
    owner_id: Uuid,
    versions: Vec<VersionInfo>,
    created_at: chrono::DateTime<Utc>,
}

#[derive(Serialize)]
pub struct VersionInfo {
    version: String,
    checksum: String,
    size: i64,
    downloads: i64,
    yanked: bool,
    published_at: chrono::DateTime<Utc>,
}

async fn get_package(
    State(state): State<Arc<AppState>>,
    Path((namespace, name)): Path<(String, String)>,
) -> ApiResult<Json<PackageDetail>> {
    let pkg = state.packages.find_package(&namespace, &name)
        .ok_or_else(|| ApiError::NotFound("Package not found".into()))?;

    let versions = state.packages.list_versions(pkg.id);

    Ok(Json(PackageDetail {
        namespace: pkg.namespace,
        name: pkg.name,
        description: pkg.description,
        repository: pkg.repository,
        homepage: pkg.homepage,
        documentation: pkg.documentation,
        license: pkg.license,
        keywords: pkg.keywords,
        categories: pkg.categories,
        owner_id: pkg.owner_id,
        versions: versions
            .into_iter()
            .map(|v| VersionInfo {
                version: v.version,
                checksum: v.checksum,
                size: v.size,
                downloads: v.downloads,
                yanked: v.yanked,
                published_at: v.published_at,
            })
            .collect(),
        created_at: pkg.created_at,
    }))
}

async fn get_version(
    State(state): State<Arc<AppState>>,
    Path((namespace, name, ver)): Path<(String, String, String)>,
) -> ApiResult<Json<VersionInfo>> {
    let pkg = state.packages.find_package(&namespace, &name)
        .ok_or_else(|| ApiError::NotFound("Package not found".into()))?;

    let version = state.packages.find_version(pkg.id, &ver)
        .ok_or_else(|| ApiError::NotFound("Version not found".into()))?;

    Ok(Json(VersionInfo {
        version: version.version,
        checksum: version.checksum,
        size: version.size,
        downloads: version.downloads,
        yanked: version.yanked,
        published_at: version.published_at,
    }))
}

async fn download_package(
    State(state): State<Arc<AppState>>,
    Path((namespace, name, ver)): Path<(String, String, String)>,
) -> ApiResult<axum::response::Response> {
    use axum::body::Body;
    use axum::http::{header, Response, StatusCode};

    let pkg = state.packages.find_package(&namespace, &name)
        .ok_or_else(|| ApiError::NotFound("Package not found".into()))?;

    let mut version = state.packages.find_version(pkg.id, &ver)
        .ok_or_else(|| ApiError::NotFound("Version not found".into()))?;

    if version.yanked {
        return Err(ApiError::BadRequest("This version has been yanked".into()));
    }

    version.downloads += 1;
    state.packages.update_version(version.clone());

    let key = format!("{}/{}/{}", namespace, name, ver);
    let data = state.packages.get_package_data(&key)
        .ok_or_else(|| ApiError::NotFound("Package file not found".into()))?;

    let filename = format!("{}-{}.tar.gz", name, ver);
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/gzip")
        .header(header::CONTENT_DISPOSITION, format!("attachment; filename=\"{}\"", filename))
        .body(Body::from(data))
        .map_err(|e| ApiError::Internal(e.to_string()))
}

const RESERVED_NAMESPACES: &[&str] = &["bp", "blueprint", "stdlib", "core", "std"];

async fn publish_package(
    State(state): State<Arc<AppState>>,
    user: AuthUser,
    Path((namespace, name)): Path<(String, String)>,
    mut multipart: Multipart,
) -> ApiResult<Json<VersionInfo>> {
    if RESERVED_NAMESPACES.contains(&namespace.to_lowercase().as_str()) {
        return Err(ApiError::Forbidden(
            format!("The namespace '{}' is reserved", namespace),
        ));
    }

    let user_namespace = user.email.split('@').next().unwrap_or(&user.email);
    if namespace != user_namespace {
        return Err(ApiError::Forbidden(
            "You can only publish to your own namespace".into(),
        ));
    }

    let mut manifest_content: Option<String> = None;
    let mut package_data: Option<Vec<u8>> = None;

    while let Some(field) = multipart.next_field().await.map_err(|e| ApiError::BadRequest(e.to_string()))? {
        let field_name = field.name().unwrap_or("").to_string();
        match field_name.as_str() {
            "manifest" => {
                let data = field.bytes().await.map_err(|e| ApiError::BadRequest(e.to_string()))?;
                manifest_content = Some(String::from_utf8(data.to_vec())
                    .map_err(|_| ApiError::BadRequest("Invalid UTF-8 in manifest".into()))?);
            }
            "package" => {
                package_data = Some(field.bytes().await.map_err(|e| ApiError::BadRequest(e.to_string()))?.to_vec());
            }
            _ => {}
        }
    }

    let manifest_str = manifest_content.ok_or_else(|| ApiError::BadRequest("Missing blueprint.toml manifest".into()))?;
    let data = package_data.ok_or_else(|| ApiError::BadRequest("Missing package file".into()))?;

    let manifest = BlueprintManifest::parse(&manifest_str)
        .map_err(|e| ApiError::BadRequest(format!("Invalid manifest: {}", e)))?;

    manifest.validate()
        .map_err(|e| ApiError::BadRequest(format!("Invalid manifest: {}", e)))?;

    if manifest.package.name != name {
        return Err(ApiError::BadRequest(format!(
            "Package name in manifest ({}) doesn't match URL ({})",
            manifest.package.name, name
        )));
    }

    let pkg = match state.packages.find_package(&namespace, &name) {
        Some(p) => {
            if p.owner_id != user.id {
                return Err(ApiError::Forbidden("You don't own this package".into()));
            }
            p
        }
        None => {
            let now = Utc::now();
            state.packages.create_package(Package {
                id: Uuid::new_v4(),
                namespace: namespace.clone(),
                name: name.clone(),
                description: manifest.package.description.clone(),
                repository: manifest.package.repository.clone(),
                homepage: manifest.package.homepage.clone(),
                documentation: manifest.package.documentation.clone(),
                license: manifest.package.license.clone(),
                keywords: manifest.package.keywords.clone(),
                categories: manifest.package.categories.clone(),
                owner_id: user.id,
                created_at: now,
                updated_at: now,
            })
        }
    };

    if state.packages.find_version(pkg.id, &manifest.package.version).is_some() {
        return Err(ApiError::Conflict("Version already exists".into()));
    }

    let mut hasher = Sha256::new();
    hasher.update(&data);
    let checksum = hex::encode(hasher.finalize());

    let key = format!("{}/{}/{}", namespace, name, manifest.package.version);
    state.packages.store_package_data(&key, data.clone());

    let now = Utc::now();
    let version = state.packages.create_version(Version {
        id: Uuid::new_v4(),
        package_id: pkg.id,
        version: manifest.package.version.clone(),
        checksum: checksum.clone(),
        size: data.len() as i64,
        downloads: 0,
        yanked: false,
        published_at: now,
    });

    Ok(Json(VersionInfo {
        version: version.version,
        checksum: version.checksum,
        size: version.size,
        downloads: version.downloads,
        yanked: version.yanked,
        published_at: version.published_at,
    }))
}

async fn yank_version(
    State(state): State<Arc<AppState>>,
    user: AuthUser,
    Path((namespace, name, ver)): Path<(String, String, String)>,
) -> ApiResult<Json<serde_json::Value>> {
    let pkg = state.packages.find_package(&namespace, &name)
        .ok_or_else(|| ApiError::NotFound("Package not found".into()))?;

    if pkg.owner_id != user.id {
        return Err(ApiError::Forbidden("You don't own this package".into()));
    }

    let mut version = state.packages.find_version(pkg.id, &ver)
        .ok_or_else(|| ApiError::NotFound("Version not found".into()))?;

    version.yanked = true;
    state.packages.update_version(version);

    Ok(Json(serde_json::json!({"yanked": true})))
}

#[derive(Deserialize)]
pub struct SearchParams {
    q: String,
    page: Option<u64>,
    per_page: Option<u64>,
}

async fn search_packages(
    State(state): State<Arc<AppState>>,
    Query(params): Query<SearchParams>,
) -> ApiResult<Json<PackageListResponse>> {
    let page = params.page.unwrap_or(1);
    let per_page = params.per_page.unwrap_or(20).min(100);
    let query = params.q.to_lowercase();

    let packages = state.packages.list_packages();

    let mut package_infos = Vec::new();
    for pkg in packages {
        if pkg.name.to_lowercase().contains(&query)
            || pkg.namespace.to_lowercase().contains(&query)
            || pkg.description.as_ref().map_or(false, |d| d.to_lowercase().contains(&query))
        {
            let versions = state.packages.list_versions(pkg.id);
            let latest = versions.iter()
                .filter(|v| !v.yanked)
                .max_by_key(|v| &v.published_at);

            let total_downloads: i64 = versions.iter().map(|v| v.downloads).sum();

            package_infos.push(PackageInfo {
                namespace: pkg.namespace,
                name: pkg.name,
                description: pkg.description,
                latest_version: latest.map(|v| v.version.clone()),
                downloads: total_downloads,
                created_at: pkg.created_at,
            });
        }
    }

    let total = package_infos.len() as u64;
    let start = ((page - 1) * per_page) as usize;
    let packages: Vec<_> = package_infos.into_iter().skip(start).take(per_page as usize).collect();

    Ok(Json(PackageListResponse {
        packages,
        total,
        page,
        per_page,
    }))
}
