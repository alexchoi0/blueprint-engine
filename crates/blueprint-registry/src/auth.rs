use std::sync::Arc;

use axum::{
    extract::FromRequestParts,
    http::{header::AUTHORIZATION, request::Parts},
};
use tsa_auth::{adapter::InMemoryAdapter, Auth, AuthConfig, NoopCallbacks};

use crate::error::ApiError;
use crate::models::hash_token;
use crate::AppState;

pub type RegistryAuth = Auth<InMemoryAdapter, NoopCallbacks>;

pub fn create_auth() -> RegistryAuth {
    let adapter = InMemoryAdapter::new();
    let config = AuthConfig::default();
    Auth::new(adapter, config, NoopCallbacks)
}

pub struct AuthUser {
    pub id: uuid::Uuid,
    pub email: String,
    pub name: Option<String>,
}

#[axum::async_trait]
impl FromRequestParts<Arc<AppState>> for AuthUser {
    type Rejection = ApiError;

    async fn from_request_parts(
        parts: &mut Parts,
        state: &Arc<AppState>,
    ) -> Result<Self, Self::Rejection> {
        let auth_header = parts
            .headers
            .get(AUTHORIZATION)
            .and_then(|v| v.to_str().ok())
            .ok_or_else(|| ApiError::Unauthorized("Missing authorization header".into()))?;

        // Bearer token (session token from tsa-auth)
        if let Some(token) = auth_header.strip_prefix("Bearer ") {
            let (user, _session) = state
                .auth
                .validate_session(token)
                .await
                .map_err(|e| ApiError::Unauthorized(e.to_string()))?;

            return Ok(AuthUser {
                id: user.id,
                email: user.email,
                name: user.name,
            });
        }

        // API token (bp_xxx format)
        if let Some(token) = auth_header.strip_prefix("token ") {
            let token_hash = hash_token(token.trim());
            let api_token = state
                .packages
                .find_api_token_by_hash(&token_hash)
                .ok_or_else(|| ApiError::Unauthorized("Invalid API token".into()))?;

            // Update last used timestamp
            state.packages.update_api_token_last_used(api_token.id);

            // Look up user info from tsa-auth
            // For API tokens, we store user_id, so we need to get user info
            // Since we're using in-memory adapter, we'll use a simplified approach
            // The namespace will be derived from the token's user_id
            return Ok(AuthUser {
                id: api_token.user_id,
                email: format!("{}@token", api_token.user_id), // Placeholder
                name: None,
            });
        }

        Err(ApiError::Unauthorized("Invalid authorization format. Use 'Bearer <session>' or 'token <api_token>'".into()))
    }
}
