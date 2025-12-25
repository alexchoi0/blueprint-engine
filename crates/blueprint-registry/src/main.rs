mod api;
mod auth;
mod error;
mod html;
mod manifest;
mod models;

use std::sync::Arc;

use axum::{
    extract::{Path, Query, State},
    response::{Html, Redirect},
    routing::{get, post},
    Form, Router,
};
use axum_extra::extract::cookie::{Cookie, CookieJar};
use clap::Parser;
use maud::Markup;
use serde::Deserialize;
use tower_http::cors::CorsLayer;
use tower_http::trace::TraceLayer;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};
use uuid::Uuid;

use auth::RegistryAuth;
use html::SessionUser;
use models::PackageStore;

#[derive(Parser)]
#[command(name = "bp-server")]
#[command(author, version, about = "Blueprint package registry server")]
struct Cli {
    #[arg(long, default_value = "0.0.0.0", help = "Host to bind to")]
    host: String,

    #[arg(short, long, default_value = "3000", help = "Port to listen on")]
    port: u16,

    #[arg(short, long, help = "Enable verbose logging")]
    verbose: bool,
}

pub struct AppState {
    pub auth: RegistryAuth,
    pub packages: PackageStore,
}

fn render(markup: Markup) -> Html<String> {
    Html(markup.into_string())
}

async fn get_session_user(state: &Arc<AppState>, jar: &CookieJar) -> Option<SessionUser> {
    let token = jar.get("session")?.value();
    let (user, _session) = state.auth.validate_session(token).await.ok()?;
    Some(SessionUser {
        id: user.id,
        email: user.email,
        name: user.name,
    })
}

async fn home_page(State(state): State<Arc<AppState>>, jar: CookieJar) -> Html<String> {
    let user = get_session_user(&state, &jar).await;
    render(html::home(user.as_ref()))
}

async fn packages_page(State(state): State<Arc<AppState>>, jar: CookieJar) -> Html<String> {
    let user = get_session_user(&state, &jar).await;
    let packages = state.packages.list_packages();
    let package_data: Vec<_> = packages
        .into_iter()
        .map(|pkg| {
            let versions = state.packages.list_versions(pkg.id);
            let latest = versions
                .iter()
                .filter(|v| !v.yanked)
                .max_by_key(|v| &v.published_at)
                .map(|v| v.version.clone());
            let downloads: i64 = versions.iter().map(|v| v.downloads).sum();
            (pkg, latest, downloads)
        })
        .collect();
    render(html::packages_list(user.as_ref(), &package_data))
}

async fn package_page(
    State(state): State<Arc<AppState>>,
    jar: CookieJar,
    Path((namespace, name)): Path<(String, String)>,
) -> Html<String> {
    let user = get_session_user(&state, &jar).await;
    match state.packages.find_package(&namespace, &name) {
        Some(pkg) => {
            let versions = state.packages.list_versions(pkg.id);
            render(html::package_detail(user.as_ref(), &pkg, &versions))
        }
        None => render(html::not_found(user.as_ref())),
    }
}

#[derive(Deserialize)]
struct SearchQuery {
    q: Option<String>,
}

async fn search_page(
    State(state): State<Arc<AppState>>,
    jar: CookieJar,
    Query(query): Query<SearchQuery>,
) -> Html<String> {
    let user = get_session_user(&state, &jar).await;
    let q = query.q.unwrap_or_default();
    let query_lower = q.to_lowercase();

    let packages = state.packages.list_packages();
    let results: Vec<_> = packages
        .into_iter()
        .filter(|pkg| {
            pkg.name.to_lowercase().contains(&query_lower)
                || pkg.namespace.to_lowercase().contains(&query_lower)
                || pkg
                    .description
                    .as_ref()
                    .map_or(false, |d| d.to_lowercase().contains(&query_lower))
        })
        .map(|pkg| {
            let versions = state.packages.list_versions(pkg.id);
            let latest = versions
                .iter()
                .filter(|v| !v.yanked)
                .max_by_key(|v| &v.published_at)
                .map(|v| v.version.clone());
            let downloads: i64 = versions.iter().map(|v| v.downloads).sum();
            (pkg, latest, downloads)
        })
        .collect();

    render(html::search_results(user.as_ref(), &q, &results))
}

async fn login_page_get(State(state): State<Arc<AppState>>, jar: CookieJar) -> Html<String> {
    let user = get_session_user(&state, &jar).await;
    render(html::login_page(user.as_ref(), None))
}

#[derive(Deserialize)]
struct LoginForm {
    email: String,
    password: String,
}

async fn login_page_post(
    State(state): State<Arc<AppState>>,
    jar: CookieJar,
    Form(form): Form<LoginForm>,
) -> Result<(CookieJar, Redirect), Html<String>> {
    match state
        .auth
        .signin(&form.email, &form.password, None, None)
        .await
    {
        Ok((_user, _session, token)) => {
            let cookie = Cookie::build(("session", token))
                .path("/")
                .http_only(true)
                .build();
            Ok((jar.add(cookie), Redirect::to("/dashboard")))
        }
        Err(_) => Err(render(html::login_page(None, Some("Invalid email or password")))),
    }
}

async fn register_page_get(State(state): State<Arc<AppState>>, jar: CookieJar) -> Html<String> {
    let user = get_session_user(&state, &jar).await;
    render(html::register_page(user.as_ref(), None))
}

#[derive(Deserialize)]
struct RegisterForm {
    email: String,
    password: String,
    name: Option<String>,
}

async fn register_page_post(
    State(state): State<Arc<AppState>>,
    jar: CookieJar,
    Form(form): Form<RegisterForm>,
) -> Result<(CookieJar, Redirect), Html<String>> {
    if form.password.len() < 8 {
        return Err(render(html::register_page(
            None,
            Some("Password must be at least 8 characters"),
        )));
    }

    let name = form.name.filter(|n| !n.is_empty());

    match state.auth.signup(&form.email, &form.password, name).await {
        Ok((_user, _session, token)) => {
            let cookie = Cookie::build(("session", token))
                .path("/")
                .http_only(true)
                .build();
            Ok((jar.add(cookie), Redirect::to("/dashboard")))
        }
        Err(tsa_auth::TsaError::UserAlreadyExists) => Err(render(html::register_page(
            None,
            Some("An account with this email already exists"),
        ))),
        Err(_) => Err(render(html::register_page(
            None,
            Some("Failed to create account"),
        ))),
    }
}

async fn logout(jar: CookieJar) -> (CookieJar, Redirect) {
    let cookie = Cookie::build(("session", ""))
        .path("/")
        .http_only(true)
        .build();
    (jar.remove(cookie), Redirect::to("/"))
}

async fn dashboard_page(
    State(state): State<Arc<AppState>>,
    jar: CookieJar,
    Query(query): Query<DashboardQuery>,
) -> Result<Html<String>, Redirect> {
    let user = get_session_user(&state, &jar)
        .await
        .ok_or(Redirect::to("/login"))?;

    let packages = state.packages.list_packages_by_owner(user.id);
    let tokens = state.packages.list_api_tokens(user.id);

    Ok(render(html::dashboard(
        &user,
        &packages,
        &tokens,
        query.new_token.as_deref(),
    )))
}

#[derive(Deserialize)]
struct DashboardQuery {
    new_token: Option<String>,
}

#[derive(Deserialize)]
struct CreateTokenForm {
    name: String,
}

async fn create_token(
    State(state): State<Arc<AppState>>,
    jar: CookieJar,
    Form(form): Form<CreateTokenForm>,
) -> Result<Redirect, Redirect> {
    let user = get_session_user(&state, &jar)
        .await
        .ok_or(Redirect::to("/login"))?;

    let (_token_record, token) = state.packages.create_api_token(user.id, &form.name);

    Ok(Redirect::to(&format!(
        "/dashboard?new_token={}",
        urlencoding::encode(&token)
    )))
}

async fn delete_token(
    State(state): State<Arc<AppState>>,
    jar: CookieJar,
    Path(token_id): Path<Uuid>,
) -> Result<Redirect, Redirect> {
    let user = get_session_user(&state, &jar)
        .await
        .ok_or(Redirect::to("/login"))?;

    state.packages.delete_api_token(token_id, user.id);

    Ok(Redirect::to("/dashboard"))
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    let log_level = if cli.verbose {
        "blueprint_registry=trace,debug"
    } else {
        "blueprint_registry=info,warn"
    };

    tracing_subscriber::registry()
        .with(tracing_subscriber::EnvFilter::new(
            std::env::var("RUST_LOG").unwrap_or_else(|_| log_level.into()),
        ))
        .with(tracing_subscriber::fmt::layer())
        .init();

    let state = Arc::new(AppState {
        auth: auth::create_auth(),
        packages: PackageStore::new(),
    });

    let app = Router::new()
        .route("/", get(home_page))
        .route("/packages", get(packages_page))
        .route("/packages/{namespace}/{name}", get(package_page))
        .route("/search", get(search_page))
        .route("/login", get(login_page_get).post(login_page_post))
        .route("/register", get(register_page_get).post(register_page_post))
        .route("/logout", get(logout))
        .route("/dashboard", get(dashboard_page))
        .route("/dashboard/tokens", post(create_token))
        .route("/dashboard/tokens/{id}/delete", post(delete_token))
        .route("/health", get(|| async { "ok" }))
        .nest("/api/v1", api::routes())
        .layer(TraceLayer::new_for_http())
        .layer(CorsLayer::permissive())
        .with_state(state);

    let addr = format!("{}:{}", cli.host, cli.port);
    let listener = tokio::net::TcpListener::bind(&addr).await?;

    tracing::info!("Blueprint Registry listening on http://{}", addr);
    axum::serve(listener, app).await?;

    Ok(())
}
