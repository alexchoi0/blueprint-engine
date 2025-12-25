use maud::{html, Markup, PreEscaped, DOCTYPE};
use uuid::Uuid;

use crate::models::{ApiToken, Package, Version};

pub struct SessionUser {
    pub id: Uuid,
    pub email: String,
    pub name: Option<String>,
}

fn layout(title: &str, user: Option<&SessionUser>, content: Markup) -> Markup {
    html! {
        (DOCTYPE)
        html lang="en" class="dark" {
            head {
                meta charset="utf-8";
                meta name="viewport" content="width=device-width, initial-scale=1";
                title { (title) " - Blueprint Registry" }
                script src="https://cdn.tailwindcss.com" {}
                (PreEscaped(TAILWIND_CONFIG))
                style { (extra_css()) }
            }
            body class="min-h-screen bg-background text-foreground antialiased" {
                header class="sticky top-0 z-50 w-full border-b border-border/40 bg-background/95 backdrop-blur supports-[backdrop-filter]:bg-background/60" {
                    nav class="container mx-auto flex h-14 max-w-screen-2xl items-center px-4" {
                        a href="/" class="mr-6 flex items-center space-x-2 font-bold text-lg" {
                            span class="text-primary" { "Blueprint" }
                            span class="text-muted-foreground" { "Registry" }
                        }
                        div class="flex flex-1 items-center justify-between space-x-2 md:justify-end" {
                            div class="flex items-center space-x-4" {
                                a href="/packages" class="text-sm font-medium text-muted-foreground transition-colors hover:text-foreground" { "Packages" }
                                @if let Some(u) = user {
                                    a href="/dashboard" class="text-sm font-medium text-muted-foreground transition-colors hover:text-foreground" { "Dashboard" }
                                    span class="text-sm text-muted-foreground" { (u.email.split('@').next().unwrap_or(&u.email)) }
                                    a href="/logout" class="text-sm font-medium text-muted-foreground transition-colors hover:text-foreground" { "Logout" }
                                } @else {
                                    a href="/login" class="inline-flex items-center justify-center rounded-md text-sm font-medium ring-offset-background transition-colors focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2 border border-input bg-background hover:bg-accent hover:text-accent-foreground h-9 px-4" { "Login" }
                                }
                            }
                        }
                    }
                }
                main class="container mx-auto max-w-screen-2xl px-4 py-6" { (content) }
                footer class="border-t border-border/40 py-6 md:py-0" {
                    div class="container mx-auto flex h-14 max-w-screen-2xl items-center justify-center px-4" {
                        p class="text-sm text-muted-foreground" { "Blueprint Registry" }
                    }
                }
            }
        }
    }
}

const TAILWIND_CONFIG: &str = r#"
<script>
tailwind.config = {
  darkMode: 'class',
  theme: {
    extend: {
      colors: {
        border: 'hsl(240 3.7% 15.9%)',
        input: 'hsl(240 3.7% 15.9%)',
        ring: 'hsl(240 4.9% 83.9%)',
        background: 'hsl(240 10% 3.9%)',
        foreground: 'hsl(0 0% 98%)',
        primary: {
          DEFAULT: 'hsl(0 0% 98%)',
          foreground: 'hsl(240 5.9% 10%)',
        },
        secondary: {
          DEFAULT: 'hsl(240 3.7% 15.9%)',
          foreground: 'hsl(0 0% 98%)',
        },
        destructive: {
          DEFAULT: 'hsl(0 62.8% 30.6%)',
          foreground: 'hsl(0 0% 98%)',
        },
        muted: {
          DEFAULT: 'hsl(240 3.7% 15.9%)',
          foreground: 'hsl(240 5% 64.9%)',
        },
        accent: {
          DEFAULT: 'hsl(240 3.7% 15.9%)',
          foreground: 'hsl(0 0% 98%)',
        },
        card: {
          DEFAULT: 'hsl(240 10% 3.9%)',
          foreground: 'hsl(0 0% 98%)',
        },
      },
      borderRadius: {
        lg: '0.5rem',
        md: 'calc(0.5rem - 2px)',
        sm: 'calc(0.5rem - 4px)',
      },
    },
  },
}
</script>
"#;

fn extra_css() -> &'static str {
    r#"
    .btn {
        @apply inline-flex items-center justify-center rounded-md text-sm font-medium ring-offset-background transition-colors focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2 disabled:pointer-events-none disabled:opacity-50;
    }
    "#
}

pub fn home(user: Option<&SessionUser>) -> Markup {
    layout("Home", user, html! {
        section class="flex flex-col items-center justify-center py-20 text-center" {
            h1 class="text-4xl font-bold tracking-tight sm:text-6xl" {
                "Blueprint Registry"
            }
            p class="mt-6 text-lg leading-8 text-muted-foreground max-w-2xl" {
                "The package registry for Blueprint scripts. Discover, share, and reuse packages."
            }
            form action="/search" method="get" class="mt-10 w-full max-w-md" {
                div class="relative" {
                    input type="search" name="q" placeholder="Search packages..."
                        class="flex h-12 w-full rounded-lg border border-input bg-secondary px-4 py-2 text-sm ring-offset-background placeholder:text-muted-foreground focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2";
                }
            }
        }
        section class="py-12" {
            h2 class="text-2xl font-bold tracking-tight mb-6" { "Getting Started" }
            div class="grid gap-6 md:grid-cols-2" {
                div class="rounded-lg border border-border bg-card p-6" {
                    h3 class="font-semibold mb-2" { "Install a package" }
                    pre class="rounded-md bg-secondary p-4 font-mono text-sm overflow-x-auto" {
                        code { "blueprint add @username/package-name" }
                    }
                }
                div class="rounded-lg border border-border bg-card p-6" {
                    h3 class="font-semibold mb-2" { "Publish your own" }
                    pre class="rounded-md bg-secondary p-4 font-mono text-sm overflow-x-auto" {
                        code { "blueprint publish" }
                    }
                }
            }
        }
    })
}

pub fn packages_list(user: Option<&SessionUser>, packages: &[(Package, Option<String>, i64)]) -> Markup {
    layout("Packages", user, html! {
        div class="flex flex-col gap-6" {
            div class="flex items-center justify-between" {
                h1 class="text-3xl font-bold tracking-tight" { "Packages" }
            }
            form action="/search" method="get" class="w-full max-w-md" {
                input type="search" name="q" placeholder="Search packages..."
                    class="flex h-10 w-full rounded-md border border-input bg-secondary px-3 py-2 text-sm ring-offset-background placeholder:text-muted-foreground focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2";
            }
            div class="grid gap-4" {
                @for (pkg, latest_version, downloads) in packages {
                    (package_card(pkg, latest_version.as_deref(), *downloads))
                }
                @if packages.is_empty() {
                    div class="rounded-lg border border-dashed border-border p-8 text-center" {
                        p class="text-muted-foreground" { "No packages found." }
                    }
                }
            }
        }
    })
}

fn package_card(pkg: &Package, latest_version: Option<&str>, downloads: i64) -> Markup {
    html! {
        a href=(format!("/packages/{}/{}", pkg.namespace, pkg.name))
            class="block rounded-lg border border-border bg-card p-4 transition-colors hover:bg-accent" {
            div class="flex items-start justify-between" {
                div {
                    h3 class="font-semibold text-primary" {
                        (format!("@{}/{}", pkg.namespace, pkg.name))
                    }
                    @if let Some(desc) = &pkg.description {
                        p class="mt-1 text-sm text-muted-foreground line-clamp-2" { (desc) }
                    }
                }
            }
            div class="mt-3 flex items-center gap-3" {
                @if let Some(v) = latest_version {
                    span class="inline-flex items-center rounded-md bg-primary/10 px-2 py-1 text-xs font-medium text-primary ring-1 ring-inset ring-primary/20" {
                        (v)
                    }
                }
                span class="text-xs text-muted-foreground" {
                    (downloads) " downloads"
                }
            }
        }
    }
}

pub fn package_detail(user: Option<&SessionUser>, pkg: &Package, versions: &[Version]) -> Markup {
    let full_name = format!("@{}/{}", pkg.namespace, pkg.name);
    let latest = versions.iter().filter(|v| !v.yanked).max_by_key(|v| &v.published_at);

    layout(&full_name, user, html! {
        div class="flex flex-col gap-8" {
            div {
                h1 class="text-3xl font-bold tracking-tight" { (full_name) }
                @if let Some(desc) = &pkg.description {
                    p class="mt-2 text-muted-foreground" { (desc) }
                }
            }

            @if let Some(v) = latest {
                div class="rounded-lg border border-border bg-card p-6" {
                    h2 class="text-lg font-semibold mb-3" { "Install" }
                    pre class="rounded-md bg-secondary p-4 font-mono text-sm overflow-x-auto" {
                        code { (format!("blueprint add {}@{}", full_name, v.version)) }
                    }
                }
            }

            @if let Some(repo) = &pkg.repository {
                div {
                    h2 class="text-lg font-semibold mb-2" { "Repository" }
                    a href=(repo) class="text-primary hover:underline" { (repo) }
                }
            }

            div {
                h2 class="text-lg font-semibold mb-4" { "Versions" }
                div class="rounded-lg border border-border divide-y divide-border" {
                    @for v in versions {
                        div class=(format!("flex items-center justify-between p-4 {}", if v.yanked { "opacity-50" } else { "" })) {
                            div class="flex items-center gap-3" {
                                span class="inline-flex items-center rounded-md bg-primary/10 px-2 py-1 text-xs font-medium text-primary ring-1 ring-inset ring-primary/20" {
                                    (v.version.clone())
                                }
                                @if v.yanked {
                                    span class="inline-flex items-center rounded-md bg-destructive/10 px-2 py-1 text-xs font-medium text-destructive ring-1 ring-inset ring-destructive/20" {
                                        "yanked"
                                    }
                                }
                            }
                            div class="text-sm text-muted-foreground" {
                                span { (v.downloads) " downloads" }
                                span class="mx-2" { "·" }
                                span { (v.published_at.format("%Y-%m-%d")) }
                            }
                        }
                    }
                    @if versions.is_empty() {
                        div class="p-4 text-center text-muted-foreground" {
                            "No versions published yet."
                        }
                    }
                }
            }
        }
    })
}

pub fn search_results(user: Option<&SessionUser>, query: &str, packages: &[(Package, Option<String>, i64)]) -> Markup {
    layout(&format!("Search: {}", query), user, html! {
        div class="flex flex-col gap-6" {
            h1 class="text-3xl font-bold tracking-tight" {
                "Search results for \"" (query) "\""
            }
            form action="/search" method="get" class="w-full max-w-md" {
                input type="search" name="q" value=(query) placeholder="Search packages..."
                    class="flex h-10 w-full rounded-md border border-input bg-secondary px-3 py-2 text-sm ring-offset-background placeholder:text-muted-foreground focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2";
            }
            div class="grid gap-4" {
                @for (pkg, latest_version, downloads) in packages {
                    (package_card(pkg, latest_version.as_deref(), *downloads))
                }
                @if packages.is_empty() {
                    div class="rounded-lg border border-dashed border-border p-8 text-center" {
                        p class="text-muted-foreground" { "No packages found matching your search." }
                    }
                }
            }
        }
    })
}

pub fn login_page(user: Option<&SessionUser>, error: Option<&str>) -> Markup {
    layout("Login", user, html! {
        div class="mx-auto max-w-sm space-y-6" {
            div class="space-y-2 text-center" {
                h1 class="text-2xl font-bold" { "Login" }
                p class="text-muted-foreground" { "Enter your credentials to access your account" }
            }
            @if let Some(err) = error {
                div class="rounded-md bg-destructive/10 border border-destructive/20 p-3 text-sm text-destructive" {
                    (err)
                }
            }
            form action="/login" method="post" class="space-y-4" {
                div class="space-y-2" {
                    label for="email" class="text-sm font-medium leading-none" { "Email" }
                    input type="email" id="email" name="email" required
                        class="flex h-10 w-full rounded-md border border-input bg-secondary px-3 py-2 text-sm ring-offset-background placeholder:text-muted-foreground focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2";
                }
                div class="space-y-2" {
                    label for="password" class="text-sm font-medium leading-none" { "Password" }
                    input type="password" id="password" name="password" required
                        class="flex h-10 w-full rounded-md border border-input bg-secondary px-3 py-2 text-sm ring-offset-background placeholder:text-muted-foreground focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2";
                }
                button type="submit"
                    class="inline-flex h-10 w-full items-center justify-center rounded-md bg-primary px-4 py-2 text-sm font-medium text-primary-foreground ring-offset-background transition-colors hover:bg-primary/90 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2" {
                    "Login"
                }
            }
            p class="text-center text-sm text-muted-foreground" {
                "Don't have an account? "
                a href="/register" class="text-primary hover:underline" { "Register" }
            }
        }
    })
}

pub fn register_page(user: Option<&SessionUser>, error: Option<&str>) -> Markup {
    layout("Register", user, html! {
        div class="mx-auto max-w-sm space-y-6" {
            div class="space-y-2 text-center" {
                h1 class="text-2xl font-bold" { "Create an account" }
                p class="text-muted-foreground" { "Enter your details to get started" }
            }
            @if let Some(err) = error {
                div class="rounded-md bg-destructive/10 border border-destructive/20 p-3 text-sm text-destructive" {
                    (err)
                }
            }
            form action="/register" method="post" class="space-y-4" {
                div class="space-y-2" {
                    label for="email" class="text-sm font-medium leading-none" { "Email" }
                    input type="email" id="email" name="email" required
                        class="flex h-10 w-full rounded-md border border-input bg-secondary px-3 py-2 text-sm ring-offset-background placeholder:text-muted-foreground focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2";
                }
                div class="space-y-2" {
                    label for="password" class="text-sm font-medium leading-none" { "Password" }
                    input type="password" id="password" name="password" required minlength="8"
                        class="flex h-10 w-full rounded-md border border-input bg-secondary px-3 py-2 text-sm ring-offset-background placeholder:text-muted-foreground focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2";
                }
                div class="space-y-2" {
                    label for="name" class="text-sm font-medium leading-none" { "Name " span class="text-muted-foreground" { "(optional)" } }
                    input type="text" id="name" name="name"
                        class="flex h-10 w-full rounded-md border border-input bg-secondary px-3 py-2 text-sm ring-offset-background placeholder:text-muted-foreground focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2";
                }
                button type="submit"
                    class="inline-flex h-10 w-full items-center justify-center rounded-md bg-primary px-4 py-2 text-sm font-medium text-primary-foreground ring-offset-background transition-colors hover:bg-primary/90 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2" {
                    "Create account"
                }
            }
            p class="text-center text-sm text-muted-foreground" {
                "Already have an account? "
                a href="/login" class="text-primary hover:underline" { "Login" }
            }
        }
    })
}

pub fn dashboard(
    user: &SessionUser,
    packages: &[Package],
    tokens: &[ApiToken],
    new_token: Option<&str>,
) -> Markup {
    layout("Dashboard", Some(user), html! {
        div class="flex flex-col gap-8" {
            h1 class="text-3xl font-bold tracking-tight" { "Dashboard" }

            // Stats
            div class="grid gap-4 md:grid-cols-2 lg:grid-cols-4" {
                div class="rounded-lg border border-border bg-card p-6" {
                    div class="text-2xl font-bold" { (packages.len()) }
                    div class="text-sm text-muted-foreground" { "Packages" }
                }
                div class="rounded-lg border border-border bg-card p-6" {
                    div class="text-2xl font-bold" { (tokens.len()) }
                    div class="text-sm text-muted-foreground" { "API Tokens" }
                }
            }

            // API Tokens
            div class="rounded-lg border border-border bg-card" {
                div class="border-b border-border p-6" {
                    h2 class="text-lg font-semibold" { "API Tokens" }
                    p class="text-sm text-muted-foreground mt-1" {
                        "API tokens allow you to publish packages from the command line."
                    }
                }
                div class="p-6 space-y-4" {
                    @if let Some(token) = new_token {
                        div class="rounded-md bg-green-500/10 border border-green-500/20 p-4" {
                            p class="font-medium text-green-400" { "New token created!" }
                            p class="text-sm text-muted-foreground mt-1" {
                                "Make sure to copy this token now. You won't be able to see it again."
                            }
                            pre class="mt-3 rounded-md bg-secondary p-3 font-mono text-sm break-all" {
                                code class="text-green-400" { (token) }
                            }
                        }
                    }

                    form action="/dashboard/tokens" method="post" class="flex gap-2" {
                        input type="text" name="name" placeholder="Token name (e.g., 'laptop')" required
                            class="flex h-10 flex-1 rounded-md border border-input bg-secondary px-3 py-2 text-sm ring-offset-background placeholder:text-muted-foreground focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2";
                        button type="submit"
                            class="inline-flex h-10 items-center justify-center rounded-md bg-primary px-4 py-2 text-sm font-medium text-primary-foreground ring-offset-background transition-colors hover:bg-primary/90 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2" {
                            "Create Token"
                        }
                    }

                    @if tokens.is_empty() {
                        p class="text-sm text-muted-foreground" { "No API tokens yet." }
                    } @else {
                        div class="divide-y divide-border rounded-md border border-border" {
                            @for token in tokens {
                                div class="flex items-center justify-between p-4" {
                                    div {
                                        div class="font-medium" { (token.name.clone()) }
                                        div class="text-sm text-muted-foreground" {
                                            code class="text-xs" { (token.token_prefix.clone()) "..." }
                                            span class="mx-2" { "·" }
                                            span { "Created " (token.created_at.format("%Y-%m-%d")) }
                                            @if let Some(last_used) = token.last_used_at {
                                                span class="mx-2" { "·" }
                                                span { "Last used " (last_used.format("%Y-%m-%d")) }
                                            }
                                        }
                                    }
                                    form action=(format!("/dashboard/tokens/{}/delete", token.id)) method="post" {
                                        button type="submit"
                                            onclick="return confirm('Are you sure you want to revoke this token?');"
                                            class="inline-flex h-8 items-center justify-center rounded-md bg-destructive px-3 text-xs font-medium text-destructive-foreground ring-offset-background transition-colors hover:bg-destructive/90 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2" {
                                            "Revoke"
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }

            // Packages
            div class="rounded-lg border border-border bg-card" {
                div class="border-b border-border p-6" {
                    h2 class="text-lg font-semibold" { "Your Packages" }
                }
                div class="p-6" {
                    @if packages.is_empty() {
                        div class="text-center py-6" {
                            p class="text-muted-foreground" { "You haven't published any packages yet." }
                            p class="text-sm text-muted-foreground mt-1" {
                                "Use " code class="bg-secondary px-1 rounded" { "blueprint publish" } " to publish your first package."
                            }
                        }
                    } @else {
                        div class="grid gap-4" {
                            @for pkg in packages {
                                a href=(format!("/packages/{}/{}", pkg.namespace, pkg.name))
                                    class="block rounded-lg border border-border p-4 transition-colors hover:bg-accent" {
                                    h3 class="font-semibold" {
                                        (format!("@{}/{}", pkg.namespace, pkg.name))
                                    }
                                    @if let Some(desc) = &pkg.description {
                                        p class="mt-1 text-sm text-muted-foreground" { (desc) }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    })
}

pub fn not_found(user: Option<&SessionUser>) -> Markup {
    layout("Not Found", user, html! {
        div class="flex flex-col items-center justify-center py-20 text-center" {
            h1 class="text-4xl font-bold" { "404" }
            p class="mt-4 text-lg text-muted-foreground" { "The page you're looking for doesn't exist." }
            a href="/"
                class="mt-6 inline-flex h-10 items-center justify-center rounded-md bg-primary px-4 py-2 text-sm font-medium text-primary-foreground ring-offset-background transition-colors hover:bg-primary/90" {
                "Go Home"
            }
        }
    })
}
