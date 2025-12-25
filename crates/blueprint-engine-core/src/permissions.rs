use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum Policy {
    Allow,
    #[default]
    Deny,
    Ask,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PermissionCheck {
    Allow,
    Deny,
    Ask,
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct Permissions {
    #[serde(default)]
    pub policy: Policy,

    #[serde(default)]
    pub allow: Vec<String>,

    #[serde(default)]
    pub ask: Vec<String>,

    #[serde(default)]
    pub deny: Vec<String>,
}

impl Permissions {
    pub fn none() -> Self {
        Self {
            policy: Policy::Deny,
            allow: vec![],
            ask: vec![],
            deny: vec![],
        }
    }

    pub fn all() -> Self {
        Self {
            policy: Policy::Allow,
            allow: vec![],
            ask: vec![],
            deny: vec![],
        }
    }

    pub fn ask_all() -> Self {
        Self {
            policy: Policy::Ask,
            allow: vec![],
            ask: vec![],
            deny: vec![],
        }
    }

    pub fn check(&self, operation: &str, resource: Option<&str>) -> PermissionCheck {
        // Priority: deny > ask > allow > policy
        if self.matches_any(&self.deny, operation, resource) {
            return PermissionCheck::Deny;
        }

        if self.matches_any(&self.ask, operation, resource) {
            return PermissionCheck::Ask;
        }

        if self.matches_any(&self.allow, operation, resource) {
            return PermissionCheck::Allow;
        }

        match self.policy {
            Policy::Allow => PermissionCheck::Allow,
            Policy::Deny => PermissionCheck::Deny,
            Policy::Ask => PermissionCheck::Ask,
        }
    }

    fn matches_any(&self, rules: &[String], operation: &str, resource: Option<&str>) -> bool {
        for rule in rules {
            if self.matches_rule(rule, operation, resource) {
                return true;
            }
        }
        false
    }

    fn matches_rule(&self, rule: &str, operation: &str, resource: Option<&str>) -> bool {
        if let Some((rule_op, rule_pattern)) = rule.split_once(':') {
            if !self.matches_operation(rule_op, operation) {
                return false;
            }
            match resource {
                Some(res) => self.matches_pattern(rule_pattern, res),
                None => rule_pattern == "*",
            }
        } else {
            self.matches_operation(rule, operation) && resource.is_none()
        }
    }

    fn matches_operation(&self, rule_op: &str, operation: &str) -> bool {
        if rule_op == "*" {
            return true;
        }
        if rule_op.ends_with(".*") {
            let prefix = &rule_op[..rule_op.len() - 1];
            return operation.starts_with(prefix);
        }
        rule_op == operation
    }

    fn matches_pattern(&self, pattern: &str, value: &str) -> bool {
        if pattern == "*" {
            return true;
        }

        if pattern.starts_with("*.") {
            let suffix = &pattern[1..];
            let host = extract_host(value);
            return host.ends_with(suffix) || host == &pattern[2..];
        }

        if pattern.contains('*') {
            if let Ok(glob) = glob::Pattern::new(pattern) {
                if glob.matches(value) {
                    return true;
                }
            }
            let prefix = pattern.trim_end_matches('*');
            if !prefix.is_empty() && value.starts_with(prefix) {
                return true;
            }
        }

        if is_url(value) {
            let host = extract_host(value);
            return host == pattern;
        }

        pattern == value
    }

    pub fn check_fs_read(&self, path: &str) -> PermissionCheck {
        self.check("fs.read", Some(path))
    }

    pub fn check_fs_write(&self, path: &str) -> PermissionCheck {
        self.check("fs.write", Some(path))
    }

    pub fn check_fs_delete(&self, path: &str) -> PermissionCheck {
        self.check("fs.delete", Some(path))
    }

    pub fn check_http(&self, url: &str) -> PermissionCheck {
        self.check("net.http", Some(url))
    }

    pub fn check_ws(&self, url: &str) -> PermissionCheck {
        self.check("net.ws", Some(url))
    }

    pub fn check_process_run(&self, binary: &str) -> PermissionCheck {
        let bin_name = std::path::Path::new(binary)
            .file_name()
            .and_then(|s| s.to_str())
            .unwrap_or(binary);

        let check = self.check("process.run", Some(binary));
        if matches!(check, PermissionCheck::Allow) {
            return check;
        }
        if binary != bin_name {
            let name_check = self.check("process.run", Some(bin_name));
            if matches!(name_check, PermissionCheck::Allow) {
                return name_check;
            }
        }
        check
    }

    pub fn check_process_shell(&self) -> PermissionCheck {
        self.check("process.shell", None)
    }

    pub fn check_env_read(&self, var: &str) -> PermissionCheck {
        self.check("env.read", Some(var))
    }

    pub fn check_env_write(&self) -> PermissionCheck {
        self.check("env.write", None)
    }
}

fn is_url(s: &str) -> bool {
    s.starts_with("http://")
        || s.starts_with("https://")
        || s.starts_with("ws://")
        || s.starts_with("wss://")
}

fn extract_host(url: &str) -> &str {
    let without_scheme = url
        .strip_prefix("https://")
        .or_else(|| url.strip_prefix("http://"))
        .or_else(|| url.strip_prefix("wss://"))
        .or_else(|| url.strip_prefix("ws://"))
        .unwrap_or(url);

    without_scheme
        .split('/')
        .next()
        .unwrap_or(without_scheme)
        .split(':')
        .next()
        .unwrap_or(without_scheme)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_policy_default_deny() {
        let perms = Permissions::none();
        assert_eq!(perms.check_fs_read("/etc/passwd"), PermissionCheck::Deny);
        assert_eq!(
            perms.check_http("https://example.com"),
            PermissionCheck::Deny
        );
        assert_eq!(perms.check_process_shell(), PermissionCheck::Deny);
    }

    #[test]
    fn test_policy_allow_all() {
        let perms = Permissions::all();
        assert_eq!(perms.check_fs_read("/etc/passwd"), PermissionCheck::Allow);
        assert_eq!(
            perms.check_http("https://example.com"),
            PermissionCheck::Allow
        );
        assert_eq!(perms.check_process_shell(), PermissionCheck::Allow);
    }

    #[test]
    fn test_policy_ask_all() {
        let perms = Permissions::ask_all();
        assert_eq!(perms.check_fs_read("/etc/passwd"), PermissionCheck::Ask);
        assert_eq!(
            perms.check_http("https://example.com"),
            PermissionCheck::Ask
        );
        assert_eq!(perms.check_process_shell(), PermissionCheck::Ask);
    }

    #[test]
    fn test_allow_patterns() {
        let perms = Permissions {
            policy: Policy::Deny,
            allow: vec![
                "fs.read:./data/*".to_string(),
                "fs.read:/tmp/*".to_string(),
                "net.http:api.github.com".to_string(),
                "net.http:*.internal.corp".to_string(),
                "process.run:git".to_string(),
                "process.run:jq".to_string(),
                "env.read:HOME".to_string(),
            ],
            ask: vec![],
            deny: vec![],
        };

        assert_eq!(
            perms.check_fs_read("./data/file.json"),
            PermissionCheck::Allow
        );
        assert_eq!(perms.check_fs_read("/tmp/test"), PermissionCheck::Allow);
        assert_eq!(perms.check_fs_read("/etc/passwd"), PermissionCheck::Deny);

        assert_eq!(
            perms.check_http("https://api.github.com/repos"),
            PermissionCheck::Allow
        );
        assert_eq!(
            perms.check_http("https://foo.internal.corp/api"),
            PermissionCheck::Allow
        );
        assert_eq!(perms.check_http("https://evil.com"), PermissionCheck::Deny);

        assert_eq!(perms.check_process_run("git"), PermissionCheck::Allow);
        assert_eq!(
            perms.check_process_run("/usr/bin/git"),
            PermissionCheck::Allow
        );
        assert_eq!(perms.check_process_run("rm"), PermissionCheck::Deny);

        assert_eq!(perms.check_env_read("HOME"), PermissionCheck::Allow);
        assert_eq!(perms.check_env_read("SECRET"), PermissionCheck::Deny);
    }

    #[test]
    fn test_ask_patterns() {
        let perms = Permissions {
            policy: Policy::Deny,
            allow: vec!["fs.read:./config/*".to_string()],
            ask: vec!["fs.read:*".to_string(), "net.http:*".to_string()],
            deny: vec!["process.shell".to_string()],
        };

        assert_eq!(
            perms.check_fs_read("./config/settings.json"),
            PermissionCheck::Allow
        );
        assert_eq!(perms.check_fs_read("/etc/passwd"), PermissionCheck::Ask);
        assert_eq!(
            perms.check_http("https://example.com"),
            PermissionCheck::Ask
        );
        assert_eq!(perms.check_process_shell(), PermissionCheck::Deny);
        assert_eq!(perms.check_process_run("git"), PermissionCheck::Deny);
    }

    #[test]
    fn test_priority_deny_over_ask_over_allow() {
        // Test: deny > ask > allow
        let perms = Permissions {
            policy: Policy::Allow,
            allow: vec!["fs.read:*".to_string()],
            ask: vec!["fs.read:/home/*".to_string()],
            deny: vec!["fs.read:/etc/*".to_string()],
        };

        // allow matches but nothing higher priority
        assert_eq!(perms.check_fs_read("./data/file"), PermissionCheck::Allow);
        // ask overrides allow
        assert_eq!(perms.check_fs_read("/home/user/file"), PermissionCheck::Ask);
        // deny overrides both ask and allow
        assert_eq!(perms.check_fs_read("/etc/passwd"), PermissionCheck::Deny);
    }

    #[test]
    fn test_ask_overrides_allow() {
        let perms = Permissions {
            policy: Policy::Deny,
            allow: vec!["net.http:*".to_string()],
            ask: vec!["net.http:*.dangerous.com".to_string()],
            deny: vec![],
        };

        assert_eq!(perms.check_http("https://safe.com"), PermissionCheck::Allow);
        assert_eq!(
            perms.check_http("https://foo.dangerous.com"),
            PermissionCheck::Ask
        );
    }

    #[test]
    fn test_wildcard_operation() {
        let perms = Permissions {
            policy: Policy::Deny,
            allow: vec!["fs.*:./workspace/*".to_string()],
            ask: vec![],
            deny: vec![],
        };

        assert_eq!(
            perms.check_fs_read("./workspace/file"),
            PermissionCheck::Allow
        );
        assert_eq!(
            perms.check_fs_write("./workspace/file"),
            PermissionCheck::Allow
        );
        assert_eq!(
            perms.check_fs_delete("./workspace/file"),
            PermissionCheck::Allow
        );
        assert_eq!(perms.check_fs_read("/etc/passwd"), PermissionCheck::Deny);
    }

    #[test]
    fn test_deserialize_permissions() {
        let json = r#"{
            "policy": "deny",
            "allow": [
                "fs.read:./data/*",
                "net.http:api.github.com",
                "process.run:git"
            ],
            "ask": [
                "net.http:*"
            ],
            "deny": [
                "process.shell"
            ]
        }"#;

        let perms: Permissions = serde_json::from_str(json).unwrap();

        assert_eq!(perms.policy, Policy::Deny);
        assert_eq!(perms.check_fs_read("./data/test"), PermissionCheck::Allow);
        assert_eq!(
            perms.check_http("https://api.github.com"),
            PermissionCheck::Allow
        );
        assert_eq!(perms.check_http("https://other.com"), PermissionCheck::Ask);
        assert_eq!(perms.check_process_shell(), PermissionCheck::Deny);
    }

    #[test]
    fn test_extract_host() {
        assert_eq!(
            extract_host("https://api.example.com/v1"),
            "api.example.com"
        );
        assert_eq!(extract_host("http://localhost:8080/path"), "localhost");
        assert_eq!(
            extract_host("wss://stream.example.com"),
            "stream.example.com"
        );
    }
}
