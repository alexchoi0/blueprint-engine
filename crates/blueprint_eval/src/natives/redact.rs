use std::collections::HashMap;
use std::sync::Arc;

use blueprint_core::{NativeFunction, Result, Value};
use regex::Regex;
use once_cell::sync::Lazy;

use crate::eval::Evaluator;

pub fn register(evaluator: &mut Evaluator) {
    evaluator.register_native(NativeFunction::new("redact_pii", redact_pii));
    evaluator.register_native(NativeFunction::new("redact_secrets", redact_secrets));
}

static PII_PATTERNS: Lazy<Vec<(&'static str, Regex)>> = Lazy::new(|| {
    vec![
        // Email addresses
        ("EMAIL", Regex::new(r"[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}").unwrap()),

        // US Phone numbers (various formats)
        ("PHONE", Regex::new(r"(?:\+?1[-.\s]?)?\(?[0-9]{3}\)?[-.\s]?[0-9]{3}[-.\s]?[0-9]{4}").unwrap()),

        // SSN (XXX-XX-XXXX)
        ("SSN", Regex::new(r"\b[0-9]{3}-[0-9]{2}-[0-9]{4}\b").unwrap()),

        // Credit card numbers (basic patterns for major cards)
        ("CREDIT_CARD", Regex::new(r"\b(?:4[0-9]{12}(?:[0-9]{3})?|5[1-5][0-9]{14}|3[47][0-9]{13}|6(?:011|5[0-9]{2})[0-9]{12})\b").unwrap()),

        // Credit card with spaces/dashes
        ("CREDIT_CARD", Regex::new(r"\b(?:4[0-9]{3}|5[1-5][0-9]{2}|3[47][0-9]{2}|6(?:011|5[0-9]{2}))[-\s]?[0-9]{4}[-\s]?[0-9]{4}[-\s]?[0-9]{4}\b").unwrap()),

        // IPv4 addresses
        ("IP_ADDRESS", Regex::new(r"\b(?:(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\.){3}(?:25[0-5]|2[0-4][0-9]|[01]?[0-9][0-9]?)\b").unwrap()),

        // IPv6 addresses (simplified)
        ("IP_ADDRESS", Regex::new(r"\b(?:[0-9a-fA-F]{1,4}:){7}[0-9a-fA-F]{1,4}\b").unwrap()),

        // Date of birth patterns (MM/DD/YYYY, YYYY-MM-DD, etc.)
        ("DOB", Regex::new(r"\b(?:0[1-9]|1[0-2])[/-](?:0[1-9]|[12][0-9]|3[01])[/-](?:19|20)[0-9]{2}\b").unwrap()),
        ("DOB", Regex::new(r"\b(?:19|20)[0-9]{2}[/-](?:0[1-9]|1[0-2])[/-](?:0[1-9]|[12][0-9]|3[01])\b").unwrap()),

        // US Passport number
        ("PASSPORT", Regex::new(r"\b[0-9]{9}\b").unwrap()),

        // US Driver's license (generic pattern - varies by state)
        ("DRIVERS_LICENSE", Regex::new(r"\b[A-Z][0-9]{7,8}\b").unwrap()),
    ]
});

static SECRET_PATTERNS: Lazy<Vec<(&'static str, Regex)>> = Lazy::new(|| {
    vec![
        // AWS Access Key ID
        ("AWS_KEY", Regex::new(r"\b(A3T[A-Z0-9]|AKIA|AGPA|AIDA|AROA|AIPA|ANPA|ANVA|ASIA)[A-Z0-9]{16}\b").unwrap()),

        // AWS Secret Access Key
        ("AWS_SECRET", Regex::new(r#"(?i)aws_secret_access_key\s*[=:]\s*['"]?([A-Za-z0-9/+=]{40})['"]?"#).unwrap()),

        // Stripe API keys
        ("STRIPE_KEY", Regex::new(r"\b(sk_live_|pk_live_|sk_test_|pk_test_)[a-zA-Z0-9]{24,}\b").unwrap()),

        // GitHub tokens
        ("GITHUB_TOKEN", Regex::new(r"\b(ghp_|gho_|ghu_|ghs_|ghr_)[a-zA-Z0-9]{36,}\b").unwrap()),

        // GitHub fine-grained PAT
        ("GITHUB_TOKEN", Regex::new(r"\bgithub_pat_[a-zA-Z0-9]{22}_[a-zA-Z0-9]{59}\b").unwrap()),

        // GitLab tokens
        ("GITLAB_TOKEN", Regex::new(r"\bglpat-[a-zA-Z0-9\-]{20,}\b").unwrap()),

        // Slack tokens
        ("SLACK_TOKEN", Regex::new(r"\bxox[baprs]-[0-9]{10,13}-[0-9]{10,13}-[a-zA-Z0-9]{24}\b").unwrap()),

        // Discord tokens
        ("DISCORD_TOKEN", Regex::new(r"\b[MN][A-Za-z\d]{23,}\.[\w-]{6}\.[\w-]{27}\b").unwrap()),

        // Twilio API keys
        ("TWILIO_KEY", Regex::new(r"\bSK[a-f0-9]{32}\b").unwrap()),

        // SendGrid API keys
        ("SENDGRID_KEY", Regex::new(r"\bSG\.[a-zA-Z0-9_-]{22}\.[a-zA-Z0-9_-]{43}\b").unwrap()),

        // Mailchimp API keys
        ("MAILCHIMP_KEY", Regex::new(r"\b[a-f0-9]{32}-us[0-9]{1,2}\b").unwrap()),

        // Google API keys
        ("GOOGLE_API_KEY", Regex::new(r"\bAIza[0-9A-Za-z\-_]{35}\b").unwrap()),

        // Heroku API keys
        ("HEROKU_KEY", Regex::new(r#"(?i)heroku.*['"][0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}['"]"#).unwrap()),

        // Private keys
        ("PRIVATE_KEY", Regex::new(r"-----BEGIN (?:RSA |EC |DSA |OPENSSH )?PRIVATE KEY-----").unwrap()),

        // Bearer tokens
        ("BEARER_TOKEN", Regex::new(r"(?i)bearer\s+[a-zA-Z0-9_\-\.]+").unwrap()),

        // Generic API key patterns
        ("API_KEY", Regex::new(r#"(?i)(?:api[_-]?key|apikey|api[_-]?secret)\s*[=:]\s*['"]?([a-zA-Z0-9_\-]{16,64})['"]?"#).unwrap()),

        // Generic secret patterns
        ("SECRET", Regex::new(r#"(?i)(?:secret|password|passwd|pwd)\s*[=:]\s*['"]?([^\s'\"]{8,64})['"]?"#).unwrap()),

        // Generic token patterns
        ("TOKEN", Regex::new(r#"(?i)(?:access[_-]?token|auth[_-]?token)\s*[=:]\s*['"]?([a-zA-Z0-9_\-\.]{16,})['"]?"#).unwrap()),

        // JWT tokens
        ("JWT", Regex::new(r"\beyJ[a-zA-Z0-9_-]*\.eyJ[a-zA-Z0-9_-]*\.[a-zA-Z0-9_-]*\b").unwrap()),

        // Base64 encoded secrets (long base64 strings that look like keys)
        ("BASE64_SECRET", Regex::new(r"\b[A-Za-z0-9+/]{40,}={0,2}\b").unwrap()),
    ]
});

fn calculate_entropy(s: &str) -> f64 {
    if s.is_empty() {
        return 0.0;
    }

    let mut freq: HashMap<char, usize> = HashMap::new();
    for c in s.chars() {
        *freq.entry(c).or_insert(0) += 1;
    }

    let len = s.len() as f64;
    freq.values()
        .map(|&count| {
            let p = count as f64 / len;
            -p * p.log2()
        })
        .sum()
}

fn find_high_entropy_strings(text: &str, threshold: f64, min_len: usize) -> Vec<(usize, usize)> {
    let word_regex = Regex::new(r"[a-zA-Z0-9+/=_\-]{16,}").unwrap();
    let mut matches = Vec::new();

    for m in word_regex.find_iter(text) {
        let word = m.as_str();
        if word.len() >= min_len {
            let entropy = calculate_entropy(word);
            if entropy >= threshold {
                matches.push((m.start(), m.end()));
            }
        }
    }

    matches
}

async fn redact_pii(args: Vec<Value>, kwargs: HashMap<String, Value>) -> Result<Value> {
    if args.len() != 1 {
        return Err(blueprint_core::BlueprintError::ArgumentError {
            message: format!("redact_pii() takes exactly 1 argument ({} given)", args.len()),
        });
    }

    let text = args[0].as_string()?;
    let mut result = text.clone();

    let exclude: Vec<String> = if let Some(exc) = kwargs.get("exclude") {
        match exc {
            Value::List(list) => {
                let items = list.read().await;
                items.iter()
                    .filter_map(|v| v.as_string().ok())
                    .map(|s| s.to_uppercase())
                    .collect()
            }
            Value::String(s) => vec![s.to_uppercase()],
            _ => vec![],
        }
    } else {
        vec![]
    };

    let mut replacements: Vec<(usize, usize, String)> = Vec::new();

    for (label, pattern) in PII_PATTERNS.iter() {
        if exclude.contains(&label.to_uppercase().to_string()) {
            continue;
        }
        for m in pattern.find_iter(&text) {
            replacements.push((m.start(), m.end(), format!("[{}]", label)));
        }
    }

    replacements.sort_by(|a, b| b.0.cmp(&a.0));

    for (start, end, replacement) in replacements {
        result.replace_range(start..end, &replacement);
    }

    Ok(Value::String(Arc::new(result)))
}

async fn redact_secrets(args: Vec<Value>, kwargs: HashMap<String, Value>) -> Result<Value> {
    if args.len() != 1 {
        return Err(blueprint_core::BlueprintError::ArgumentError {
            message: format!("redact_secrets() takes exactly 1 argument ({} given)", args.len()),
        });
    }

    let text = args[0].as_string()?;
    let mut result = text.clone();

    let entropy_threshold = kwargs
        .get("entropy_threshold")
        .and_then(|v| v.as_float().ok())
        .unwrap_or(4.5);

    let min_length = kwargs
        .get("min_length")
        .and_then(|v| v.as_int().ok())
        .unwrap_or(20) as usize;

    let exclude: Vec<String> = if let Some(exc) = kwargs.get("exclude") {
        match exc {
            Value::List(list) => {
                let items = list.read().await;
                items.iter()
                    .filter_map(|v| v.as_string().ok())
                    .map(|s| s.to_uppercase())
                    .collect()
            }
            Value::String(s) => vec![s.to_uppercase()],
            _ => vec![],
        }
    } else {
        vec![]
    };

    let skip_entropy = exclude.contains(&"HIGH_ENTROPY_SECRET".to_string())
        || exclude.contains(&"ENTROPY".to_string());

    let mut replacements: Vec<(usize, usize, String)> = Vec::new();

    for (label, pattern) in SECRET_PATTERNS.iter() {
        if exclude.contains(&label.to_uppercase().to_string()) {
            continue;
        }
        for m in pattern.find_iter(&text) {
            replacements.push((m.start(), m.end(), format!("[{}]", label)));
        }
    }

    if !skip_entropy {
        for (start, end) in find_high_entropy_strings(&text, entropy_threshold, min_length) {
            let mut overlaps = false;
            for (rs, re, _) in &replacements {
                if (start >= *rs && start < *re) || (end > *rs && end <= *re) {
                    overlaps = true;
                    break;
                }
            }
            if !overlaps {
                replacements.push((start, end, "[HIGH_ENTROPY_SECRET]".to_string()));
            }
        }
    }

    replacements.sort_by(|a, b| b.0.cmp(&a.0));

    for (start, end, replacement) in replacements {
        if start < result.len() && end <= result.len() {
            result.replace_range(start..end, &replacement);
        }
    }

    Ok(Value::String(Arc::new(result)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_entropy_calculation() {
        assert!(calculate_entropy("aaaa") < calculate_entropy("abcd"));
        assert!(calculate_entropy("aB3$xY9!") > 2.5);

        let random_key = "aK3bX9mZ2nQ5wE8rT1yU4iO7pL0sD6fG";
        assert!(calculate_entropy(random_key) > 4.0);
    }

    #[test]
    fn test_email_pattern() {
        let pattern = &PII_PATTERNS.iter().find(|(l, _)| *l == "EMAIL").unwrap().1;
        assert!(pattern.is_match("test@example.com"));
        assert!(pattern.is_match("user.name+tag@domain.co.uk"));
    }

    #[test]
    fn test_aws_key_pattern() {
        let pattern = &SECRET_PATTERNS.iter().find(|(l, _)| *l == "AWS_KEY").unwrap().1;
        assert!(pattern.is_match("AKIAIOSFODNN7EXAMPLE"));
    }

    #[test]
    fn test_stripe_key_pattern() {
        let pattern = &SECRET_PATTERNS.iter().find(|(l, _)| *l == "STRIPE_KEY").unwrap().1;
        // Use test prefix which is safe
        assert!(pattern.is_match("sk_test_00000000000000000000000000"));
        assert!(pattern.is_match("pk_test_00000000000000000000000000"));
    }
}
