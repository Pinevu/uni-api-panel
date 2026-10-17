use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use serde_json::Value;

/// OAuth client used by the Antigravity IDE.  Both values are supplied through
/// the environment so this repository never carries the installed-app
/// credentials; deployments set ANTIGRAVITY_CLIENT_ID / ANTIGRAVITY_CLIENT_SECRET.
const AUTH_ENDPOINT: &str = "https://accounts.google.com/o/oauth2/v2/auth";
const TOKEN_ENDPOINT: &str = "https://oauth2.googleapis.com/token";
const USERINFO_ENDPOINT: &str = "https://www.googleapis.com/oauth2/v2/userinfo?alt=json";
const LOAD_CODE_ASSIST_URL: &str =
    "https://cloudcode-pa.googleapis.com/v1internal:loadCodeAssist";
/// Every Antigravity request is served by this loopback redirect; the desktop
/// client runs a local listener.  Server deployments use the headless flow and
/// paste the resulting authorization code back into the panel.
pub const DEFAULT_REDIRECT_URI: &str = "http://localhost:51121/oauth-callback";
const SCOPES: &str = "https://www.googleapis.com/auth/cloud-platform \
https://www.googleapis.com/auth/userinfo.email \
https://www.googleapis.com/auth/userinfo.profile \
https://www.googleapis.com/auth/cclog \
https://www.googleapis.com/auth/experimentsandconfigs";
pub const USER_AGENT: &str = "antigravity/hub/2.13.0 darwin/arm64";

#[derive(Clone)]
pub struct AntigravityOAuth {
    cache: Arc<Mutex<HashMap<String, CachedToken>>>,
}

#[derive(Clone)]
struct CachedToken {
    access_token: String,
    expires_at: Instant,
}

pub struct TokenBundle {
    pub access_token: String,
    pub refresh_token: Option<String>,
    pub expires_in: Option<u64>,
}

impl Default for AntigravityOAuth {
    fn default() -> Self {
        Self::new()
    }
}

impl AntigravityOAuth {
    pub fn new() -> Self {
        Self {
            cache: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Cached bearer for a refresh token, if it is still valid.
    fn cached(&self, refresh_token: &str) -> Option<String> {
        let cache = self.cache.lock().unwrap_or_else(|error| error.into_inner());
        cache
            .get(refresh_token)
            .filter(|entry| entry.expires_at > Instant::now())
            .map(|entry| entry.access_token.clone())
    }

    fn store(&self, refresh_token: &str, access_token: String, ttl: u64) {
        let mut cache = self.cache.lock().unwrap_or_else(|error| error.into_inner());
        cache.insert(
            refresh_token.to_owned(),
            CachedToken {
                access_token,
                expires_at: Instant::now() + Duration::from_secs(ttl),
            },
        );
    }

    /// Exchange a stored refresh token for a live access token, reusing the
    /// cached bearer until shortly before it expires.
    pub async fn resolve(&self, refresh_token: &str, proxy: Option<&str>) -> Result<String, String> {
        let token = refresh_token.trim();
        if token.is_empty() {
            return Err("Antigravity refresh token is empty".into());
        }
        if let Some(access_token) = self.cached(token) {
            return Ok(access_token);
        }

        let (client_id, client_secret) = oauth_client()?;
        let bundle = request_token(
            &[
                ("client_id", &client_id),
                ("client_secret", &client_secret),
                ("grant_type", "refresh_token"),
                ("refresh_token", token),
            ],
            proxy,
        )
        .await?;
        let ttl = bundle
            .expires_in
            .unwrap_or(3600)
            .saturating_sub(60)
            .max(30);
        self.store(token, bundle.access_token.clone(), ttl);
        Ok(bundle.access_token)
    }

    pub fn build_auth_url(state: &str, redirect_uri: &str) -> Result<String, String> {
        let (client_id, _) = oauth_client()?;
        let mut pairs = vec![
            ("client_id", client_id.as_str()),
            ("redirect_uri", redirect_uri),
            ("response_type", "code"),
            ("scope", SCOPES),
            ("access_type", "offline"),
            ("prompt", "consent"),
            ("include_granted_scopes", "true"),
        ];
        if !state.is_empty() {
            pairs.push(("state", state));
        }
        Ok(format!("{AUTH_ENDPOINT}?{}", encode_pairs(&pairs)))
    }

    /// Accept either a bare authorization code or the full callback URL the
    /// browser failed to open (http://localhost:51121/oauth-callback?code=...).
    /// Accept a bare authorization code, `code=...`, or any callback URL.
    ///
    /// Safari hides the scheme in its address bar, so the common paste is
    /// `localhost:51121/oauth-callback?state=...&code=...` with no `http://`
    /// prefix; that must still be treated as a URL, not as a raw code.
    pub fn parse_auth_code(input: &str) -> Option<String> {
        let raw = input.trim();
        if raw.is_empty() {
            return None;
        }
        // "code=..." pasted on its own.
        if let Some(rest) = raw.strip_prefix("code=") {
            let value = percent_decode(rest.split('&').next().unwrap_or_default());
            let value = value.trim();
            return (!value.is_empty()).then(|| value.to_owned());
        }
        if let Some(index) = raw.find('?') {
            for pair in raw[index + 1..].split('&') {
                let mut parts = pair.splitn(2, '=');
                if parts.next().unwrap_or_default() != "code" {
                    continue;
                }
                let value = percent_decode(parts.next().unwrap_or_default());
                let value = value.trim();
                if !value.is_empty() {
                    return Some(value.to_owned());
                }
            }
            return None;
        }
        // A bare code must not look like a URL or a leftover query string.
        if raw.contains('&') || raw.contains(' ') || raw.contains("://") {
            return None;
        }
        Some(raw.to_owned())
    }

    pub async fn exchange_code(
        code: &str,
        redirect_uri: &str,
        proxy: Option<&str>,
    ) -> Result<TokenBundle, String> {
        let (client_id, client_secret) = oauth_client()?;
        request_token(
            &[
                ("client_id", &client_id),
                ("client_secret", &client_secret),
                ("grant_type", "authorization_code"),
                ("code", code),
                ("redirect_uri", redirect_uri),
            ],
            proxy,
        )
        .await
    }

    pub async fn fetch_email(access_token: &str, proxy: Option<&str>) -> Option<String> {
        let client = build_client(proxy).ok()?;
        let response = client
            .get(USERINFO_ENDPOINT)
            .header("authorization", format!("Bearer {access_token}"))
            .header("accept", "application/json")
            .timeout(Duration::from_secs(20))
            .send()
            .await
            .ok()?;
        if !response.status().is_success() {
            return None;
        }
        let info: Value = response.json().await.ok()?;
        info.get("email")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_owned)
    }

    /// Resolve the Code Assist project ID bound to the signed-in Google account.
    pub async fn fetch_project(access_token: &str, proxy: Option<&str>) -> Option<String> {
        let client = build_client(proxy).ok()?;
        let response = client
            .post(LOAD_CODE_ASSIST_URL)
            .header("authorization", format!("Bearer {access_token}"))
            .header("content-type", "application/json")
            .header("accept", "*/*")
            .header("user-agent", USER_AGENT)
            .timeout(Duration::from_secs(20))
            .body(r#"{"metadata":{"ideType":"ANTIGRAVITY"}}"#)
            .send()
            .await
            .ok()?;
        if !response.status().is_success() {
            return None;
        }
        let payload: Value = response.json().await.ok()?;
        for key in ["cloudaicompanionProject", "projectId", "project"] {
            match payload.get(key) {
                Some(Value::String(value)) if !value.trim().is_empty() => {
                    return Some(value.trim().to_owned())
                }
                Some(Value::Object(inner)) => {
                    if let Some(id) = inner.get("id").and_then(Value::as_str) {
                        if !id.trim().is_empty() {
                            return Some(id.trim().to_owned());
                        }
                    }
                }
                _ => {}
            }
        }
        None
    }
}

/// Minimal percent-decoding for the authorization code query parameter.
fn percent_decode(value: &str) -> String {
    let bytes = value.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'%' && index + 2 < bytes.len() {
            let hex = &value[index + 1..index + 3];
            if let Ok(byte) = u8::from_str_radix(hex, 16) {
                out.push(byte);
                index += 3;
                continue;
            }
        }
        out.push(bytes[index]);
        index += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// Percent-encode a form body.  Written by hand because
/// `url::form_urlencoded::Serializer` is not `Send`, and holding one across an
/// await would make the whole request future non-Send.
fn encode_pairs(pairs: &[(&str, &str)]) -> String {
    let encode = |value: &str| -> String {
        url::form_urlencoded::byte_serialize(value.as_bytes()).collect()
    };
    pairs
        .iter()
        .map(|(key, value)| format!("{}={}", encode(key), encode(value)))
        .collect::<Vec<_>>()
        .join("&")
}

/// Read the installed-app OAuth client from the environment.
fn oauth_client() -> Result<(String, String), String> {
    let read = |name: &str| {
        std::env::var(name)
            .ok()
            .map(|value| value.trim().to_owned())
            .filter(|value| !value.is_empty())
            .ok_or_else(|| {
                format!("Antigravity OAuth is not configured: set {name} for this deployment")
            })
    };
    Ok((
        read("ANTIGRAVITY_CLIENT_ID")?,
        read("ANTIGRAVITY_CLIENT_SECRET")?,
    ))
}

fn build_client(proxy: Option<&str>) -> Result<reqwest::Client, String> {
    let mut builder = reqwest::Client::builder().timeout(Duration::from_secs(30));
    if let Some(proxy_url) = proxy.filter(|value| !value.trim().is_empty()) {
        let proxy = reqwest::Proxy::all(proxy_url).map_err(|error| format!("invalid proxy: {error}"))?;
        builder = builder.proxy(proxy);
    }
    builder
        .build()
        .map_err(|error| format!("build Antigravity OAuth client: {error}"))
}

fn unix_millis() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|value| value.as_millis())
        .unwrap_or_default()
}

async fn request_token(
    params: &[(&str, &str)],
    proxy: Option<&str>,
) -> Result<TokenBundle, String> {
    let client = build_client(proxy)?;
    let body = encode_pairs(params);
    let response = client
        .post(TOKEN_ENDPOINT)
        .header("content-type", "application/x-www-form-urlencoded")
        .header("accept", "application/json")
        .header("user-agent", USER_AGENT)
        .timeout(Duration::from_secs(30))
        .body(body)
        .send()
        .await
        .map_err(|error| format!("Antigravity token request failed: {error}"))?;

    let status = response.status();
    let payload: Value = response
        .json()
        .await
        .map_err(|error| format!("Antigravity token response is not JSON: {error}"))?;

    if !status.is_success() {
        let description = payload
            .get("error_description")
            .or_else(|| payload.get("error"))
            .and_then(Value::as_str)
            .unwrap_or("unknown error");
        return Err(format!("Antigravity token endpoint rejected ({status}): {description}"));
    }

    let access_token = payload
        .get("access_token")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| "Antigravity token response is missing access_token".to_owned())?
        .to_owned();

    Ok(TokenBundle {
        access_token,
        refresh_token: payload
            .get("refresh_token")
            .and_then(Value::as_str)
            .map(str::to_owned),
        expires_in: payload.get("expires_in").and_then(Value::as_u64),
    })
}

/// Build a short opaque state value; the callback is validated socially (the
/// user pastes it) so only uniqueness matters here.
pub fn random_state() -> String {
    format!("ag{:x}{:x}", unix_millis(), std::process::id())
}



/// Antigravity quota windows exposed by the Code Assist backend.
pub struct QuotaReport {
    pub plan: String,
    pub tier_id: String,
    pub groups: Vec<QuotaGroup>,
}

#[derive(Clone)]
pub struct QuotaWindow {
    pub remaining: f64,
    pub reset_time: Option<String>,
}

pub struct QuotaGroup {
    pub key: &'static str,
    pub label: &'static str,
    pub five_hour: Option<QuotaWindow>,
    pub weekly: Option<QuotaWindow>,
    pub models: Vec<String>,
}

const QUOTA_SUMMARY_URLS: [&str; 2] = [
    "https://daily-cloudcode-pa.googleapis.com/v1internal:retrieveUserQuotaSummary",
    "https://cloudcode-pa.googleapis.com/v1internal:retrieveUserQuotaSummary",
];

impl AntigravityOAuth {
    pub async fn fetch_quota(
        refresh_token: &str,
        project: &str,
        proxy: Option<&str>,
    ) -> Result<QuotaReport, String> {
        let oauth = AntigravityOAuth::new();
        let access = oauth.resolve(refresh_token, proxy).await?;
        let client = build_client(proxy)?;

        let (plan, tier_id) = fetch_plan(&client, &access).await;
        let models = fetch_group_models(&client, &access, project).await;

        if let Some(groups) = fetch_quota_summary(&client, &access, project).await {
            return Ok(QuotaReport {
                plan,
                tier_id,
                groups: attach_models(groups, models),
            });
        }

        // Older accounts only expose the per-model five-hour buckets.
        let groups = fetch_legacy_groups(&client, &access, project, models).await?;
        Ok(QuotaReport {
            plan,
            tier_id,
            groups,
        })
    }
}

fn window_from(value: &Value) -> Option<QuotaWindow> {
    let remaining = value.get("remainingFraction").and_then(Value::as_f64)?;
    Some(QuotaWindow {
        remaining: remaining.clamp(0.0, 1.0),
        reset_time: value
            .get("resetTime")
            .and_then(Value::as_str)
            .map(str::to_owned),
    })
}

/// `retrieveUserQuotaSummary` returns one entry per model family with both a
/// weekly and a five-hour bucket -- the same data the reference panel shows.
async fn fetch_quota_summary(
    client: &reqwest::Client,
    access: &str,
    project: &str,
) -> Option<Vec<QuotaGroup>> {
    let body = if project.trim().is_empty() {
        "{}".to_owned()
    } else {
        serde_json::json!({ "project": project.trim() }).to_string()
    };
    for url in QUOTA_SUMMARY_URLS {
        let response = client
            .post(url)
            .header("authorization", format!("Bearer {access}"))
            .header("content-type", "application/json")
            .header("user-agent", USER_AGENT)
            .timeout(Duration::from_secs(25))
            .body(body.clone())
            .send()
            .await;
        let Ok(response) = response else { continue };
        if !response.status().is_success() {
            continue;
        }
        let Ok(payload) = response.json::<Value>().await else {
            continue;
        };
        let Some(entries) = payload.get("groups").and_then(Value::as_array) else {
            continue;
        };
        let mut gemini = QuotaGroup {
            key: "gemini",
            label: "Gemini 模型",
            five_hour: None,
            weekly: None,
            models: Vec::new(),
        };
        let mut claude = QuotaGroup {
            key: "claude",
            label: "Claude 和 GPT 模型",
            five_hour: None,
            weekly: None,
            models: Vec::new(),
        };
        let mut seen_gemini = false;
        let mut seen_claude = false;
        for entry in entries {
            let display = entry
                .get("displayName")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_ascii_lowercase();
            let buckets = entry
                .get("buckets")
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default();
            let looks_claude = display.contains("claude")
                || display.contains("gpt")
                || buckets.iter().any(|bucket| {
                    bucket
                        .get("bucketId")
                        .and_then(Value::as_str)
                        .is_some_and(|id| id.starts_with("3p"))
                });
            let group = if looks_claude { &mut claude } else { &mut gemini };
            let seen = if looks_claude {
                &mut seen_claude
            } else {
                &mut seen_gemini
            };
            if !*seen {
                for bucket in &buckets {
                    let window = bucket
                        .get("window")
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_ascii_lowercase();
                    let Some(parsed) = window_from(bucket) else {
                        continue;
                    };
                    if window.contains("week") {
                        group.weekly = Some(match group.weekly.take() {
                            Some(existing) if existing.remaining <= parsed.remaining => existing,
                            _ => parsed,
                        });
                    } else {
                        group.five_hour = Some(match group.five_hour.take() {
                            Some(existing) if existing.remaining <= parsed.remaining => existing,
                            _ => parsed,
                        });
                    }
                }
                *seen = true;
            }
        }
        let mut groups = Vec::new();
        if seen_gemini {
            groups.push(gemini);
        }
        if seen_claude {
            groups.push(claude);
        }
        if !groups.is_empty() {
            return Some(groups);
        }
    }
    None
}

/// Legacy fallback: per-model five-hour buckets from `retrieveUserQuota`.
async fn fetch_legacy_groups(
    client: &reqwest::Client,
    access: &str,
    project: &str,
    models: Vec<(String, bool)>,
) -> Result<Vec<QuotaGroup>, String> {
    let body = if project.trim().is_empty() {
        "{}".to_owned()
    } else {
        serde_json::json!({ "project": project.trim() }).to_string()
    };
    let response = client
        .post("https://cloudcode-pa.googleapis.com/v1internal:retrieveUserQuota")
        .header("authorization", format!("Bearer {access}"))
        .header("content-type", "application/json")
        .header("user-agent", USER_AGENT)
        .timeout(Duration::from_secs(25))
        .body(body)
        .send()
        .await
        .map_err(|error| format!("Antigravity quota request failed: {error}"))?;
    if !response.status().is_success() {
        return Err(format!(
            "Antigravity quota endpoint returned HTTP {}",
            response.status()
        ));
    }
    let payload: Value = response
        .json()
        .await
        .map_err(|error| format!("Antigravity quota response is not JSON: {error}"))?;
    let buckets = payload
        .get("buckets")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();

    let mut gemini = QuotaGroup {
        key: "gemini",
        label: "Gemini 模型",
        five_hour: None,
        weekly: None,
        models: Vec::new(),
    };
    let mut claude = QuotaGroup {
        key: "claude",
        label: "Claude 和 GPT 模型",
        five_hour: None,
        weekly: None,
        models: Vec::new(),
    };
    for bucket in buckets {
        let model = bucket
            .get("modelId")
            .and_then(Value::as_str)
            .unwrap_or_default();
        if model.is_empty() {
            continue;
        }
        let lower = model.to_ascii_lowercase();
        let is_claude = lower.starts_with("claude") || lower.contains("gpt");
        let Some(parsed) = window_from(&bucket) else {
            continue;
        };
        let group = if is_claude { &mut claude } else { &mut gemini };
        group.five_hour = Some(match group.five_hour.take() {
            Some(existing) if existing.remaining <= parsed.remaining => existing,
            _ => parsed,
        });
        group.models.push(model.to_owned());
    }
    let mut groups = Vec::new();
    if gemini.five_hour.is_some() {
        groups.push(gemini);
    }
    if claude.five_hour.is_some() {
        groups.push(claude);
    }
    for group in groups.iter_mut() {
        if group.models.is_empty() {
            group.models = models
                .iter()
                .filter(|(_, is_claude)| *is_claude == (group.key == "claude"))
                .map(|(name, _)| name.clone())
                .collect();
        }
    }
    Ok(groups)
}

/// Per-model list used only for the "N models" caption.
async fn fetch_group_models(
    client: &reqwest::Client,
    access: &str,
    project: &str,
) -> Vec<(String, bool)> {
    let body = if project.trim().is_empty() {
        "{}".to_owned()
    } else {
        serde_json::json!({ "project": project.trim() }).to_string()
    };
    let response = client
        .post("https://cloudcode-pa.googleapis.com/v1internal:retrieveUserQuota")
        .header("authorization", format!("Bearer {access}"))
        .header("content-type", "application/json")
        .header("user-agent", USER_AGENT)
        .timeout(Duration::from_secs(20))
        .body(body)
        .send()
        .await;
    let Ok(response) = response else {
        return Vec::new();
    };
    if !response.status().is_success() {
        return Vec::new();
    }
    let Ok(payload) = response.json::<Value>().await else {
        return Vec::new();
    };
    payload
        .get("buckets")
        .and_then(Value::as_array)
        .map(|buckets| {
            buckets
                .iter()
                .filter_map(|bucket| {
                    let model = bucket.get("modelId").and_then(Value::as_str)?;
                    let lower = model.to_ascii_lowercase();
                    let is_claude = lower.starts_with("claude") || lower.contains("gpt");
                    Some((model.to_owned(), is_claude))
                })
                .collect()
        })
        .unwrap_or_default()
}

fn attach_models(
    mut groups: Vec<QuotaGroup>,
    models: Vec<(String, bool)>,
) -> Vec<QuotaGroup> {
    if models.is_empty() {
        return groups;
    }
    for group in groups.iter_mut() {
        let want_claude = group.key == "claude";
        group.models = models
            .iter()
            .filter(|(_, is_claude)| *is_claude == want_claude)
            .map(|(name, _)| name.clone())
            .collect();
    }
    groups
}

/// Map Google's verbose tier names onto the short labels used in the panel.
fn short_plan_name(name: &str) -> String {
    let lower = name.to_ascii_lowercase();
    if lower.contains("ultra") {
        "Ultra".into()
    } else if lower.contains("pro") {
        "Pro".into()
    } else if lower.contains("free") || lower.is_empty() {
        "Free".into()
    } else {
        name.to_owned()
    }
}

async fn fetch_plan(client: &reqwest::Client, access: &str) -> (String, String) {
    let response = client
        .post(LOAD_CODE_ASSIST_URL)
        .header("authorization", format!("Bearer {access}"))
        .header("content-type", "application/json")
        .header("user-agent", USER_AGENT)
        .timeout(Duration::from_secs(20))
        .body(r#"{"metadata":{"ideType":"ANTIGRAVITY"}}"#)
        .send()
        .await;
    let Ok(response) = response else {
        return ("Free".into(), "free-tier".into());
    };
    if !response.status().is_success() {
        return ("Free".into(), "free-tier".into());
    }
    let Ok(payload) = response.json::<Value>().await else {
        return ("Free".into(), "free-tier".into());
    };
    let Some(tier) = payload.get("paidTier").filter(|value| value.is_object()) else {
        return ("Free".into(), "free-tier".into());
    };
    let name = tier
        .get("name")
        .and_then(Value::as_str)
        .unwrap_or("Free")
        .to_owned();
    let id = tier
        .get("id")
        .and_then(Value::as_str)
        .unwrap_or("free-tier")
        .to_owned();
    (short_plan_name(&name), id)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_full_callback_urls() {
        let code = AntigravityOAuth::parse_auth_code(
            "http://localhost:51121/oauth-callback?state=abc&code=4%2F0AXYz",
        );
        assert_eq!(code.as_deref(), Some("4/0AXYz"));
    }

    #[test]
    fn parses_callback_urls_without_a_scheme() {
        // Safari hides the scheme in its address bar.
        let code = AntigravityOAuth::parse_auth_code(
            "localhost:51121/oauth-callback?state=abc&code=4%2F0AXYz",
        );
        assert_eq!(code.as_deref(), Some("4/0AXYz"));
    }

    #[test]
    fn parses_bare_codes_and_code_prefixes() {
        assert_eq!(
            AntigravityOAuth::parse_auth_code("4/0AXYz").as_deref(),
            Some("4/0AXYz")
        );
        assert_eq!(
            AntigravityOAuth::parse_auth_code("code=4%2F0AXYz").as_deref(),
            Some("4/0AXYz")
        );
    }

    #[test]
    fn rejects_callbacks_without_a_code() {
        assert!(AntigravityOAuth::parse_auth_code(
            "localhost:51121/oauth-callback?state=abc"
        )
        .is_none());
        assert!(AntigravityOAuth::parse_auth_code("   ").is_none());
    }
}
