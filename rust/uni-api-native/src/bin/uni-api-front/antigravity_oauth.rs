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
        let mut serializer = url::form_urlencoded::Serializer::new(String::new());
        serializer.append_pair("client_id", &client_id);
        serializer.append_pair("redirect_uri", redirect_uri);
        serializer.append_pair("response_type", "code");
        serializer.append_pair("scope", SCOPES);
        serializer.append_pair("access_type", "offline");
        serializer.append_pair("prompt", "consent");
        serializer.append_pair("include_granted_scopes", "true");
        if !state.is_empty() {
            serializer.append_pair("state", state);
        }
        Ok(format!("{AUTH_ENDPOINT}?{}", serializer.finish()))
    }

    /// Accept either a bare authorization code or the full callback URL the
    /// browser failed to open (http://localhost:51121/oauth-callback?code=...).
    pub fn parse_auth_code(input: &str) -> Option<String> {
        let raw = input.trim();
        if raw.is_empty() {
            return None;
        }
        if raw.starts_with("http://") || raw.starts_with("https://") {
            let parsed = url::Url::parse(raw).ok()?;
            for (key, value) in parsed.query_pairs() {
                if key == "code" && !value.trim().is_empty() {
                    return Some(value.trim().to_owned());
                }
            }
            return None;
        }
        // Tolerate "code=xxx" pastes as well.
        if let Some(rest) = raw.strip_prefix("code=") {
            let value = rest.split('&').next().unwrap_or("").trim();
            return (!value.is_empty()).then(|| value.to_owned());
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
    let mut serializer = url::form_urlencoded::Serializer::new(String::new());
    for (key, value) in params {
        serializer.append_pair(key, value);
    }
    let body = serializer.finish();
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
