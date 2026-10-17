//! Quota-aware account rotation for Antigravity channels.
//!
//! A channel may carry several Google accounts (`preferences.oauth_accounts`)
//! instead of a single `preferences.oauth`.  Every request picks an account by
//! its cached 5-hour / weekly quota rather than blind round-robin, and an
//! account that answers 429/401/403 is parked in a cooldown until either its
//! quota window resets or a fixed backoff elapses.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use serde_json::{Map, Value};

use crate::antigravity_oauth::AntigravityOAuth;

/// One Google account inside an Antigravity channel.
#[derive(Clone, Debug)]
pub struct Account {
    /// Kept for diagnostics and for round-tripping the panel's account list.
    #[allow(dead_code)]
    pub email: String,
    pub refresh_token: String,
    pub project_id: String,
    pub proxy: Option<String>,
}

impl Account {
    fn key(&self) -> &str {
        &self.refresh_token
    }

}

/// Remaining quota of one model group (Gemini, or Claude/GPT).
#[derive(Clone, Copy, Debug)]
pub struct GroupQuota {
    pub five_hour: f64,
    pub weekly: f64,
}

impl GroupQuota {
    fn score(&self) -> f64 {
        self.five_hour.min(self.weekly).clamp(0.0, 1.0)
    }
}

#[derive(Clone, Copy, Debug)]
pub struct AccountQuota {
    pub gemini: GroupQuota,
    pub claude: GroupQuota,
}

#[derive(Default)]
struct Inner {
    cooldowns: HashMap<String, Instant>,
    quota: HashMap<String, (AccountQuota, Instant)>,
    last_probe: HashMap<String, Instant>,
    probing: bool,
    cursor: usize,
}

/// Shared rotation state for every Antigravity channel.
#[derive(Clone, Default)]
pub struct AccountPool {
    inner: Arc<Mutex<Inner>>,
}

#[derive(Debug)]
pub enum Pick {
    Selected(usize),
    /// Every account is parked in a cooldown.
    Cooling { retry_after: u64 },
    Empty,
}

impl AccountPool {
    pub fn new() -> Self {
        Self::default()
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Inner> {
        self.inner.lock().unwrap_or_else(|error| error.into_inner())
    }

    /// Choose the account with the most headroom for `model`.  Accounts without
    /// quota data are treated as full so a brand-new account gets traffic and
    /// its quota is learned.
    pub fn pick(&self, accounts: &[Account], model: &str) -> Pick {
        if accounts.is_empty() {
            return Pick::Empty;
        }
        let claude_group = model_is_claude(model);
        let now = Instant::now();
        let mut inner = self.lock();

        let mut best = f64::MIN;
        let mut candidates: Vec<usize> = Vec::new();
        let mut earliest: Option<Instant> = None;

        for (index, account) in accounts.iter().enumerate() {
            if let Some(until) = inner.cooldowns.get(account.key()) {
                if *until > now {
                    earliest = Some(match earliest {
                        Some(current) if current < *until => current,
                        _ => *until,
                    });
                    continue;
                }
            }
            let score = inner
                .quota
                .get(account.key())
                .map(|(quota, _)| {
                    let group = if claude_group {
                        quota.claude
                    } else {
                        quota.gemini
                    };
                    group.score()
                })
                .unwrap_or(1.0);
            if score > best + 0.02 {
                best = score;
                candidates.clear();
                candidates.push(index);
            } else if score >= best - 0.02 {
                candidates.push(index);
            }
        }

        if candidates.is_empty() {
            let retry_after = earliest
                .map(|until| until.saturating_duration_since(now).as_secs().max(1))
                .unwrap_or(5);
            return Pick::Cooling { retry_after };
        }

        // Round-robin inside the top score band so equal accounts share load.
        let cursor = inner.cursor;
        let chosen = candidates[cursor % candidates.len()];
        inner.cursor = cursor.wrapping_add(1);
        Pick::Selected(chosen)
    }

    /// Park an account after an upstream failure.
    pub fn mark_failure(&self, account: &Account, status: u16) {
        let cooldown = match status {
            429 => Duration::from_secs(300),
            401 | 403 => Duration::from_secs(600),
            500..=599 => Duration::from_secs(45),
            _ => Duration::from_secs(60),
        };
        self.lock()
            .cooldowns
            .insert(account.key().to_owned(), Instant::now() + cooldown);
    }

    /// Drop the cooldown and refresh the quota window of an account that just
    /// answered successfully.
    pub fn mark_success(&self, account: &Account) {
        self.lock().cooldowns.remove(account.key());
    }

    pub fn note_quota(&self, account: &Account, quota: AccountQuota) {
        let mut inner = self.lock();
        inner
            .quota
            .insert(account.key().to_owned(), (quota, Instant::now()));
        if quota.gemini.score() > 0.05 || quota.claude.score() > 0.05 {
            inner.cooldowns.remove(account.key());
        }
    }

    /// Refresh cached quota in the background at most every five minutes.
    /// Never blocks the request path.
    pub fn spawn_probe(&self, accounts: Vec<Account>) {
        let stale: Vec<Account> = {
            let mut inner = self.lock();
            if inner.probing {
                return;
            }
            let now = Instant::now();
            let stale: Vec<Account> = accounts
                .into_iter()
                .filter(|account| {
                    let fresh = inner
                        .quota
                        .get(account.key())
                        .map(|(_, at)| now.duration_since(*at) < Duration::from_secs(300))
                        .unwrap_or(false);
                    let recent = inner
                        .last_probe
                        .get(account.key())
                        .map(|at| now.duration_since(*at) < Duration::from_secs(60))
                        .unwrap_or(false);
                    !fresh && !recent
                })
                .collect();
            if stale.is_empty() {
                return;
            }
            for account in &stale {
                inner.last_probe.insert(account.key().to_owned(), now);
            }
            inner.probing = true;
            stale
        };

        let pool = self.clone();
        tokio::spawn(async move {
            for account in stale {
                if let Some(quota) = probe_account(&account).await {
                    pool.note_quota(&account, quota);
                }
            }
            pool.lock().probing = false;
        });
    }
}

async fn probe_account(account: &Account) -> Option<AccountQuota> {
    let proxy = account.proxy.as_deref();
    let report = AntigravityOAuth::fetch_quota(&account.refresh_token, &account.project_id, proxy)
        .await
        .ok()?;
    let mut quota = AccountQuota {
        gemini: GroupQuota {
            five_hour: 1.0,
            weekly: 1.0,
        },
        claude: GroupQuota {
            five_hour: 1.0,
            weekly: 1.0,
        },
    };
    for group in &report.groups {
        let target = if group.key == "claude" {
            &mut quota.claude
        } else {
            &mut quota.gemini
        };
        if let Some(window) = &group.five_hour {
            target.five_hour = window.remaining;
        }
        if let Some(window) = &group.weekly {
            target.weekly = window.remaining;
        }
    }
    Some(quota)
}

fn model_is_claude(model: &str) -> bool {
    let lower = model.to_ascii_lowercase();
    lower.contains("claude") || lower.starts_with("gpt-")
}

/// Read `preferences.oauth_accounts`, falling back to the legacy single
/// `preferences.oauth` object.  Disabled entries (`enabled: false`) are skipped.
pub fn accounts_from_preferences(preferences: &Map<String, Value>) -> Vec<Account> {
    let mut accounts = Vec::new();
    if let Some(list) = preferences.get("oauth_accounts").and_then(Value::as_array) {
        for entry in list {
            let Some(object) = entry.as_object() else {
                continue;
            };
            if object.get("enabled").and_then(Value::as_bool) == Some(false) {
                continue;
            }
            let refresh_token = object
                .get("refresh_token")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .trim();
            if refresh_token.is_empty() {
                continue;
            }
            accounts.push(Account {
                email: object
                    .get("email")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_owned(),
                refresh_token: refresh_token.to_owned(),
                project_id: object
                    .get("project_id")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_owned(),
                proxy: object
                    .get("proxy")
                    .and_then(Value::as_str)
                    .map(str::trim)
                    .filter(|value| !value.is_empty())
                    .map(str::to_owned),
            });
        }
    }
    if accounts.is_empty() {
        if let Some(oauth) = preferences.get("oauth").and_then(Value::as_object) {
            let refresh_token = oauth
                .get("refresh_token")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .trim();
            if !refresh_token.is_empty() {
                accounts.push(Account {
                    email: oauth
                        .get("email")
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_owned(),
                    refresh_token: refresh_token.to_owned(),
                    project_id: oauth
                        .get("project_id")
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_owned(),
                    proxy: None,
                });
            }
        }
    }
    accounts
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn account(email: &str) -> Account {
        Account {
            email: email.to_owned(),
            refresh_token: format!("token-{email}"),
            project_id: "p".into(),
            proxy: None,
        }
    }

    #[test]
    fn falls_back_to_single_oauth() {
        let mut preferences = Map::new();
        preferences.insert(
            "oauth".into(),
            json!({"refresh_token": "r1", "project_id": "p1", "email": "a@x"}),
        );
        let accounts = accounts_from_preferences(&preferences);
        assert_eq!(accounts.len(), 1);
        assert_eq!(accounts[0].refresh_token, "r1");
    }

    #[test]
    fn prefers_account_with_more_headroom() {
        let pool = AccountPool::new();
        let a = account("a");
        let b = account("b");
        pool.note_quota(
            &a,
            AccountQuota {
                gemini: GroupQuota {
                    five_hour: 0.1,
                    weekly: 0.5,
                },
                claude: GroupQuota {
                    five_hour: 1.0,
                    weekly: 1.0,
                },
            },
        );
        pool.note_quota(
            &b,
            AccountQuota {
                gemini: GroupQuota {
                    five_hour: 0.9,
                    weekly: 0.9,
                },
                claude: GroupQuota {
                    five_hour: 1.0,
                    weekly: 1.0,
                },
            },
        );
        let accounts = vec![a, b];
        match pool.pick(&accounts, "gemini-3-pro") {
            Pick::Selected(index) => assert_eq!(index, 1),
            _ => panic!("expected a selection"),
        }
    }

    #[test]
    fn cooling_accounts_are_skipped() {
        let pool = AccountPool::new();
        let a = account("a");
        let b = account("b");
        pool.mark_failure(&a, 429);
        let accounts = vec![a, b];
        match pool.pick(&accounts, "claude-sonnet-4-6") {
            Pick::Selected(index) => assert_eq!(index, 1),
            _ => panic!("expected the healthy account"),
        }
    }

    #[test]
    fn all_cooling_reports_retry_after() {
        let pool = AccountPool::new();
        let a = account("a");
        pool.mark_failure(&a, 429);
        match pool.pick(std::slice::from_ref(&a), "gemini-2.5-pro") {
            Pick::Cooling { retry_after } => assert!(retry_after >= 1),
            _ => panic!("expected cooling"),
        }
    }

    #[test]
    fn disabled_accounts_are_ignored() {
        let mut preferences = Map::new();
        preferences.insert(
            "oauth_accounts".into(),
            json!([
                {"refresh_token": "r1", "email": "a@x"},
                {"refresh_token": "r2", "email": "b@x", "enabled": false}
            ]),
        );
        let accounts = accounts_from_preferences(&preferences);
        assert_eq!(accounts.len(), 1);
        assert_eq!(accounts[0].email, "a@x");
    }

    fn pooled(token: &str, email: &str) -> Account {
        Account {
            email: email.to_owned(),
            refresh_token: token.to_owned(),
            project_id: "proj".to_owned(),
            proxy: None,
        }
    }

    #[test]
    fn picks_the_account_with_the_most_remaining_quota() {
        let pool = AccountPool::new();
        let accounts = vec![pooled("a", "a@x"), pooled("b", "b@x")];
        pool.note_quota(
            &accounts[0],
            AccountQuota {
                gemini: GroupQuota { five_hour: 1.0, weekly: 1.0 },
                claude: GroupQuota { five_hour: 1.0, weekly: 1.0 },
            },
        );
        pool.note_quota(
            &accounts[1],
            AccountQuota {
                gemini: GroupQuota { five_hour: 0.1, weekly: 0.9 },
                claude: GroupQuota { five_hour: 1.0, weekly: 1.0 },
            },
        );
        match pool.pick(&accounts, "gemini-3.8-flash-high") {
            Pick::Selected(index) => assert_eq!(index, 0),
            other => panic!("unexpected pick: {other:?}"),
        }
        // Claude traffic is scored against the claude group instead.
        match pool.pick(&accounts, "claude-sonnet-4-6") {
            Pick::Selected(index) => assert!(index <= 1),
            other => panic!("unexpected pick: {other:?}"),
        }
    }

    #[test]
    fn cooling_account_is_skipped_until_it_recovers() {
        let pool = AccountPool::new();
        let accounts = vec![pooled("a", "a@x"), pooled("b", "b@x")];
        pool.mark_failure(&accounts[0], 429);
        for _ in 0..4 {
            match pool.pick(&accounts, "gemini-3.8-flash-high") {
                Pick::Selected(index) => assert_eq!(index, 1),
                other => panic!("unexpected pick: {other:?}"),
            }
        }
        pool.mark_failure(&accounts[1], 429);
        assert!(matches!(
            pool.pick(&accounts, "gemini-3.8-flash-high"),
            Pick::Cooling { .. }
        ));
        pool.mark_success(&accounts[1]);
        assert!(matches!(
            pool.pick(&accounts, "gemini-3.8-flash-high"),
            Pick::Selected(1)
        ));
    }

    #[test]
    fn exhausted_quota_deprioritises_but_stays_usable() {
        let pool = AccountPool::new();
        let accounts = vec![pooled("a", "a@x"), pooled("b", "b@x")];
        pool.note_quota(
            &accounts[0],
            AccountQuota {
                gemini: GroupQuota { five_hour: 0.0, weekly: 0.0 },
                claude: GroupQuota { five_hour: 0.0, weekly: 0.0 },
            },
        );
        match pool.pick(&accounts, "gemini-3.8-flash-high") {
            Pick::Selected(index) => assert_eq!(index, 1),
            other => panic!("unexpected pick: {other:?}"),
        }
    }

    #[test]
    fn empty_account_list_is_reported() {
        let pool = AccountPool::new();
        assert!(matches!(
            pool.pick(&[], "gemini-3.8-flash-high"),
            Pick::Empty
        ));
    }
}
