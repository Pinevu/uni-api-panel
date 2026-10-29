use std::path::PathBuf;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use rusqlite::{params, Connection};
use serde_json::{json, Value};
use tokio::sync::mpsc;
use tokio_postgres::{Client, NoTls};

const DEFAULT_WRITE_QUEUE: usize = 4096;

#[derive(Clone)]
pub struct Persistence {
    backend: Arc<Backend>,
    writer: Option<mpsc::Sender<WriteEvent>>,
}

enum Backend {
    Disabled,
    Sqlite { path: PathBuf },
    Postgres { client: Arc<Client> },
}

#[derive(Clone, Debug, Default)]
pub struct RequestStat {
    pub request_id: String,
    pub trace_id: String,
    pub endpoint: String,
    pub client_ip: String,
    pub process_time: f64,
    pub first_response_time: f64,
    pub provider: String,
    pub model: String,
    pub api_key: String,
    pub is_flagged: bool,
    pub text: String,
    pub prompt_tokens: i64,
    pub completion_tokens: i64,
    pub total_tokens: i64,
    /// Prompt tokens served from the upstream provider's prompt cache.
    pub cached_tokens: i64,
    pub prompt_price: f64,
    pub completion_price: f64,
    pub timing_spans: String,
}

#[derive(Clone, Debug, Default)]
pub struct ChannelStat {
    pub request_id: String,
    pub provider: String,
    pub model: String,
    pub api_key: String,
    pub provider_api_key: String,
    pub success: bool,
}

enum WriteEvent {
    Request(RequestStat),
    Channel(ChannelStat),
}

impl Persistence {
    pub async fn initialize(disabled: bool) -> Result<Self, String> {
        if disabled {
            return Ok(Self {
                backend: Arc::new(Backend::Disabled),
                writer: None,
            });
        }
        let backend = match std::env::var("DB_TYPE")
            .unwrap_or_else(|_| "sqlite".into())
            .trim()
            .to_ascii_lowercase()
            .as_str()
        {
            "sqlite" => {
                let path = PathBuf::from(
                    std::env::var("DB_PATH").unwrap_or_else(|_| "./data/stats.db".into()),
                );
                initialize_sqlite(path.clone()).await?;
                Arc::new(Backend::Sqlite { path })
            }
            "postgres" | "postgresql" => {
                let mut config = tokio_postgres::Config::new();
                config
                    .host(std::env::var("DB_HOST").unwrap_or_else(|_| "localhost".into()))
                    .port(
                        std::env::var("DB_PORT")
                            .ok()
                            .and_then(|value| value.parse().ok())
                            .unwrap_or(5432),
                    )
                    .user(std::env::var("DB_USER").unwrap_or_else(|_| "postgres".into()))
                    .password(
                        std::env::var("DB_PASSWORD").unwrap_or_else(|_| "mysecretpassword".into()),
                    )
                    .dbname(std::env::var("DB_NAME").unwrap_or_else(|_| "postgres".into()));
                let (client, connection) = config
                    .connect(NoTls)
                    .await
                    .map_err(|error| format!("connect PostgreSQL stats database: {error}"))?;
                tokio::spawn(async move {
                    if let Err(error) = connection.await {
                        eprintln!(
                            "{}",
                            json!({
                                "event_type": "rust_persistence_connection_error",
                                "error": error.to_string(),
                            })
                        );
                    }
                });
                initialize_postgres(&client).await?;
                Arc::new(Backend::Postgres {
                    client: Arc::new(client),
                })
            }
            other => return Err(format!("unsupported DB_TYPE for Rust runtime: {other}")),
        };
        let queue_capacity = std::env::var("RUST_STATS_WRITE_QUEUE_CAPACITY")
            .ok()
            .and_then(|value| value.parse::<usize>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(DEFAULT_WRITE_QUEUE);
        let (writer, receiver) = mpsc::channel(queue_capacity);
        spawn_writer(backend.clone(), receiver);
        Ok(Self {
            backend,
            writer: Some(writer),
        })
    }

    pub fn disabled(&self) -> bool {
        matches!(self.backend.as_ref(), Backend::Disabled)
    }

    pub fn record_request(&self, stat: RequestStat) {
        let Some(writer) = &self.writer else {
            return;
        };
        if writer.try_send(WriteEvent::Request(stat)).is_err() {
            eprintln!(
                "{}",
                json!({
                    "event_type": "rust_persistence_write_dropped",
                    "record_type": "request_stat",
                })
            );
        }
    }

    pub fn record_channel(&self, stat: ChannelStat) {
        let Some(writer) = &self.writer else {
            return;
        };
        if writer.try_send(WriteEvent::Channel(stat)).is_err() {
            eprintln!(
                "{}",
                json!({
                    "event_type": "rust_persistence_write_dropped",
                    "record_type": "channel_stat",
                })
            );
        }
    }

    pub async fn stats_summary(&self, hours: i64) -> Result<Value, String> {
        match self.backend.as_ref() {
            Backend::Disabled => Ok(json!({"stats": {}})),
            Backend::Sqlite { path } => {
                let path = path.clone();
                tokio::task::spawn_blocking(move || sqlite_stats_summary(&path, hours))
                    .await
                    .map_err(|error| format!("join SQLite stats query: {error}"))?
            }
            Backend::Postgres { client } => postgres_stats_summary(client, hours).await,
        }
    }

    /// Deletes request/channel statistics older than `retention_days`.
    /// Returns `(request_rows, channel_rows)`.
    pub async fn prune_stats(&self, retention_days: i64) -> Result<(u64, u64), String> {
        let days = retention_days.clamp(1, 3650);
        match self.backend.as_ref() {
            Backend::Disabled => Ok((0, 0)),
            Backend::Sqlite { path } => {
                let path = path.clone();
                tokio::task::spawn_blocking(move || sqlite_prune_stats(&path, days))
                    .await
                    .map_err(|error| format!("join SQLite stats prune: {error}"))?
            }
            Backend::Postgres { client } => {
                let requests = client
                    .execute(
                        "DELETE FROM request_stats WHERE timestamp < NOW() - ($1::bigint * INTERVAL '1 day')",
                        &[&days],
                    )
                    .await
                    .map_err(|error| format!("prune PostgreSQL request stats: {error}"))?;
                let channels = client
                    .execute(
                        "DELETE FROM channel_stats WHERE timestamp < NOW() - ($1::bigint * INTERVAL '1 day')",
                        &[&days],
                    )
                    .await
                    .map_err(|error| format!("prune PostgreSQL channel stats: {error}"))?;
                Ok((requests, channels))
            }
        }
    }

    /// Moves historical statistics from a renamed channel onto its new name so
    /// the monitor page keeps a single continuous record.
    pub async fn rename_channel(&self, from: &str, to: &str) -> Result<u64, String> {
        if from == to || from.is_empty() || to.is_empty() {
            return Ok(0);
        }
        match self.backend.as_ref() {
            Backend::Disabled => Ok(0),
            Backend::Sqlite { path } => {
                let path = path.clone();
                let from = from.to_owned();
                let to = to.to_owned();
                tokio::task::spawn_blocking(move || sqlite_rename_channel(&path, &from, &to))
                    .await
                    .map_err(|error| format!("join SQLite channel rename: {error}"))?
            }
            Backend::Postgres { client } => {
                let channels = client
                    .execute(
                        "UPDATE channel_stats SET provider = $2 WHERE provider = $1",
                        &[&from, &to],
                    )
                    .await
                    .map_err(|error| format!("rename PostgreSQL channel stats: {error}"))?;
                client
                    .execute(
                        "UPDATE request_stats SET provider = $2 WHERE provider = $1",
                        &[&from, &to],
                    )
                    .await
                    .map_err(|error| format!("rename PostgreSQL request stats: {error}"))?;
                Ok(channels)
            }
        }
    }

    pub async fn token_usage(
        &self,
        api_key: Option<&str>,
        model: Option<&str>,
        start_unix: Option<i64>,
        end_unix: Option<i64>,
    ) -> Result<Value, String> {
        match self.backend.as_ref() {
            Backend::Disabled => Ok(json!({"usage": [], "query_details": {}})),
            Backend::Sqlite { path } => {
                let path = path.clone();
                let api_key = api_key.map(str::to_owned);
                let model = model.map(str::to_owned);
                tokio::task::spawn_blocking(move || {
                    sqlite_token_usage(
                        &path,
                        api_key.as_deref(),
                        model.as_deref(),
                        start_unix,
                        end_unix,
                    )
                })
                .await
                .map_err(|error| format!("join SQLite token query: {error}"))?
            }
            Backend::Postgres { client } => {
                postgres_token_usage(client, api_key, model, start_unix, end_unix).await
            }
        }
    }

    pub async fn channel_key_rankings(
        &self,
        provider: &str,
        start_unix: Option<i64>,
        end_unix: Option<i64>,
    ) -> Result<Value, String> {
        match self.backend.as_ref() {
            Backend::Disabled => Ok(json!({"rankings": [], "query_details": {}})),
            Backend::Sqlite { path } => {
                let path = path.clone();
                let provider = provider.to_owned();
                tokio::task::spawn_blocking(move || {
                    sqlite_channel_rankings(&path, &provider, start_unix, end_unix)
                })
                .await
                .map_err(|error| format!("join SQLite channel query: {error}"))?
            }
            Backend::Postgres { client } => {
                postgres_channel_rankings(client, provider, start_unix, end_unix).await
            }
        }
    }

    pub async fn total_cost(&self, api_key: &str, start_unix: i64) -> Result<f64, String> {
        match self.backend.as_ref() {
            Backend::Disabled => Ok(0.0),
            Backend::Sqlite { path } => {
                let path = path.clone();
                let api_key = api_key.to_owned();
                tokio::task::spawn_blocking(move || {
                    let connection = sqlite_connection(&path)?;
                    connection
                        .query_row(
                            "SELECT COALESCE(SUM((COALESCE(prompt_tokens, 0) * COALESCE(prompt_price, 0.3) + COALESCE(completion_tokens, 0) * COALESCE(completion_price, 1.0)) / 1000000.0), 0.0) FROM request_stats WHERE api_key = ?1 AND timestamp >= datetime(?2, 'unixepoch')",
                            params![api_key, start_unix],
                            |row| row.get::<_, f64>(0),
                        )
                        .map_err(|error| format!("query SQLite API-key cost: {error}"))
                })
                .await
                .map_err(|error| format!("join SQLite API-key cost query: {error}"))?
            }
            Backend::Postgres { client } => client
                .query_one(
                    "SELECT COALESCE(SUM((COALESCE(prompt_tokens, 0) * COALESCE(prompt_price, 0.3) + COALESCE(completion_tokens, 0) * COALESCE(completion_price, 1.0)) / 1000000.0), 0.0)::DOUBLE PRECISION FROM request_stats WHERE api_key = $1 AND timestamp >= to_timestamp($2)",
                    &[&api_key, &start_unix],
                )
                .await
                .map(|row| row.get::<_, f64>(0))
                .map_err(|error| format!("query PostgreSQL API-key cost: {error}")),
        }
    }
}

fn spawn_writer(backend: Arc<Backend>, mut receiver: mpsc::Receiver<WriteEvent>) {
    tokio::spawn(async move {
        while let Some(event) = receiver.recv().await {
            let result = match (backend.as_ref(), event) {
                (Backend::Disabled, _) => Ok(()),
                (Backend::Sqlite { path }, event) => {
                    let path = path.clone();
                    tokio::task::spawn_blocking(move || sqlite_write(&path, event))
                        .await
                        .map_err(|error| format!("join SQLite stats writer: {error}"))
                        .and_then(|result| result)
                }
                (Backend::Postgres { client }, event) => postgres_write(client, event).await,
            };
            if let Err(error) = result {
                eprintln!(
                    "{}",
                    json!({
                        "event_type": "rust_persistence_write_error",
                        "error": error,
                    })
                );
            }
        }
    });
}

async fn initialize_sqlite(path: PathBuf) -> Result<(), String> {
    tokio::task::spawn_blocking(move || {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|error| format!("create SQLite stats directory: {error}"))?;
        }
        let connection = sqlite_connection(&path)?;
        connection
            .execute_batch(SQLITE_SCHEMA)
            .map_err(|error| format!("initialize SQLite stats schema: {error}"))?;
        // Existing databases predate the cache column; add it in place. SQLite has
        // no `ADD COLUMN IF NOT EXISTS`, so a duplicate-column error is expected
        // and means the migration already ran.
        if let Err(error) = connection.execute(
            "ALTER TABLE request_stats ADD COLUMN cached_tokens INTEGER DEFAULT 0",
            [],
        ) {
            let message = error.to_string();
            if !message.contains("duplicate column name") {
                return Err(format!("migrate SQLite cached_tokens column: {message}"));
            }
        }
        Ok(())
    })
    .await
    .map_err(|error| format!("join SQLite schema initialization: {error}"))?
}

fn sqlite_connection(path: &PathBuf) -> Result<Connection, String> {
    let connection = Connection::open(path)
        .map_err(|error| format!("open SQLite stats database {}: {error}", path.display()))?;
    connection
        .busy_timeout(std::time::Duration::from_secs(5))
        .map_err(|error| format!("configure SQLite busy timeout: {error}"))?;
    connection
        .pragma_update(None, "journal_mode", "WAL")
        .map_err(|error| format!("configure SQLite WAL: {error}"))?;
    Ok(connection)
}

fn sqlite_prune_stats(path: &PathBuf, days: i64) -> Result<(u64, u64), String> {
    let connection = sqlite_connection(path)?;
    let modifier = format!("-{days} days");
    let requests = connection
        .execute(
            "DELETE FROM request_stats WHERE timestamp < datetime('now', ?1)",
            params![modifier],
        )
        .map_err(|error| format!("prune SQLite request stats: {error}"))?;
    let channels = connection
        .execute(
            "DELETE FROM channel_stats WHERE timestamp < datetime('now', ?1)",
            params![modifier],
        )
        .map_err(|error| format!("prune SQLite channel stats: {error}"))?;
    Ok((requests as u64, channels as u64))
}

fn sqlite_rename_channel(path: &PathBuf, from: &str, to: &str) -> Result<u64, String> {
    let connection = sqlite_connection(path)?;
    let channels = connection
        .execute(
            "UPDATE channel_stats SET provider = ?2 WHERE provider = ?1",
            params![from, to],
        )
        .map_err(|error| format!("rename SQLite channel stats: {error}"))?;
    connection
        .execute(
            "UPDATE request_stats SET provider = ?2 WHERE provider = ?1",
            params![from, to],
        )
        .map_err(|error| format!("rename SQLite request stats: {error}"))?;
    Ok(channels as u64)
}

fn sqlite_write(path: &PathBuf, event: WriteEvent) -> Result<(), String> {
    let connection = sqlite_connection(path)?;
    match event {
        WriteEvent::Request(stat) => connection
            .execute(
                "INSERT INTO request_stats (request_id, trace_id, endpoint, client_ip, process_time, first_response_time, provider, model, api_key, is_flagged, text, prompt_tokens, completion_tokens, total_tokens, cached_tokens, prompt_price, completion_price, timing_spans) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18)",
                params![
                    stat.request_id,
                    stat.trace_id,
                    stat.endpoint,
                    stat.client_ip,
                    stat.process_time,
                    stat.first_response_time,
                    stat.provider,
                    stat.model,
                    stat.api_key,
                    stat.is_flagged,
                    stat.text,
                    stat.prompt_tokens,
                    stat.completion_tokens,
                    stat.total_tokens,
                    stat.cached_tokens,
                    stat.prompt_price,
                    stat.completion_price,
                    stat.timing_spans,
                ],
            )
            .map(|_| ())
            .map_err(|error| format!("insert SQLite request stat: {error}")),
        WriteEvent::Channel(stat) => connection
            .execute(
                "INSERT INTO channel_stats (request_id, provider, model, api_key, provider_api_key, success) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                params![
                    stat.request_id,
                    stat.provider,
                    stat.model,
                    stat.api_key,
                    stat.provider_api_key,
                    stat.success,
                ],
            )
            .map(|_| ())
            .map_err(|error| format!("insert SQLite channel stat: {error}")),
    }
}

async fn initialize_postgres(client: &Client) -> Result<(), String> {
    client
        .batch_execute(POSTGRES_SCHEMA)
        .await
        .map_err(|error| format!("initialize PostgreSQL stats schema: {error}"))?;
    client
        .execute(
            "ALTER TABLE request_stats ADD COLUMN IF NOT EXISTS cached_tokens BIGINT DEFAULT 0",
            &[],
        )
        .await
        .map_err(|error| format!("migrate PostgreSQL cached_tokens column: {error}"))?;
    Ok(())
}

async fn postgres_write(client: &Client, event: WriteEvent) -> Result<(), String> {
    match event {
        WriteEvent::Request(stat) => client
            .execute(
                "INSERT INTO request_stats (request_id, trace_id, endpoint, client_ip, process_time, first_response_time, provider, model, api_key, is_flagged, text, prompt_tokens, completion_tokens, total_tokens, cached_tokens, prompt_price, completion_price, timing_spans) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18)",
                &[
                    &stat.request_id,
                    &stat.trace_id,
                    &stat.endpoint,
                    &stat.client_ip,
                    &stat.process_time,
                    &stat.first_response_time,
                    &stat.provider,
                    &stat.model,
                    &stat.api_key,
                    &stat.is_flagged,
                    &stat.text,
                    &stat.prompt_tokens,
                    &stat.completion_tokens,
                    &stat.total_tokens,
                    &stat.cached_tokens,
                    &stat.prompt_price,
                    &stat.completion_price,
                    &stat.timing_spans,
                ],
            )
            .await
            .map(|_| ())
            .map_err(|error| format!("insert PostgreSQL request stat: {error}")),
        WriteEvent::Channel(stat) => client
            .execute(
                "INSERT INTO channel_stats (request_id, provider, model, api_key, provider_api_key, success) VALUES ($1,$2,$3,$4,$5,$6)",
                &[
                    &stat.request_id,
                    &stat.provider,
                    &stat.model,
                    &stat.api_key,
                    &stat.provider_api_key,
                    &stat.success,
                ],
            )
            .await
            .map(|_| ())
            .map_err(|error| format!("insert PostgreSQL channel stat: {error}")),
    }
}

fn sqlite_stats_summary(path: &PathBuf, hours: i64) -> Result<Value, String> {
    let connection = sqlite_connection(path)?;
    let cutoff = unix_seconds().saturating_sub(hours.max(1).saturating_mul(3600));
    let channel_model = sqlite_grouped_success(
        &connection,
        "SELECT provider, model, COUNT(*), SUM(CASE WHEN success THEN 1 ELSE 0 END) FROM channel_stats WHERE timestamp >= datetime(?1, 'unixepoch') GROUP BY provider, model",
        cutoff,
        true,
    )?;
    let channel = sqlite_grouped_success(
        &connection,
        "SELECT provider, '', COUNT(*), SUM(CASE WHEN success THEN 1 ELSE 0 END) FROM channel_stats WHERE timestamp >= datetime(?1, 'unixepoch') GROUP BY provider",
        cutoff,
        false,
    )?;
    let cache_model = sqlite_cache_rates(&connection, cutoff, true)?;
    let cache_channel = sqlite_cache_rates(&connection, cutoff, false)?;
    Ok(json!({
        "time_range": format!("Last {} hours", hours.max(1)),
        "channel_model_success_rates": merge_cache_rates(channel_model, &cache_model, true),
        "channel_success_rates": merge_cache_rates(channel, &cache_channel, false),
        "model_request_counts": sqlite_grouped_counts(&connection, "model", cutoff)?,
        "endpoint_request_counts": sqlite_grouped_counts(&connection, "endpoint", cutoff)?,
        "ip_request_counts": sqlite_grouped_counts(&connection, "client_ip", cutoff)?,
    }))
}

/// Prompt-cache hit accounting: `(provider, model, prompt_tokens, cached_tokens)`.
/// `request_stats` carries the per-request token split, `channel_stats` only has
/// success flags, so cache rates are aggregated separately and merged in.
fn sqlite_cache_rates(
    connection: &Connection,
    cutoff: i64,
    include_model: bool,
) -> Result<Vec<(String, String, i64, i64)>, String> {
    let sql = if include_model {
        "SELECT provider, model, COALESCE(SUM(prompt_tokens), 0), COALESCE(SUM(cached_tokens), 0) FROM request_stats WHERE timestamp >= datetime(?1, 'unixepoch') GROUP BY provider, model"
    } else {
        "SELECT provider, '', COALESCE(SUM(prompt_tokens), 0), COALESCE(SUM(cached_tokens), 0) FROM request_stats WHERE timestamp >= datetime(?1, 'unixepoch') GROUP BY provider"
    };
    let mut statement = connection
        .prepare(sql)
        .map_err(|error| format!("prepare SQLite cache query: {error}"))?;
    let rows = statement
        .query_map([cutoff], |row| {
            Ok((
                row.get::<_, Option<String>>(0)?.unwrap_or_default(),
                row.get::<_, Option<String>>(1)?.unwrap_or_default(),
                row.get::<_, Option<i64>>(2)?.unwrap_or(0),
                row.get::<_, Option<i64>>(3)?.unwrap_or(0),
            ))
        })
        .map_err(|error| format!("query SQLite cache stats: {error}"))?;
    Ok(rows.filter_map(Result::ok).collect())
}

/// Attaches `prompt_tokens` / `cached_tokens` / `cache_hit_rate` to success rows.
fn merge_cache_rates(
    rows: Vec<Value>,
    cache: &[(String, String, i64, i64)],
    include_model: bool,
) -> Vec<Value> {
    rows.into_iter()
        .map(|mut row| {
            let provider = row
                .get("provider")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_owned();
            let model = if include_model {
                row.get("model")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_owned()
            } else {
                String::new()
            };
            let matched = cache
                .iter()
                .find(|(name, cached_model, _, _)| *name == provider && *cached_model == model);
            let (prompt, cached) = matched
                .map(|(_, _, prompt, cached)| (*prompt, *cached))
                .unwrap_or((0, 0));
            let rate = if prompt > 0 {
                cached as f64 / prompt as f64
            } else {
                0.0
            };
            if let Some(object) = row.as_object_mut() {
                object.insert("prompt_tokens".into(), json!(prompt));
                object.insert("cached_tokens".into(), json!(cached));
                object.insert("cache_hit_rate".into(), json!(rate));
            }
            row
        })
        .collect()
}

fn sqlite_grouped_success(
    connection: &Connection,
    sql: &str,
    cutoff: i64,
    include_model: bool,
) -> Result<Vec<Value>, String> {
    let mut statement = connection
        .prepare(sql)
        .map_err(|error| format!("prepare SQLite success query: {error}"))?;
    let rows = statement
        .query_map([cutoff], |row| {
            let provider: String = row.get(0)?;
            let model: String = row.get(1)?;
            let total: i64 = row.get(2)?;
            let success: i64 = row.get::<_, Option<i64>>(3)?.unwrap_or(0);
            Ok((provider, model, total, success))
        })
        .map_err(|error| format!("query SQLite success stats: {error}"))?;
    let mut values = rows
        .filter_map(Result::ok)
        .map(|(provider, model, total, success)| {
            let rate = if total > 0 {
                success as f64 / total as f64
            } else {
                0.0
            };
            if include_model {
                json!({"provider": provider, "model": model, "success_rate": rate, "total_requests": total})
            } else {
                json!({"provider": provider, "success_rate": rate, "total_requests": total})
            }
        })
        .collect::<Vec<_>>();
    values.sort_by(|left, right| {
        right["success_rate"]
            .as_f64()
            .partial_cmp(&left["success_rate"].as_f64())
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    Ok(values)
}

fn sqlite_grouped_counts(
    connection: &Connection,
    column: &str,
    cutoff: i64,
) -> Result<Vec<Value>, String> {
    let sql = format!(
        "SELECT {column}, COUNT(*) FROM request_stats WHERE timestamp >= datetime(?1, 'unixepoch') GROUP BY {column} ORDER BY COUNT(*) DESC"
    );
    let mut statement = connection
        .prepare(&sql)
        .map_err(|error| format!("prepare SQLite grouped count: {error}"))?;
    let rows = statement
        .query_map([cutoff], |row| {
            Ok((
                row.get::<_, Option<String>>(0)?.unwrap_or_default(),
                row.get::<_, i64>(1)?,
            ))
        })
        .map_err(|error| format!("query SQLite grouped count: {error}"))?;
    let key = if column == "client_ip" { "ip" } else { column };
    Ok(rows
        .filter_map(Result::ok)
        .map(|(value, count)| json!({key: value, "count": count}))
        .collect())
}

fn sqlite_token_usage(
    path: &PathBuf,
    api_key: Option<&str>,
    model: Option<&str>,
    start_unix: Option<i64>,
    end_unix: Option<i64>,
) -> Result<Value, String> {
    let connection = sqlite_connection(path)?;
    let mut statement = connection
        .prepare(
            "SELECT api_key, model, SUM(prompt_tokens), SUM(completion_tokens), SUM(total_tokens), COUNT(*) FROM request_stats WHERE (?1 IS NULL OR api_key = ?1) AND (?2 IS NULL OR model = ?2) AND (?3 IS NULL OR timestamp >= datetime(?3, 'unixepoch')) AND (?4 IS NULL OR timestamp < datetime(?4, 'unixepoch')) AND model IS NOT NULL AND model != '' GROUP BY api_key, model",
        )
        .map_err(|error| format!("prepare SQLite token query: {error}"))?;
    let rows = statement
        .query_map(params![api_key, model, start_unix, end_unix], |row| {
            Ok(json!({
                "api_key_prefix": mask_key(&row.get::<_, String>(0)?),
                "model": row.get::<_, String>(1)?,
                "total_prompt_tokens": row.get::<_, Option<i64>>(2)?.unwrap_or(0),
                "total_completion_tokens": row.get::<_, Option<i64>>(3)?.unwrap_or(0),
                "total_tokens": row.get::<_, Option<i64>>(4)?.unwrap_or(0),
                "request_count": row.get::<_, i64>(5)?,
            }))
        })
        .map_err(|error| format!("query SQLite token usage: {error}"))?;
    Ok(json!({
        "usage": rows.filter_map(Result::ok).collect::<Vec<_>>(),
        "query_details": {
            "api_key_filter": api_key,
            "model_filter": model,
            "start_datetime": start_unix,
            "end_datetime": end_unix,
        }
    }))
}

fn sqlite_channel_rankings(
    path: &PathBuf,
    provider: &str,
    start_unix: Option<i64>,
    end_unix: Option<i64>,
) -> Result<Value, String> {
    let connection = sqlite_connection(path)?;
    let start = start_unix.unwrap_or_else(|| unix_seconds().saturating_sub(86_400));
    let mut statement = connection
        .prepare(
            "SELECT provider_api_key, COUNT(*), SUM(CASE WHEN success THEN 1 ELSE 0 END) FROM channel_stats WHERE provider = ?1 AND timestamp >= datetime(?2, 'unixepoch') AND (?3 IS NULL OR timestamp < datetime(?3, 'unixepoch')) AND provider_api_key IS NOT NULL GROUP BY provider_api_key",
        )
        .map_err(|error| format!("prepare SQLite channel ranking: {error}"))?;
    let rows = statement
        .query_map(params![provider, start, end_unix], |row| {
            let total = row.get::<_, i64>(1)?;
            let success = row.get::<_, Option<i64>>(2)?.unwrap_or(0);
            Ok(json!({
                "api_key": row.get::<_, String>(0)?,
                "success_count": success,
                "total_requests": total,
                "success_rate": if total > 0 { success as f64 / total as f64 } else { 0.0 },
            }))
        })
        .map_err(|error| format!("query SQLite channel ranking: {error}"))?;
    let mut rankings = rows.filter_map(Result::ok).collect::<Vec<_>>();
    rankings.sort_by(|left, right| {
        right["success_rate"]
            .as_f64()
            .partial_cmp(&left["success_rate"].as_f64())
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    Ok(json!({
        "rankings": rankings,
        "query_details": {"provider_name": provider, "start_datetime": start, "end_datetime": end_unix},
    }))
}

async fn postgres_stats_summary(client: &Client, hours: i64) -> Result<Value, String> {
    let channel_model = client
        .query(
            "SELECT provider, model, COUNT(*), SUM(CASE WHEN success THEN 1 ELSE 0 END) FROM channel_stats WHERE timestamp >= NOW() - ($1::text || ' hours')::interval GROUP BY provider, model",
            &[&hours.max(1)],
        )
        .await
        .map_err(|error| format!("query PostgreSQL channel/model stats: {error}"))?;
    let channel = client
        .query(
            "SELECT provider, COUNT(*), SUM(CASE WHEN success THEN 1 ELSE 0 END) FROM channel_stats WHERE timestamp >= NOW() - ($1::text || ' hours')::interval GROUP BY provider",
            &[&hours.max(1)],
        )
        .await
        .map_err(|error| format!("query PostgreSQL channel stats: {error}"))?;
    async fn counts(client: &Client, column: &str, hours: i64) -> Result<Vec<Value>, String> {
        let sql = format!(
            "SELECT {column}, COUNT(*) FROM request_stats WHERE timestamp >= NOW() - ($1::text || ' hours')::interval GROUP BY {column} ORDER BY COUNT(*) DESC"
        );
        let rows = client
            .query(&sql, &[&hours])
            .await
            .map_err(|error| format!("query PostgreSQL grouped count: {error}"))?;
        let key = if column == "client_ip" { "ip" } else { column };
        Ok(rows
            .into_iter()
            .map(|row| {
                json!({
                    key: row.get::<_, Option<String>>(0).unwrap_or_default(),
                    "count": row.get::<_, i64>(1),
                })
            })
            .collect())
    }
    let success_rows = |rows: Vec<tokio_postgres::Row>, include_model: bool| {
        rows.into_iter()
            .map(|row| {
                let total = row.get::<_, i64>(if include_model { 2 } else { 1 });
                let success = row
                    .get::<_, Option<i64>>(if include_model { 3 } else { 2 })
                    .unwrap_or(0);
                if include_model {
                    json!({
                        "provider": row.get::<_, String>(0),
                        "model": row.get::<_, String>(1),
                        "success_rate": if total > 0 { success as f64 / total as f64 } else { 0.0 },
                        "total_requests": total,
                    })
                } else {
                    json!({
                        "provider": row.get::<_, String>(0),
                        "success_rate": if total > 0 { success as f64 / total as f64 } else { 0.0 },
                        "total_requests": total,
                    })
                }
            })
            .collect::<Vec<_>>()
    };
    let cache_model = postgres_cache_rates(client, hours, true).await?;
    let cache_channel = postgres_cache_rates(client, hours, false).await?;
    Ok(json!({
        "time_range": format!("Last {} hours", hours.max(1)),
        "channel_model_success_rates": merge_cache_rates(success_rows(channel_model, true), &cache_model, true),
        "channel_success_rates": merge_cache_rates(success_rows(channel, false), &cache_channel, false),
        "model_request_counts": counts(client, "model", hours.max(1)).await?,
        "endpoint_request_counts": counts(client, "endpoint", hours.max(1)).await?,
        "ip_request_counts": counts(client, "client_ip", hours.max(1)).await?,
    }))
}

async fn postgres_cache_rates(
    client: &Client,
    hours: i64,
    include_model: bool,
) -> Result<Vec<(String, String, i64, i64)>, String> {
    let sql = if include_model {
        "SELECT provider, model, COALESCE(SUM(prompt_tokens), 0)::bigint, COALESCE(SUM(cached_tokens), 0)::bigint FROM request_stats WHERE timestamp >= NOW() - ($1::text || ' hours')::interval GROUP BY provider, model"
    } else {
        "SELECT provider, ''::text, COALESCE(SUM(prompt_tokens), 0)::bigint, COALESCE(SUM(cached_tokens), 0)::bigint FROM request_stats WHERE timestamp >= NOW() - ($1::text || ' hours')::interval GROUP BY provider"
    };
    let rows = client
        .query(sql, &[&hours.max(1)])
        .await
        .map_err(|error| format!("query PostgreSQL cache stats: {error}"))?;
    Ok(rows
        .into_iter()
        .map(|row| {
            (
                row.get::<_, Option<String>>(0).unwrap_or_default(),
                row.get::<_, Option<String>>(1).unwrap_or_default(),
                row.get::<_, Option<i64>>(2).unwrap_or(0),
                row.get::<_, Option<i64>>(3).unwrap_or(0),
            )
        })
        .collect())
}

async fn postgres_token_usage(
    client: &Client,
    api_key: Option<&str>,
    model: Option<&str>,
    start_unix: Option<i64>,
    end_unix: Option<i64>,
) -> Result<Value, String> {
    let rows = client
        .query(
            "SELECT api_key, model, SUM(prompt_tokens), SUM(completion_tokens), SUM(total_tokens), COUNT(*) FROM request_stats WHERE ($1::text IS NULL OR api_key = $1) AND ($2::text IS NULL OR model = $2) AND ($3::bigint IS NULL OR timestamp >= to_timestamp($3)) AND ($4::bigint IS NULL OR timestamp < to_timestamp($4)) AND model IS NOT NULL AND model != '' GROUP BY api_key, model",
            &[&api_key, &model, &start_unix, &end_unix],
        )
        .await
        .map_err(|error| format!("query PostgreSQL token usage: {error}"))?;
    Ok(json!({
        "usage": rows.into_iter().map(|row| {
            let key = row.get::<_, String>(0);
            json!({
                "api_key_prefix": mask_key(&key),
                "model": row.get::<_, String>(1),
                "total_prompt_tokens": row.get::<_, Option<i64>>(2).unwrap_or(0),
                "total_completion_tokens": row.get::<_, Option<i64>>(3).unwrap_or(0),
                "total_tokens": row.get::<_, Option<i64>>(4).unwrap_or(0),
                "request_count": row.get::<_, i64>(5),
            })
        }).collect::<Vec<_>>(),
        "query_details": {
            "api_key_filter": api_key,
            "model_filter": model,
            "start_datetime": start_unix,
            "end_datetime": end_unix,
        }
    }))
}

async fn postgres_channel_rankings(
    client: &Client,
    provider: &str,
    start_unix: Option<i64>,
    end_unix: Option<i64>,
) -> Result<Value, String> {
    let start = start_unix.unwrap_or_else(|| unix_seconds().saturating_sub(86_400));
    let rows = client
        .query(
            "SELECT provider_api_key, COUNT(*), SUM(CASE WHEN success THEN 1 ELSE 0 END) FROM channel_stats WHERE provider = $1 AND timestamp >= to_timestamp($2) AND ($3::bigint IS NULL OR timestamp < to_timestamp($3)) AND provider_api_key IS NOT NULL GROUP BY provider_api_key",
            &[&provider, &start, &end_unix],
        )
        .await
        .map_err(|error| format!("query PostgreSQL channel rankings: {error}"))?;
    let mut rankings = rows
        .into_iter()
        .map(|row| {
            let total = row.get::<_, i64>(1);
            let success = row.get::<_, Option<i64>>(2).unwrap_or(0);
            json!({
                "api_key": row.get::<_, String>(0),
                "success_count": success,
                "total_requests": total,
                "success_rate": if total > 0 { success as f64 / total as f64 } else { 0.0 },
            })
        })
        .collect::<Vec<_>>();
    rankings.sort_by(|left, right| {
        right["success_rate"]
            .as_f64()
            .partial_cmp(&left["success_rate"].as_f64())
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    Ok(json!({
        "rankings": rankings,
        "query_details": {"provider_name": provider, "start_datetime": start, "end_datetime": end_unix},
    }))
}

fn mask_key(value: &str) -> String {
    if value.len() > 7 {
        let suffix = value
            .char_indices()
            .rev()
            .nth(3)
            .map(|(index, _)| &value[index..])
            .unwrap_or(value);
        format!("{}...{suffix}", &value[..7.min(value.len())])
    } else {
        value.to_owned()
    }
}

/// How many days of request/channel statistics are kept.  Configurable via
/// `STATS_RETENTION_DAYS` (default 30, clamped to 1..=3650).
pub fn retention_days() -> i64 {
    std::env::var("STATS_RETENTION_DAYS")
        .ok()
        .and_then(|value| value.trim().parse::<i64>().ok())
        .filter(|days| *days >= 1)
        .unwrap_or(30)
        .min(3650)
}

fn unix_seconds() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
        .min(i64::MAX as u64) as i64
}

const SQLITE_SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS request_stats (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    request_id TEXT,
    trace_id TEXT,
    endpoint TEXT,
    client_ip TEXT,
    process_time REAL,
    first_response_time REAL,
    provider TEXT,
    model TEXT,
    api_key TEXT,
    is_flagged INTEGER DEFAULT 0,
    text TEXT,
    prompt_tokens INTEGER DEFAULT 0,
    completion_tokens INTEGER DEFAULT 0,
    total_tokens INTEGER DEFAULT 0,
    cached_tokens INTEGER DEFAULT 0,
    prompt_price REAL DEFAULT 0,
    completion_price REAL DEFAULT 0,
    timing_spans TEXT,
    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS ix_request_stats_trace_id ON request_stats(trace_id);
CREATE INDEX IF NOT EXISTS ix_request_stats_provider ON request_stats(provider);
CREATE INDEX IF NOT EXISTS ix_request_stats_model ON request_stats(model);
CREATE INDEX IF NOT EXISTS ix_request_stats_api_key ON request_stats(api_key);
CREATE INDEX IF NOT EXISTS ix_request_stats_timestamp ON request_stats(timestamp);
CREATE TABLE IF NOT EXISTS channel_stats (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    request_id TEXT,
    provider TEXT,
    model TEXT,
    api_key TEXT,
    provider_api_key TEXT,
    success INTEGER DEFAULT 0,
    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX IF NOT EXISTS ix_channel_stats_provider ON channel_stats(provider);
CREATE INDEX IF NOT EXISTS ix_channel_stats_model ON channel_stats(model);
CREATE INDEX IF NOT EXISTS ix_channel_stats_provider_api_key ON channel_stats(provider_api_key);
CREATE INDEX IF NOT EXISTS ix_channel_stats_timestamp ON channel_stats(timestamp);
"#;

const POSTGRES_SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS request_stats (
    id BIGSERIAL PRIMARY KEY,
    request_id VARCHAR,
    trace_id VARCHAR,
    endpoint VARCHAR,
    client_ip VARCHAR,
    process_time DOUBLE PRECISION,
    first_response_time DOUBLE PRECISION,
    provider VARCHAR,
    model VARCHAR,
    api_key VARCHAR,
    is_flagged BOOLEAN DEFAULT FALSE,
    text TEXT,
    prompt_tokens BIGINT DEFAULT 0,
    completion_tokens BIGINT DEFAULT 0,
    total_tokens BIGINT DEFAULT 0,
    cached_tokens BIGINT DEFAULT 0,
    prompt_price DOUBLE PRECISION DEFAULT 0,
    completion_price DOUBLE PRECISION DEFAULT 0,
    timing_spans TEXT,
    timestamp TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS ix_request_stats_trace_id ON request_stats(trace_id);
CREATE INDEX IF NOT EXISTS ix_request_stats_provider ON request_stats(provider);
CREATE INDEX IF NOT EXISTS ix_request_stats_model ON request_stats(model);
CREATE INDEX IF NOT EXISTS ix_request_stats_api_key ON request_stats(api_key);
CREATE INDEX IF NOT EXISTS ix_request_stats_timestamp ON request_stats(timestamp);
CREATE TABLE IF NOT EXISTS channel_stats (
    id BIGSERIAL PRIMARY KEY,
    request_id VARCHAR,
    provider VARCHAR,
    model VARCHAR,
    api_key VARCHAR,
    provider_api_key VARCHAR,
    success BOOLEAN DEFAULT FALSE,
    timestamp TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS ix_channel_stats_provider ON channel_stats(provider);
CREATE INDEX IF NOT EXISTS ix_channel_stats_model ON channel_stats(model);
CREATE INDEX IF NOT EXISTS ix_channel_stats_provider_api_key ON channel_stats(provider_api_key);
CREATE INDEX IF NOT EXISTS ix_channel_stats_timestamp ON channel_stats(timestamp);
"#;

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn disabled_backend_matches_python_empty_contract() {
        let persistence = Persistence::initialize(true).await.unwrap();
        assert!(persistence.disabled());
        assert_eq!(
            persistence.stats_summary(24).await.unwrap(),
            json!({"stats": {}})
        );
    }

    #[test]
    fn cache_rates_attach_hit_rate_to_matching_rows_only() {
        let rows = vec![
            json!({"provider": "alpha", "success_rate": 1.0}),
            json!({"provider": "beta", "success_rate": 0.5}),
            json!({"provider": "gamma", "success_rate": 0.9}),
        ];
        let cache = vec![
            ("alpha".to_string(), String::new(), 1000, 800),
            ("beta".to_string(), String::new(), 0, 0),
        ];
        let merged = merge_cache_rates(rows, &cache, false);

        assert_eq!(merged[0]["prompt_tokens"], json!(1000));
        assert_eq!(merged[0]["cached_tokens"], json!(800));
        assert_eq!(merged[0]["cache_hit_rate"], json!(0.8));

        // Zero prompt tokens must not divide by zero or report a fake hit rate.
        assert_eq!(merged[1]["prompt_tokens"], json!(0));
        assert_eq!(merged[1]["cache_hit_rate"], json!(0.0));

        // A channel with no cache rows still gets explicit zeros for the UI.
        assert_eq!(merged[2]["prompt_tokens"], json!(0));
        assert_eq!(merged[2]["cached_tokens"], json!(0));
        assert_eq!(merged[2]["cache_hit_rate"], json!(0.0));
    }

    #[tokio::test]
    async fn retention_prunes_old_rows_and_rename_moves_history() {
        let dir = std::env::temp_dir().join(format!("uni-api-stats-{}", std::process::id()));
        let _ = std::fs::create_dir_all(&dir);
        let path = dir.join("stats.db");
        let _ = std::fs::remove_file(&path);
        initialize_sqlite(path.clone()).await.unwrap();

        let connection = sqlite_connection(&path).unwrap();
        for (request, offset) in [("r1", "-40 days"), ("r2", "0 days")] {
            connection
                .execute(
                    "INSERT INTO channel_stats (request_id, provider, model, api_key, provider_api_key, success, timestamp) VALUES (?1, 'old-name', 'm', 'k', 'pk', 1, datetime('now', ?2))",
                    params![request, offset],
                )
                .unwrap();
        }
        drop(connection);

        let persistence = Persistence {
            backend: Arc::new(Backend::Sqlite { path: path.clone() }),
            writer: None,
        };
        assert_eq!(persistence.prune_stats(30).await.unwrap(), (0, 1));
        assert_eq!(persistence.rename_channel("old-name", "new-name").await.unwrap(), 1);

        let connection = sqlite_connection(&path).unwrap();
        let remaining: i64 = connection
            .query_row(
                "SELECT COUNT(*) FROM channel_stats WHERE provider = 'new-name'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        drop(connection);
        assert_eq!(remaining, 1);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn masks_long_keys_without_exposing_the_middle() {
        assert_eq!(mask_key("sk-1234567890"), "sk-1234...7890");
        assert_eq!(mask_key("short"), "short");
    }
}
