use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use chrono::Utc;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::types::MessagesRequest;

#[derive(Debug, Clone)]
pub struct ResponseCacheConfig {
    pub enabled: bool,
    pub dir: PathBuf,
    pub ttl_seconds: u64,
    pub cleanup_interval_seconds: u64,
}

#[derive(Debug, Clone)]
pub struct ResponseCache {
    config: ResponseCacheConfig,
    cleanup_started: Arc<Mutex<bool>>,
}

#[derive(Debug, Serialize, Deserialize)]
struct CacheEnvelope {
    version: u8,
    created_at: i64,
    expires_at: i64,
    kind: CacheKind,
    response: serde_json::Value,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum CacheKind {
    NonStream,
}

impl ResponseCache {
    pub fn new(config: ResponseCacheConfig) -> Option<Self> {
        if !config.enabled || config.ttl_seconds == 0 {
            return None;
        }

        if let Err(err) = std::fs::create_dir_all(&config.dir) {
            tracing::warn!(
                "response cache disabled because cache directory could not be created: {}",
                err
            );
            return None;
        }

        Some(Self {
            config,
            cleanup_started: Arc::new(Mutex::new(false)),
        })
    }

    pub fn start_cleanup_task(&self) {
        let mut started = self.cleanup_started.lock();
        if *started {
            return;
        }
        *started = true;
        drop(started);

        let cache = self.clone();
        tokio::spawn(async move {
            cache.cleanup_expired();

            let interval_seconds = cache.config.cleanup_interval_seconds.max(60);
            let mut interval = tokio::time::interval(Duration::from_secs(interval_seconds));
            loop {
                interval.tick().await;
                cache.cleanup_expired();
            }
        });
    }

    pub fn key_for_request(&self, req: &MessagesRequest) -> Option<String> {
        if req.stream || has_non_deterministic_tool(req) {
            return None;
        }

        let canonical = serde_json::to_value(req).ok()?;
        let canonical = canonicalize_json(canonical);
        let data = serde_json::to_vec(&canonical).ok()?;
        let mut hasher = Sha256::new();
        hasher.update(b"kiro-rs-response-cache-v1");
        hasher.update(&data);
        Some(format!("{:x}", hasher.finalize()))
    }

    pub fn get_non_stream(&self, key: &str) -> Option<serde_json::Value> {
        let envelope = self.read_envelope(key)?;
        if envelope.kind != CacheKind::NonStream || envelope.expires_at <= now_ts() {
            return None;
        }
        Some(envelope.response)
    }

    pub fn put_non_stream(&self, key: &str, response: &serde_json::Value) {
        let now = now_ts();
        let ttl = i64::try_from(self.config.ttl_seconds).unwrap_or(i64::MAX);
        let envelope = CacheEnvelope {
            version: 1,
            created_at: now,
            expires_at: now.saturating_add(ttl),
            kind: CacheKind::NonStream,
            response: response.clone(),
        };

        if let Err(err) = self.write_envelope(key, &envelope) {
            tracing::warn!("failed to write response cache entry {}: {}", key, err);
        }
    }

    pub fn cleanup_expired(&self) {
        let now = now_ts();
        let entries = match std::fs::read_dir(&self.config.dir) {
            Ok(entries) => entries,
            Err(err) => {
                tracing::warn!("failed to scan response cache directory: {}", err);
                return;
            }
        };

        let mut removed = 0usize;
        for entry in entries.flatten() {
            let path = entry.path();
            if path.extension().and_then(|e| e.to_str()) != Some("json") {
                continue;
            }

            let should_remove = match read_envelope_from_path(&path) {
                Ok(envelope) => envelope.expires_at <= now,
                Err(_) => true,
            };

            if should_remove && std::fs::remove_file(&path).is_ok() {
                removed += 1;
            }
        }

        if removed > 0 {
            tracing::info!("response cache cleanup removed {} expired entries", removed);
        }
    }

    fn path_for_key(&self, key: &str) -> Option<PathBuf> {
        if !is_valid_key(key) {
            return None;
        }
        Some(self.config.dir.join(format!("{}.json", key)))
    }

    fn temp_path_for_key(&self, key: &str) -> Option<PathBuf> {
        if !is_valid_key(key) {
            return None;
        }
        Some(self.config.dir.join(format!("{}.json.tmp", key)))
    }

    fn read_envelope(&self, key: &str) -> Option<CacheEnvelope> {
        let path = self.path_for_key(key)?;
        read_envelope_from_path(&path).ok()
    }

    fn write_envelope(&self, key: &str, envelope: &CacheEnvelope) -> anyhow::Result<()> {
        std::fs::create_dir_all(&self.config.dir)
            .with_context(|| format!("create cache dir {}", self.config.dir.display()))?;

        let path = self
            .path_for_key(key)
            .ok_or_else(|| anyhow::anyhow!("invalid cache key"))?;
        let tmp = self
            .temp_path_for_key(key)
            .ok_or_else(|| anyhow::anyhow!("invalid cache key"))?;
        let json = serde_json::to_vec_pretty(envelope)?;
        std::fs::write(&tmp, json)?;
        if path.exists() {
            std::fs::remove_file(&path)?;
        }
        std::fs::rename(&tmp, &path)?;
        Ok(())
    }
}

fn read_envelope_from_path(path: &Path) -> anyhow::Result<CacheEnvelope> {
    let content = std::fs::read_to_string(path)?;
    Ok(serde_json::from_str(&content)?)
}

fn has_non_deterministic_tool(req: &MessagesRequest) -> bool {
    req.tools
        .as_ref()
        .map(|tools| tools.iter().any(|tool| tool.name == "web_search"))
        .unwrap_or(false)
}

fn canonicalize_json(value: serde_json::Value) -> serde_json::Value {
    match value {
        serde_json::Value::Object(map) => {
            let sorted: BTreeMap<_, _> = map
                .into_iter()
                .map(|(k, v)| (k, canonicalize_json(v)))
                .collect();
            serde_json::Value::Object(sorted.into_iter().collect())
        }
        serde_json::Value::Array(values) => {
            serde_json::Value::Array(values.into_iter().map(canonicalize_json).collect())
        }
        other => other,
    }
}

fn is_valid_key(key: &str) -> bool {
    key.len() == 64 && key.bytes().all(|b| b.is_ascii_hexdigit())
}

fn now_ts() -> i64 {
    Utc::now().timestamp()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::anthropic::types::{Message, Tool};

    fn temp_cache_dir(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!("kiro-response-cache-{}-{}", name, uuid::Uuid::new_v4()))
    }

    fn request(stream: bool) -> MessagesRequest {
        MessagesRequest {
            model: "claude-sonnet-4-5-20250929".to_string(),
            max_tokens: 128,
            messages: vec![Message {
                role: "user".to_string(),
                content: serde_json::json!("hello"),
            }],
            stream,
            system: None,
            tools: None,
            tool_choice: None,
            thinking: None,
            output_config: None,
            metadata: None,
        }
    }

    fn cache(dir: PathBuf, ttl_seconds: u64) -> ResponseCache {
        ResponseCache::new(ResponseCacheConfig {
            enabled: true,
            dir,
            ttl_seconds,
            cleanup_interval_seconds: 60,
        })
        .unwrap()
    }

    #[test]
    fn key_is_stable_for_same_request() {
        let dir = temp_cache_dir("stable");
        let cache = cache(dir.clone(), 60);
        let key1 = cache.key_for_request(&request(false)).unwrap();
        let key2 = cache.key_for_request(&request(false)).unwrap();
        assert_eq!(key1, key2);
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn stream_requests_are_not_cached() {
        let dir = temp_cache_dir("stream");
        let cache = cache(dir.clone(), 60);
        assert!(cache.key_for_request(&request(true)).is_none());
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn web_search_requests_are_not_cached() {
        let dir = temp_cache_dir("websearch");
        let cache = cache(dir.clone(), 60);
        let mut req = request(false);
        req.tools = Some(vec![Tool {
            tool_type: Some("web_search_20250305".to_string()),
            name: "web_search".to_string(),
            description: String::new(),
            input_schema: Default::default(),
            max_uses: Some(5),
        }]);
        assert!(cache.key_for_request(&req).is_none());
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn cache_round_trips_non_stream_response() {
        let dir = temp_cache_dir("roundtrip");
        let cache = cache(dir.clone(), 60);
        let key = cache.key_for_request(&request(false)).unwrap();
        let response = serde_json::json!({"type": "message", "content": []});
        cache.put_non_stream(&key, &response);
        assert_eq!(cache.get_non_stream(&key), Some(response));
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn cleanup_removes_expired_entries() {
        let dir = temp_cache_dir("cleanup");
        let cache = cache(dir.clone(), 1);
        let key = cache.key_for_request(&request(false)).unwrap();
        let envelope = CacheEnvelope {
            version: 1,
            created_at: now_ts() - 10,
            expires_at: now_ts() - 1,
            kind: CacheKind::NonStream,
            response: serde_json::json!({"expired": true}),
        };
        cache.write_envelope(&key, &envelope).unwrap();
        cache.cleanup_expired();
        assert!(cache.get_non_stream(&key).is_none());
        assert!(!cache.path_for_key(&key).unwrap().exists());
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn expired_entry_can_be_rewritten_before_cleanup_runs() {
        let dir = temp_cache_dir("rewrite");
        let cache = cache(dir.clone(), 60);
        let key = cache.key_for_request(&request(false)).unwrap();
        let expired = CacheEnvelope {
            version: 1,
            created_at: now_ts() - 10,
            expires_at: now_ts() - 1,
            kind: CacheKind::NonStream,
            response: serde_json::json!({"expired": true}),
        };
        cache.write_envelope(&key, &expired).unwrap();

        let fresh = serde_json::json!({"fresh": true});
        cache.put_non_stream(&key, &fresh);

        assert_eq!(cache.get_non_stream(&key), Some(fresh));
        let _ = std::fs::remove_dir_all(dir);
    }
}
