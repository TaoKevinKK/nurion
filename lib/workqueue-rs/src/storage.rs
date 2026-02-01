// Copyright 2025 nurion team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// SlateDB storage layer for WorkQueue
//
// Key Schema (Sequence-based model for O(1) claim):
//   meta:{queue} -> QueueMeta {claim_seq, push_seq}
//   pending:{queue}:{seq:020d} -> msg_id
//   msg:{queue}:{msg_id} -> Message JSON
//   claimed:{queue}:{msg_id} -> ClaimInfo JSON
//   acked:{queue}:{ts:020d}:{msg_id} -> ""
//   state:{namespace}:{key} -> value bytes

use std::collections::HashMap;
use slatedb::{Db, WriteBatch};

use crate::types::{now_nanos, ClaimInfo, Message};

pub type StorageError = Box<dyn std::error::Error + Send + Sync>;

/// Queue metadata for O(1) claim operations
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct QueueMeta {
    pub claim_seq: u64,
    pub push_seq: u64,
}

impl Default for QueueMeta {
    fn default() -> Self {
        Self { claim_seq: 0, push_seq: 0 }
    }
}

/// Options for ack operations
#[derive(Default)]
pub struct AckOptions<'a> {
    pub downstream_queue: Option<&'a str>,
    pub downstream_messages: Option<&'a [Message]>,
    pub state_namespace: Option<&'a str>,
    pub state_puts: Option<&'a HashMap<String, Vec<u8>>>,
    pub state_deletes: Option<&'a [String]>,
}

/// WorkQueue storage backed by SlateDB
pub struct WorkQueueStorage {
    db: Db,
}

impl WorkQueueStorage {
    pub async fn new(db_path: &str) -> Result<Self, StorageError> {
        let object_store = Db::resolve_object_store(db_path)?;
        let db = Db::open("/", object_store).await?;
        Ok(Self { db })
    }

    // === Key Generation ===

    fn meta_key(queue: &str) -> Vec<u8> {
        format!("meta:{}", queue).into_bytes()
    }

    fn pending_key(queue: &str, seq: u64) -> Vec<u8> {
        format!("pending:{}:{:020}", queue, seq).into_bytes()
    }

    fn msg_key(queue: &str, msg_id: &str) -> Vec<u8> {
        format!("msg:{}:{}", queue, msg_id).into_bytes()
    }

    fn claimed_key(queue: &str, msg_id: &str) -> Vec<u8> {
        format!("claimed:{}:{}", queue, msg_id).into_bytes()
    }

    fn acked_key(queue: &str, timestamp_ns: u64, msg_id: &str) -> Vec<u8> {
        format!("acked:{}:{:020}:{}", queue, timestamp_ns, msg_id).into_bytes()
    }

    fn state_key(namespace: &str, key: &str) -> Vec<u8> {
        format!("state:{}:{}", namespace, key).into_bytes()
    }

    // === Queue Metadata ===

    pub async fn get_meta(&self, queue: &str) -> Result<QueueMeta, StorageError> {
        match self.db.get(&Self::meta_key(queue)).await? {
            Some(data) => Ok(serde_json::from_slice(&data)?),
            None => Ok(QueueMeta::default()),
        }
    }

    pub async fn create_queue(&self, queue: &str) -> Result<(), StorageError> {
        let key = Self::meta_key(queue);
        if self.db.get(&key).await?.is_none() {
            self.db.put(&key, &serde_json::to_vec(&QueueMeta::default())?).await?;
            self.db.flush().await?;
        }
        Ok(())
    }

    // === Push Operations ===

    /// Push messages to queue (single or batch)
    pub async fn push_messages(&self, queue: &str, messages: &[Message]) -> Result<(), StorageError> {
        if messages.is_empty() {
            return Ok(());
        }

        let meta = self.get_meta(queue).await?;
        let mut batch = WriteBatch::new();

        for (i, msg) in messages.iter().enumerate() {
            let seq = meta.push_seq + i as u64;
            batch.put(&Self::msg_key(queue, &msg.msg_id), &serde_json::to_vec(msg)?);
            batch.put(&Self::pending_key(queue, seq), msg.msg_id.as_bytes());
        }

        let new_meta = QueueMeta {
            push_seq: meta.push_seq + messages.len() as u64,
            ..meta
        };
        batch.put(&Self::meta_key(queue), &serde_json::to_vec(&new_meta)?);

        self.db.write(batch).await?;
        Ok(())
    }

    /// Push a single message (convenience wrapper)
    pub async fn push_message(&self, queue: &str, msg: &Message) -> Result<(), StorageError> {
        self.push_messages(queue, std::slice::from_ref(msg)).await
    }

    // === Claim Operations ===

    /// Claim messages from queue - O(1) per message
    pub async fn claim_messages(
        &self,
        queue: &str,
        batch_size: usize,
        worker_id: &str,
        lease_id: &str,
    ) -> Result<Vec<Message>, StorageError> {
        let meta = self.get_meta(queue).await?;

        if meta.claim_seq >= meta.push_seq {
            return Ok(Vec::new());
        }

        let mut batch = WriteBatch::new();
        let mut claimed = Vec::new();
        let mut new_claim_seq = meta.claim_seq;

        for seq in meta.claim_seq..meta.push_seq {
            if claimed.len() >= batch_size {
                break;
            }

            let pending_key = Self::pending_key(queue, seq);
            if let Some(msg_id_bytes) = self.db.get(&pending_key).await? {
                let msg_id = String::from_utf8_lossy(&msg_id_bytes).to_string();

                if let Some(msg_data) = self.db.get(&Self::msg_key(queue, &msg_id)).await? {
                    let msg: Message = serde_json::from_slice(&msg_data)?;

                    batch.delete(&pending_key);

                    let claim_info = ClaimInfo::new(msg_id.clone(), worker_id.to_string(), lease_id.to_string());
                    batch.put(&Self::claimed_key(queue, &msg_id), &serde_json::to_vec(&claim_info)?);

                    claimed.push(msg);
                }
            }
            new_claim_seq = seq + 1;
        }

        if !claimed.is_empty() {
            let new_meta = QueueMeta { claim_seq: new_claim_seq, ..meta };
            batch.put(&Self::meta_key(queue), &serde_json::to_vec(&new_meta)?);
            self.db.write(batch).await?;
        }

        Ok(claimed)
    }

    // === Ack Operations (unified) ===

    /// Core ack operation with optional downstream push and state updates
    async fn ack_internal(
        &self,
        queue: &str,
        msg_ids: &[String],
        opts: AckOptions<'_>,
    ) -> Result<(), StorageError> {
        if msg_ids.is_empty() && opts.downstream_messages.map_or(true, |m| m.is_empty()) {
            return Ok(());
        }

        let now_ns = now_nanos();
        let mut batch = WriteBatch::new();

        // 1. Move messages from claimed to acked
        for msg_id in msg_ids {
            batch.delete(&Self::claimed_key(queue, msg_id));
            batch.put(&Self::acked_key(queue, now_ns, msg_id), &[]);
        }

        // 2. Push downstream messages if provided
        if let (Some(downstream_queue), Some(messages)) = (opts.downstream_queue, opts.downstream_messages) {
            if !messages.is_empty() {
                let downstream_meta = self.get_meta(downstream_queue).await?;

                for (i, msg) in messages.iter().enumerate() {
                    let seq = downstream_meta.push_seq + i as u64;
                    batch.put(&Self::msg_key(downstream_queue, &msg.msg_id), &serde_json::to_vec(msg)?);
                    batch.put(&Self::pending_key(downstream_queue, seq), msg.msg_id.as_bytes());
                }

                let new_meta = QueueMeta {
                    push_seq: downstream_meta.push_seq + messages.len() as u64,
                    ..downstream_meta
                };
                batch.put(&Self::meta_key(downstream_queue), &serde_json::to_vec(&new_meta)?);
            }
        }

        // 3. Update state if provided
        if let Some(namespace) = opts.state_namespace {
            if let Some(puts) = opts.state_puts {
                for (key, value) in puts {
                    batch.put(&Self::state_key(namespace, key), value);
                }
            }
            if let Some(deletes) = opts.state_deletes {
                for key in deletes {
                    batch.delete(&Self::state_key(namespace, key));
                }
            }
        }

        self.db.write(batch).await?;
        Ok(())
    }

    /// Acknowledge messages (move to acked)
    pub async fn ack_messages(&self, queue: &str, msg_ids: &[String]) -> Result<(), StorageError> {
        self.ack_internal(queue, msg_ids, AckOptions::default()).await
    }

    /// Acknowledge with state updates
    pub async fn ack_with_state(
        &self,
        queue: &str,
        msg_ids: &[String],
        namespace: &str,
        state_puts: &HashMap<String, Vec<u8>>,
        state_deletes: &[String],
    ) -> Result<(), StorageError> {
        self.ack_internal(queue, msg_ids, AckOptions {
            state_namespace: Some(namespace),
            state_puts: Some(state_puts),
            state_deletes: Some(state_deletes),
            ..Default::default()
        }).await
    }

    /// Acknowledge upstream and push to downstream
    pub async fn ack_and_forward(
        &self,
        upstream_queue: &str,
        upstream_msg_ids: &[String],
        downstream_queue: &str,
        downstream_messages: &[Message],
    ) -> Result<(), StorageError> {
        self.ack_internal(upstream_queue, upstream_msg_ids, AckOptions {
            downstream_queue: Some(downstream_queue),
            downstream_messages: Some(downstream_messages),
            ..Default::default()
        }).await
    }

    /// Acknowledge upstream, push downstream, and update state
    pub async fn ack_forward_with_state(
        &self,
        upstream_queue: &str,
        upstream_msg_ids: &[String],
        downstream_queue: &str,
        downstream_messages: &[Message],
        namespace: &str,
        state_puts: &HashMap<String, Vec<u8>>,
        state_deletes: &[String],
    ) -> Result<(), StorageError> {
        self.ack_internal(upstream_queue, upstream_msg_ids, AckOptions {
            downstream_queue: Some(downstream_queue),
            downstream_messages: Some(downstream_messages),
            state_namespace: Some(namespace),
            state_puts: Some(state_puts),
            state_deletes: Some(state_deletes),
        }).await
    }

    // === Nack Operations ===

    /// Return messages to pending queue (at tail)
    pub async fn nack_messages(&self, queue: &str, msg_ids: &[String]) -> Result<(), StorageError> {
        if msg_ids.is_empty() {
            return Ok(());
        }

        let meta = self.get_meta(queue).await?;
        let mut batch = WriteBatch::new();

        for (i, msg_id) in msg_ids.iter().enumerate() {
            batch.delete(&Self::claimed_key(queue, msg_id));
            batch.put(&Self::pending_key(queue, meta.push_seq + i as u64), msg_id.as_bytes());
        }

        let new_meta = QueueMeta {
            push_seq: meta.push_seq + msg_ids.len() as u64,
            ..meta
        };
        batch.put(&Self::meta_key(queue), &serde_json::to_vec(&new_meta)?);

        self.db.write(batch).await?;
        Ok(())
    }

    // === Scan Operations ===

    /// Scan all claimed messages (optionally filtered by queue)
    pub async fn scan_claimed(&self, queue: Option<&str>) -> Result<Vec<(String, String, ClaimInfo)>, StorageError> {
        let prefix = match queue {
            Some(q) => format!("claimed:{}:", q).into_bytes(),
            None => b"claimed:".to_vec(),
        };

        let mut results = Vec::new();
        let mut iter = self.db.scan_prefix(&prefix).await?;

        while let Ok(Some(kv)) = iter.next().await {
            let key_str = String::from_utf8_lossy(&kv.key);
            let parts: Vec<&str> = key_str.split(':').collect();

            if parts.len() >= 3 {
                if let Ok(claim_info) = serde_json::from_slice::<ClaimInfo>(&kv.value) {
                    results.push((parts[1].to_string(), parts[2].to_string(), claim_info));
                }
            }
        }

        Ok(results)
    }

    /// Scan all acked messages (optionally filtered by queue)
    pub async fn scan_acked(&self, queue: Option<&str>) -> Result<Vec<(String, u64, String)>, StorageError> {
        let prefix = match queue {
            Some(q) => format!("acked:{}:", q).into_bytes(),
            None => b"acked:".to_vec(),
        };

        let mut results = Vec::new();
        let mut iter = self.db.scan_prefix(&prefix).await?;

        while let Ok(Some(kv)) = iter.next().await {
            let key_str = String::from_utf8_lossy(&kv.key);
            let parts: Vec<&str> = key_str.split(':').collect();

            if parts.len() >= 4 {
                if let Ok(timestamp_ns) = parts[2].parse::<u64>() {
                    results.push((parts[1].to_string(), timestamp_ns, parts[3].to_string()));
                }
            }
        }

        Ok(results)
    }

    // === Query Operations ===

    pub async fn get_queue_stats(&self, queue: &str) -> Result<(u64, u64), StorageError> {
        let meta = self.get_meta(queue).await?;
        let pending_count = meta.push_seq.saturating_sub(meta.claim_seq);
        let claimed_count = self.scan_claimed(Some(queue)).await?.len() as u64;
        Ok((pending_count, claimed_count))
    }

    // === Delete Operations ===

    pub async fn delete_queue(&self, queue: &str) -> Result<usize, StorageError> {
        let meta = self.get_meta(queue).await?;
        let claimed = self.scan_claimed(Some(queue)).await?;
        let acked = self.scan_acked(Some(queue)).await?;

        let mut batch = WriteBatch::new();
        let mut deleted = 0;

        batch.delete(&Self::meta_key(queue));

        // Delete pending entries and messages
        for seq in meta.claim_seq..meta.push_seq {
            let pending_key = Self::pending_key(queue, seq);
            if let Some(msg_id_bytes) = self.db.get(&pending_key).await? {
                let msg_id = String::from_utf8_lossy(&msg_id_bytes).to_string();
                batch.delete(&pending_key);
                batch.delete(&Self::msg_key(queue, &msg_id));
                deleted += 1;
            }
        }

        // Delete claimed entries and messages
        for (_, msg_id, _) in &claimed {
            batch.delete(&Self::claimed_key(queue, msg_id));
            batch.delete(&Self::msg_key(queue, msg_id));
            deleted += 1;
        }

        // Delete acked entries and messages
        for (_, ts, msg_id) in &acked {
            batch.delete(&Self::acked_key(queue, *ts, msg_id));
            batch.delete(&Self::msg_key(queue, msg_id));
            deleted += 1;
        }

        if deleted > 0 || !claimed.is_empty() || !acked.is_empty() {
            self.db.write(batch).await?;
        }

        Ok(deleted)
    }

    // === GC and Recovery ===

    pub async fn gc_acked_messages(&self, retention_ns: u64) -> Result<usize, StorageError> {
        let cutoff_ns = now_nanos().saturating_sub(retention_ns);
        let all_acked = self.scan_acked(None).await?;

        let mut batch = WriteBatch::new();
        let mut deleted = 0;

        for (queue, timestamp_ns, msg_id) in all_acked {
            if timestamp_ns < cutoff_ns {
                batch.delete(&Self::acked_key(&queue, timestamp_ns, &msg_id));
                batch.delete(&Self::msg_key(&queue, &msg_id));
                deleted += 1;
            }
        }

        if deleted > 0 {
            self.db.write(batch).await?;
            tracing::info!("GC deleted {} acked messages", deleted);
        }

        Ok(deleted)
    }

    pub async fn recover_expired_claims(&self, timeout_secs: f64) -> Result<usize, StorageError> {
        let now = crate::types::now_secs();
        let all_claimed = self.scan_claimed(None).await?;

        // Group expired by queue
        let mut expired_by_queue: HashMap<String, Vec<String>> = HashMap::new();
        for (queue, msg_id, claim_info) in all_claimed {
            if now - claim_info.claimed_at > timeout_secs {
                expired_by_queue.entry(queue).or_default().push(msg_id);
            }
        }

        let mut total = 0;
        for (queue, msg_ids) in expired_by_queue {
            self.nack_messages(&queue, &msg_ids).await?;
            total += msg_ids.len();
        }

        if total > 0 {
            tracing::info!("Recovered {} expired claims", total);
        }

        Ok(total)
    }

    // === State Operations ===

    pub async fn state_get_batch(
        &self,
        namespace: &str,
        keys: &[String],
    ) -> Result<HashMap<String, Vec<u8>>, StorageError> {
        let mut results = HashMap::new();
        for key in keys {
            if let Some(data) = self.db.get(&Self::state_key(namespace, key)).await? {
                results.insert(key.clone(), data.to_vec());
            }
        }
        Ok(results)
    }

    pub async fn state_put_batch(
        &self,
        namespace: &str,
        puts: &HashMap<String, Vec<u8>>,
        deletes: &[String],
    ) -> Result<(usize, usize), StorageError> {
        let mut batch = WriteBatch::new();

        for (key, value) in puts {
            batch.put(&Self::state_key(namespace, key), value);
        }
        for key in deletes {
            batch.delete(&Self::state_key(namespace, key));
        }

        self.db.write(batch).await?;
        Ok((puts.len(), deletes.len()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    static TEST_COUNTER: AtomicUsize = AtomicUsize::new(0);

    async fn create_temp_storage() -> WorkQueueStorage {
        let counter = TEST_COUNTER.fetch_add(1, Ordering::SeqCst);
        let temp_dir = std::env::temp_dir().join(format!("workqueue_test_{}", counter));
        let _ = std::fs::remove_dir_all(&temp_dir);
        WorkQueueStorage::new(&format!("file://{}", temp_dir.display())).await.unwrap()
    }

    #[tokio::test]
    async fn test_push_and_claim() {
        let storage = create_temp_storage().await;
        let queue = "test-queue";

        storage.create_queue(queue).await.unwrap();

        let msg1 = Message::new(queue.to_string(), b"hello".to_vec());
        let msg2 = Message::new(queue.to_string(), b"world".to_vec());

        storage.push_message(queue, &msg1).await.unwrap();
        storage.push_message(queue, &msg2).await.unwrap();

        let meta = storage.get_meta(queue).await.unwrap();
        assert_eq!(meta.claim_seq, 0);
        assert_eq!(meta.push_seq, 2);

        let claimed = storage.claim_messages(queue, 2, "worker-1", "lease-1").await.unwrap();
        assert_eq!(claimed.len(), 2);
        assert_eq!(claimed[0].payload, b"hello");
        assert_eq!(claimed[1].payload, b"world");

        let meta = storage.get_meta(queue).await.unwrap();
        assert_eq!(meta.claim_seq, 2);
        assert_eq!(meta.push_seq, 2);
    }

    #[tokio::test]
    async fn test_push_batch() {
        let storage = create_temp_storage().await;
        let queue = "test-queue";

        storage.create_queue(queue).await.unwrap();

        let messages: Vec<Message> = (0..5)
            .map(|i| Message::new(queue.to_string(), format!("msg{}", i).into_bytes()))
            .collect();

        storage.push_messages(queue, &messages).await.unwrap();

        let meta = storage.get_meta(queue).await.unwrap();
        assert_eq!(meta.push_seq, 5);

        let claimed = storage.claim_messages(queue, 5, "worker-1", "lease-1").await.unwrap();
        assert_eq!(claimed.len(), 5);
    }

    #[tokio::test]
    async fn test_ack() {
        let storage = create_temp_storage().await;
        let queue = "test-queue";

        storage.create_queue(queue).await.unwrap();

        let msg = Message::new(queue.to_string(), b"hello".to_vec());
        let msg_id = msg.msg_id.clone();

        storage.push_message(queue, &msg).await.unwrap();
        storage.claim_messages(queue, 1, "worker-1", "lease-1").await.unwrap();
        storage.ack_messages(queue, &[msg_id.clone()]).await.unwrap();

        // Claim info should be gone, message in acked
        let claimed = storage.scan_claimed(Some(queue)).await.unwrap();
        assert!(claimed.is_empty());

        let acked = storage.scan_acked(Some(queue)).await.unwrap();
        assert_eq!(acked.len(), 1);
    }

    #[tokio::test]
    async fn test_nack() {
        let storage = create_temp_storage().await;
        let queue = "test-queue";

        storage.create_queue(queue).await.unwrap();

        let msg = Message::new(queue.to_string(), b"hello".to_vec());
        let msg_id = msg.msg_id.clone();

        storage.push_message(queue, &msg).await.unwrap();
        storage.claim_messages(queue, 1, "worker-1", "lease-1").await.unwrap();

        let meta = storage.get_meta(queue).await.unwrap();
        assert_eq!(meta.claim_seq, 1);
        assert_eq!(meta.push_seq, 1);

        storage.nack_messages(queue, &[msg_id.clone()]).await.unwrap();

        let meta = storage.get_meta(queue).await.unwrap();
        assert_eq!(meta.claim_seq, 1);
        assert_eq!(meta.push_seq, 2);

        let claimed_again = storage.claim_messages(queue, 1, "worker-1", "lease-1").await.unwrap();
        assert_eq!(claimed_again.len(), 1);
        assert_eq!(claimed_again[0].msg_id, msg_id);
    }

    #[tokio::test]
    async fn test_ack_and_forward() {
        let storage = create_temp_storage().await;

        storage.create_queue("upstream").await.unwrap();
        storage.create_queue("downstream").await.unwrap();

        let upstream_msg = Message::new("upstream".to_string(), b"input".to_vec());
        let upstream_id = upstream_msg.msg_id.clone();
        storage.push_message("upstream", &upstream_msg).await.unwrap();
        storage.claim_messages("upstream", 1, "worker-1", "lease-1").await.unwrap();

        let downstream_msgs: Vec<Message> = (0..2)
            .map(|i| Message::new("downstream".to_string(), format!("out{}", i).into_bytes()))
            .collect();

        storage.ack_and_forward("upstream", &[upstream_id], "downstream", &downstream_msgs).await.unwrap();

        let acked = storage.scan_acked(Some("upstream")).await.unwrap();
        assert_eq!(acked.len(), 1);

        let meta = storage.get_meta("downstream").await.unwrap();
        assert_eq!(meta.push_seq, 2);

        let downstream_claimed = storage.claim_messages("downstream", 2, "worker-2", "lease-2").await.unwrap();
        assert_eq!(downstream_claimed.len(), 2);
    }

    #[tokio::test]
    async fn test_gc_acked_messages() {
        let storage = create_temp_storage().await;
        let queue = "test-queue";

        storage.create_queue(queue).await.unwrap();

        for i in 0..5 {
            let msg = Message::new(queue.to_string(), format!("msg{}", i).into_bytes());
            let msg_id = msg.msg_id.clone();
            storage.push_message(queue, &msg).await.unwrap();
            storage.claim_messages(queue, 1, "worker-1", "lease-1").await.unwrap();
            storage.ack_messages(queue, &[msg_id]).await.unwrap();
        }

        let acked = storage.scan_acked(Some(queue)).await.unwrap();
        assert_eq!(acked.len(), 5);

        let deleted = storage.gc_acked_messages(0).await.unwrap();
        assert_eq!(deleted, 5);

        let acked = storage.scan_acked(Some(queue)).await.unwrap();
        assert!(acked.is_empty());
    }

    #[tokio::test]
    async fn test_queue_stats() {
        let storage = create_temp_storage().await;
        let queue = "test-queue";

        storage.create_queue(queue).await.unwrap();

        for i in 0..5 {
            let msg = Message::new(queue.to_string(), format!("msg{}", i).into_bytes());
            storage.push_message(queue, &msg).await.unwrap();
        }

        let (pending, claimed) = storage.get_queue_stats(queue).await.unwrap();
        assert_eq!(pending, 5);
        assert_eq!(claimed, 0);

        storage.claim_messages(queue, 3, "worker-1", "lease-1").await.unwrap();

        let (pending, claimed) = storage.get_queue_stats(queue).await.unwrap();
        assert_eq!(pending, 2);
        assert_eq!(claimed, 3);
    }

    #[tokio::test]
    async fn test_state_operations() {
        let storage = create_temp_storage().await;
        let namespace = "job1/stage1";

        let mut puts = HashMap::new();
        puts.insert("key1".to_string(), b"value1".to_vec());
        puts.insert("key2".to_string(), b"value2".to_vec());

        storage.state_put_batch(namespace, &puts, &[]).await.unwrap();

        let keys = vec!["key1".to_string(), "key2".to_string(), "key3".to_string()];
        let values = storage.state_get_batch(namespace, &keys).await.unwrap();
        assert_eq!(values.len(), 2);
        assert_eq!(values.get("key1"), Some(&b"value1".to_vec()));
    }

    #[tokio::test]
    async fn test_ack_with_state() {
        let storage = create_temp_storage().await;
        let queue = "test-queue";
        let namespace = "job1/stage1";

        storage.create_queue(queue).await.unwrap();

        let msg = Message::new(queue.to_string(), b"hello".to_vec());
        let msg_id = msg.msg_id.clone();

        storage.push_message(queue, &msg).await.unwrap();
        storage.claim_messages(queue, 1, "worker-1", "lease-1").await.unwrap();

        let mut state_puts = HashMap::new();
        state_puts.insert("seen_key".to_string(), b"1".to_vec());

        storage.ack_with_state(queue, &[msg_id], namespace, &state_puts, &[]).await.unwrap();

        let values = storage.state_get_batch(namespace, &["seen_key".to_string()]).await.unwrap();
        assert_eq!(values.get("seen_key"), Some(&b"1".to_vec()));
    }

    #[tokio::test]
    async fn test_delete_queue() {
        let storage = create_temp_storage().await;
        let queue = "test-queue";

        storage.create_queue(queue).await.unwrap();

        for i in 0..5 {
            let msg = Message::new(queue.to_string(), format!("msg{}", i).into_bytes());
            storage.push_message(queue, &msg).await.unwrap();
        }
        storage.claim_messages(queue, 2, "worker-1", "lease-1").await.unwrap();

        let deleted = storage.delete_queue(queue).await.unwrap();
        assert!(deleted > 0);

        let meta = storage.get_meta(queue).await.unwrap();
        assert_eq!(meta.claim_seq, 0);
        assert_eq!(meta.push_seq, 0);
    }

    #[tokio::test]
    async fn test_recover_expired_claims() {
        let storage = create_temp_storage().await;
        let queue = "test-queue";

        storage.create_queue(queue).await.unwrap();

        let msg = Message::new(queue.to_string(), b"hello".to_vec());
        storage.push_message(queue, &msg).await.unwrap();
        storage.claim_messages(queue, 1, "worker-1", "lease-1").await.unwrap();

        let recovered = storage.recover_expired_claims(0.0).await.unwrap();
        assert_eq!(recovered, 1);

        let claimed = storage.claim_messages(queue, 1, "worker-2", "lease-2").await.unwrap();
        assert_eq!(claimed.len(), 1);
    }
}
