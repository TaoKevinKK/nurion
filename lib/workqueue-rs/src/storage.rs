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
use tokio::time::{sleep, Duration};

use slatedb::{Db, DbRead, Error as SlateError, ErrorKind, IsolationLevel, WriteBatch};

use crate::types::{now_nanos, ClaimInfo, ClaimedMessage, Message, QueueGroupMeta};

pub type StorageError = Box<dyn std::error::Error + Send + Sync>;

const MAX_TXN_RETRIES: usize = 5;

fn is_txn_conflict(err: &SlateError) -> bool {
    err.kind() == ErrorKind::Transaction
}

/// Queue metadata for O(1) operations
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct QueueMeta {
    pub claim_seq: u64,
    pub push_seq: u64,
    /// Number of currently claimed messages (O(1) stats)
    #[serde(default)]
    pub claimed_count: u64,
    /// Total messages ever pushed (lifetime counter)
    #[serde(default)]
    pub total_pushed: u64,
    /// Total messages ever acked (lifetime counter)
    #[serde(default)]
    pub total_acked: u64,
}

/// Options for ack operations
#[derive(Default)]
pub struct AckOptions<'a> {
    pub downstream_queue: Option<&'a str>,
    pub downstream_messages: Option<&'a [Message]>,
    pub state_namespace: Option<&'a str>,
    pub state_puts: Option<&'a HashMap<String, Vec<u8>>>,
    pub state_deletes: Option<&'a [String]>,
    pub claim_tokens: Option<&'a [String]>,
    pub lease_id: Option<&'a str>,
    pub worker_id: Option<&'a str>,
}

/// WorkQueue storage backed by SlateDB
pub struct WorkQueueStorage {
    db: Db,
    /// Per-group round-robin counters for O(1) steal path in claim_from_group
    steal_rr: std::sync::Mutex<HashMap<String, u64>>,
}

impl WorkQueueStorage {
    pub async fn new(db_path: &str) -> Result<Self, StorageError> {
        let object_store = Db::resolve_object_store(db_path)?;
        let db = Db::open("/", object_store).await?;
        Ok(Self {
            db,
            steal_rr: std::sync::Mutex::new(HashMap::new()),
        })
    }

    /// Close the storage gracefully.
    /// This should be called before dropping the storage to ensure all background
    /// tasks are properly shut down and avoid "channel closed" panics.
    #[allow(dead_code)]
    pub async fn close(&self) -> Result<(), StorageError> {
        self.db.close().await?;
        Ok(())
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

    fn finished_key(queue: &str) -> Vec<u8> {
        format!("finished:{}", queue).into_bytes()
    }

    fn group_meta_key(group_name: &str) -> Vec<u8> {
        format!("group_meta:{}", group_name).into_bytes()
    }

    // === Queue Metadata ===

    async fn get_meta_from_reader<R: DbRead + Sync + ?Sized>(
        reader: &R,
        queue: &str,
    ) -> Result<QueueMeta, StorageError> {
        match reader.get(Self::meta_key(queue)).await? {
            Some(data) => Ok(serde_json::from_slice(&data)?),
            None => Ok(QueueMeta::default()),
        }
    }

    async fn get_claim_info_from_reader<R: DbRead + Sync + ?Sized>(
        reader: &R,
        queue: &str,
        msg_id: &str,
    ) -> Result<ClaimInfo, StorageError> {
        let key = Self::claimed_key(queue, msg_id);
        let data = reader
            .get(&key)
            .await?
            .ok_or_else(|| SlateError::invalid(format!("Message not claimed: {}", msg_id)))?;
        Ok(serde_json::from_slice(&data)?)
    }

    pub async fn get_meta(&self, queue: &str) -> Result<QueueMeta, StorageError> {
        Self::get_meta_from_reader(&self.db, queue).await
    }

    pub async fn create_queue(&self, queue: &str) -> Result<(), StorageError> {
        let key = Self::meta_key(queue);
        if self.db.get(&key).await?.is_none() {
            self.db
                .put(&key, &serde_json::to_vec(&QueueMeta::default())?)
                .await?;
            self.db.flush().await?;
        }
        Ok(())
    }

    // === Push Operations ===

    /// Push messages to queue (single or batch)
    pub async fn push_messages(
        &self,
        queue: &str,
        messages: &[Message],
    ) -> Result<(), StorageError> {
        if messages.is_empty() {
            return Ok(());
        }

        for attempt in 0..MAX_TXN_RETRIES {
            let txn = self.db.begin(IsolationLevel::SerializableSnapshot).await?;
            let meta = Self::get_meta_from_reader(&txn, queue).await?;

            for (i, msg) in messages.iter().enumerate() {
                let seq = meta.push_seq + i as u64;
                txn.put(Self::msg_key(queue, &msg.msg_id), &serde_json::to_vec(msg)?)?;
                txn.put(Self::pending_key(queue, seq), msg.msg_id.as_bytes())?;
            }

            let msg_count = messages.len() as u64;
            let new_meta = QueueMeta {
                push_seq: meta.push_seq + msg_count,
                total_pushed: meta.total_pushed + msg_count,
                ..meta
            };
            txn.put(Self::meta_key(queue), &serde_json::to_vec(&new_meta)?)?;

            match txn.commit().await {
                Ok(()) => return Ok(()),
                Err(e) if is_txn_conflict(&e) && attempt + 1 < MAX_TXN_RETRIES => {
                    sleep(Duration::from_millis(5 * (attempt as u64 + 1))).await;
                    continue;
                }
                Err(e) => return Err(Box::new(e)),
            }
        }

        Err(Box::new(SlateError::transaction(
            "push_messages exceeded retry budget".to_string(),
        )))
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
    ) -> Result<Vec<ClaimedMessage>, StorageError> {
        for attempt in 0..MAX_TXN_RETRIES {
            let txn = self.db.begin(IsolationLevel::SerializableSnapshot).await?;
            let meta = Self::get_meta_from_reader(&txn, queue).await?;

            if meta.claim_seq >= meta.push_seq {
                return Ok(Vec::new());
            }

            let mut claimed = Vec::new();
            let mut new_claim_seq = meta.claim_seq;

            for seq in meta.claim_seq..meta.push_seq {
                if claimed.len() >= batch_size {
                    break;
                }

                let pending_key = Self::pending_key(queue, seq);
                if let Some(msg_id_bytes) = txn.get(&pending_key).await? {
                    let msg_id = String::from_utf8_lossy(&msg_id_bytes).to_string();

                    if let Some(msg_data) = txn.get(&Self::msg_key(queue, &msg_id)).await? {
                        let msg: Message = serde_json::from_slice(&msg_data)?;

                        txn.delete(&pending_key)?;

                        let claim_info = ClaimInfo::new(
                            msg_id.clone(),
                            worker_id.to_string(),
                            lease_id.to_string(),
                        );
                        txn.put(
                            Self::claimed_key(queue, &msg_id),
                            &serde_json::to_vec(&claim_info)?,
                        )?;

                        claimed.push(ClaimedMessage {
                            message: msg,
                            claim_token: claim_info.claim_token.clone(),
                        });
                    }
                }
                new_claim_seq = seq + 1;
            }

            if claimed.is_empty() {
                return Ok(Vec::new());
            }

            let new_meta = QueueMeta {
                claim_seq: new_claim_seq,
                claimed_count: meta.claimed_count + claimed.len() as u64,
                ..meta
            };
            txn.put(Self::meta_key(queue), &serde_json::to_vec(&new_meta)?)?;

            match txn.commit().await {
                Ok(()) => return Ok(claimed),
                Err(e) if is_txn_conflict(&e) && attempt + 1 < MAX_TXN_RETRIES => {
                    sleep(Duration::from_millis(5 * (attempt as u64 + 1))).await;
                    continue;
                }
                Err(e) => return Err(Box::new(e)),
            }
        }

        Err(Box::new(SlateError::transaction(
            "claim_messages exceeded retry budget".to_string(),
        )))
    }

    // === Ack Operations (unified) ===

    /// Core ack operation with optional downstream push and state updates
    async fn ack_internal(
        &self,
        queue: &str,
        msg_ids: &[String],
        opts: AckOptions<'_>,
    ) -> Result<(), StorageError> {
        if msg_ids.is_empty() && opts.downstream_messages.is_none_or(|m| m.is_empty()) {
            return Ok(());
        }

        let claim_tokens = if msg_ids.is_empty() {
            None
        } else {
            let tokens = opts.claim_tokens.ok_or_else(|| {
                SlateError::invalid("claim_tokens is required for ack".to_string())
            })?;
            if tokens.len() != msg_ids.len() {
                return Err(Box::new(SlateError::invalid(format!(
                    "claim_tokens length mismatch (msg_ids={}, claim_tokens={})",
                    msg_ids.len(),
                    tokens.len()
                ))));
            }
            Some(tokens)
        };

        let expected_lease_id = opts.lease_id.filter(|v| !v.is_empty());
        let expected_worker_id = opts.worker_id.filter(|v| !v.is_empty());

        for attempt in 0..MAX_TXN_RETRIES {
            let txn = self.db.begin(IsolationLevel::SerializableSnapshot).await?;

            let now_ns = now_nanos();
            let ack_count = msg_ids.len() as u64;

            // 1. Validate claims + move messages from claimed to acked + update upstream meta
            if !msg_ids.is_empty() {
                for (msg_id, token) in msg_ids.iter().zip(claim_tokens.unwrap().iter()) {
                    let claim_info = Self::get_claim_info_from_reader(&txn, queue, msg_id).await?;
                    if claim_info.claim_token != *token {
                        return Err(Box::new(SlateError::invalid(format!(
                            "claim_token mismatch for msg_id {}",
                            msg_id
                        ))));
                    }
                    if let Some(expected) = expected_lease_id {
                        if claim_info.lease_id != expected {
                            return Err(Box::new(SlateError::invalid(format!(
                                "lease_id mismatch for msg_id {}",
                                msg_id
                            ))));
                        }
                    }
                    if let Some(expected) = expected_worker_id {
                        if claim_info.worker_id != expected {
                            return Err(Box::new(SlateError::invalid(format!(
                                "worker_id mismatch for msg_id {}",
                                msg_id
                            ))));
                        }
                    }
                }

                let upstream_meta = Self::get_meta_from_reader(&txn, queue).await?;
                for msg_id in msg_ids {
                    txn.delete(Self::claimed_key(queue, msg_id))?;
                    txn.put(Self::acked_key(queue, now_ns, msg_id), [])?;
                }
                let new_upstream_meta = QueueMeta {
                    claimed_count: upstream_meta.claimed_count.saturating_sub(ack_count),
                    total_acked: upstream_meta.total_acked + ack_count,
                    ..upstream_meta
                };
                txn.put(
                    Self::meta_key(queue),
                    &serde_json::to_vec(&new_upstream_meta)?,
                )?;
            }

            // 2. Push downstream messages if provided
            if let (Some(downstream_queue), Some(messages)) =
                (opts.downstream_queue, opts.downstream_messages)
            {
                if !messages.is_empty() {
                    let downstream_meta =
                        Self::get_meta_from_reader(&txn, downstream_queue).await?;
                    let msg_count = messages.len() as u64;

                    for (i, msg) in messages.iter().enumerate() {
                        let seq = downstream_meta.push_seq + i as u64;
                        txn.put(
                            Self::msg_key(downstream_queue, &msg.msg_id),
                            &serde_json::to_vec(msg)?,
                        )?;
                        txn.put(
                            Self::pending_key(downstream_queue, seq),
                            msg.msg_id.as_bytes(),
                        )?;
                    }

                    let new_meta = QueueMeta {
                        push_seq: downstream_meta.push_seq + msg_count,
                        total_pushed: downstream_meta.total_pushed + msg_count,
                        ..downstream_meta
                    };
                    txn.put(
                        Self::meta_key(downstream_queue),
                        &serde_json::to_vec(&new_meta)?,
                    )?;
                }
            }

            // 3. Update state if provided
            if let Some(namespace) = opts.state_namespace {
                if let Some(puts) = opts.state_puts {
                    for (key, value) in puts {
                        txn.put(Self::state_key(namespace, key), value)?;
                    }
                }
                if let Some(deletes) = opts.state_deletes {
                    for key in deletes {
                        txn.delete(Self::state_key(namespace, key))?;
                    }
                }
            }

            match txn.commit().await {
                Ok(()) => return Ok(()),
                Err(e) if is_txn_conflict(&e) && attempt + 1 < MAX_TXN_RETRIES => {
                    sleep(Duration::from_millis(5 * (attempt as u64 + 1))).await;
                    continue;
                }
                Err(e) => return Err(Box::new(e)),
            }
        }

        Err(Box::new(SlateError::transaction(
            "ack_internal exceeded retry budget".to_string(),
        )))
    }

    /// Acknowledge messages (move to acked)
    pub async fn ack_messages(
        &self,
        queue: &str,
        msg_ids: &[String],
        claim_tokens: &[String],
        worker_id: &str,
        lease_id: &str,
    ) -> Result<(), StorageError> {
        self.ack_internal(
            queue,
            msg_ids,
            AckOptions {
                claim_tokens: Some(claim_tokens),
                worker_id: Some(worker_id),
                lease_id: Some(lease_id),
                ..Default::default()
            },
        )
        .await
    }

    /// Acknowledge with state updates
    #[allow(clippy::too_many_arguments)]
    pub async fn ack_with_state(
        &self,
        queue: &str,
        msg_ids: &[String],
        claim_tokens: &[String],
        worker_id: &str,
        lease_id: &str,
        namespace: &str,
        state_puts: &HashMap<String, Vec<u8>>,
        state_deletes: &[String],
    ) -> Result<(), StorageError> {
        self.ack_internal(
            queue,
            msg_ids,
            AckOptions {
                state_namespace: Some(namespace),
                state_puts: Some(state_puts),
                state_deletes: Some(state_deletes),
                claim_tokens: Some(claim_tokens),
                worker_id: Some(worker_id),
                lease_id: Some(lease_id),
                ..Default::default()
            },
        )
        .await
    }

    /// Acknowledge upstream and push to downstream
    #[allow(clippy::too_many_arguments)]
    pub async fn ack_and_forward(
        &self,
        upstream_queue: &str,
        upstream_msg_ids: &[String],
        upstream_claim_tokens: &[String],
        worker_id: &str,
        lease_id: &str,
        downstream_queue: &str,
        downstream_messages: &[Message],
    ) -> Result<(), StorageError> {
        self.ack_internal(
            upstream_queue,
            upstream_msg_ids,
            AckOptions {
                downstream_queue: Some(downstream_queue),
                downstream_messages: Some(downstream_messages),
                claim_tokens: Some(upstream_claim_tokens),
                worker_id: Some(worker_id),
                lease_id: Some(lease_id),
                ..Default::default()
            },
        )
        .await
    }

    /// Acknowledge upstream, push downstream, and update state
    #[allow(clippy::too_many_arguments)]
    pub async fn ack_forward_with_state(
        &self,
        upstream_queue: &str,
        upstream_msg_ids: &[String],
        upstream_claim_tokens: &[String],
        worker_id: &str,
        lease_id: &str,
        downstream_queue: &str,
        downstream_messages: &[Message],
        namespace: &str,
        state_puts: &HashMap<String, Vec<u8>>,
        state_deletes: &[String],
    ) -> Result<(), StorageError> {
        self.ack_internal(
            upstream_queue,
            upstream_msg_ids,
            AckOptions {
                downstream_queue: Some(downstream_queue),
                downstream_messages: Some(downstream_messages),
                state_namespace: Some(namespace),
                state_puts: Some(state_puts),
                state_deletes: Some(state_deletes),
                claim_tokens: Some(upstream_claim_tokens),
                worker_id: Some(worker_id),
                lease_id: Some(lease_id),
            },
        )
        .await
    }

    // === Nack Operations ===

    /// Return messages to pending queue (at tail)
    pub async fn nack_messages(
        &self,
        queue: &str,
        msg_ids: &[String],
        claim_tokens: &[String],
        worker_id: &str,
        lease_id: &str,
    ) -> Result<(), StorageError> {
        self.nack_messages_internal(
            queue,
            msg_ids,
            Some(claim_tokens),
            Some(worker_id),
            Some(lease_id),
            None,
            None,
            None,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn nack_messages_with_state(
        &self,
        queue: &str,
        msg_ids: &[String],
        claim_tokens: &[String],
        worker_id: &str,
        lease_id: &str,
        state_namespace: &str,
        state_puts: &HashMap<String, Vec<u8>>,
        state_deletes: &[String],
    ) -> Result<(), StorageError> {
        self.nack_messages_internal(
            queue,
            msg_ids,
            Some(claim_tokens),
            Some(worker_id),
            Some(lease_id),
            Some(state_namespace),
            Some(state_puts),
            Some(state_deletes),
        )
        .await
    }

    /// Return messages to pending queue without claim validation (recovery path).
    pub async fn nack_messages_unchecked(
        &self,
        queue: &str,
        msg_ids: &[String],
    ) -> Result<(), StorageError> {
        self.nack_messages_internal(queue, msg_ids, None, None, None, None, None, None)
            .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn nack_messages_internal(
        &self,
        queue: &str,
        msg_ids: &[String],
        claim_tokens: Option<&[String]>,
        worker_id: Option<&str>,
        lease_id: Option<&str>,
        state_namespace: Option<&str>,
        state_puts: Option<&HashMap<String, Vec<u8>>>,
        state_deletes: Option<&[String]>,
    ) -> Result<(), StorageError> {
        if msg_ids.is_empty() {
            return Ok(());
        }

        if let Some(tokens) = claim_tokens {
            if tokens.len() != msg_ids.len() {
                return Err(Box::new(SlateError::invalid(format!(
                    "claim_tokens length mismatch (msg_ids={}, claim_tokens={})",
                    msg_ids.len(),
                    tokens.len()
                ))));
            }
        }

        let expected_worker_id = worker_id.filter(|v| !v.is_empty());
        let expected_lease_id = lease_id.filter(|v| !v.is_empty());

        for attempt in 0..MAX_TXN_RETRIES {
            let txn = self.db.begin(IsolationLevel::SerializableSnapshot).await?;

            if let Some(tokens) = claim_tokens {
                for (msg_id, token) in msg_ids.iter().zip(tokens.iter()) {
                    let claim_info = Self::get_claim_info_from_reader(&txn, queue, msg_id).await?;
                    if claim_info.claim_token != *token {
                        return Err(Box::new(SlateError::invalid(format!(
                            "claim_token mismatch for msg_id {}",
                            msg_id
                        ))));
                    }
                    if let Some(expected) = expected_lease_id {
                        if claim_info.lease_id != expected {
                            return Err(Box::new(SlateError::invalid(format!(
                                "lease_id mismatch for msg_id {}",
                                msg_id
                            ))));
                        }
                    }
                    if let Some(expected) = expected_worker_id {
                        if claim_info.worker_id != expected {
                            return Err(Box::new(SlateError::invalid(format!(
                                "worker_id mismatch for msg_id {}",
                                msg_id
                            ))));
                        }
                    }
                }
            }

            let meta = Self::get_meta_from_reader(&txn, queue).await?;
            let nack_count = msg_ids.len() as u64;

            for (i, msg_id) in msg_ids.iter().enumerate() {
                txn.delete(Self::claimed_key(queue, msg_id))?;
                txn.put(
                    Self::pending_key(queue, meta.push_seq + i as u64),
                    msg_id.as_bytes(),
                )?;
            }

            let new_meta = QueueMeta {
                push_seq: meta.push_seq + nack_count,
                claimed_count: meta.claimed_count.saturating_sub(nack_count),
                ..meta
            };
            txn.put(Self::meta_key(queue), &serde_json::to_vec(&new_meta)?)?;

            let has_state_updates = state_namespace.is_some()
                && (state_puts.is_some_and(|puts| !puts.is_empty())
                    || state_deletes.is_some_and(|deletes| !deletes.is_empty()));
            if has_state_updates {
                let namespace = state_namespace.unwrap();
                if let Some(puts) = state_puts {
                    for (key, value) in puts {
                        txn.put(Self::state_key(namespace, key), value)?;
                    }
                }
                if let Some(deletes) = state_deletes {
                    for key in deletes {
                        txn.delete(Self::state_key(namespace, key))?;
                    }
                }
            }

            match txn.commit().await {
                Ok(()) => return Ok(()),
                Err(e) if is_txn_conflict(&e) && attempt + 1 < MAX_TXN_RETRIES => {
                    sleep(Duration::from_millis(5 * (attempt as u64 + 1))).await;
                    continue;
                }
                Err(e) => return Err(Box::new(e)),
            }
        }

        Err(Box::new(SlateError::transaction(
            "nack_messages exceeded retry budget".to_string(),
        )))
    }

    // === Scan Operations ===

    /// Scan all claimed messages (optionally filtered by queue)
    pub async fn scan_claimed(
        &self,
        queue: Option<&str>,
    ) -> Result<Vec<(String, String, ClaimInfo)>, StorageError> {
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
    pub async fn scan_acked(
        &self,
        queue: Option<&str>,
    ) -> Result<Vec<(String, u64, String)>, StorageError> {
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

    /// Get queue stats - O(1) using counters in meta
    pub async fn get_queue_stats(&self, queue: &str) -> Result<QueueMeta, StorageError> {
        self.get_meta(queue).await
    }

    // === Delete Operations ===

    pub async fn delete_queue(&self, queue: &str) -> Result<usize, StorageError> {
        let meta = self.get_meta(queue).await?;
        let claimed = self.scan_claimed(Some(queue)).await?;
        let acked = self.scan_acked(Some(queue)).await?;

        let mut batch = WriteBatch::new();
        let mut deleted = 0;

        batch.delete(Self::meta_key(queue));

        // Delete pending entries and messages
        for seq in meta.claim_seq..meta.push_seq {
            let pending_key = Self::pending_key(queue, seq);
            if let Some(msg_id_bytes) = self.db.get(&pending_key).await? {
                let msg_id = String::from_utf8_lossy(&msg_id_bytes).to_string();
                batch.delete(&pending_key);
                batch.delete(Self::msg_key(queue, &msg_id));
                deleted += 1;
            }
        }

        // Delete claimed entries and messages
        for (_, msg_id, _) in &claimed {
            batch.delete(Self::claimed_key(queue, msg_id));
            batch.delete(Self::msg_key(queue, msg_id));
            deleted += 1;
        }

        // Delete acked entries and messages
        for (_, ts, msg_id) in &acked {
            batch.delete(Self::acked_key(queue, *ts, msg_id));
            batch.delete(Self::msg_key(queue, msg_id));
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
                batch.delete(Self::acked_key(&queue, timestamp_ns, &msg_id));
                batch.delete(Self::msg_key(&queue, &msg_id));
                deleted += 1;
            }
        }

        if deleted > 0 {
            self.db.write(batch).await?;
            tracing::info!("GC deleted {} acked messages", deleted);
        }

        Ok(deleted)
    }

    pub async fn recover_expired_claims(
        &self,
        timeout_secs: f64,
        active_leases: Option<&HashMap<String, f64>>,
    ) -> Result<usize, StorageError> {
        let now = crate::types::now_secs();
        let all_claimed = self.scan_claimed(None).await?;

        // Group expired by queue
        let mut expired_by_queue: HashMap<String, Vec<ClaimInfo>> = HashMap::new();
        for (queue, msg_id, claim_info) in all_claimed {
            if let Some(leases) = active_leases {
                if let Some(last_seen) = leases.get(&claim_info.lease_id) {
                    if now - *last_seen <= timeout_secs {
                        continue;
                    }
                }
            }

            if now - claim_info.claimed_at > timeout_secs {
                let mut info = claim_info.clone();
                info.msg_id = msg_id;
                expired_by_queue.entry(queue).or_default().push(info);
            }
        }

        let mut total = 0;
        for (queue, claims) in expired_by_queue {
            let msg_ids: Vec<String> = claims.iter().map(|c| c.msg_id.clone()).collect();
            self.nack_messages_unchecked(&queue, &msg_ids).await?;

            let mut puts = HashMap::new();
            let ts_ns = now_nanos();
            for claim in &claims {
                let key = format!("timeout:{}:{}:{}", queue, ts_ns, claim.msg_id);
                let event = serde_json::json!({
                    "event_type": "timeout",
                    "timestamp_ns": ts_ns,
                    "timestamp": now,
                    "queue": queue,
                    "msg_id": claim.msg_id,
                    "worker_id": claim.worker_id,
                    "lease_id": claim.lease_id,
                    "claimed_at": claim.claimed_at,
                    "input_rows": 0,
                    "input_bytes": 0,
                    "output_rows": 0,
                    "output_bytes": 0,
                    "processing_ms": 0,
                    "queue_wait_ms": 0,
                    "reason": "claim_timeout",
                });
                puts.insert(key, serde_json::to_vec(&event)?);
            }
            if !puts.is_empty() {
                let _ = self.state_put_batch("wq_events", &puts, &[]).await?;
            }

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
            batch.put(Self::state_key(namespace, key), value);
        }
        for key in deletes {
            batch.delete(Self::state_key(namespace, key));
        }

        self.db.write(batch).await?;
        Ok((puts.len(), deletes.len()))
    }

    pub async fn state_scan_prefix(
        &self,
        namespace: &str,
        prefix: &str,
        limit: usize,
    ) -> Result<Vec<(String, Vec<u8>)>, StorageError> {
        let key_prefix = format!("state:{}:{}", namespace, prefix).into_bytes();
        let mut iter = self.db.scan_prefix(&key_prefix).await?;
        let mut results = Vec::new();
        let namespace_prefix = format!("state:{}:", namespace);

        while let Ok(Some(kv)) = iter.next().await {
            let key_str = String::from_utf8_lossy(&kv.key);
            if let Some(suffix) = key_str.strip_prefix(&namespace_prefix) {
                results.push((suffix.to_string(), kv.value.to_vec()));
                if limit > 0 && results.len() >= limit {
                    break;
                }
            }
        }

        Ok(results)
    }

    pub async fn list_queues(&self) -> Result<Vec<String>, StorageError> {
        let mut iter = self.db.scan_prefix(b"meta:").await?;
        let mut queues = Vec::new();
        while let Ok(Some(kv)) = iter.next().await {
            let key_str = String::from_utf8_lossy(&kv.key);
            if let Some(queue) = key_str.strip_prefix("meta:") {
                queues.push(queue.to_string());
            }
        }
        Ok(queues)
    }

    // === Queue Completion API ===

    /// Mark a queue as finished (no more messages will be pushed)
    pub async fn mark_queue_finished(&self, queue: &str) -> Result<(), StorageError> {
        self.db.put(&Self::finished_key(queue), b"1").await?;
        self.db.flush().await?;
        tracing::info!("Queue {} marked as finished", queue);
        Ok(())
    }

    /// Check if queue is finished (marked by upstream)
    pub async fn is_queue_finished(&self, queue: &str) -> Result<bool, StorageError> {
        Ok(self.db.get(&Self::finished_key(queue)).await?.is_some())
    }

    /// Check if queue is finished AND drained (safe for worker to exit)
    /// Returns (finished, drained, pending_count, claimed_count)
    pub async fn check_queue_completion(
        &self,
        queue: &str,
    ) -> Result<(bool, bool, u64, u64), StorageError> {
        let finished = self.is_queue_finished(queue).await?;
        let meta = self.get_meta(queue).await?;

        let pending_count = meta.push_seq.saturating_sub(meta.claim_seq);
        let claimed_count = meta.claimed_count;

        // Queue is only drained if:
        // 1. No pending messages (pending_count == 0)
        // 2. No in-flight messages (claimed_count == 0)
        // 3. Queue has actually received messages (total_pushed > 0) OR is explicitly finished
        // This prevents false "drained" when queue is empty but hasn't been used yet,
        // while allowing safe exit for empty finished queues.
        let drained =
            pending_count == 0 && claimed_count == 0 && (meta.total_pushed > 0 || finished);

        Ok((finished, drained, pending_count, claimed_count))
    }

    /// Clear the finished flag (for queue reuse/testing)
    #[allow(dead_code)]
    pub async fn clear_queue_finished(&self, queue: &str) -> Result<(), StorageError> {
        self.db.delete(&Self::finished_key(queue)).await?;
        Ok(())
    }

    // =========================================================================
    // QueueGroup Operations
    // =========================================================================

    /// Get group metadata. Returns None if group doesn't exist.
    pub async fn get_group_meta(
        &self,
        group_name: &str,
    ) -> Result<Option<QueueGroupMeta>, StorageError> {
        match self.db.get(&Self::group_meta_key(group_name)).await? {
            Some(data) => Ok(Some(serde_json::from_slice(&data)?)),
            None => Ok(None),
        }
    }

    /// Create a queue group with N partition queues atomically.
    /// Idempotent: returns existing group if it already exists.
    pub async fn create_queue_group(
        &self,
        group_name: &str,
        num_partitions: u32,
    ) -> Result<QueueGroupMeta, StorageError> {
        // Check for existing group
        if let Some(existing) = self.get_group_meta(group_name).await? {
            return Ok(existing);
        }

        let meta = QueueGroupMeta::new(group_name.to_string(), num_partitions);

        // Create group_meta + all partition queue metas in one WriteBatch
        let mut batch = WriteBatch::new();
        batch.put(
            Self::group_meta_key(group_name),
            &serde_json::to_vec(&meta)?,
        );
        for queue_name in &meta.partition_queues {
            let queue_meta = QueueMeta::default();
            batch.put(
                Self::meta_key(queue_name),
                &serde_json::to_vec(&queue_meta)?,
            );
        }
        self.db.write(batch).await?;

        tracing::info!(
            "Created queue group '{}' with {} partitions",
            group_name,
            num_partitions
        );
        Ok(meta)
    }

    /// Atomic ack upstream + push to multiple downstream partition queues.
    ///
    /// All operations happen in a single SlateDB transaction:
    /// 1. Validate and ack upstream messages
    /// 2. Push payloads to each partition's queue
    /// 3. Update state
    ///
    /// `partition_payloads`: Vec of (partition_index, messages) pairs.
    #[allow(clippy::too_many_arguments)]
    pub async fn ack_and_scatter(
        &self,
        upstream_queue: &str,
        upstream_msg_ids: &[String],
        upstream_claim_tokens: &[String],
        worker_id: &str,
        lease_id: &str,
        group_name: &str,
        partition_payloads: &[(u32, Vec<Message>)],
        state_namespace: Option<&str>,
        state_puts: Option<&HashMap<String, Vec<u8>>>,
        state_deletes: Option<&[String]>,
    ) -> Result<Vec<String>, StorageError> {
        // Resolve group
        let group = self
            .get_group_meta(group_name)
            .await?
            .ok_or_else(|| SlateError::invalid(format!("Queue group not found: {}", group_name)))?;

        // Validate partition indices
        for (pid, _) in partition_payloads {
            if *pid >= group.num_partitions {
                return Err(Box::new(SlateError::invalid(format!(
                    "Partition {} out of range [0, {})",
                    pid, group.num_partitions
                ))));
            }
        }

        let expected_lease_id = if lease_id.is_empty() {
            None
        } else {
            Some(lease_id)
        };
        let expected_worker_id = if worker_id.is_empty() {
            None
        } else {
            Some(worker_id)
        };

        for attempt in 0..MAX_TXN_RETRIES {
            let txn = self.db.begin(IsolationLevel::SerializableSnapshot).await?;

            let now_ns = now_nanos();
            let ack_count = upstream_msg_ids.len() as u64;
            let mut all_new_msg_ids = Vec::new();

            // 1. Validate claims and ack upstream
            if !upstream_msg_ids.is_empty() {
                if upstream_claim_tokens.len() != upstream_msg_ids.len() {
                    return Err(Box::new(SlateError::invalid(
                        "claim_tokens length must match msg_ids".to_string(),
                    )));
                }

                for (msg_id, token) in upstream_msg_ids.iter().zip(upstream_claim_tokens.iter()) {
                    let claim_info =
                        Self::get_claim_info_from_reader(&txn, upstream_queue, msg_id).await?;
                    if claim_info.claim_token != *token {
                        return Err(Box::new(SlateError::invalid(format!(
                            "claim_token mismatch for msg_id {}",
                            msg_id
                        ))));
                    }
                    if let Some(expected) = expected_lease_id {
                        if claim_info.lease_id != expected {
                            return Err(Box::new(SlateError::invalid(format!(
                                "lease_id mismatch for msg_id {}",
                                msg_id
                            ))));
                        }
                    }
                    if let Some(expected) = expected_worker_id {
                        if claim_info.worker_id != expected {
                            return Err(Box::new(SlateError::invalid(format!(
                                "worker_id mismatch for msg_id {}",
                                msg_id
                            ))));
                        }
                    }
                }

                let upstream_meta = Self::get_meta_from_reader(&txn, upstream_queue).await?;
                for msg_id in upstream_msg_ids {
                    txn.delete(Self::claimed_key(upstream_queue, msg_id))?;
                    txn.put(Self::acked_key(upstream_queue, now_ns, msg_id), [])?;
                }
                let new_upstream_meta = QueueMeta {
                    claimed_count: upstream_meta.claimed_count.saturating_sub(ack_count),
                    total_acked: upstream_meta.total_acked + ack_count,
                    ..upstream_meta
                };
                txn.put(
                    Self::meta_key(upstream_queue),
                    &serde_json::to_vec(&new_upstream_meta)?,
                )?;
            }

            // 2. Push to each partition queue
            for (pid, messages) in partition_payloads {
                if messages.is_empty() {
                    continue;
                }
                let partition_queue = &group.partition_queues[*pid as usize];
                let partition_meta = Self::get_meta_from_reader(&txn, partition_queue).await?;
                let msg_count = messages.len() as u64;

                for (i, msg) in messages.iter().enumerate() {
                    let seq = partition_meta.push_seq + i as u64;
                    txn.put(
                        Self::msg_key(partition_queue, &msg.msg_id),
                        &serde_json::to_vec(msg)?,
                    )?;
                    txn.put(
                        Self::pending_key(partition_queue, seq),
                        msg.msg_id.as_bytes(),
                    )?;
                    all_new_msg_ids.push(msg.msg_id.clone());
                }

                let new_partition_meta = QueueMeta {
                    push_seq: partition_meta.push_seq + msg_count,
                    total_pushed: partition_meta.total_pushed + msg_count,
                    ..partition_meta
                };
                txn.put(
                    Self::meta_key(partition_queue),
                    &serde_json::to_vec(&new_partition_meta)?,
                )?;
            }

            // 3. State updates
            if let Some(namespace) = state_namespace {
                if let Some(puts) = state_puts {
                    for (key, value) in puts {
                        txn.put(Self::state_key(namespace, key), value)?;
                    }
                }
                if let Some(deletes) = state_deletes {
                    for key in deletes {
                        txn.delete(Self::state_key(namespace, key))?;
                    }
                }
            }

            match txn.commit().await {
                Ok(()) => return Ok(all_new_msg_ids),
                Err(e) if is_txn_conflict(&e) && attempt + 1 < MAX_TXN_RETRIES => {
                    sleep(Duration::from_millis(5 * (attempt as u64 + 1))).await;
                    continue;
                }
                Err(e) => return Err(Box::new(e)),
            }
        }

        Err(Box::new(SlateError::transaction(
            "ack_and_scatter exceeded retry budget".to_string(),
        )))
    }

    /// Claim from a partition group (O(1) per call).
    ///
    /// Assigned path: tries claim_messages directly on each assigned partition
    /// (no meta reads, no sort). A is small (2-4), so this is effectively O(1).
    ///
    /// Steal path: probes ONE unassigned partition per call via round-robin
    /// counter, avoiding full-scan. The caller's retry loop rotates through all
    /// partitions across successive calls.
    #[allow(clippy::too_many_arguments)]
    pub async fn claim_from_group(
        &self,
        group_name: &str,
        batch_size: usize,
        worker_id: &str,
        lease_id: &str,
        assigned_partitions: &[u32],
        allow_steal: bool,
        steal_pending_threshold: u64,
    ) -> Result<(Vec<ClaimedMessage>, String, u32), StorageError> {
        let group = self
            .get_group_meta(group_name)
            .await?
            .ok_or_else(|| SlateError::invalid(format!("Queue group not found: {}", group_name)))?;

        // O(A) where A is small (2-4): try claim directly, no meta reads needed.
        // Round-robin starting offset prevents starvation when one worker owns
        // multiple partitions and earlier ones always have work.
        if !assigned_partitions.is_empty() {
            let a_len = assigned_partitions.len() as u64;
            let rr_assigned = {
                let mut map = self.steal_rr.lock().unwrap();
                let key = format!("{}_assigned_{}", group_name, worker_id);
                let counter = map.entry(key).or_insert(0);
                let val = *counter;
                *counter = val.wrapping_add(1);
                val
            };
            for offset in 0..a_len {
                let idx = ((rr_assigned + offset) % a_len) as usize;
                let pid = assigned_partitions[idx];
                if (pid as usize) < group.partition_queues.len() {
                    let queue_name = &group.partition_queues[pid as usize];
                    let claimed = self
                        .claim_messages(queue_name, batch_size, worker_id, lease_id)
                        .await?;
                    if !claimed.is_empty() {
                        return Ok((claimed, queue_name.to_string(), pid));
                    }
                }
            }
        }

        // Steal: round-robin through unassigned partitions.
        // When threshold == 0: skip meta reads, just try claim_messages directly
        // (each is O(1) single-key lookup). When threshold > 0: read meta for
        // ONE candidate only (bounded cost).
        if allow_steal {
            let total = group.partition_queues.len() as u64;
            if total > 0 {
                let rr = {
                    let mut map = self.steal_rr.lock().unwrap();
                    let counter = map.entry(group_name.to_string()).or_insert(0);
                    let val = *counter;
                    *counter = val.wrapping_add(1);
                    val
                };

                for offset in 0..total {
                    let pid = ((rr + offset) % total) as u32;
                    if assigned_partitions.contains(&pid) {
                        continue;
                    }
                    let queue_name = &group.partition_queues[pid as usize];
                    if steal_pending_threshold > 0 {
                        let meta = self.get_meta(queue_name).await?;
                        let pending = meta.push_seq.saturating_sub(meta.claim_seq);
                        if pending <= steal_pending_threshold {
                            continue;
                        }
                    }
                    let claimed = self
                        .claim_messages(queue_name, batch_size, worker_id, lease_id)
                        .await?;
                    if !claimed.is_empty() {
                        return Ok((claimed, queue_name.to_string(), pid));
                    }
                }
            }
        }

        // Nothing to claim
        Ok((Vec::new(), String::new(), 0))
    }

    /// Check if all queues in a group are finished and drained.
    pub async fn check_group_completion(
        &self,
        group_name: &str,
    ) -> Result<(bool, bool, Vec<(u32, u64, u64, bool)>), StorageError> {
        let group = self
            .get_group_meta(group_name)
            .await?
            .ok_or_else(|| SlateError::invalid(format!("Queue group not found: {}", group_name)))?;

        let mut all_finished = true;
        let mut all_drained = true;
        let mut partition_statuses = Vec::new();

        for (i, queue_name) in group.partition_queues.iter().enumerate() {
            let (finished, drained, pending, claimed) =
                self.check_queue_completion(queue_name).await?;
            if !finished {
                all_finished = false;
            }
            if !drained {
                all_drained = false;
            }
            partition_statuses.push((i as u32, pending, claimed, finished));
        }

        Ok((all_finished, all_drained, partition_statuses))
    }

    /// Get stats for all partitions in a group, with skew detection.
    /// Returns (group_meta, partition_stats_vec).
    pub async fn get_group_stats(
        &self,
        group_name: &str,
    ) -> Result<(QueueGroupMeta, Vec<(u32, QueueMeta)>), StorageError> {
        let group = self
            .get_group_meta(group_name)
            .await?
            .ok_or_else(|| SlateError::invalid(format!("Queue group not found: {}", group_name)))?;

        let mut stats = Vec::new();
        for (i, queue_name) in group.partition_queues.iter().enumerate() {
            let meta = self.get_meta(queue_name).await?;
            stats.push((i as u32, meta));
        }

        Ok((group, stats))
    }

    /// Mark all queues in a group as finished.
    pub async fn mark_group_finished(&self, group_name: &str) -> Result<u32, StorageError> {
        let group = self
            .get_group_meta(group_name)
            .await?
            .ok_or_else(|| SlateError::invalid(format!("Queue group not found: {}", group_name)))?;

        let mut batch = WriteBatch::new();
        for queue_name in &group.partition_queues {
            batch.put(Self::finished_key(queue_name), b"1");
        }
        self.db.write(batch).await?;

        // Clean up in-memory round-robin counters for this group
        // Use delimiter-aware prefix to avoid matching groups whose name
        // starts with the same prefix (e.g., "job_s1_output" vs "job_s1_output_extra")
        let prefix = format!("{}_", group_name);
        {
            let mut map = self.steal_rr.lock().unwrap();
            map.retain(|k, _| !k.starts_with(&prefix) && k != group_name);
        }

        tracing::info!(
            "Marked all {} queues in group '{}' as finished",
            group.num_partitions,
            group_name
        );
        Ok(group.num_partitions)
    }
}

#[cfg(test)]
#[allow(clippy::await_holding_lock)] // SIM_TIME_LOCK is intentionally held across awaits to serialize time-sensitive tests
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    static TEST_COUNTER: AtomicUsize = AtomicUsize::new(0);

    async fn create_temp_storage() -> WorkQueueStorage {
        let counter = TEST_COUNTER.fetch_add(1, Ordering::SeqCst);
        let temp_dir = std::env::temp_dir().join(format!("workqueue_test_{}", counter));
        let _ = std::fs::remove_dir_all(&temp_dir);
        WorkQueueStorage::new(&format!("file://{}", temp_dir.display()))
            .await
            .unwrap()
    }

    fn split_claims(claimed: &[ClaimedMessage]) -> (Vec<String>, Vec<String>) {
        let msg_ids = claimed.iter().map(|c| c.message.msg_id.clone()).collect();
        let claim_tokens = claimed.iter().map(|c| c.claim_token.clone()).collect();
        (msg_ids, claim_tokens)
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

        let claimed = storage
            .claim_messages(queue, 2, "worker-1", "lease-1")
            .await
            .unwrap();
        assert_eq!(claimed.len(), 2);
        assert_eq!(claimed[0].message.payload, b"hello");
        assert_eq!(claimed[1].message.payload, b"world");

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

        let claimed = storage
            .claim_messages(queue, 5, "worker-1", "lease-1")
            .await
            .unwrap();
        assert_eq!(claimed.len(), 5);
    }

    #[tokio::test]
    async fn test_ack() {
        let storage = create_temp_storage().await;
        let queue = "test-queue";

        storage.create_queue(queue).await.unwrap();

        let msg = Message::new(queue.to_string(), b"hello".to_vec());
        storage.push_message(queue, &msg).await.unwrap();
        let claimed = storage
            .claim_messages(queue, 1, "worker-1", "lease-1")
            .await
            .unwrap();
        let (msg_ids, claim_tokens) = split_claims(&claimed);
        storage
            .ack_messages(queue, &msg_ids, &claim_tokens, "worker-1", "lease-1")
            .await
            .unwrap();

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
        storage.push_message(queue, &msg).await.unwrap();
        let claimed = storage
            .claim_messages(queue, 1, "worker-1", "lease-1")
            .await
            .unwrap();
        let (msg_ids, claim_tokens) = split_claims(&claimed);
        let msg_id = msg_ids[0].clone();

        let meta = storage.get_meta(queue).await.unwrap();
        assert_eq!(meta.claim_seq, 1);
        assert_eq!(meta.push_seq, 1);

        storage
            .nack_messages(queue, &msg_ids, &claim_tokens, "worker-1", "lease-1")
            .await
            .unwrap();

        let meta = storage.get_meta(queue).await.unwrap();
        assert_eq!(meta.claim_seq, 1);
        assert_eq!(meta.push_seq, 2);

        let claimed_again = storage
            .claim_messages(queue, 1, "worker-1", "lease-1")
            .await
            .unwrap();
        assert_eq!(claimed_again.len(), 1);
        assert_eq!(claimed_again[0].message.msg_id, msg_id);
    }

    #[tokio::test]
    async fn test_ack_rejects_wrong_claim_token() {
        let storage = create_temp_storage().await;
        let queue = "test-queue";

        storage.create_queue(queue).await.unwrap();

        let msg = Message::new(queue.to_string(), b"hello".to_vec());
        storage.push_message(queue, &msg).await.unwrap();
        let claimed = storage
            .claim_messages(queue, 1, "worker-1", "lease-1")
            .await
            .unwrap();
        let (msg_ids, _claim_tokens) = split_claims(&claimed);

        let result = storage
            .ack_messages(
                queue,
                &msg_ids,
                &[String::from("bad-token")],
                "worker-1",
                "lease-1",
            )
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_ack_rejects_wrong_lease() {
        let storage = create_temp_storage().await;
        let queue = "test-queue";

        storage.create_queue(queue).await.unwrap();

        let msg = Message::new(queue.to_string(), b"hello".to_vec());
        storage.push_message(queue, &msg).await.unwrap();
        let claimed = storage
            .claim_messages(queue, 1, "worker-1", "lease-1")
            .await
            .unwrap();
        let (msg_ids, claim_tokens) = split_claims(&claimed);

        let result = storage
            .ack_messages(queue, &msg_ids, &claim_tokens, "worker-1", "lease-bad")
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_nack_rejects_wrong_worker() {
        let storage = create_temp_storage().await;
        let queue = "test-queue";

        storage.create_queue(queue).await.unwrap();

        let msg = Message::new(queue.to_string(), b"hello".to_vec());
        storage.push_message(queue, &msg).await.unwrap();
        let claimed = storage
            .claim_messages(queue, 1, "worker-1", "lease-1")
            .await
            .unwrap();
        let (msg_ids, claim_tokens) = split_claims(&claimed);

        let result = storage
            .nack_messages(queue, &msg_ids, &claim_tokens, "worker-2", "lease-1")
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_ack_and_forward_rejects_wrong_claim_token() {
        let storage = create_temp_storage().await;

        storage.create_queue("upstream").await.unwrap();
        storage.create_queue("downstream").await.unwrap();

        let upstream_msg = Message::new("upstream".to_string(), b"input".to_vec());
        storage
            .push_message("upstream", &upstream_msg)
            .await
            .unwrap();
        let claimed = storage
            .claim_messages("upstream", 1, "worker-1", "lease-1")
            .await
            .unwrap();

        let downstream_msgs: Vec<Message> =
            vec![Message::new("downstream".to_string(), b"out".to_vec())];

        let result = storage
            .ack_and_forward(
                "upstream",
                &[claimed[0].message.msg_id.clone()],
                &[String::from("bad-token")],
                "worker-1",
                "lease-1",
                "downstream",
                &downstream_msgs,
            )
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_claim_token_changes_after_nack() {
        let storage = create_temp_storage().await;
        let queue = "test-queue";

        storage.create_queue(queue).await.unwrap();

        let msg = Message::new(queue.to_string(), b"hello".to_vec());
        storage.push_message(queue, &msg).await.unwrap();

        let claimed = storage
            .claim_messages(queue, 1, "worker-1", "lease-1")
            .await
            .unwrap();
        let (msg_ids, claim_tokens) = split_claims(&claimed);

        storage
            .nack_messages(queue, &msg_ids, &claim_tokens, "worker-1", "lease-1")
            .await
            .unwrap();

        let claimed_again = storage
            .claim_messages(queue, 1, "worker-1", "lease-1")
            .await
            .unwrap();
        assert_eq!(claimed_again.len(), 1);
        assert_ne!(claimed_again[0].claim_token, claim_tokens[0]);
    }

    #[tokio::test]
    async fn test_ack_rejects_stale_token_after_reclaim() {
        use crate::types::{advance_sim_time_secs, set_sim_time_nanos, SIM_TIME_LOCK};
        let _guard = SIM_TIME_LOCK.lock().unwrap();
        set_sim_time_nanos(1_900_000_000_000_000_000);

        let storage = create_temp_storage().await;
        let queue = "test-queue";

        storage.create_queue(queue).await.unwrap();

        let msg = Message::new(queue.to_string(), b"hello".to_vec());
        storage.push_message(queue, &msg).await.unwrap();

        let claimed = storage
            .claim_messages(queue, 1, "worker-1", "lease-1")
            .await
            .unwrap();
        let (msg_ids, claim_tokens) = split_claims(&claimed);

        // Advance sim time so claim is expired
        advance_sim_time_secs(1.0);
        let recovered = storage.recover_expired_claims(0.0, None).await.unwrap();
        assert_eq!(recovered, 1);

        let claimed_again = storage
            .claim_messages(queue, 1, "worker-1", "lease-1")
            .await
            .unwrap();
        assert_eq!(claimed_again.len(), 1);

        let result = storage
            .ack_messages(queue, &msg_ids, &claim_tokens, "worker-1", "lease-1")
            .await;
        assert!(result.is_err());

        set_sim_time_nanos(0);
    }

    #[tokio::test]
    async fn test_recover_respects_active_leases() {
        use crate::types::{advance_sim_time_secs, set_sim_time_nanos, SIM_TIME_LOCK};
        let _guard = SIM_TIME_LOCK.lock().unwrap();
        set_sim_time_nanos(2_100_000_000_000_000_000);

        let storage = create_temp_storage().await;
        let queue = "test-queue";

        storage.create_queue(queue).await.unwrap();

        let msg = Message::new(queue.to_string(), b"hello".to_vec());
        storage.push_message(queue, &msg).await.unwrap();
        let claimed = storage
            .claim_messages(queue, 1, "worker-1", "lease-1")
            .await
            .unwrap();

        advance_sim_time_secs(1.0);

        let mut active = HashMap::new();
        // Set last_seen to current sim time to guarantee "active" for zero timeout.
        active.insert("lease-1".to_string(), crate::types::now_secs());

        let recovered = storage
            .recover_expired_claims(0.0, Some(&active))
            .await
            .unwrap();
        assert_eq!(recovered, 0);

        let still_claimed = storage.scan_claimed(Some(queue)).await.unwrap();
        assert_eq!(still_claimed.len(), 1);
        assert_eq!(still_claimed[0].1, claimed[0].message.msg_id);

        set_sim_time_nanos(0);
    }

    #[tokio::test]
    async fn test_queue_completion_empty_finished() {
        let storage = create_temp_storage().await;
        let queue = "test-queue";

        storage.create_queue(queue).await.unwrap();
        storage.mark_queue_finished(queue).await.unwrap();

        let (finished, drained, pending, claimed) =
            storage.check_queue_completion(queue).await.unwrap();
        assert!(finished);
        assert!(drained);
        assert_eq!(pending, 0);
        assert_eq!(claimed, 0);
    }

    #[tokio::test]
    async fn test_ack_and_forward() {
        let storage = create_temp_storage().await;

        storage.create_queue("upstream").await.unwrap();
        storage.create_queue("downstream").await.unwrap();

        let upstream_msg = Message::new("upstream".to_string(), b"input".to_vec());
        storage
            .push_message("upstream", &upstream_msg)
            .await
            .unwrap();
        let claimed = storage
            .claim_messages("upstream", 1, "worker-1", "lease-1")
            .await
            .unwrap();
        let (upstream_ids, upstream_tokens) = split_claims(&claimed);

        let downstream_msgs: Vec<Message> = (0..2)
            .map(|i| Message::new("downstream".to_string(), format!("out{}", i).into_bytes()))
            .collect();

        storage
            .ack_and_forward(
                "upstream",
                &upstream_ids,
                &upstream_tokens,
                "worker-1",
                "lease-1",
                "downstream",
                &downstream_msgs,
            )
            .await
            .unwrap();

        let acked = storage.scan_acked(Some("upstream")).await.unwrap();
        assert_eq!(acked.len(), 1);

        let meta = storage.get_meta("downstream").await.unwrap();
        assert_eq!(meta.push_seq, 2);

        let downstream_claimed = storage
            .claim_messages("downstream", 2, "worker-2", "lease-2")
            .await
            .unwrap();
        assert_eq!(downstream_claimed.len(), 2);
    }

    #[tokio::test]
    async fn test_gc_acked_messages() {
        use crate::types::{advance_sim_time_secs, set_sim_time_nanos, SIM_TIME_LOCK};
        let _guard = SIM_TIME_LOCK.lock().unwrap();
        set_sim_time_nanos(1_800_000_000_000_000_000);

        let storage = create_temp_storage().await;
        let queue = "test-queue";

        storage.create_queue(queue).await.unwrap();

        for i in 0..5 {
            let msg = Message::new(queue.to_string(), format!("msg{}", i).into_bytes());
            storage.push_message(queue, &msg).await.unwrap();
            let claimed = storage
                .claim_messages(queue, 1, "worker-1", "lease-1")
                .await
                .unwrap();
            let (msg_ids, claim_tokens) = split_claims(&claimed);
            storage
                .ack_messages(queue, &msg_ids, &claim_tokens, "worker-1", "lease-1")
                .await
                .unwrap();
        }

        let acked = storage.scan_acked(Some(queue)).await.unwrap();
        assert_eq!(acked.len(), 5);

        // Advance time so messages are older than retention=0
        advance_sim_time_secs(1.0);

        let deleted = storage.gc_acked_messages(0).await.unwrap();
        assert_eq!(deleted, 5);

        let acked = storage.scan_acked(Some(queue)).await.unwrap();
        assert!(acked.is_empty());

        set_sim_time_nanos(0);
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

        let meta = storage.get_queue_stats(queue).await.unwrap();
        let pending = meta.push_seq.saturating_sub(meta.claim_seq);
        assert_eq!(pending, 5);
        assert_eq!(meta.claimed_count, 0);
        assert_eq!(meta.total_pushed, 5);

        storage
            .claim_messages(queue, 3, "worker-1", "lease-1")
            .await
            .unwrap();

        let meta = storage.get_queue_stats(queue).await.unwrap();
        let pending = meta.push_seq.saturating_sub(meta.claim_seq);
        assert_eq!(pending, 2);
        assert_eq!(meta.claimed_count, 3);
    }

    #[tokio::test]
    async fn test_state_operations() {
        let storage = create_temp_storage().await;
        let namespace = "job1/stage1";

        let mut puts = HashMap::new();
        puts.insert("key1".to_string(), b"value1".to_vec());
        puts.insert("key2".to_string(), b"value2".to_vec());

        storage
            .state_put_batch(namespace, &puts, &[])
            .await
            .unwrap();

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

        storage.push_message(queue, &msg).await.unwrap();
        let claimed = storage
            .claim_messages(queue, 1, "worker-1", "lease-1")
            .await
            .unwrap();
        let (msg_ids, claim_tokens) = split_claims(&claimed);

        let mut state_puts = HashMap::new();
        state_puts.insert("seen_key".to_string(), b"1".to_vec());

        storage
            .ack_with_state(
                queue,
                &msg_ids,
                &claim_tokens,
                "worker-1",
                "lease-1",
                namespace,
                &state_puts,
                &[],
            )
            .await
            .unwrap();

        let values = storage
            .state_get_batch(namespace, &["seen_key".to_string()])
            .await
            .unwrap();
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
        storage
            .claim_messages(queue, 2, "worker-1", "lease-1")
            .await
            .unwrap();

        let deleted = storage.delete_queue(queue).await.unwrap();
        assert!(deleted > 0);

        let meta = storage.get_meta(queue).await.unwrap();
        assert_eq!(meta.claim_seq, 0);
        assert_eq!(meta.push_seq, 0);
    }

    #[tokio::test]
    async fn test_recover_expired_claims() {
        use crate::types::{advance_sim_time_secs, set_sim_time_nanos, SIM_TIME_LOCK};
        let _guard = SIM_TIME_LOCK.lock().unwrap();
        set_sim_time_nanos(2_000_000_000_000_000_000);

        let storage = create_temp_storage().await;
        let queue = "test-queue";

        storage.create_queue(queue).await.unwrap();

        let msg = Message::new(queue.to_string(), b"hello".to_vec());
        storage.push_message(queue, &msg).await.unwrap();
        storage
            .claim_messages(queue, 1, "worker-1", "lease-1")
            .await
            .unwrap();

        advance_sim_time_secs(1.0);
        let recovered = storage.recover_expired_claims(0.0, None).await.unwrap();
        assert_eq!(recovered, 1);

        let claimed = storage
            .claim_messages(queue, 1, "worker-2", "lease-2")
            .await
            .unwrap();
        assert_eq!(claimed.len(), 1);

        set_sim_time_nanos(0);
    }

    #[tokio::test]
    async fn test_counter_correctness_full_lifecycle() {
        // Verify O(1) counters are maintained correctly through full message lifecycle
        let storage = create_temp_storage().await;
        let queue = "test-queue";

        storage.create_queue(queue).await.unwrap();

        // Initial state
        let meta = storage.get_queue_stats(queue).await.unwrap();
        assert_eq!(meta.claimed_count, 0);
        assert_eq!(meta.total_pushed, 0);
        assert_eq!(meta.total_acked, 0);

        // Push 10 messages
        let mut msg_ids = Vec::new();
        for i in 0..10 {
            let msg = Message::new(queue.to_string(), format!("msg{}", i).into_bytes());
            msg_ids.push(msg.msg_id.clone());
            storage.push_message(queue, &msg).await.unwrap();
        }

        let meta = storage.get_queue_stats(queue).await.unwrap();
        assert_eq!(meta.claimed_count, 0, "No messages claimed yet");
        assert_eq!(meta.total_pushed, 10, "10 messages pushed");
        assert_eq!(meta.total_acked, 0, "No messages acked yet");

        // Claim 5 messages
        let claimed = storage
            .claim_messages(queue, 5, "worker-1", "lease-1")
            .await
            .unwrap();
        assert_eq!(claimed.len(), 5);
        let (claimed_ids, claimed_tokens) = split_claims(&claimed);

        let meta = storage.get_queue_stats(queue).await.unwrap();
        assert_eq!(meta.claimed_count, 5, "5 messages claimed");
        assert_eq!(meta.total_pushed, 10);
        assert_eq!(meta.total_acked, 0);

        // Ack 3 messages
        let ack_ids: Vec<String> = claimed_ids[0..3].to_vec();
        let ack_tokens: Vec<String> = claimed_tokens[0..3].to_vec();
        storage
            .ack_messages(queue, &ack_ids, &ack_tokens, "worker-1", "lease-1")
            .await
            .unwrap();

        let meta = storage.get_queue_stats(queue).await.unwrap();
        assert_eq!(meta.claimed_count, 2, "5 - 3 = 2 claimed");
        assert_eq!(meta.total_pushed, 10);
        assert_eq!(meta.total_acked, 3, "3 messages acked");

        // Nack 2 messages (return to pending)
        let nack_ids: Vec<String> = claimed_ids[3..5].to_vec();
        let nack_tokens: Vec<String> = claimed_tokens[3..5].to_vec();
        storage
            .nack_messages(queue, &nack_ids, &nack_tokens, "worker-1", "lease-1")
            .await
            .unwrap();

        let meta = storage.get_queue_stats(queue).await.unwrap();
        assert_eq!(meta.claimed_count, 0, "All claimed messages handled");
        assert_eq!(meta.total_pushed, 10);
        assert_eq!(meta.total_acked, 3);

        // Verify pending count: 10 original - 5 claimed + 2 nacked back = 7 pending
        let pending = meta.push_seq.saturating_sub(meta.claim_seq);
        assert_eq!(pending, 7, "7 messages pending (5 unclaimed + 2 nacked)");

        // Claim and ack remaining
        let remaining = storage
            .claim_messages(queue, 10, "worker-2", "lease-2")
            .await
            .unwrap();
        assert_eq!(remaining.len(), 7);

        let (remaining_ids, remaining_tokens) = split_claims(&remaining);
        storage
            .ack_messages(
                queue,
                &remaining_ids,
                &remaining_tokens,
                "worker-2",
                "lease-2",
            )
            .await
            .unwrap();

        let meta = storage.get_queue_stats(queue).await.unwrap();
        assert_eq!(meta.claimed_count, 0, "All messages processed");
        assert_eq!(meta.total_pushed, 10);
        assert_eq!(
            meta.total_acked, 10,
            "All 10 messages acked (including re-acked nacked ones)"
        );
    }

    #[tokio::test]
    async fn test_counter_correctness_ack_and_forward() {
        // Verify counters are correct with ack_and_forward
        let storage = create_temp_storage().await;

        storage.create_queue("upstream").await.unwrap();
        storage.create_queue("downstream").await.unwrap();

        // Push to upstream
        for i in 0..5 {
            let msg = Message::new("upstream".to_string(), format!("msg{}", i).into_bytes());
            storage.push_message("upstream", &msg).await.unwrap();
        }

        // Claim from upstream
        let claimed = storage
            .claim_messages("upstream", 5, "worker-1", "lease-1")
            .await
            .unwrap();
        assert_eq!(claimed.len(), 5);

        // Ack upstream and forward to downstream (2 outputs per input)
        for msg in &claimed {
            let downstream_msgs: Vec<Message> = (0..2)
                .map(|i| {
                    Message::new(
                        "downstream".to_string(),
                        format!("out-{}-{}", msg.message.msg_id, i).into_bytes(),
                    )
                })
                .collect();
            storage
                .ack_and_forward(
                    "upstream",
                    std::slice::from_ref(&msg.message.msg_id),
                    std::slice::from_ref(&msg.claim_token),
                    "worker-1",
                    "lease-1",
                    "downstream",
                    &downstream_msgs,
                )
                .await
                .unwrap();
        }

        // Verify upstream counters
        let upstream_meta = storage.get_queue_stats("upstream").await.unwrap();
        assert_eq!(
            upstream_meta.claimed_count, 0,
            "All upstream claimed messages acked"
        );
        assert_eq!(upstream_meta.total_pushed, 5);
        assert_eq!(upstream_meta.total_acked, 5);

        // Verify downstream counters
        let downstream_meta = storage.get_queue_stats("downstream").await.unwrap();
        assert_eq!(
            downstream_meta.claimed_count, 0,
            "No downstream messages claimed yet"
        );
        assert_eq!(
            downstream_meta.total_pushed, 10,
            "5 inputs * 2 outputs = 10"
        );
        assert_eq!(downstream_meta.total_acked, 0);

        // Verify downstream pending
        let downstream_pending = downstream_meta
            .push_seq
            .saturating_sub(downstream_meta.claim_seq);
        assert_eq!(downstream_pending, 10);
    }

    // =========================================================================
    // QueueGroup tests
    // =========================================================================

    #[tokio::test]
    async fn test_create_queue_group() {
        let storage = create_temp_storage().await;

        let group = storage.create_queue_group("grp1", 4).await.unwrap();
        assert_eq!(group.name, "grp1");
        assert_eq!(group.num_partitions, 4);
        assert_eq!(group.partition_queues.len(), 4);
        assert_eq!(group.partition_queues[0], "grp1_p0");
        assert_eq!(group.partition_queues[3], "grp1_p3");
        assert_eq!(group.version, 0);

        // Each partition queue should have metadata
        for q in &group.partition_queues {
            let meta = storage.get_meta(q).await.unwrap();
            assert_eq!(meta.push_seq, 0);
            assert_eq!(meta.claim_seq, 0);
        }

        // Idempotent: creating again returns the same group
        let group2 = storage.create_queue_group("grp1", 4).await.unwrap();
        assert_eq!(group2.name, group.name);
        assert_eq!(group2.num_partitions, group.num_partitions);
    }

    #[tokio::test]
    async fn test_ack_and_scatter() {
        let storage = create_temp_storage().await;

        // Setup: upstream queue with 2 messages, downstream group with 3 partitions
        storage.create_queue("upstream").await.unwrap();
        let group = storage.create_queue_group("grp1", 3).await.unwrap();

        let msg1 = Message::new("upstream".to_string(), b"a".to_vec());
        let msg2 = Message::new("upstream".to_string(), b"b".to_vec());
        storage.push_message("upstream", &msg1).await.unwrap();
        storage.push_message("upstream", &msg2).await.unwrap();

        let claimed = storage
            .claim_messages("upstream", 2, "w1", "l1")
            .await
            .unwrap();
        assert_eq!(claimed.len(), 2);
        let (msg_ids, claim_tokens) = split_claims(&claimed);

        // Build partition payloads: 2 messages to p0, 1 to p2
        let out_p0 = vec![
            Message::new(group.partition_queues[0].clone(), b"x1".to_vec()),
            Message::new(group.partition_queues[0].clone(), b"x2".to_vec()),
        ];
        let out_p2 = vec![Message::new(
            group.partition_queues[2].clone(),
            b"y1".to_vec(),
        )];
        let partition_payloads: Vec<(u32, Vec<Message>)> = vec![(0, out_p0), (2, out_p2)];

        let new_ids = storage
            .ack_and_scatter(
                "upstream",
                &msg_ids,
                &claim_tokens,
                "w1",
                "l1",
                "grp1",
                &partition_payloads,
                None,
                None,
                None,
            )
            .await
            .unwrap();
        assert_eq!(new_ids.len(), 3); // 2 + 1

        // Upstream should be fully acked
        let upstream_meta = storage.get_queue_stats("upstream").await.unwrap();
        assert_eq!(upstream_meta.claimed_count, 0);
        assert_eq!(upstream_meta.total_acked, 2);

        // Partition p0 should have 2 pending
        let p0_meta = storage.get_meta(&group.partition_queues[0]).await.unwrap();
        assert_eq!(p0_meta.push_seq, 2);
        assert_eq!(p0_meta.total_pushed, 2);

        // Partition p1 should be empty
        let p1_meta = storage.get_meta(&group.partition_queues[1]).await.unwrap();
        assert_eq!(p1_meta.push_seq, 0);

        // Partition p2 should have 1 pending
        let p2_meta = storage.get_meta(&group.partition_queues[2]).await.unwrap();
        assert_eq!(p2_meta.push_seq, 1);
        assert_eq!(p2_meta.total_pushed, 1);

        // Verify we can claim from partition queues
        let claimed_p0 = storage
            .claim_messages(&group.partition_queues[0], 10, "w2", "l2")
            .await
            .unwrap();
        assert_eq!(claimed_p0.len(), 2);
        assert_eq!(claimed_p0[0].message.payload, b"x1");
        assert_eq!(claimed_p0[1].message.payload, b"x2");
    }

    #[tokio::test]
    async fn test_ack_and_scatter_rejects_wrong_token() {
        let storage = create_temp_storage().await;

        storage.create_queue("upstream").await.unwrap();
        storage.create_queue_group("grp1", 2).await.unwrap();

        let msg = Message::new("upstream".to_string(), b"a".to_vec());
        storage.push_message("upstream", &msg).await.unwrap();

        let claimed = storage
            .claim_messages("upstream", 1, "w1", "l1")
            .await
            .unwrap();
        let (msg_ids, _) = split_claims(&claimed);

        // Use wrong claim token
        let result = storage
            .ack_and_scatter(
                "upstream",
                &msg_ids,
                &["wrong-token".to_string()],
                "w1",
                "l1",
                "grp1",
                &[],
                None,
                None,
                None,
            )
            .await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("claim_token"));
    }

    #[tokio::test]
    async fn test_ack_and_scatter_rejects_invalid_partition() {
        let storage = create_temp_storage().await;

        storage.create_queue("upstream").await.unwrap();
        storage.create_queue_group("grp1", 2).await.unwrap();

        // Partition 5 is out of range for a group with 2 partitions
        let out = vec![Message::new("grp1_p5".to_string(), b"x".to_vec())];
        let result = storage
            .ack_and_scatter(
                "upstream",
                &[],
                &[],
                "w1",
                "l1",
                "grp1",
                &[(5, out)],
                None,
                None,
                None,
            )
            .await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("out of range"));
    }

    #[tokio::test]
    async fn test_ack_and_scatter_with_state() {
        let storage = create_temp_storage().await;

        storage.create_queue("upstream").await.unwrap();
        storage.create_queue_group("grp1", 2).await.unwrap();

        let msg = Message::new("upstream".to_string(), b"a".to_vec());
        storage.push_message("upstream", &msg).await.unwrap();

        let claimed = storage
            .claim_messages("upstream", 1, "w1", "l1")
            .await
            .unwrap();
        let (msg_ids, claim_tokens) = split_claims(&claimed);

        let mut state_puts = HashMap::new();
        state_puts.insert("cursor".to_string(), b"offset_42".to_vec());

        storage
            .ack_and_scatter(
                "upstream",
                &msg_ids,
                &claim_tokens,
                "w1",
                "l1",
                "grp1",
                &[],
                Some("ns1"),
                Some(&state_puts),
                None,
            )
            .await
            .unwrap();

        // Verify state was written atomically
        let vals = storage
            .state_get_batch("ns1", &["cursor".to_string()])
            .await
            .unwrap();
        assert_eq!(vals.get("cursor"), Some(&b"offset_42".to_vec()));
    }

    #[tokio::test]
    async fn test_claim_from_group_assigned() {
        let storage = create_temp_storage().await;

        let group = storage.create_queue_group("grp1", 3).await.unwrap();

        // Push messages: 5 to p0, 2 to p1, 0 to p2
        for i in 0..5 {
            let msg = Message::new(group.partition_queues[0].clone(), format!("p0_{i}").into());
            storage
                .push_message(&group.partition_queues[0], &msg)
                .await
                .unwrap();
        }
        for i in 0..2 {
            let msg = Message::new(group.partition_queues[1].clone(), format!("p1_{i}").into());
            storage
                .push_message(&group.partition_queues[1], &msg)
                .await
                .unwrap();
        }

        // Worker assigned to p0 and p1 — should claim from p0 first (highest pending)
        let (claimed, source_queue, source_pid) = storage
            .claim_from_group("grp1", 3, "w1", "l1", &[0, 1], false, 0)
            .await
            .unwrap();
        assert_eq!(claimed.len(), 3);
        assert_eq!(source_queue, group.partition_queues[0]);
        assert_eq!(source_pid, 0);
    }

    #[tokio::test]
    async fn test_claim_from_group_empty() {
        let storage = create_temp_storage().await;

        storage.create_queue_group("grp1", 2).await.unwrap();

        // No messages — claim should return empty
        let (claimed, source_queue, _) = storage
            .claim_from_group("grp1", 5, "w1", "l1", &[0, 1], false, 0)
            .await
            .unwrap();
        assert!(claimed.is_empty());
        assert!(source_queue.is_empty());
    }

    #[tokio::test]
    async fn test_claim_from_group_work_stealing() {
        let storage = create_temp_storage().await;

        let group = storage.create_queue_group("grp1", 3).await.unwrap();

        // Push 10 messages to p2 only (p0 and p1 are empty)
        for i in 0..10 {
            let msg = Message::new(group.partition_queues[2].clone(), format!("p2_{i}").into());
            storage
                .push_message(&group.partition_queues[2], &msg)
                .await
                .unwrap();
        }

        // Worker assigned to p0 only, allow_steal=false — should get nothing
        let (claimed, _, _) = storage
            .claim_from_group("grp1", 5, "w1", "l1", &[0], false, 0)
            .await
            .unwrap();
        assert!(claimed.is_empty());

        // Worker assigned to p0 only, allow_steal=true, threshold=0 — should steal from p2
        let (claimed, source_queue, source_pid) = storage
            .claim_from_group("grp1", 3, "w1", "l1", &[0], true, 0)
            .await
            .unwrap();
        assert_eq!(claimed.len(), 3);
        assert_eq!(source_queue, group.partition_queues[2]);
        assert_eq!(source_pid, 2);

        // With threshold=100, should NOT steal (p2 only has 7 remaining)
        let (claimed, _, _) = storage
            .claim_from_group("grp1", 5, "w2", "l2", &[0], true, 100)
            .await
            .unwrap();
        assert!(claimed.is_empty());
    }

    #[tokio::test]
    async fn test_group_completion() {
        let storage = create_temp_storage().await;

        let group = storage.create_queue_group("grp1", 2).await.unwrap();

        // Not finished yet — empty queues without total_pushed > 0 are NOT drained
        let (all_finished, all_drained, statuses) =
            storage.check_group_completion("grp1").await.unwrap();
        assert!(!all_finished);
        assert!(!all_drained); // empty unused queues are not considered drained
        assert_eq!(statuses.len(), 2);

        // Mark finished
        let marked = storage.mark_group_finished("grp1").await.unwrap();
        assert_eq!(marked, 2);

        // Now should be finished AND drained (no messages)
        let (all_finished, all_drained, _) = storage.check_group_completion("grp1").await.unwrap();
        assert!(all_finished);
        assert!(all_drained);

        // Push a message — drained should become false
        let msg = Message::new(group.partition_queues[0].clone(), b"late".to_vec());
        storage
            .push_message(&group.partition_queues[0], &msg)
            .await
            .unwrap();

        let (all_finished, all_drained, statuses) =
            storage.check_group_completion("grp1").await.unwrap();
        assert!(all_finished);
        assert!(!all_drained);
        // p0 has 1 pending, p1 has 0
        assert_eq!(statuses[0].1, 1); // pending
        assert_eq!(statuses[1].1, 0);
    }

    #[tokio::test]
    async fn test_get_group_stats() {
        let storage = create_temp_storage().await;

        let group = storage.create_queue_group("grp1", 3).await.unwrap();

        // Push varying amounts
        for _ in 0..5 {
            let msg = Message::new(group.partition_queues[0].clone(), b"x".to_vec());
            storage
                .push_message(&group.partition_queues[0], &msg)
                .await
                .unwrap();
        }
        for _ in 0..2 {
            let msg = Message::new(group.partition_queues[1].clone(), b"y".to_vec());
            storage
                .push_message(&group.partition_queues[1], &msg)
                .await
                .unwrap();
        }

        let (group_meta, stats) = storage.get_group_stats("grp1").await.unwrap();
        assert_eq!(group_meta.name, "grp1");
        assert_eq!(stats.len(), 3);
        assert_eq!(stats[0].1.total_pushed, 5); // p0
        assert_eq!(stats[1].1.total_pushed, 2); // p1
        assert_eq!(stats[2].1.total_pushed, 0); // p2
    }

    #[tokio::test]
    async fn test_group_not_found() {
        let storage = create_temp_storage().await;

        let result = storage
            .claim_from_group("nonexistent", 1, "w1", "l1", &[0], false, 0)
            .await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("not found"));
    }
}
