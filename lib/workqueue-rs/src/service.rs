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

// gRPC WorkQueue Service Implementation
//
// Storage-only model: All operations go directly to storage.
// State is only used for:
// 1. Claim locks (serialize concurrent claims per queue)
// 2. Lease management (track worker heartbeats)

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::timeout;
use tokio_stream::{wrappers::ReceiverStream, Stream, StreamExt};
use tonic::{Request, Response, Status, Streaming};

use crate::state::WorkQueueState;
use crate::storage::WorkQueueStorage;
use crate::types::Message;

// Generated protobuf types
pub mod proto {
    tonic::include_proto!("workqueue");
}

use proto::work_queue_server::WorkQueue;
use proto::*;

/// WorkQueue gRPC service implementation
pub struct WorkQueueService {
    state: Arc<WorkQueueState>,
    storage: Arc<WorkQueueStorage>,
}

impl WorkQueueService {
    pub fn new(state: Arc<WorkQueueState>, storage: Arc<WorkQueueStorage>) -> Self {
        Self { state, storage }
    }

    /// Convert internal Message to proto Message
    fn to_proto_message(msg: &Message) -> proto::Message {
        proto::Message {
            msg_id: msg.msg_id.clone(),
            queue: msg.queue.clone(),
            payload: msg.payload.clone(),
            created_at: msg.created_at,
            metadata: msg.metadata.clone(),
        }
    }
}

#[tonic::async_trait]
impl WorkQueue for WorkQueueService {
    // =========================================================================
    // Consumer API
    // =========================================================================

    async fn claim(&self, request: Request<ClaimRequest>) -> Result<Response<ClaimResponse>, Status> {
        let req = request.into_inner();

        let batch_size = if req.batch_size > 0 {
            req.batch_size as usize
        } else {
            1
        };

        // Get claim lock for this queue (serialize concurrent claims)
        let queue_state = self.state.get_or_create_queue(&req.queue);
        let _claim_guard = queue_state.claim_lock.lock().await;

        // Claim directly from storage - O(1) per message!
        let claimed = match self
            .storage
            .claim_messages(&req.queue, batch_size, &req.worker_id, &req.lease_id)
            .await
        {
            Ok(msgs) => msgs,
            Err(e) => {
                tracing::error!("Failed to claim: {}", e);
                return Err(Status::internal("Storage error"));
            }
        };

        let proto_messages: Vec<proto::Message> = claimed
            .iter()
            .map(Self::to_proto_message)
            .collect();

        // Check if there are more messages
        let has_more = match self.storage.get_meta(&req.queue).await {
            Ok(meta) => meta.claim_seq < meta.push_seq,
            Err(_) => false,
        };

        Ok(Response::new(ClaimResponse {
            messages: proto_messages,
            has_more,
        }))
    }

    async fn ack(&self, request: Request<AckRequest>) -> Result<Response<AckResponse>, Status> {
        let req = request.into_inner();

        // Check if we have state updates
        let has_state_updates = !req.state_namespace.is_empty()
            && (!req.state_puts.is_empty() || !req.state_deletes.is_empty());

        // Ack directly in storage
        let result = if has_state_updates {
            let state_puts: HashMap<String, Vec<u8>> = req.state_puts.into_iter().collect();
            self.storage
                .ack_with_state(
                    &req.queue,
                    &req.msg_ids,
                    &req.state_namespace,
                    &state_puts,
                    &req.state_deletes,
                )
                .await
        } else {
            self.storage.ack_messages(&req.queue, &req.msg_ids).await
        };

        match result {
            Ok(()) => Ok(Response::new(AckResponse {
                acked_count: req.msg_ids.len() as i32,
                failed_ids: vec![],
            })),
            Err(e) => {
                tracing::error!("Failed to ack: {}", e);
                Err(Status::internal("Storage error"))
            }
        }
    }

    async fn nack(&self, request: Request<NackRequest>) -> Result<Response<NackResponse>, Status> {
        let req = request.into_inner();

        // Nack directly in storage (returns messages to pending at tail)
        match self.storage.nack_messages(&req.queue, &req.msg_ids).await {
            Ok(()) => Ok(Response::new(NackResponse {
                nacked_count: req.msg_ids.len() as i32,
            })),
            Err(e) => {
                tracing::error!("Failed to nack: {}", e);
                Err(Status::internal("Storage error"))
            }
        }
    }

    async fn ack_and_forward(
        &self,
        request: Request<AckAndForwardRequest>,
    ) -> Result<Response<AckAndForwardResponse>, Status> {
        let req = request.into_inner();

        // Build downstream messages
        let downstream_messages: Vec<Message> = req
            .downstream_payloads
            .iter()
            .map(|payload| Message::new(req.downstream_queue.clone(), payload.clone()))
            .collect();

        let new_msg_ids: Vec<String> = downstream_messages
            .iter()
            .map(|m| m.msg_id.clone())
            .collect();

        // Check if we have state updates
        let has_state_updates = !req.state_namespace.is_empty()
            && (!req.state_puts.is_empty() || !req.state_deletes.is_empty());

        // Atomic persist
        let result = if has_state_updates {
            let state_puts: HashMap<String, Vec<u8>> = req.state_puts.into_iter().collect();
            self.storage
                .ack_forward_with_state(
                    &req.upstream_queue,
                    &req.upstream_msg_ids,
                    &req.downstream_queue,
                    &downstream_messages,
                    &req.state_namespace,
                    &state_puts,
                    &req.state_deletes,
                )
                .await
        } else {
            self.storage
                .ack_and_forward(
                    &req.upstream_queue,
                    &req.upstream_msg_ids,
                    &req.downstream_queue,
                    &downstream_messages,
                )
                .await
        };

        match result {
            Ok(()) => Ok(Response::new(AckAndForwardResponse {
                new_msg_ids,
                success: true,
            })),
            Err(e) => {
                tracing::error!("Failed ack_and_forward: {}", e);
                Ok(Response::new(AckAndForwardResponse {
                    new_msg_ids: vec![],
                    success: false,
                }))
            }
        }
    }

    // =========================================================================
    // Producer API
    // =========================================================================

    async fn push(&self, request: Request<PushRequest>) -> Result<Response<PushResponse>, Status> {
        let req = request.into_inner();

        let msg = Message::with_metadata(req.queue.clone(), req.payload, req.metadata);
        let msg_id = msg.msg_id.clone();

        // Push directly to storage
        match self.storage.push_message(&req.queue, &msg).await {
            Ok(()) => Ok(Response::new(PushResponse { msg_id })),
            Err(e) => {
                tracing::error!("Failed to push: {}", e);
                Err(Status::internal("Storage error"))
            }
        }
    }

    async fn push_batch(
        &self,
        request: Request<PushBatchRequest>,
    ) -> Result<Response<PushBatchResponse>, Status> {
        let req = request.into_inner();

        let messages: Vec<Message> = req
            .payloads
            .iter()
            .map(|payload| Message::new(req.queue.clone(), payload.clone()))
            .collect();

        let msg_ids: Vec<String> = messages.iter().map(|m| m.msg_id.clone()).collect();

        // Push batch directly to storage
        match self.storage.push_messages(&req.queue, &messages).await {
            Ok(()) => Ok(Response::new(PushBatchResponse { msg_ids })),
            Err(e) => {
                tracing::error!("Failed to push batch: {}", e);
                Err(Status::internal("Storage error"))
            }
        }
    }

    // =========================================================================
    // State API
    // =========================================================================

    async fn state_get(
        &self,
        request: Request<StateGetRequest>,
    ) -> Result<Response<StateGetResponse>, Status> {
        let req = request.into_inner();

        if req.namespace.is_empty() {
            return Err(Status::invalid_argument("namespace is required"));
        }

        match self.storage.state_get_batch(&req.namespace, &req.keys).await {
            Ok(values) => Ok(Response::new(StateGetResponse { values })),
            Err(e) => {
                tracing::error!("Failed to get state: {}", e);
                Err(Status::internal("Storage error"))
            }
        }
    }

    async fn state_put(
        &self,
        request: Request<StatePutRequest>,
    ) -> Result<Response<StatePutResponse>, Status> {
        let req = request.into_inner();

        if req.namespace.is_empty() {
            return Err(Status::invalid_argument("namespace is required"));
        }

        let puts: HashMap<String, Vec<u8>> = req.puts.into_iter().collect();

        match self
            .storage
            .state_put_batch(&req.namespace, &puts, &req.deletes)
            .await
        {
            Ok((puts_count, deletes_count)) => Ok(Response::new(StatePutResponse {
                puts_count: puts_count as i32,
                deletes_count: deletes_count as i32,
            })),
            Err(e) => {
                tracing::error!("Failed to put state: {}", e);
                Err(Status::internal("Storage error"))
            }
        }
    }

    // =========================================================================
    // Heartbeat (simplified - no lease tracking for now)
    // =========================================================================

    type HeartbeatStreamStream =
        Pin<Box<dyn Stream<Item = Result<HeartbeatPong, Status>> + Send>>;

    async fn heartbeat_stream(
        &self,
        request: Request<Streaming<HeartbeatPing>>,
    ) -> Result<Response<Self::HeartbeatStreamStream>, Status> {
        let mut stream = request.into_inner();

        let (tx, rx) = mpsc::channel(16);

        // Heartbeat receive timeout: close connection if no ping received within 30s
        const HEARTBEAT_TIMEOUT: Duration = Duration::from_secs(30);

        tokio::spawn(async move {
            // Generate a lease ID for this connection
            let lease_id = uuid::Uuid::now_v7().to_string();

            loop {
                // Wait for next ping with timeout
                match timeout(HEARTBEAT_TIMEOUT, stream.next()).await {
                    Ok(Some(Ok(_ping))) => {
                        // Simple pong response - always use the generated lease_id
                        let pong = HeartbeatPong {
                            lease_id: lease_id.clone(),
                            ok: true,
                            next_ping_ms: 5000,
                        };

                        if tx.send(Ok(pong)).await.is_err() {
                            break;
                        }
                    }
                    Ok(Some(Err(e))) => {
                        tracing::warn!("Heartbeat stream error: {}", e);
                        break;
                    }
                    Ok(None) => {
                        // Stream ended normally
                        break;
                    }
                    Err(_) => {
                        // Timeout - no heartbeat received within timeout period
                        tracing::debug!("Heartbeat timeout, closing connection");
                        break;
                    }
                }
            }
        });

        let output_stream = ReceiverStream::new(rx);
        Ok(Response::new(Box::pin(output_stream)))
    }

    // =========================================================================
    // Admin API
    // =========================================================================

    async fn create_queue(
        &self,
        request: Request<CreateQueueRequest>,
    ) -> Result<Response<CreateQueueResponse>, Status> {
        let req = request.into_inner();

        // Create in storage
        match self.storage.create_queue(&req.queue).await {
            Ok(()) => {
                // Also create in state (for claim lock)
                self.state.get_or_create_queue(&req.queue);
                Ok(Response::new(CreateQueueResponse { created: true }))
            }
            Err(e) => {
                tracing::error!("Failed to create queue: {}", e);
                Err(Status::internal("Storage error"))
            }
        }
    }

    async fn delete_queue(
        &self,
        request: Request<DeleteQueueRequest>,
    ) -> Result<Response<DeleteQueueResponse>, Status> {
        let req = request.into_inner();

        // Delete from storage
        match self.storage.delete_queue(&req.queue).await {
            Ok(deleted) => {
                // Also delete from state
                self.state.delete_queue(&req.queue);
                Ok(Response::new(DeleteQueueResponse {
                    deleted: deleted > 0,
                    messages_deleted: deleted as i32,
                }))
            }
            Err(e) => {
                tracing::error!("Failed to delete queue: {}", e);
                Err(Status::internal("Storage error"))
            }
        }
    }

    async fn get_stats(
        &self,
        request: Request<GetStatsRequest>,
    ) -> Result<Response<GetStatsResponse>, Status> {
        let req = request.into_inner();

        // Get stats from storage
        let queues_to_check: Vec<String> = if req.queue.is_empty() {
            self.state.list_queues()
        } else {
            vec![req.queue.clone()]
        };

        let mut queues: HashMap<String, QueueStats> = HashMap::new();

        for queue in queues_to_check {
            match self.storage.get_queue_stats(&queue).await {
                Ok(meta) => {
                    let pending_count = meta.push_seq.saturating_sub(meta.claim_seq);
                    queues.insert(
                        queue.clone(),
                        QueueStats {
                            queue: queue.clone(),
                            pending_count: pending_count as i64,
                            claimed_count: meta.claimed_count as i64,
                            total_pushed: meta.total_pushed as i64,
                            total_acked: meta.total_acked as i64,
                        },
                    );
                }
                Err(e) => {
                    tracing::warn!("Failed to get stats for queue {}: {}", queue, e);
                }
            }
        }

        Ok(Response::new(GetStatsResponse {
            queues,
            total_workers: 0, // Not tracking workers in this simplified model
            uptime_secs: 0,
        }))
    }

    // =========================================================================
    // Queue Completion API
    // =========================================================================

    async fn mark_queue_finished(
        &self,
        request: Request<MarkQueueFinishedRequest>,
    ) -> Result<Response<MarkQueueFinishedResponse>, Status> {
        let req = request.into_inner();

        if req.queue.is_empty() {
            return Err(Status::invalid_argument("queue is required"));
        }

        match self.storage.mark_queue_finished(&req.queue).await {
            Ok(()) => Ok(Response::new(MarkQueueFinishedResponse { success: true })),
            Err(e) => {
                tracing::error!("Failed to mark queue finished: {}", e);
                Err(Status::internal("Storage error"))
            }
        }
    }

    async fn is_queue_finished(
        &self,
        request: Request<IsQueueFinishedRequest>,
    ) -> Result<Response<IsQueueFinishedResponse>, Status> {
        let req = request.into_inner();

        if req.queue.is_empty() {
            return Err(Status::invalid_argument("queue is required"));
        }

        match self.storage.check_queue_completion(&req.queue).await {
            Ok((finished, drained, pending_count, claimed_count)) => {
                Ok(Response::new(IsQueueFinishedResponse {
                    finished,
                    drained,
                    safe_to_exit: finished && drained,
                    pending_count: pending_count as i64,
                    claimed_count: claimed_count as i64,
                }))
            }
            Err(e) => {
                tracing::error!("Failed to check queue finished: {}", e);
                Err(Status::internal("Storage error"))
            }
        }
    }
}
