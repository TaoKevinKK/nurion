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

// Timeout recovery and GC for WorkQueue
//
// This module handles:
// 1. Runtime recovery: reclaim messages from dead workers (expired claims)
// 2. Garbage collection: delete acked messages after retention period
//
// Note: With the new storage-only model, startup recovery is automatic -
// storage is the source of truth, no memory state needs rebuilding.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::task::JoinHandle;
use tokio::time::{interval, Duration};

use crate::storage::WorkQueueStorage;
use crate::types::WorkQueueConfig;

/// Recovery task manager - recovers expired claims
pub struct RecoveryTask {
    storage: Arc<WorkQueueStorage>,
    config: WorkQueueConfig,
    running: Arc<AtomicBool>,
    handle: Option<JoinHandle<()>>,
}

impl RecoveryTask {
    pub fn new(storage: Arc<WorkQueueStorage>, config: WorkQueueConfig) -> Self {
        Self {
            storage,
            config,
            running: Arc::new(AtomicBool::new(false)),
            handle: None,
        }
    }

    /// Start the background recovery task
    pub fn start(&mut self) {
        if self.running.load(Ordering::SeqCst) {
            return;
        }

        self.running.store(true, Ordering::SeqCst);

        let storage = self.storage.clone();
        let running = self.running.clone();
        let interval_secs = self.config.recovery_interval_secs;
        let timeout_secs = self.config.claim_timeout_secs;

        let handle = tokio::spawn(async move {
            let mut ticker = interval(Duration::from_secs_f64(interval_secs));

            while running.load(Ordering::SeqCst) {
                ticker.tick().await;

                // Recovery is now handled entirely by storage
                if let Err(e) = storage.recover_expired_claims(timeout_secs).await {
                    tracing::error!("Recovery error: {}", e);
                }
            }
        });

        self.handle = Some(handle);
        tracing::info!(
            "Recovery task started (interval: {}s, timeout: {}s)",
            interval_secs,
            timeout_secs
        );
    }

    /// Stop the recovery task
    pub fn stop(&mut self) {
        self.running.store(false, Ordering::SeqCst);

        if let Some(handle) = self.handle.take() {
            handle.abort();
        }

        tracing::info!("Recovery task stopped");
    }
}

impl Drop for RecoveryTask {
    fn drop(&mut self) {
        self.stop();
    }
}

/// GC task manager for cleaning up acked messages
pub struct GcTask {
    storage: Arc<WorkQueueStorage>,
    config: WorkQueueConfig,
    running: Arc<AtomicBool>,
    handle: Option<JoinHandle<()>>,
}

impl GcTask {
    pub fn new(storage: Arc<WorkQueueStorage>, config: WorkQueueConfig) -> Self {
        Self {
            storage,
            config,
            running: Arc::new(AtomicBool::new(false)),
            handle: None,
        }
    }

    /// Start the background GC task
    pub fn start(&mut self) {
        if self.running.load(Ordering::SeqCst) {
            return;
        }

        self.running.store(true, Ordering::SeqCst);

        let storage = self.storage.clone();
        let running = self.running.clone();
        let interval_secs = self.config.gc_interval_secs;
        let retention_secs = self.config.acked_retention_secs;
        let retention_ns = (retention_secs * 1_000_000_000.0) as u64;

        let handle = tokio::spawn(async move {
            let mut ticker = interval(Duration::from_secs_f64(interval_secs));

            while running.load(Ordering::SeqCst) {
                ticker.tick().await;

                if let Err(e) = storage.gc_acked_messages(retention_ns).await {
                    tracing::error!("GC error: {}", e);
                }
            }
        });

        self.handle = Some(handle);
        tracing::info!(
            "GC task started (interval: {}s, retention: {}s)",
            interval_secs,
            retention_secs
        );
    }

    /// Stop the GC task
    pub fn stop(&mut self) {
        self.running.store(false, Ordering::SeqCst);

        if let Some(handle) = self.handle.take() {
            handle.abort();
        }

        tracing::info!("GC task stopped");
    }
}

impl Drop for GcTask {
    fn drop(&mut self) {
        self.stop();
    }
}
