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

// WorkQueue Python bindings using PyO3

use pyo3::prelude::*;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use tokio::runtime::Runtime;

mod recovery;
mod server;
mod service;
mod state;
mod storage;
mod types;

use server::WorkQueueBrokerInner;
use types::WorkQueueConfig;

/// Broker error type exposed to Python
#[pyclass]
#[derive(Clone)]
pub struct BrokerError {
    #[pyo3(get)]
    pub kind: String,
    #[pyo3(get)]
    pub message: String,
}

#[pymethods]
impl BrokerError {
    #[new]
    fn new(kind: String, message: String) -> Self {
        Self { kind, message }
    }

    fn __repr__(&self) -> String {
        format!("BrokerError(kind='{}', message='{}')", self.kind, self.message)
    }

    fn __str__(&self) -> String {
        format!("{}: {}", self.kind, self.message)
    }
}

/// Broker configuration
#[pyclass]
#[derive(Clone)]
pub struct BrokerConfig {
    #[pyo3(get, set)]
    pub db_path: String,
    #[pyo3(get, set)]
    pub host: String,
    #[pyo3(get, set)]
    pub port: u16,
    #[pyo3(get, set)]
    pub claim_timeout_secs: f64,
    #[pyo3(get, set)]
    pub recovery_interval_secs: f64,
    #[pyo3(get, set)]
    pub max_queue_depth: usize,
    #[pyo3(get, set)]
    pub acked_retention_secs: f64,
    #[pyo3(get, set)]
    pub gc_interval_secs: f64,
}

#[pymethods]
impl BrokerConfig {
    #[new]
    #[pyo3(signature = (db_path, host="0.0.0.0".to_string(), port=0, claim_timeout_secs=60.0, recovery_interval_secs=10.0, max_queue_depth=0, acked_retention_secs=3600.0, gc_interval_secs=60.0))]
    fn new(
        db_path: String,
        host: String,
        port: u16,
        claim_timeout_secs: f64,
        recovery_interval_secs: f64,
        max_queue_depth: usize,
        acked_retention_secs: f64,
        gc_interval_secs: f64,
    ) -> Self {
        Self {
            db_path,
            host,
            port,
            claim_timeout_secs,
            recovery_interval_secs,
            max_queue_depth,
            acked_retention_secs,
            gc_interval_secs,
        }
    }

    fn __repr__(&self) -> String {
        format!(
            "BrokerConfig(db_path='{}', host='{}', port={}, claim_timeout_secs={}, gc_interval_secs={})",
            self.db_path, self.host, self.port, self.claim_timeout_secs, self.gc_interval_secs
        )
    }
}

impl From<BrokerConfig> for WorkQueueConfig {
    fn from(config: BrokerConfig) -> Self {
        WorkQueueConfig {
            db_path: config.db_path,
            host: config.host,
            port: config.port,
            claim_timeout_secs: config.claim_timeout_secs,
            recovery_interval_secs: config.recovery_interval_secs,
            max_queue_depth: config.max_queue_depth,
            acked_retention_secs: config.acked_retention_secs,
            gc_interval_secs: config.gc_interval_secs,
        }
    }
}

/// WorkQueue Broker - embedded work queue server
#[pyclass]
pub struct WorkQueueBroker {
    config: BrokerConfig,
    handle: Option<JoinHandle<()>>,
    running: Arc<AtomicBool>,
    event_handler: Option<PyObject>,
    actual_port: Arc<Mutex<Option<u16>>>,
}

#[pymethods]
impl WorkQueueBroker {
    #[new]
    #[pyo3(signature = (config, event_handler=None))]
    fn new(config: BrokerConfig, event_handler: Option<PyObject>) -> Self {
        Self {
            config,
            handle: None,
            running: Arc::new(AtomicBool::new(false)),
            event_handler,
            actual_port: Arc::new(Mutex::new(None)),
        }
    }

    /// Start the broker (non-blocking)
    fn start(&mut self, py: Python<'_>) -> PyResult<()> {
        if self.running.load(Ordering::SeqCst) {
            return Err(pyo3::exceptions::PyRuntimeError::new_err(
                "Broker is already running",
            ));
        }

        self.running.store(true, Ordering::SeqCst);

        let config: WorkQueueConfig = self.config.clone().into();
        let handler = self.event_handler.as_ref().map(|h| h.clone_ref(py));
        let running = self.running.clone();
        let actual_port = self.actual_port.clone();

        let handle = std::thread::spawn(move || {
            // Initialize tracing
            let _ = tracing_subscriber::fmt().try_init();

            let rt = match Runtime::new() {
                Ok(rt) => rt,
                Err(e) => {
                    running.store(false, Ordering::SeqCst);
                    if let Some(h) = &handler {
                        Python::with_gil(|py| {
                            let error = BrokerError::new(
                                "runtime_error".to_string(),
                                format!("Failed to create runtime: {}", e),
                            );
                            let _ = h.call_method1(py, "on_fatal", (error,));
                        });
                    }
                    return;
                }
            };

            rt.block_on(async {
                match WorkQueueBrokerInner::new(config).await {
                    Ok(mut broker) => {
                        match broker.start().await {
                            Ok(port) => {
                                *actual_port.lock().unwrap() = Some(port);

                                // Trigger on_started callback
                                if let Some(h) = &handler {
                                    Python::with_gil(|py| {
                                        let _ = h.call_method1(py, "on_started", (port,));
                                    });
                                }

                                // Keep running until stopped
                                while running.load(Ordering::SeqCst) {
                                    tokio::time::sleep(tokio::time::Duration::from_millis(100))
                                        .await;
                                }

                                broker.stop();

                                // Trigger on_stopped callback
                                if let Some(h) = &handler {
                                    Python::with_gil(|py| {
                                        let _ = h.call_method0(py, "on_stopped");
                                    });
                                }
                            }
                            Err(e) => {
                                running.store(false, Ordering::SeqCst);
                                if let Some(h) = &handler {
                                    Python::with_gil(|py| {
                                        let error = BrokerError::new(
                                            "start_failed".to_string(),
                                            e.to_string(),
                                        );
                                        let _ = h.call_method1(py, "on_fatal", (error,));
                                    });
                                }
                            }
                        }
                    }
                    Err(e) => {
                        running.store(false, Ordering::SeqCst);
                        if let Some(h) = &handler {
                            Python::with_gil(|py| {
                                let error =
                                    BrokerError::new("init_failed".to_string(), e.to_string());
                                let _ = h.call_method1(py, "on_fatal", (error,));
                            });
                        }
                    }
                }
            });
        });

        self.handle = Some(handle);
        Ok(())
    }

    /// Stop the broker
    fn stop(&mut self, py: Python<'_>) -> PyResult<()> {
        self.running.store(false, Ordering::SeqCst);

        if let Some(handle) = self.handle.take() {
            py.allow_threads(|| {
                let _ = handle.join();
            });
        }

        Ok(())
    }

    /// Check if broker is running
    fn is_running(&self) -> bool {
        self.running.load(Ordering::SeqCst)
    }

    /// Get the actual port the broker is listening on
    fn get_port(&self) -> Option<u16> {
        *self.actual_port.lock().unwrap()
    }

    /// Get the broker URL (host:port)
    fn get_broker_url(&self) -> Option<String> {
        self.get_port()
            .map(|port| format!("{}:{}", self.config.host, port))
    }

    fn __repr__(&self) -> String {
        let status = if self.is_running() {
            "running"
        } else {
            "stopped"
        };
        format!(
            "WorkQueueBroker(config={:?}, status={})",
            self.config.__repr__(),
            status
        )
    }
}

/// Python module definition
#[pymodule]
fn workqueue_py(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<BrokerConfig>()?;
    m.add_class::<BrokerError>()?;
    m.add_class::<WorkQueueBroker>()?;
    Ok(())
}
