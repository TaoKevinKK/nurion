// ---- Jobs ----

export interface QueueStats {
  pending_count: number;
  claimed_count: number;
  total_pushed: number;
  total_acked: number;
}

export interface Stage {
  stage_id: string;
  status: string;
  operator_class: string;
  num_workers: number;
  start_time?: number;
  end_time?: number;
  duration?: number;
  queue_stats?: QueueStats;
}

export interface Job {
  job_id: string;
  status: string;
  name?: string;
  start_time?: number;
  end_time?: number;
  duration?: number;
  num_stages?: number;
  stages?: Stage[];
  dag_edges?: Record<string, string[]>;
  checkpoint?: Record<string, unknown>;
  error?: string;
}

export interface JobsListResponse {
  jobs: Job[];
  total: number;
}

// ---- Workers ----

export interface Worker {
  worker_id: string;
  stage_id: string;
  status: string;
  start_time?: number;
  end_time?: number;
  duration?: number;
  splits_processed?: number;
  actor_id?: string;
  node_id?: string;
  pid?: number;
  error?: string;
}

// ---- Events ----

export interface NurionEvent {
  event_type: string;
  stage_id: string;
  worker_id?: string;
  split_id?: string;
  timestamp: number;
  message?: string;
  error?: string;
  details?: Record<string, unknown>;
}

// ---- Lineage ----

export interface SplitLineage {
  split_id: string;
  stage_id: string;
  worker_id?: string;
  parent_split_id?: string;
  status?: string;
  timestamp?: number;
  metadata?: Record<string, unknown>;
}

export interface SplitTrace {
  splits: SplitLineage[];
  edges: Array<{ source: string; target: string }>;
  root_split_id: string;
}

// ---- Configuration ----

export interface Configuration {
  job_config: Record<string, unknown>;
  stage_configs: Record<string, unknown>;
  environment?: Record<string, unknown>;
}

// ---- Serve ----

export interface ServeModel {
  model_id: string;
  model_source?: string;
  status: string;
  tensor_parallel_size?: number;
  min_workers?: number;
  max_workers?: number;
  current_workers?: number;
  [key: string]: unknown;
}

export interface ServeWorker {
  worker_id: string;
  model_id: string;
  status: string;
  gpu_ids?: number[];
  start_time?: number;
  [key: string]: unknown;
}

export interface ServeEvent {
  event_type: string;
  model_id: string;
  worker_id?: string;
  timestamp: number;
  message?: string;
  [key: string]: unknown;
}
