import apiClient from "./client";
import type { ServeModel, ServeWorker, ServeEvent } from "./types";

export async function fetchServeModels(
  limit = 100,
): Promise<ServeModel[]> {
  const { data } = await apiClient.get<ServeModel[]>("/serve/models", {
    params: { limit },
  });
  return data;
}

export async function fetchServeWorkers(
  modelId?: string,
  limit = 100,
): Promise<ServeWorker[]> {
  const params: Record<string, unknown> = { limit };
  if (modelId) params.model_id = modelId;
  const { data } = await apiClient.get<ServeWorker[]>("/serve/workers", {
    params,
  });
  return data;
}

export async function fetchServeEvents(
  modelId?: string,
  limit = 100,
): Promise<ServeEvent[]> {
  const params: Record<string, unknown> = { limit };
  if (modelId) params.model_id = modelId;
  const { data } = await apiClient.get<ServeEvent[]>("/serve/events", {
    params,
  });
  return data;
}
