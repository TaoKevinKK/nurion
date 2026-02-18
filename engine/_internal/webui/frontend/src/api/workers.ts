import apiClient from "./client";
import type { Worker } from "./types";

export async function fetchWorkers(
  jobId: string,
  params?: { stage_id?: string; worker_id?: string; limit?: number; offset?: number },
): Promise<Worker[]> {
  const { data } = await apiClient.get<Worker[]>(`/jobs/${jobId}/workers`, {
    params,
  });
  return data;
}

export async function fetchWorkerLogs(
  jobId: string,
  workerId: string,
  tail = 200,
): Promise<string> {
  const { data } = await apiClient.get<string>(
    `/jobs/${jobId}/workers/${workerId}/logs`,
    { params: { tail }, responseType: "text" as unknown as undefined },
  );
  return data;
}

export async function fetchWorkerStacktrace(
  jobId: string,
  workerId: string,
): Promise<string> {
  const { data } = await apiClient.get<string>(
    `/jobs/${jobId}/workers/${workerId}/stacktrace`,
    { responseType: "text" as unknown as undefined },
  );
  return data;
}
