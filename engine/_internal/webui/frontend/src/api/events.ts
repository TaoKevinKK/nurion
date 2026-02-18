import apiClient from "./client";
import type { NurionEvent } from "./types";

export async function fetchEvents(
  jobId: string,
  params?: {
    stage_id?: string;
    worker_id?: string;
    event_type?: string;
    limit?: number;
  },
): Promise<NurionEvent[]> {
  const { data } = await apiClient.get<NurionEvent[]>(
    `/jobs/${jobId}/events`,
    { params },
  );
  return data;
}
