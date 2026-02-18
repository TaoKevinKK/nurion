import apiClient from "./client";
import type { Job, JobsListResponse } from "./types";

export async function fetchJobs(
  status?: string,
  limit = 100,
  offset = 0,
): Promise<JobsListResponse> {
  const params: Record<string, unknown> = { limit, offset };
  if (status) params.status = status;
  const { data } = await apiClient.get<JobsListResponse>("/jobs", { params });
  return data;
}

export async function fetchJob(jobId: string): Promise<Job> {
  const { data } = await apiClient.get<Job>(`/jobs/${jobId}`);
  return data;
}
