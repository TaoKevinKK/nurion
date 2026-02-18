import apiClient from "./client";
import type { Stage } from "./types";

export async function fetchStage(
  jobId: string,
  stageId: string,
): Promise<Stage> {
  const { data } = await apiClient.get<Stage>(
    `/jobs/${jobId}/stages/${stageId}`,
  );
  return data;
}
