import apiClient from "./client";
import type { SplitLineage, SplitTrace } from "./types";

export async function fetchSplitLineage(
  jobId: string,
  splitId: string,
): Promise<SplitLineage> {
  const { data } = await apiClient.get<SplitLineage>(
    `/jobs/${jobId}/lineage/splits/${splitId}`,
  );
  return data;
}

export async function fetchSplitTrace(
  jobId: string,
  splitId: string,
): Promise<SplitTrace> {
  const { data } = await apiClient.get<SplitTrace>(
    `/jobs/${jobId}/lineage/splits/${splitId}/trace`,
  );
  return data;
}
