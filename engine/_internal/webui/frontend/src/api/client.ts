import axios from "axios";

const basePath =
  ((window as unknown as Record<string, unknown>).__NURION_BASE_PATH__ as string) ?? "";

const apiClient = axios.create({
  baseURL: `${basePath}/api`,
  timeout: 30000,
  headers: { "Content-Type": "application/json" },
});

export default apiClient;
