import dayjs from "dayjs";
import duration from "dayjs/plugin/duration";

dayjs.extend(duration);

export function formatDuration(seconds?: number): string {
  if (seconds == null) return "-";
  if (seconds < 1) return `${Math.round(seconds * 1000)}ms`;
  if (seconds < 60) return `${seconds.toFixed(1)}s`;
  if (seconds < 3600) {
    const m = Math.floor(seconds / 60);
    const s = Math.floor(seconds % 60);
    return `${m}m ${s}s`;
  }
  const h = Math.floor(seconds / 3600);
  const m = Math.floor((seconds % 3600) / 60);
  return `${h}h ${m}m`;
}

export function formatBytes(bytes?: number): string {
  if (bytes == null) return "-";
  let value = bytes;
  for (const unit of ["B", "KB", "MB", "GB", "TB"]) {
    if (value < 1024) return `${value.toFixed(1)}${unit}`;
    value /= 1024;
  }
  return `${value.toFixed(1)}PB`;
}

export function formatNumber(num?: number): string {
  if (num == null) return "0";
  if (num >= 1e9) return `${(num / 1e9).toFixed(1)}B`;
  if (num >= 1e6) return `${(num / 1e6).toFixed(1)}M`;
  if (num >= 1e3) return `${(num / 1e3).toFixed(1)}K`;
  return String(num);
}

export function formatDatetime(timestamp?: number): string {
  if (timestamp == null) return "-";
  return dayjs.unix(timestamp).format("YYYY-MM-DD HH:mm:ss");
}
