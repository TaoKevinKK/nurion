import { Tag } from "antd";
import {
  CheckCircleOutlined,
  CloseCircleOutlined,
  SyncOutlined,
  ClockCircleOutlined,
  ExclamationCircleOutlined,
} from "@ant-design/icons";

const statusConfig: Record<string, { color: string; icon: React.ReactNode }> = {
  RUNNING: { color: "processing", icon: <SyncOutlined spin /> },
  COMPLETED: { color: "success", icon: <CheckCircleOutlined /> },
  FAILED: { color: "error", icon: <CloseCircleOutlined /> },
  PENDING: { color: "default", icon: <ClockCircleOutlined /> },
  IDLE: { color: "default", icon: <ClockCircleOutlined /> },
  ALIVE: { color: "processing", icon: <SyncOutlined spin /> },
  DEAD: { color: "error", icon: <CloseCircleOutlined /> },
  STARTING: { color: "warning", icon: <ExclamationCircleOutlined /> },
  UNKNOWN: { color: "default", icon: <ExclamationCircleOutlined /> },
};

interface StatusBadgeProps {
  status: string;
}

export default function StatusBadge({ status }: StatusBadgeProps) {
  const cfg = statusConfig[status] ?? { color: "default", icon: null };
  return (
    <Tag color={cfg.color} icon={cfg.icon}>
      {status}
    </Tag>
  );
}
