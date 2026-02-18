import { Progress, Space, Typography } from "antd";
import type { QueueStats } from "../api/types";

const { Text } = Typography;

interface QueueStatsBarProps {
  stats?: QueueStats;
}

export default function QueueStatsBar({ stats }: QueueStatsBarProps) {
  if (!stats) return <Text type="secondary">No queue data</Text>;

  const { pending_count, claimed_count, total_pushed, total_acked } = stats;
  const percent = total_pushed > 0 ? Math.round((total_acked / total_pushed) * 100) : 0;

  return (
    <Space direction="vertical" style={{ width: "100%" }} size="small">
      <Progress
        percent={percent}
        size="small"
        format={() => `${total_acked}/${total_pushed}`}
      />
      <Space size="large">
        <Text type="secondary">Pending: {pending_count}</Text>
        <Text type="secondary">Claimed: {claimed_count}</Text>
        <Text type="secondary">Acked: {total_acked}</Text>
        <Text type="secondary">Pushed: {total_pushed}</Text>
      </Space>
    </Space>
  );
}
