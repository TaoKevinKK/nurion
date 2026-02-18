import { useParams, Link } from "react-router-dom";
import { useQuery } from "@tanstack/react-query";
import {
  Breadcrumb,
  Card,
  Descriptions,
  Space,
  Table,
  Typography,
} from "antd";
import type { ColumnsType } from "antd/es/table";
import { fetchStage } from "../api/stages";
import { fetchWorkers } from "../api/workers";
import type { Worker } from "../api/types";
import StatusBadge from "../components/StatusBadge";
import MetricCards from "../components/MetricCards";
import QueueStatsBar from "../components/QueueStatsBar";
import { formatDuration, formatDatetime } from "../utils/format";

export default function StageDetailPage() {
  const { jobId, stageId } = useParams<{ jobId: string; stageId: string }>();

  const { data: stage, isLoading: stageLoading } = useQuery({
    queryKey: ["stage", jobId, stageId],
    queryFn: () => fetchStage(jobId!, stageId!),
    enabled: !!jobId && !!stageId,
    refetchInterval: (query) =>
      query.state.data?.status === "RUNNING" ? 3000 : false,
  });

  const { data: workers, isLoading: workersLoading } = useQuery({
    queryKey: ["workers", jobId, stageId],
    queryFn: () => fetchWorkers(jobId!, { stage_id: stageId, limit: 500 }),
    enabled: !!jobId && !!stageId,
    refetchInterval: stage?.status === "RUNNING" ? 3000 : false,
  });

  if (stageLoading || !stage) return <Card loading />;

  const qs = stage.queue_stats;
  const metrics = [
    { title: "Status", value: stage.status },
    { title: "Workers", value: stage.num_workers },
    { title: "Acked", value: qs?.total_acked ?? 0 },
    { title: "Pushed", value: qs?.total_pushed ?? 0 },
    { title: "Pending", value: qs?.pending_count ?? 0 },
    { title: "Claimed", value: qs?.claimed_count ?? 0 },
  ];

  const workerColumns: ColumnsType<Worker> = [
    {
      title: "Worker ID",
      dataIndex: "worker_id",
      key: "worker_id",
      render: (id: string) => (
        <Link to={`/jobs/${jobId}/workers/${id}`}>
          <Typography.Text code style={{ fontSize: 12 }}>
            {id}
          </Typography.Text>
        </Link>
      ),
    },
    {
      title: "Status",
      dataIndex: "status",
      key: "status",
      width: 120,
      render: (s: string) => <StatusBadge status={s} />,
    },
    {
      title: "Splits",
      dataIndex: "splits_processed",
      key: "splits_processed",
      width: 80,
    },
    {
      title: "Duration",
      dataIndex: "duration",
      key: "duration",
      width: 100,
      render: (d?: number) => formatDuration(d),
    },
    {
      title: "Started",
      dataIndex: "start_time",
      key: "start_time",
      width: 180,
      render: (t?: number) => formatDatetime(t),
    },
  ];

  return (
    <Space direction="vertical" size="middle" style={{ width: "100%" }}>
      <Breadcrumb
        items={[
          { title: <Link to="/">Jobs</Link> },
          { title: <Link to={`/jobs/${jobId}`}>{jobId?.slice(0, 12)}</Link> },
          { title: stageId },
        ]}
      />

      <MetricCards metrics={metrics} />

      <Descriptions bordered size="small" column={2}>
        <Descriptions.Item label="Stage ID">
          <Typography.Text code>{stage.stage_id}</Typography.Text>
        </Descriptions.Item>
        <Descriptions.Item label="Status">
          <StatusBadge status={stage.status} />
        </Descriptions.Item>
        <Descriptions.Item label="Operator">
          {stage.operator_class}
        </Descriptions.Item>
        <Descriptions.Item label="Workers">
          {stage.num_workers}
        </Descriptions.Item>
        <Descriptions.Item label="Started">
          {formatDatetime(stage.start_time)}
        </Descriptions.Item>
        <Descriptions.Item label="Duration">
          {formatDuration(stage.duration)}
        </Descriptions.Item>
      </Descriptions>

      <Card title="Queue Status" size="small">
        <QueueStatsBar stats={stage.queue_stats} />
      </Card>

      <Card title="Workers" size="small">
        <Table
          rowKey="worker_id"
          columns={workerColumns}
          dataSource={workers ?? []}
          loading={workersLoading}
          size="small"
          pagination={{ pageSize: 20 }}
        />
      </Card>
    </Space>
  );
}
