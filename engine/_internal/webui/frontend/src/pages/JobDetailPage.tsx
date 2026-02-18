import { useParams, Link, useNavigate } from "react-router-dom";
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
import { fetchJob } from "../api/jobs";
import type { Stage } from "../api/types";
import StatusBadge from "../components/StatusBadge";
import MetricCards from "../components/MetricCards";
import DAGVisualization from "../components/DAGVisualization";
import QueueStatsBar from "../components/QueueStatsBar";
import { formatDuration, formatDatetime } from "../utils/format";

export default function JobDetailPage() {
  const { jobId } = useParams<{ jobId: string }>();
  const navigate = useNavigate();

  const { data: job, isLoading } = useQuery({
    queryKey: ["job", jobId],
    queryFn: () => fetchJob(jobId!),
    enabled: !!jobId,
    refetchInterval: (query) =>
      query.state.data?.status === "RUNNING" ? 3000 : false,
  });

  if (isLoading || !job) {
    return <Card loading />;
  }

  const stages = job.stages ?? [];
  const dagEdges = job.dag_edges ?? {};

  const totalAcked = stages.reduce(
    (sum, s) => sum + (s.queue_stats?.total_acked ?? 0),
    0,
  );
  const totalPushed = stages.reduce(
    (sum, s) => sum + (s.queue_stats?.total_pushed ?? 0),
    0,
  );

  const metrics = [
    { title: "Status", value: job.status },
    { title: "Stages", value: stages.length },
    { title: "Duration", value: formatDuration(job.duration) },
    { title: "Processed", value: `${totalAcked}/${totalPushed}` },
  ];

  const stageColumns: ColumnsType<Stage> = [
    {
      title: "Stage ID",
      dataIndex: "stage_id",
      key: "stage_id",
      render: (id: string) => (
        <Link to={`/jobs/${jobId}/stages/${id}`}>
          <Typography.Text code>{id}</Typography.Text>
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
      title: "Operator",
      dataIndex: "operator_class",
      key: "operator_class",
      ellipsis: true,
    },
    {
      title: "Workers",
      dataIndex: "num_workers",
      key: "num_workers",
      width: 80,
    },
    {
      title: "Queue",
      key: "queue_stats",
      width: 280,
      render: (_: unknown, record: Stage) => (
        <QueueStatsBar stats={record.queue_stats} />
      ),
    },
    {
      title: "Duration",
      dataIndex: "duration",
      key: "duration",
      width: 100,
      render: (d?: number) => formatDuration(d),
    },
  ];

  return (
    <Space direction="vertical" size="middle" style={{ width: "100%" }}>
      <Breadcrumb
        items={[
          { title: <Link to="/">Jobs</Link> },
          { title: job.job_id.slice(0, 12) },
        ]}
      />

      <MetricCards metrics={metrics} />

      <Descriptions bordered size="small" column={2}>
        <Descriptions.Item label="Job ID">
          <Typography.Text copyable code>
            {job.job_id}
          </Typography.Text>
        </Descriptions.Item>
        <Descriptions.Item label="Status">
          <StatusBadge status={job.status} />
        </Descriptions.Item>
        <Descriptions.Item label="Started">
          {formatDatetime(job.start_time)}
        </Descriptions.Item>
        <Descriptions.Item label="Ended">
          {formatDatetime(job.end_time)}
        </Descriptions.Item>
        <Descriptions.Item label="Duration">
          {formatDuration(job.duration)}
        </Descriptions.Item>
        <Descriptions.Item label="Stages">{stages.length}</Descriptions.Item>
        {job.error && (
          <Descriptions.Item label="Error" span={2}>
            <Typography.Text type="danger">{job.error}</Typography.Text>
          </Descriptions.Item>
        )}
      </Descriptions>

      {stages.length > 0 && (
        <Card title="Pipeline DAG" size="small">
          <DAGVisualization
            stages={stages}
            dagEdges={dagEdges}
            onNodeClick={(stageId) =>
              navigate(`/jobs/${jobId}/stages/${stageId}`)
            }
          />
        </Card>
      )}

      <Card
        title="Stages"
        size="small"
        extra={
          <Space>
            <Link to={`/jobs/${jobId}/events`}>Events</Link>
            <Link to={`/jobs/${jobId}/lineage`}>Lineage</Link>
            <Link to={`/jobs/${jobId}/configuration`}>Config</Link>
          </Space>
        }
      >
        <Table
          rowKey="stage_id"
          columns={stageColumns}
          dataSource={stages}
          size="small"
          pagination={false}
        />
      </Card>
    </Space>
  );
}
