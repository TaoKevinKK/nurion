import { useParams, Link } from "react-router-dom";
import { useQuery } from "@tanstack/react-query";
import {
  Breadcrumb,
  Card,
  Descriptions,
  Space,
  Tabs,
  Timeline,
  Typography,
} from "antd";
import { fetchWorkers, fetchWorkerLogs, fetchWorkerStacktrace } from "../api/workers";
import { fetchEvents } from "../api/events";
import StatusBadge from "../components/StatusBadge";
import LogViewer from "../components/LogViewer";
import { formatDuration, formatDatetime } from "../utils/format";

export default function WorkerDetailPage() {
  const { jobId, workerId } = useParams<{
    jobId: string;
    workerId: string;
  }>();

  const { data: workers } = useQuery({
    queryKey: ["worker", jobId, workerId],
    queryFn: () => fetchWorkers(jobId!, { worker_id: workerId, limit: 1 }),
    enabled: !!jobId && !!workerId,
  });

  const worker = workers?.[0];

  const { data: events } = useQuery({
    queryKey: ["workerEvents", jobId, workerId],
    queryFn: () => fetchEvents(jobId!, { worker_id: workerId, limit: 50 }),
    enabled: !!jobId && !!workerId,
  });

  const { data: logs } = useQuery({
    queryKey: ["workerLogs", jobId, workerId],
    queryFn: () => fetchWorkerLogs(jobId!, workerId!, 500),
    enabled: !!jobId && !!workerId,
  });

  const { data: stacktrace } = useQuery({
    queryKey: ["workerStacktrace", jobId, workerId],
    queryFn: () => fetchWorkerStacktrace(jobId!, workerId!),
    enabled: !!jobId && !!workerId && worker?.status === "RUNNING",
  });

  return (
    <Space direction="vertical" size="middle" style={{ width: "100%" }}>
      <Breadcrumb
        items={[
          { title: <Link to="/">Jobs</Link> },
          { title: <Link to={`/jobs/${jobId}`}>{jobId?.slice(0, 12)}</Link> },
          {
            title: (
              <Link to={`/jobs/${jobId}/stages/${worker?.stage_id}`}>
                {worker?.stage_id ?? "..."}
              </Link>
            ),
          },
          { title: workerId },
        ]}
      />

      <Descriptions bordered size="small" column={2}>
        <Descriptions.Item label="Worker ID">
          <Typography.Text code>{workerId}</Typography.Text>
        </Descriptions.Item>
        <Descriptions.Item label="Status">
          <StatusBadge status={worker?.status ?? "UNKNOWN"} />
        </Descriptions.Item>
        <Descriptions.Item label="Stage">
          {worker?.stage_id ?? "-"}
        </Descriptions.Item>
        <Descriptions.Item label="Splits Processed">
          {worker?.splits_processed ?? "-"}
        </Descriptions.Item>
        <Descriptions.Item label="Started">
          {formatDatetime(worker?.start_time)}
        </Descriptions.Item>
        <Descriptions.Item label="Duration">
          {formatDuration(worker?.duration)}
        </Descriptions.Item>
        {worker?.actor_id && (
          <Descriptions.Item label="Actor ID">
            <Typography.Text code>{worker.actor_id}</Typography.Text>
          </Descriptions.Item>
        )}
        {worker?.pid && (
          <Descriptions.Item label="PID">{worker.pid}</Descriptions.Item>
        )}
        {worker?.error && (
          <Descriptions.Item label="Error" span={2}>
            <Typography.Text type="danger">{worker.error}</Typography.Text>
          </Descriptions.Item>
        )}
      </Descriptions>

      <Tabs
        items={[
          {
            key: "logs",
            label: "Logs",
            children: (
              <LogViewer content={logs ?? "Loading..."} height={500} />
            ),
          },
          {
            key: "stacktrace",
            label: "Stacktrace",
            children: (
              <LogViewer
                content={stacktrace ?? "Not available (worker may not be running)"}
                height={400}
              />
            ),
          },
          {
            key: "events",
            label: "Events",
            children: (
              <Card size="small">
                <Timeline
                  mode="left"
                  items={(events ?? []).map((e) => ({
                    color:
                      e.event_type === "nack"
                        ? "red"
                        : e.event_type === "timeout"
                          ? "orange"
                          : "green",
                    label: formatDatetime(e.timestamp),
                    children: (
                      <Space direction="vertical" size={0}>
                        <Typography.Text strong>
                          {e.event_type}
                        </Typography.Text>
                        {e.message && (
                          <Typography.Text type="secondary" style={{ fontSize: 12 }}>
                            {e.message}
                          </Typography.Text>
                        )}
                      </Space>
                    ),
                  }))}
                />
              </Card>
            ),
          },
        ]}
      />
    </Space>
  );
}
