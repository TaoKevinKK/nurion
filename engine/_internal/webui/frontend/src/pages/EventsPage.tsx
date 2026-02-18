import { useState } from "react";
import { useParams, Link } from "react-router-dom";
import { useQuery } from "@tanstack/react-query";
import {
  Breadcrumb,
  Card,
  Select,
  Space,
  Table,
  Timeline,
  Segmented,
  Typography,
} from "antd";
import type { ColumnsType } from "antd/es/table";
import { fetchEvents } from "../api/events";
import type { NurionEvent } from "../api/types";
import { formatDatetime } from "../utils/format";

export default function EventsPage() {
  const { jobId } = useParams<{ jobId: string }>();
  const [eventType, setEventType] = useState<string | undefined>();
  const [viewMode, setViewMode] = useState<string>("table");

  const { data: events, isLoading } = useQuery({
    queryKey: ["events", jobId, eventType],
    queryFn: () =>
      fetchEvents(jobId!, { event_type: eventType, limit: 500 }),
    enabled: !!jobId,
  });

  const columns: ColumnsType<NurionEvent> = [
    {
      title: "Time",
      dataIndex: "timestamp",
      key: "timestamp",
      width: 180,
      render: (t: number) => formatDatetime(t),
    },
    {
      title: "Type",
      dataIndex: "event_type",
      key: "event_type",
      width: 100,
      render: (t: string) => {
        const color =
          t === "nack" ? "error" : t === "timeout" ? "warning" : "success";
        return (
          <Typography.Text type={color === "success" ? undefined : (color as "danger" | "warning")}>
            {t}
          </Typography.Text>
        );
      },
    },
    {
      title: "Stage",
      dataIndex: "stage_id",
      key: "stage_id",
      render: (id: string) =>
        id ? (
          <Link to={`/jobs/${jobId}/stages/${id}`}>
            <Typography.Text code>{id}</Typography.Text>
          </Link>
        ) : (
          "-"
        ),
    },
    {
      title: "Worker",
      dataIndex: "worker_id",
      key: "worker_id",
      render: (id?: string) =>
        id ? (
          <Link to={`/jobs/${jobId}/workers/${id}`}>
            <Typography.Text code style={{ fontSize: 12 }}>
              {id}
            </Typography.Text>
          </Link>
        ) : (
          "-"
        ),
    },
    {
      title: "Message",
      dataIndex: "message",
      key: "message",
      ellipsis: true,
    },
    {
      title: "Error",
      dataIndex: "error",
      key: "error",
      ellipsis: true,
      render: (err?: string) =>
        err ? (
          <Typography.Text type="danger" ellipsis>
            {err}
          </Typography.Text>
        ) : (
          "-"
        ),
    },
  ];

  const timelineItems = (events ?? []).map((e) => ({
    color:
      e.event_type === "nack"
        ? "red"
        : e.event_type === "timeout"
          ? "orange"
          : "green",
    label: formatDatetime(e.timestamp),
    children: (
      <Space direction="vertical" size={0}>
        <Typography.Text strong>{e.event_type}</Typography.Text>
        <Typography.Text type="secondary" style={{ fontSize: 12 }}>
          {e.stage_id} {e.worker_id ? `/ ${e.worker_id}` : ""}
        </Typography.Text>
        {e.message && (
          <Typography.Text style={{ fontSize: 12 }}>{e.message}</Typography.Text>
        )}
      </Space>
    ),
  }));

  return (
    <Space direction="vertical" size="middle" style={{ width: "100%" }}>
      <Breadcrumb
        items={[
          { title: <Link to="/">Jobs</Link> },
          { title: <Link to={`/jobs/${jobId}`}>{jobId?.slice(0, 12)}</Link> },
          { title: "Events" },
        ]}
      />

      <Space>
        <Select
          placeholder="Filter by type"
          allowClear
          style={{ width: 160 }}
          onChange={(v) => setEventType(v)}
          options={[
            { value: "ack", label: "Ack" },
            { value: "nack", label: "Nack" },
            { value: "timeout", label: "Timeout" },
          ]}
        />
        <Segmented
          options={["table", "timeline"]}
          value={viewMode}
          onChange={(v) => setViewMode(v as string)}
        />
      </Space>

      {viewMode === "table" ? (
        <Table
          rowKey={(_, i) => String(i)}
          columns={columns}
          dataSource={events ?? []}
          loading={isLoading}
          size="small"
          pagination={{ pageSize: 50 }}
        />
      ) : (
        <Card size="small">
          <Timeline mode="left" items={timelineItems} />
        </Card>
      )}
    </Space>
  );
}
