import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { Space, Table, Tabs, Typography, Select } from "antd";
import type { ColumnsType } from "antd/es/table";
import { fetchServeModels, fetchServeWorkers, fetchServeEvents } from "../api/serve";
import type { ServeModel, ServeWorker, ServeEvent } from "../api/types";
import StatusBadge from "../components/StatusBadge";
import { formatDatetime } from "../utils/format";

export default function ServeOverviewPage() {
  const [selectedModel, setSelectedModel] = useState<string>();

  const { data: models, isLoading: modelsLoading } = useQuery({
    queryKey: ["serveModels"],
    queryFn: () => fetchServeModels(),
    refetchInterval: 5000,
  });

  const { data: workers, isLoading: workersLoading } = useQuery({
    queryKey: ["serveWorkers", selectedModel],
    queryFn: () => fetchServeWorkers(selectedModel),
    refetchInterval: 5000,
  });

  const { data: events, isLoading: eventsLoading } = useQuery({
    queryKey: ["serveEvents", selectedModel],
    queryFn: () => fetchServeEvents(selectedModel),
    refetchInterval: 5000,
  });

  const modelColumns: ColumnsType<ServeModel> = [
    {
      title: "Model ID",
      dataIndex: "model_id",
      key: "model_id",
      render: (id: string) => <Typography.Text code>{id}</Typography.Text>,
    },
    {
      title: "Status",
      dataIndex: "status",
      key: "status",
      width: 120,
      render: (s: string) => <StatusBadge status={s} />,
    },
    {
      title: "Source",
      dataIndex: "model_source",
      key: "model_source",
      ellipsis: true,
    },
    {
      title: "TP Size",
      dataIndex: "tensor_parallel_size",
      key: "tensor_parallel_size",
      width: 80,
    },
    {
      title: "Workers",
      key: "workers",
      width: 120,
      render: (_: unknown, r: ServeModel) =>
        `${r.current_workers ?? 0} / ${r.max_workers ?? "-"}`,
    },
  ];

  const workerColumns: ColumnsType<ServeWorker> = [
    {
      title: "Worker ID",
      dataIndex: "worker_id",
      key: "worker_id",
      render: (id: string) => <Typography.Text code>{id}</Typography.Text>,
    },
    {
      title: "Model",
      dataIndex: "model_id",
      key: "model_id",
    },
    {
      title: "Status",
      dataIndex: "status",
      key: "status",
      width: 120,
      render: (s: string) => <StatusBadge status={s} />,
    },
    {
      title: "GPUs",
      dataIndex: "gpu_ids",
      key: "gpu_ids",
      render: (ids?: number[]) => ids?.join(", ") ?? "-",
    },
    {
      title: "Started",
      dataIndex: "start_time",
      key: "start_time",
      width: 180,
      render: (t?: number) => formatDatetime(t),
    },
  ];

  const eventColumns: ColumnsType<ServeEvent> = [
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
      width: 120,
    },
    {
      title: "Model",
      dataIndex: "model_id",
      key: "model_id",
    },
    {
      title: "Worker",
      dataIndex: "worker_id",
      key: "worker_id",
      render: (id?: string) =>
        id ? <Typography.Text code>{id}</Typography.Text> : "-",
    },
    {
      title: "Message",
      dataIndex: "message",
      key: "message",
      ellipsis: true,
    },
  ];

  const modelOptions = (models ?? []).map((m) => ({
    value: m.model_id,
    label: m.model_id,
  }));

  return (
    <Space direction="vertical" size="middle" style={{ width: "100%" }}>
      <Typography.Title level={4}>Model Serving</Typography.Title>

      <Select
        placeholder="Filter by model"
        allowClear
        style={{ width: 300 }}
        options={modelOptions}
        onChange={(v) => setSelectedModel(v)}
      />

      <Tabs
        items={[
          {
            key: "models",
            label: `Models (${models?.length ?? 0})`,
            children: (
              <Table
                rowKey="model_id"
                columns={modelColumns}
                dataSource={models ?? []}
                loading={modelsLoading}
                size="small"
                pagination={false}
              />
            ),
          },
          {
            key: "workers",
            label: `Workers (${workers?.length ?? 0})`,
            children: (
              <Table
                rowKey="worker_id"
                columns={workerColumns}
                dataSource={workers ?? []}
                loading={workersLoading}
                size="small"
                pagination={{ pageSize: 20 }}
              />
            ),
          },
          {
            key: "events",
            label: `Events (${events?.length ?? 0})`,
            children: (
              <Table
                rowKey={(_, i) => String(i)}
                columns={eventColumns}
                dataSource={events ?? []}
                loading={eventsLoading}
                size="small"
                pagination={{ pageSize: 50 }}
              />
            ),
          },
        ]}
      />
    </Space>
  );
}
