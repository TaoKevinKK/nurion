import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { Table, Tabs, Input, Space, Typography } from "antd";
import type { ColumnsType } from "antd/es/table";
import { Link } from "react-router-dom";
import { fetchJobs } from "../api/jobs";
import type { Job } from "../api/types";
import StatusBadge from "../components/StatusBadge";
import { formatDuration, formatDatetime } from "../utils/format";

interface Props {
  defaultTab?: string;
}

export default function JobsListPage({ defaultTab }: Props) {
  const [activeTab, setActiveTab] = useState(defaultTab ?? "all");
  const [search, setSearch] = useState("");

  const statusFilter = activeTab === "all" ? undefined : activeTab.toUpperCase();

  const { data, isLoading } = useQuery({
    queryKey: ["jobs", statusFilter],
    queryFn: () => fetchJobs(statusFilter),
    refetchInterval: statusFilter === "RUNNING" || !statusFilter ? 3000 : false,
  });

  const jobs = (data?.jobs ?? []).filter(
    (j) =>
      !search ||
      j.job_id.toLowerCase().includes(search.toLowerCase()) ||
      j.name?.toLowerCase().includes(search.toLowerCase()),
  );

  const columns: ColumnsType<Job> = [
    {
      title: "Job ID",
      dataIndex: "job_id",
      key: "job_id",
      render: (id: string) => (
        <Link to={`/jobs/${id}`}>
          <Typography.Text code style={{ fontSize: 12 }}>
            {id.slice(0, 12)}...
          </Typography.Text>
        </Link>
      ),
    },
    {
      title: "Name",
      dataIndex: "name",
      key: "name",
      render: (name?: string) => name ?? "-",
    },
    {
      title: "Status",
      dataIndex: "status",
      key: "status",
      width: 120,
      render: (s: string) => <StatusBadge status={s} />,
    },
    {
      title: "Stages",
      dataIndex: "num_stages",
      key: "num_stages",
      width: 80,
      render: (n?: number) => n ?? "-",
    },
    {
      title: "Duration",
      dataIndex: "duration",
      key: "duration",
      width: 120,
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
      <Tabs
        activeKey={activeTab}
        onChange={setActiveTab}
        items={[
          { key: "all", label: "All Jobs" },
          { key: "running", label: "Running" },
          { key: "completed", label: "Completed" },
          { key: "failed", label: "Failed" },
        ]}
      />
      <Input.Search
        placeholder="Search by Job ID or name..."
        allowClear
        onChange={(e) => setSearch(e.target.value)}
        style={{ maxWidth: 400 }}
      />
      <Table
        rowKey="job_id"
        columns={columns}
        dataSource={jobs}
        loading={isLoading}
        size="small"
        pagination={{ pageSize: 20, showSizeChanger: true }}
      />
    </Space>
  );
}
