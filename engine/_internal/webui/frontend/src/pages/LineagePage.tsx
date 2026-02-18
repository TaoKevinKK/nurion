import { useState } from "react";
import { useParams, Link } from "react-router-dom";
import { useQuery } from "@tanstack/react-query";
import {
  Breadcrumb,
  Card,
  Input,
  Space,
  Steps,
  Typography,
  Descriptions,
  Empty,
} from "antd";
import { fetchSplitTrace } from "../api/lineage";
import { formatDatetime } from "../utils/format";

export default function LineagePage() {
  const { jobId } = useParams<{ jobId: string }>();
  const [splitId, setSplitId] = useState("");
  const [searchId, setSearchId] = useState("");

  const { data: trace, isLoading } = useQuery({
    queryKey: ["lineage", jobId, searchId],
    queryFn: () => fetchSplitTrace(jobId!, searchId),
    enabled: !!jobId && !!searchId,
  });

  const handleSearch = (value: string) => {
    if (value.trim()) setSearchId(value.trim());
  };

  return (
    <Space direction="vertical" size="middle" style={{ width: "100%" }}>
      <Breadcrumb
        items={[
          { title: <Link to="/">Jobs</Link> },
          { title: <Link to={`/jobs/${jobId}`}>{jobId?.slice(0, 12)}</Link> },
          { title: "Lineage" },
        ]}
      />

      <Input.Search
        placeholder="Enter Split ID to trace..."
        allowClear
        enterButton="Trace"
        value={splitId}
        onChange={(e) => setSplitId(e.target.value)}
        onSearch={handleSearch}
        style={{ maxWidth: 500 }}
        loading={isLoading}
      />

      {trace && trace.splits.length > 0 ? (
        <Card title={`Trace: ${trace.root_split_id}`} size="small">
          <Steps
            direction="vertical"
            size="small"
            current={trace.splits.length - 1}
            items={trace.splits.map((s) => ({
              title: (
                <Typography.Text code>{s.split_id}</Typography.Text>
              ),
              description: (
                <Descriptions size="small" column={3}>
                  <Descriptions.Item label="Stage">
                    {s.stage_id}
                  </Descriptions.Item>
                  <Descriptions.Item label="Worker">
                    {s.worker_id ?? "-"}
                  </Descriptions.Item>
                  <Descriptions.Item label="Time">
                    {formatDatetime(s.timestamp)}
                  </Descriptions.Item>
                  {s.parent_split_id && (
                    <Descriptions.Item label="Parent">
                      <Typography.Text code>
                        {s.parent_split_id}
                      </Typography.Text>
                    </Descriptions.Item>
                  )}
                </Descriptions>
              ),
            }))}
          />
        </Card>
      ) : searchId && !isLoading ? (
        <Empty description="Split not found" />
      ) : null}
    </Space>
  );
}
