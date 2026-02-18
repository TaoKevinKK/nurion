import { useParams, Link } from "react-router-dom";
import { useQuery } from "@tanstack/react-query";
import {
  Breadcrumb,
  Card,
  Collapse,
  Descriptions,
  Space,
  Typography,
} from "antd";
import apiClient from "../api/client";
import type { Configuration } from "../api/types";

async function fetchConfiguration(jobId: string): Promise<Configuration> {
  // Configuration is embedded in job detail from get_job_archive
  // Use the job detail endpoint and extract config, or call a dedicated endpoint if available
  const { data } = await apiClient.get<Configuration>(
    `/jobs/${jobId}`,
  );
  // The job detail includes config fields; fall back to a reasonable shape
  const raw = data as unknown as Record<string, unknown>;
  return {
    job_config: (raw.config as Record<string, unknown>) ?? {},
    stage_configs: (raw.stage_configs as Record<string, unknown>) ?? {},
    environment: (raw.environment as Record<string, unknown>) ?? {},
  };
}

function renderObject(obj: Record<string, unknown>) {
  return (
    <Descriptions bordered size="small" column={1}>
      {Object.entries(obj).map(([key, value]) => (
        <Descriptions.Item key={key} label={key}>
          <Typography.Text code style={{ fontSize: 12 }}>
            {typeof value === "object" ? JSON.stringify(value, null, 2) : String(value)}
          </Typography.Text>
        </Descriptions.Item>
      ))}
    </Descriptions>
  );
}

export default function ConfigurationPage() {
  const { jobId } = useParams<{ jobId: string }>();

  const { data: config, isLoading } = useQuery({
    queryKey: ["configuration", jobId],
    queryFn: () => fetchConfiguration(jobId!),
    enabled: !!jobId,
  });

  if (isLoading) return <Card loading />;

  const jobConfig = config?.job_config ?? {};
  const stageConfigs = config?.stage_configs ?? {};
  const environment = config?.environment ?? {};

  return (
    <Space direction="vertical" size="middle" style={{ width: "100%" }}>
      <Breadcrumb
        items={[
          { title: <Link to="/">Jobs</Link> },
          { title: <Link to={`/jobs/${jobId}`}>{jobId?.slice(0, 12)}</Link> },
          { title: "Configuration" },
        ]}
      />

      <Card title="Job Configuration" size="small">
        {Object.keys(jobConfig).length > 0 ? (
          renderObject(jobConfig)
        ) : (
          <Typography.Text type="secondary">No configuration data</Typography.Text>
        )}
      </Card>

      {Object.keys(stageConfigs).length > 0 && (
        <Card title="Stage Configurations" size="small">
          <Collapse
            items={Object.entries(stageConfigs).map(([stageId, cfg]) => ({
              key: stageId,
              label: stageId,
              children: renderObject(
                (cfg as Record<string, unknown>) ?? {},
              ),
            }))}
          />
        </Card>
      )}

      {Object.keys(environment).length > 0 && (
        <Card title="Environment" size="small">
          {renderObject(environment)}
        </Card>
      )}
    </Space>
  );
}
