import { Card, Typography } from "antd";
import type { Stage } from "../api/types";

interface DAGVisualizationProps {
  stages: Stage[];
  dagEdges: Record<string, string[]>;
  onNodeClick?: (stageId: string) => void;
}

/**
 * Pipeline DAG visualization.
 * Uses a simple CSS-based layout. Can be upgraded to @ant-design/charts later.
 */
export default function DAGVisualization({
  stages,
  dagEdges,
  onNodeClick,
}: DAGVisualizationProps) {
  if (!stages.length) {
    return <Typography.Text type="secondary">No stages</Typography.Text>;
  }

  // Build adjacency and compute layers via topological sort
  const inDegree: Record<string, number> = {};
  const adj: Record<string, string[]> = {};
  for (const s of stages) {
    inDegree[s.stage_id] = 0;
    adj[s.stage_id] = [];
  }
  for (const [src, targets] of Object.entries(dagEdges)) {
    for (const tgt of targets) {
      adj[src]?.push(tgt);
      if (tgt in inDegree) inDegree[tgt] = (inDegree[tgt] ?? 0) + 1;
    }
  }

  const layers: string[][] = [];
  let queue = Object.keys(inDegree).filter((k) => inDegree[k] === 0);
  while (queue.length > 0) {
    layers.push([...queue]);
    const next: string[] = [];
    for (const node of queue) {
      for (const tgt of adj[node] ?? []) {
        inDegree[tgt]!--;
        if (inDegree[tgt] === 0) next.push(tgt);
      }
    }
    queue = next;
  }

  const stageMap = new Map(stages.map((s) => [s.stage_id, s]));

  const statusColors: Record<string, string> = {
    RUNNING: "#1677ff",
    COMPLETED: "#52c41a",
    FAILED: "#ff4d4f",
    PENDING: "#d9d9d9",
  };

  return (
    <div style={{ display: "flex", gap: 32, overflowX: "auto", padding: "8px 0" }}>
      {layers.map((layer, i) => (
        <div
          key={i}
          style={{
            display: "flex",
            flexDirection: "column",
            gap: 12,
            alignItems: "center",
            position: "relative",
          }}
        >
          {layer.map((sid) => {
            const s = stageMap.get(sid);
            return (
              <Card
                key={sid}
                size="small"
                hoverable
                onClick={() => onNodeClick?.(sid)}
                style={{
                  minWidth: 160,
                  borderLeft: `4px solid ${statusColors[s?.status ?? "PENDING"] ?? "#d9d9d9"}`,
                  cursor: "pointer",
                }}
              >
                <div style={{ fontWeight: 600, fontSize: 13 }}>{sid}</div>
                <div style={{ fontSize: 11, color: "#888" }}>
                  {s?.operator_class ?? ""}
                </div>
                <div style={{ fontSize: 11, color: "#888" }}>
                  {s?.num_workers ?? 0} workers
                </div>
              </Card>
            );
          })}
          {i < layers.length - 1 && (
            <div
              style={{
                position: "absolute",
                right: -20,
                top: "50%",
                transform: "translateY(-50%)",
                fontSize: 18,
                color: "#bbb",
              }}
            >
              →
            </div>
          )}
        </div>
      ))}
    </div>
  );
}
