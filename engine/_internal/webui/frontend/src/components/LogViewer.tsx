import { Typography } from "antd";

interface LogViewerProps {
  content: string;
  height?: number;
}

export default function LogViewer({ content, height = 500 }: LogViewerProps) {
  return (
    <pre
      style={{
        background: "#1e1e1e",
        color: "#d4d4d4",
        padding: 16,
        borderRadius: 6,
        overflow: "auto",
        height,
        fontSize: 12,
        fontFamily: "'SF Mono', Menlo, Monaco, 'Courier New', monospace",
        lineHeight: 1.6,
        margin: 0,
        whiteSpace: "pre-wrap",
        wordBreak: "break-all",
      }}
    >
      {content || (
        <Typography.Text type="secondary">No logs available</Typography.Text>
      )}
    </pre>
  );
}
