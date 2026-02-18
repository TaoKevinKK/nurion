import { Card, Col, Row, Statistic } from "antd";

interface Metric {
  title: string;
  value: string | number;
  suffix?: string;
  precision?: number;
}

interface MetricCardsProps {
  metrics: Metric[];
}

export default function MetricCards({ metrics }: MetricCardsProps) {
  return (
    <Row gutter={[16, 16]}>
      {metrics.map((m) => (
        <Col key={m.title} xs={12} sm={8} md={6} lg={4}>
          <Card size="small">
            <Statistic
              title={m.title}
              value={m.value}
              suffix={m.suffix}
              precision={m.precision}
            />
          </Card>
        </Col>
      ))}
    </Row>
  );
}
