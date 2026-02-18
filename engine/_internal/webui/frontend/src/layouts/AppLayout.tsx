import { useState } from "react";
import { Outlet, useNavigate, useLocation } from "react-router-dom";
import { Layout, Menu, Typography, theme } from "antd";
import {
  DashboardOutlined,
  PlayCircleOutlined,
  CheckCircleOutlined,
  CloudServerOutlined,
} from "@ant-design/icons";

const { Header, Sider, Content } = Layout;

const menuItems = [
  { key: "/", icon: <DashboardOutlined />, label: "Overview" },
  { key: "/running", icon: <PlayCircleOutlined />, label: "Running" },
  { key: "/completed", icon: <CheckCircleOutlined />, label: "Completed" },
  { key: "/serve", icon: <CloudServerOutlined />, label: "Serve" },
];

export default function AppLayout() {
  const [collapsed, setCollapsed] = useState(false);
  const navigate = useNavigate();
  const location = useLocation();
  const { token } = theme.useToken();

  // Determine selected key
  const selectedKey =
    menuItems.find((m) => m.key !== "/" && location.pathname.startsWith(m.key))
      ?.key ?? "/";

  return (
    <Layout style={{ minHeight: "100vh" }}>
      <Sider
        collapsible
        collapsed={collapsed}
        onCollapse={setCollapsed}
        theme="light"
        style={{
          borderRight: `1px solid ${token.colorBorderSecondary}`,
        }}
      >
        <div
          style={{
            height: 48,
            display: "flex",
            alignItems: "center",
            justifyContent: "center",
            borderBottom: `1px solid ${token.colorBorderSecondary}`,
          }}
        >
          <Typography.Title
            level={5}
            style={{ margin: 0, whiteSpace: "nowrap" }}
          >
            {collapsed ? "N" : "Nurion"}
          </Typography.Title>
        </div>
        <Menu
          mode="inline"
          selectedKeys={[selectedKey]}
          items={menuItems}
          onClick={({ key }) => navigate(key)}
          style={{ borderRight: 0 }}
        />
      </Sider>
      <Layout>
        <Header
          style={{
            background: token.colorBgContainer,
            padding: "0 24px",
            borderBottom: `1px solid ${token.colorBorderSecondary}`,
            display: "flex",
            alignItems: "center",
            height: 48,
          }}
        >
          <Typography.Text type="secondary" style={{ fontSize: 13 }}>
            Debug UI v0.1.0
          </Typography.Text>
        </Header>
        <Content style={{ margin: 16, overflow: "auto" }}>
          <Outlet />
        </Content>
      </Layout>
    </Layout>
  );
}
