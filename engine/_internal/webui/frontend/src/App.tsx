import { Routes, Route } from "react-router-dom";
import AppLayout from "./layouts/AppLayout";
import JobsListPage from "./pages/JobsListPage";
import JobDetailPage from "./pages/JobDetailPage";
import StageDetailPage from "./pages/StageDetailPage";
import WorkerDetailPage from "./pages/WorkerDetailPage";
import EventsPage from "./pages/EventsPage";
import LineagePage from "./pages/LineagePage";
import ConfigurationPage from "./pages/ConfigurationPage";
import ServeOverviewPage from "./pages/ServeOverviewPage";

export default function App() {
  return (
    <Routes>
      <Route element={<AppLayout />}>
        <Route index element={<JobsListPage />} />
        <Route path="running" element={<JobsListPage defaultTab="running" />} />
        <Route path="completed" element={<JobsListPage defaultTab="completed" />} />
        <Route path="jobs/:jobId" element={<JobDetailPage />} />
        <Route path="jobs/:jobId/stages/:stageId" element={<StageDetailPage />} />
        <Route path="jobs/:jobId/workers/:workerId" element={<WorkerDetailPage />} />
        <Route path="jobs/:jobId/events" element={<EventsPage />} />
        <Route path="jobs/:jobId/lineage" element={<LineagePage />} />
        <Route path="jobs/:jobId/configuration" element={<ConfigurationPage />} />
        <Route path="serve" element={<ServeOverviewPage />} />
      </Route>
    </Routes>
  );
}
