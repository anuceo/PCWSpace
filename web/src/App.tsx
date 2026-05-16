import { Routes, Route, Navigate } from 'react-router-dom';
import Layout from './components/Layout';
import Dashboard from './pages/Dashboard';
import SessionPage from './pages/SessionPage';
import ArtifactsPage from './pages/ArtifactsPage';
import WorkflowsPage from './pages/WorkflowsPage';
import TimelinePage from './pages/TimelinePage';

export default function App() {
  return (
    <Routes>
      <Route element={<Layout />}>
        <Route path="/" element={<Dashboard />} />
        <Route path="/session/:id" element={<SessionPage />} />
        <Route path="/artifacts" element={<ArtifactsPage />} />
        <Route path="/workflows" element={<WorkflowsPage />} />
        <Route path="/timeline/:sessionId" element={<TimelinePage />} />
        <Route path="*" element={<Navigate to="/" />} />
      </Route>
    </Routes>
  );
}
