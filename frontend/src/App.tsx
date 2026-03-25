import { Routes, Route } from 'react-router-dom';
import Layout from './components/Layout/Layout';
import DashboardPage from './pages/DashboardPage';
import AgenciesPage from './pages/AgenciesPage';
import AssetsPage from './pages/AssetsPage';
import ChatbotPage from './pages/ChatbotPage';
import WeaviatePage from './pages/WeaviatePage';
import Neo4jPage from './pages/Neo4jPage';
import ExperimentTrackerPage from './pages/ExperimentTrackerPage';

function App() {
  return (
    <Layout>
      <Routes>
        <Route path="/" element={<DashboardPage />} />
        <Route path="/agencies" element={<AgenciesPage />} />
        <Route path="/assets" element={<AssetsPage />} />
        <Route path="/chat" element={<ChatbotPage />} />
        <Route path="/weaviate" element={<WeaviatePage />} />
        <Route path="/neo4j" element={<Neo4jPage />} />
        <Route path="/experiments" element={<ExperimentTrackerPage />} />
      </Routes>
    </Layout>
  );
}

export default App;
