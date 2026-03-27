import { Routes, Route } from 'react-router-dom';
import Layout from './components/Layout/Layout';
import DashboardPage from './pages/DashboardPage';
import AgenciesPage from './pages/AgenciesPage';
import AssetsPage from './pages/AssetsPage';
import ChatbotPage from './pages/ChatbotPage';
import WeaviatePage from './pages/WeaviatePage';
import Neo4jPage from './pages/Neo4jPage';
import ExperimentTrackerPage from './pages/ExperimentTrackerPage';
import ArchitecturePage from './pages/ArchitecturePage';
import McpInspectorPage from './pages/McpInspectorPage';
import CatalogPage from './pages/CatalogPage';
import LearnPage from './pages/LearnPage';
import SwaggerPage from './pages/SwaggerPage';
import RedocPage from './pages/RedocPage';
import OpenApiPage from './pages/OpenApiPage';

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
        <Route path="/architecture" element={<ArchitecturePage />} />
        <Route path="/swagger" element={<SwaggerPage />} />
        <Route path="/redoc" element={<RedocPage />} />
        <Route path="/openapi" element={<OpenApiPage />} />
        <Route path="/catalog" element={<CatalogPage />} />
        <Route path="/learn" element={<LearnPage />} />
        <Route path="/mcp" element={<McpInspectorPage />} />
      </Routes>
    </Layout>
  );
}

export default App;
