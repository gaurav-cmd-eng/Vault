import pytest
from httpx import AsyncClient, ASGITransport
from app.main import app

@pytest.mark.asyncio
async def test_health_endpoint():
    """Verify GET /api/health returns 200 with valid health schema."""
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        response = await ac.get("/api/health")
        assert response.status_code == 200
        data = response.json()
        assert data["status"] in ("ok", "degraded")
        assert data["database_status"] == "connected"
        assert data["active_nodes"] > 0
        assert data["total_nodes"] == 5

@pytest.mark.asyncio
async def test_nodes_endpoint():
    """Verify GET /api/nodes returns 200 with list of node telemetry."""
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        response = await ac.get("/api/nodes")
        assert response.status_code == 200
        nodes = response.json()
        assert isinstance(nodes, list)
        assert len(nodes) == 5
        
        first = nodes[0]
        assert "node_id" in first
        assert "status" in first
        assert "stored_object_count" in first
        assert "storage_used_bytes" in first
        assert "capacity_bytes" in first
        assert "last_heartbeat" in first

@pytest.mark.asyncio
async def test_config_endpoint():
    """Verify GET /api/config returns cluster configuration."""
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        response = await ac.get("/api/config")
        assert response.status_code == 200
        config = response.json()
        assert config["num_nodes"] == 5
        assert config["default_replication_factor"] == 3
        assert config["write_quorum"] == 2
        assert config["read_quorum"] == 2
        assert config["max_upload_size_bytes"] > 0

@pytest.mark.asyncio
async def test_root_endpoint():
    """Verify GET / returns application info and docs link."""
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        response = await ac.get("/")
        assert response.status_code == 200
        data = response.json()
        assert data["app"] == "Vault Object Storage"
        assert data["docs_url"] == "/docs"
