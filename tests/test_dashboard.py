import io
import pytest
from httpx import AsyncClient, ASGITransport
from app.main import app
from app.database.db import db
from app.storage.node_manager import node_manager
from app.events.event_bus import event_bus
from app.events.types import EventType

@pytest.fixture(autouse=True)
async def setup_cluster():
    """Ensure database schema and storage node pool are initialized in a clean state before each test."""
    await db.init_schema()
    await db.execute("DELETE FROM replicas")
    await db.execute("DELETE FROM objects")
    node_manager.initialize()
    await node_manager.sync_database()
    for n in node_manager.list_nodes():
        await node_manager.set_node_online(n.node_id)
        if n.objects_dir.exists():
            for f in n.objects_dir.iterdir():
                if f.is_file():
                    try:
                        f.unlink()
                    except OSError:
                        pass

@pytest.mark.asyncio
async def test_get_cluster_stats_structure_and_values():
    """Tests 1, 2, 3: GET /api/stats returns 200, required fields, and reflects 5-node cluster."""
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        res = await ac.get("/api/stats")
        assert res.status_code == 200
        data = res.json()

        # Test 2: Contains all required fields
        required_fields = [
            "total_nodes",
            "active_nodes",
            "offline_nodes",
            "total_objects",
            "total_storage_used_bytes",
            "healthy_replicas",
            "corrupt_replicas",
            "missing_replicas",
            "cluster_status",
        ]
        for field in required_fields:
            assert field in data, f"Missing required field: {field}"

        # Test 3: Reflects initialized 5-node cluster
        assert data["total_nodes"] == 5
        assert data["active_nodes"] == 5
        assert data["offline_nodes"] == 0
        assert data["cluster_status"] == "OK"
        assert isinstance(data["total_objects"], int)
        assert isinstance(data["total_storage_used_bytes"], int)
        assert data["corrupt_replicas"] == 0

@pytest.mark.asyncio
async def test_cluster_stats_reflects_node_degradation():
    """Verify cluster_status changes to DEGRADED when a node goes offline."""
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        # Take node_1 offline
        await ac.post("/api/nodes/node_1/offline")

        res = await ac.get("/api/stats")
        assert res.status_code == 200
        data = res.json()
        assert data["active_nodes"] == 4
        assert data["offline_nodes"] == 1
        assert data["cluster_status"] == "DEGRADED"

        # Restore node_1
        await ac.post("/api/nodes/node_1/online")
        res2 = await ac.get("/api/stats")
        assert res2.json()["cluster_status"] == "OK"

@pytest.mark.asyncio
async def test_get_events_feed_structure():
    """Tests 4, 5: GET /api/events returns 200 and expected array structure."""
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        res = await ac.get("/api/events?limit=10")
        assert res.status_code == 200
        events = res.json()
        assert isinstance(events, list)

        if events:
            first = events[0]
            assert "id" in first
            assert "event_type" in first
            assert "target" in first
            assert "message" in first
            assert "timestamp" in first

@pytest.mark.asyncio
async def test_generated_event_appears_in_events_feed():
    """Test 6: Generate an event and verify it appears at the top of GET /api/events."""
    test_marker = "dashboard_event_test_marker_999"
    await event_bus.emit(
        EventType.OBJECT_UPLOAD_STARTED,
        target=test_marker,
        message="Synthetic dashboard test event",
        details={"test": True, "value": 42},
    )

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        res = await ac.get("/api/events?limit=20")
        assert res.status_code == 200
        events = res.json()

        found = next((e for e in events if e["target"] == test_marker), None)
        assert found is not None
        assert found["event_type"] == EventType.OBJECT_UPLOAD_STARTED.value
        assert found["message"] == "Synthetic dashboard test event"
        assert found["details"]["value"] == 42
