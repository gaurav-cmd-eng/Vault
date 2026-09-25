import io
import hashlib
import pytest
from httpx import AsyncClient, ASGITransport
from app.main import app
from app.database.db import db
from app.storage.node_manager import node_manager
from app.config import settings

@pytest.fixture(autouse=True)
async def setup_cluster():
    """Ensure database schema and storage node pool are initialized and online before each test."""
    await db.init_schema()
    node_manager.initialize()
    await node_manager.sync_database()
    # Ensure all nodes are online
    for node in node_manager.list_nodes():
        await node_manager.set_node_online(node.node_id)

@pytest.mark.asyncio
async def test_node_offline_and_online_simulation():
    """Tests 1, 2, 3: Node can be taken offline, brought online, and state is reported correctly."""
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        target_node = "node_2"

        # 1. Take node offline
        off_res = await ac.post(f"/api/nodes/{target_node}/offline")
        assert off_res.status_code == 200
        off_data = off_res.json()
        assert off_data["node_id"] == target_node
        assert off_data["previous_state"] == "ONLINE"
        assert off_data["new_state"] == "OFFLINE"

        # Verify state in GET /api/nodes
        nodes_res = await ac.get("/api/nodes")
        node_info = next(n for n in nodes_res.json() if n["node_id"] == target_node)
        assert node_info["status"] == "OFFLINE"
        assert node_info["is_online"] is False

        # 2. Bring node back online
        on_res = await ac.post(f"/api/nodes/{target_node}/online")
        assert on_res.status_code == 200
        on_data = on_res.json()
        assert on_data["node_id"] == target_node
        assert on_data["previous_state"] == "OFFLINE"
        assert on_data["new_state"] == "ONLINE"

        # Verify state back to ONLINE
        nodes_res2 = await ac.get("/api/nodes")
        node_info2 = next(n for n in nodes_res2.json() if n["node_id"] == target_node)
        assert node_info2["status"] == "ONLINE"
        assert node_info2["is_online"] is True

@pytest.mark.asyncio
async def test_corruption_injection_and_detection():
    """Tests 6, 7, 8: Corruption injection modifies only selected replica, metadata checksum unchanged, verification detects it."""
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        content = b"Phase 3 corruption injection test payload."
        original_hash = hashlib.sha256(content).hexdigest()

        # Upload with 3 replicas
        files = {"file": ("corrupt_test.txt", io.BytesIO(content), "text/plain")}
        up_res = await ac.post("/api/objects?replication_factor=3", files=files)
        assert up_res.status_code == 201
        obj_data = up_res.json()
        obj_id = obj_data["object_id"]
        replica_nodes = obj_data["replica_nodes"]
        corrupt_target = replica_nodes[0]
        unaffected_node = replica_nodes[1]

        # Corrupt exactly one replica
        c_res = await ac.post(f"/api/objects/{obj_id}/replicas/{corrupt_target}/corrupt")
        assert c_res.status_code == 200
        c_data = c_res.json()
        assert c_data["object_id"] == obj_id
        assert c_data["node_id"] == corrupt_target
        assert c_data["original_checksum"] == original_hash
        assert c_data["corrupted_checksum"] != original_hash

        # Test 7: Verify metadata checksum remains UNCHANGED in SQLite
        meta_res = await ac.get(f"/api/objects/{obj_id}")
        assert meta_res.status_code == 200
        assert meta_res.json()["checksum"] == original_hash

        # Test 6: Verify unaffected node still has identical original content
        node_unaffected = node_manager.get_node(unaffected_node)
        assert node_unaffected.get_object_path(obj_id).read_bytes() == content

        # Test 8: Verification detects corrupted replica
        verify_res = await ac.get(f"/api/objects/{obj_id}/verify")
        assert verify_res.status_code == 200
        v_data = verify_res.json()
        assert v_data["healthy_replicas"] == 2
        assert v_data["corrupt_replicas"] == 1
        assert v_data["missing_replicas"] == 0

        corrupted_rep = next(r for r in v_data["replicas"] if r["node_id"] == corrupt_target)
        assert corrupted_rep["healthy"] is False
        assert corrupted_rep["status"] == "CORRUPTED"
        assert corrupted_rep["checksum"] == c_data["corrupted_checksum"]

@pytest.mark.asyncio
async def test_repair_corrupted_replica_and_download():
    """Tests 9, 10, 11, 12, 13: Repair finds healthy source, restores replica, verification reports 3 healthy, download matches."""
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        content = b"Self-healing repair validation payload 9999!"
        original_hash = hashlib.sha256(content).hexdigest()

        # Upload
        files = {"file": ("repair_test.txt", io.BytesIO(content), "text/plain")}
        up_res = await ac.post("/api/objects?replication_factor=3", files=files)
        obj_data = up_res.json()
        obj_id = obj_data["object_id"]
        replica_nodes = obj_data["replica_nodes"]
        corrupt_target = replica_nodes[0]

        # Corrupt one replica
        await ac.post(f"/api/objects/{obj_id}/replicas/{corrupt_target}/corrupt")

        # Run repair
        repair_res = await ac.post(f"/api/objects/{obj_id}/repair")
        assert repair_res.status_code == 200
        rep_data = repair_res.json()
        assert corrupt_target in rep_data["repaired_replicas"]
        assert rep_data["source_replica"] in replica_nodes
        assert rep_data["source_replica"] != corrupt_target

        # Test 11: SHA-256 after repair matches original
        target_node = node_manager.get_node(corrupt_target)
        repaired_bytes = target_node.get_object_path(obj_id).read_bytes()
        assert hashlib.sha256(repaired_bytes).hexdigest() == original_hash
        assert repaired_bytes == content

        # Test 12: Verification reports 3 healthy replicas
        v_res = await ac.get(f"/api/objects/{obj_id}/verify")
        assert v_res.json()["healthy_replicas"] == 3
        assert v_res.json()["corrupt_replicas"] == 0

        # Test 13: Download returns original content
        dl_res = await ac.get(f"/api/objects/{obj_id}/download")
        assert dl_res.status_code == 200
        assert dl_res.content == content
        assert dl_res.headers["X-Vault-Checksum"] == original_hash

@pytest.mark.asyncio
async def test_repair_missing_replica():
    """Test 15: Missing replica (physical file deleted) can be repaired from healthy source."""
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        content = b"Repairing missing physical replica test"
        original_hash = hashlib.sha256(content).hexdigest()

        files = {"file": ("missing_rep.txt", io.BytesIO(content), "text/plain")}
        up_res = await ac.post("/api/objects?replication_factor=3", files=files)
        obj_id = up_res.json()["object_id"]
        replica_nodes = up_res.json()["replica_nodes"]
        missing_target = replica_nodes[2]

        # Physically delete file on disk for one replica
        target_node = node_manager.get_node(missing_target)
        target_node.delete_object(obj_id)
        assert not target_node.has_object(obj_id)

        # Verification reports missing replica
        v_res = await ac.get(f"/api/objects/{obj_id}/verify")
        assert v_res.json()["healthy_replicas"] == 2
        assert v_res.json()["missing_replicas"] == 1

        # Run repair
        repair_res = await ac.post(f"/api/objects/{obj_id}/repair")
        assert repair_res.status_code == 200
        assert missing_target in repair_res.json()["repaired_replicas"]

        # Confirm file recreated and healthy
        assert target_node.has_object(obj_id)
        assert target_node.get_object_path(obj_id).read_bytes() == content

        # Final verification
        final_v = await ac.get(f"/api/objects/{obj_id}/verify")
        assert final_v.json()["healthy_replicas"] == 3

@pytest.mark.asyncio
async def test_repair_fails_when_no_healthy_source():
    """Test 14: Repair fails clearly with 400 when no healthy replica source exists."""
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        content = b"Doom test: all replicas corrupt"
        files = {"file": ("doom.txt", io.BytesIO(content), "text/plain")}
        up_res = await ac.post("/api/objects?replication_factor=2", files=files)
        obj_id = up_res.json()["object_id"]
        replica_nodes = up_res.json()["replica_nodes"]

        # Corrupt ALL replicas
        for nid in replica_nodes:
            await ac.post(f"/api/objects/{obj_id}/replicas/{nid}/corrupt")

        # Attempt repair
        repair_res = await ac.post(f"/api/objects/{obj_id}/repair")
        assert repair_res.status_code == 400
        assert "no healthy source replica" in repair_res.json()["detail"].lower()

@pytest.mark.asyncio
async def test_offline_node_not_reported_as_healthy():
    """Test 16: Offline node is not incorrectly reported as healthy."""
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        content = b"Offline node reporting test"
        files = {"file": ("offline_check.txt", io.BytesIO(content), "text/plain")}
        up_res = await ac.post("/api/objects?replication_factor=3", files=files)
        obj_id = up_res.json()["object_id"]
        replica_nodes = up_res.json()["replica_nodes"]
        offline_node = replica_nodes[0]

        # Take node offline
        await ac.post(f"/api/nodes/{offline_node}/offline")

        # Verify endpoint
        v_res = await ac.get(f"/api/objects/{obj_id}/verify")
        assert v_res.status_code == 200
        data = v_res.json()
        assert data["healthy_replicas"] == 2
        assert data["unavailable_replicas"] == 1

        rep_detail = next(r for r in data["replicas"] if r["node_id"] == offline_node)
        assert rep_detail["healthy"] is False
        assert rep_detail["node_state"] == "OFFLINE"
        assert rep_detail["status"] == "UNAVAILABLE"

        # Restore online
        await ac.post(f"/api/nodes/{offline_node}/online")
