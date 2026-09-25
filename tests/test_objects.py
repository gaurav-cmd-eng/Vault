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
    """Ensure database schema and storage node pool are initialized before tests."""
    await db.init_schema()
    node_manager.initialize()
    await node_manager.sync_database()

@pytest.mark.asyncio
async def test_upload_and_metadata_creation():
    """Tests 1, 2, 3: Upload object, verify metadata created and SHA-256 is correct."""
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        content = b"Hello Vault Storage! Testing SHA-256 integrity."
        expected_hash = hashlib.sha256(content).hexdigest()
        
        files = {"file": ("hello.txt", io.BytesIO(content), "text/plain")}
        res = await ac.post("/api/objects", files=files)
        
        assert res.status_code == 201
        data = res.json()
        assert data["object_id"] is not None
        assert data["filename"] == "hello.txt"
        assert data["size"] == len(content)
        assert data["checksum"] == expected_hash
        assert data["content_type"] == "text/plain"
        assert len(data["replica_nodes"]) == settings.default_replication_factor
        assert data["version"] == 1

@pytest.mark.asyncio
async def test_replication_factor_1():
    """Test 4: Replication factor 1 works properly."""
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        content = b"Single replica payload."
        files = {"file": ("single.txt", io.BytesIO(content), "text/plain")}
        res = await ac.post("/api/objects?replication_factor=1", files=files)
        
        assert res.status_code == 201
        data = res.json()
        assert len(data["replica_nodes"]) == 1
        
        # Verify physical file exists on that single node only
        node_id = data["replica_nodes"][0]
        node = node_manager.get_node(node_id)
        assert node is not None
        file_path = node.get_object_path(data["object_id"])
        assert file_path.exists()
        assert file_path.read_bytes() == content

@pytest.mark.asyncio
async def test_replication_factor_3_physical_copies():
    """Tests 5, 6, 7: Replication factor 3 creates three identical physical copies."""
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        content = b"Replication Factor 3 Test Data with binary bytes: \x00\x01\x02\xff"
        expected_hash = hashlib.sha256(content).hexdigest()
        
        files = {"file": ("data3.bin", io.BytesIO(content), "application/octet-stream")}
        res = await ac.post("/api/objects?replication_factor=3", files=files)
        
        assert res.status_code == 201
        data = res.json()
        replica_nodes = data["replica_nodes"]
        assert len(replica_nodes) == 3
        # No duplicates
        assert len(set(replica_nodes)) == 3

        # Verify physical existence and identical content on all 3 nodes
        for nid in replica_nodes:
            node = node_manager.get_node(nid)
            assert node is not None
            path = node.get_object_path(data["object_id"])
            assert path.exists(), f"Physical file missing on {nid}"
            node_bytes = path.read_bytes()
            assert node_bytes == content
            assert hashlib.sha256(node_bytes).hexdigest() == expected_hash

@pytest.mark.asyncio
async def test_list_and_get_metadata():
    """Tests 8, 9: GET /api/objects lists the object and GET /api/objects/{id} returns metadata."""
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        content = b"Inspectable metadata content"
        files = {"file": ("inspect.txt", io.BytesIO(content), "text/plain")}
        create_res = await ac.post("/api/objects", files=files)
        obj_id = create_res.json()["object_id"]

        # List objects
        list_res = await ac.get("/api/objects")
        assert list_res.status_code == 200
        objects = list_res.json()
        found = next((o for o in objects if o["object_id"] == obj_id), None)
        assert found is not None
        assert found["filename"] == "inspect.txt"

        # Get metadata
        meta_res = await ac.get(f"/api/objects/{obj_id}")
        assert meta_res.status_code == 200
        meta = meta_res.json()
        assert meta["object_id"] == obj_id
        assert meta["filename"] == "inspect.txt"
        assert meta["size"] == len(content)

@pytest.mark.asyncio
async def test_download_and_verify_checksum():
    """Tests 10, 11: Download returns original content and verifies checksum."""
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        content = b"Critical download file content to be verified"
        expected_hash = hashlib.sha256(content).hexdigest()
        
        files = {"file": ("download_test.txt", io.BytesIO(content), "text/plain")}
        create_res = await ac.post("/api/objects", files=files)
        obj_id = create_res.json()["object_id"]

        # Download object
        dl_res = await ac.get(f"/api/objects/{obj_id}/download")
        assert dl_res.status_code == 200
        assert dl_res.content == content
        assert dl_res.headers["X-Vault-Checksum"] == expected_hash
        assert 'filename="download_test.txt"' in dl_res.headers["Content-Disposition"]

@pytest.mark.asyncio
async def test_verification_endpoint():
    """Test 12: Verify endpoint reports healthy replicas."""
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        content = b"Verification report test"
        expected_hash = hashlib.sha256(content).hexdigest()
        
        files = {"file": ("verify.txt", io.BytesIO(content), "text/plain")}
        create_res = await ac.post("/api/objects?replication_factor=3", files=files)
        obj_id = create_res.json()["object_id"]

        # Call verify endpoint
        verify_res = await ac.get(f"/api/objects/{obj_id}/verify")
        assert verify_res.status_code == 200
        report = verify_res.json()
        assert report["object_id"] == obj_id
        assert report["expected_checksum"] == expected_hash
        assert report["healthy_replicas"] == 3
        assert report["corrupt_replicas"] == 0
        assert report["missing_replicas"] == 0
        assert len(report["replicas"]) == 3
        for rep in report["replicas"]:
            assert rep["exists"] is True
            assert rep["healthy"] is True
            assert rep["checksum"] == expected_hash

@pytest.mark.asyncio
async def test_delete_removes_metadata_and_physical_files():
    """Tests 13, 14: Delete removes metadata and physical replicas from all nodes."""
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        content = b"Temporary content to be deleted"
        files = {"file": ("temp.txt", io.BytesIO(content), "text/plain")}
        create_res = await ac.post("/api/objects?replication_factor=3", files=files)
        obj_id = create_res.json()["object_id"]
        replica_nodes = create_res.json()["replica_nodes"]

        # Ensure files exist before deletion
        for nid in replica_nodes:
            node = node_manager.get_node(nid)
            assert node.get_object_path(obj_id).exists()

        # Delete
        del_res = await ac.delete(f"/api/objects/{obj_id}")
        assert del_res.status_code == 200
        assert del_res.json()["deleted"] is True

        # Verify metadata is gone
        get_meta = await ac.get(f"/api/objects/{obj_id}")
        assert get_meta.status_code == 404

        # Verify physical files removed from all nodes
        for nid in replica_nodes:
            node = node_manager.get_node(nid)
            assert not node.get_object_path(obj_id).exists()

@pytest.mark.asyncio
async def test_invalid_replication_factor_rejected():
    """Test 15: Invalid replication factor (< 1) is rejected with 400."""
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        files = {"file": ("bad_rf.txt", io.BytesIO(b"data"), "text/plain")}
        res = await ac.post("/api/objects?replication_factor=0", files=files)
        assert res.status_code == 400
        assert "at least 1" in res.json()["detail"]

@pytest.mark.asyncio
async def test_excessive_replication_factor_rejected():
    """Test 16: Replication factor greater than available nodes is rejected with 400."""
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        files = {"file": ("too_many.txt", io.BytesIO(b"data"), "text/plain")}
        # There are 5 nodes, request 10
        res = await ac.post("/api/objects?replication_factor=10", files=files)
        assert res.status_code == 400
        assert "exceeds available online storage nodes" in res.json()["detail"]

@pytest.mark.asyncio
async def test_oversized_upload_rejected():
    """Test 17: Oversized upload is rejected."""
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        # Create dummy content larger than max_upload_size
        original_max = settings.max_upload_size
        settings.max_upload_size = 100  # Set limit to 100 bytes for test
        try:
            large_content = b"X" * 150
            files = {"file": ("large.bin", io.BytesIO(large_content), "application/octet-stream")}
            res = await ac.post("/api/objects", files=files)
            assert res.status_code == 400
            assert "exceeds configured limit" in res.json()["detail"]
        finally:
            settings.max_upload_size = original_max
