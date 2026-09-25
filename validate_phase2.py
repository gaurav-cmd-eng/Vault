import asyncio
import hashlib
import json
from pathlib import Path
from httpx import AsyncClient, ASGITransport
from app.main import app
from app.database.db import db
from app.storage.node_manager import node_manager
from app.config import settings

async def run_validation():
    print("=== STARTING REAL PHASE 2 END-TO-END VALIDATION ===")
    
    # 1. Initialize cluster
    await db.init_schema()
    node_manager.initialize()
    await node_manager.sync_database()
    
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://127.0.0.1:8000") as client:
        # Sample payload
        test_filename = "vault_hackathon_demo.txt"
        test_payload = (
            b"Vault Distributed Object Storage - Real Hackathon Demo Payload.\n"
            b"Fault-tolerant storage across simulated independent nodes.\n"
            b"Timestamp: 2026-09-26T03:55:00Z\n"
            b"Integrity Guaranteed with SHA-256."
        )
        expected_checksum = hashlib.sha256(test_payload).hexdigest()
        print(f"Original File: {test_filename}")
        print(f"Payload Size: {len(test_payload)} bytes")
        print(f"Expected SHA-256: {expected_checksum}")

        # 2. Upload with replication_factor=3
        files = {"file": (test_filename, test_payload, "text/plain")}
        upload_res = await client.post("/api/objects?replication_factor=3", files=files)
        
        assert upload_res.status_code == 201, f"Upload failed: {upload_res.text}"
        upload_data = upload_res.json()
        object_id = upload_data["object_id"]
        replica_nodes = upload_data["replica_nodes"]
        
        print("\n--- 1. UPLOAD COMPLETED ---")
        print(f"Object ID: {object_id}")
        print(f"Assigned Replicas: {replica_nodes}")
        print(f"Reported Checksum: {upload_data['checksum']}")
        assert upload_data["checksum"] == expected_checksum
        assert len(replica_nodes) == 3

        # 3. Verify physical files on disk
        print("\n--- 2. PHYSICAL DISK INSPECTION ---")
        replica_paths = []
        for nid in replica_nodes:
            node = node_manager.get_node(nid)
            path = node.get_object_path(object_id)
            replica_paths.append(str(path))
            assert path.exists(), f"File does not exist on disk at {path}"
            
            # Read directly from disk without API
            disk_bytes = path.read_bytes()
            disk_checksum = hashlib.sha256(disk_bytes).hexdigest()
            print(f"Node: {nid}")
            print(f"  Path: {path}")
            print(f"  Physical Size: {len(disk_bytes)} bytes")
            print(f"  Direct SHA-256: {disk_checksum}")
            assert disk_bytes == test_payload, f"Data mismatch on node {nid}"
            assert disk_checksum == expected_checksum, f"Checksum mismatch on node {nid}"

        # 4. Download through API
        print("\n--- 3. API DOWNLOAD & CHECKSUM VERIFICATION ---")
        download_res = await client.get(f"/api/objects/{object_id}/download")
        assert download_res.status_code == 200
        downloaded_bytes = download_res.content
        download_checksum = download_res.headers.get("X-Vault-Checksum")
        
        print(f"Downloaded Size: {len(downloaded_bytes)} bytes")
        print(f"Download Header X-Vault-Checksum: {download_checksum}")
        print(f"Content-Disposition: {download_res.headers.get('Content-Disposition')}")
        assert downloaded_bytes == test_payload
        assert download_checksum == expected_checksum

        # 5. Call Replica Verification Endpoint
        print("\n--- 4. REPLICA VERIFICATION ENDPOINT ---")
        verify_res = await client.get(f"/api/objects/{object_id}/verify")
        assert verify_res.status_code == 200
        verify_data = verify_res.json()
        print(json.dumps(verify_data, indent=2))
        
        assert verify_data["healthy_replicas"] == 3
        assert verify_data["corrupt_replicas"] == 0
        assert verify_data["missing_replicas"] == 0

        print("\n=== ALL REAL PHASE 2 VALIDATION STEPS PASSED SUCCESSFULLY ===")

if __name__ == "__main__":
    asyncio.run(run_validation())
