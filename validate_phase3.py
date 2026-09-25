import asyncio
import hashlib
import json
from httpx import AsyncClient, ASGITransport
from app.main import app
from app.database.db import db
from app.storage.node_manager import node_manager

async def run_phase3_validation():
    print("=" * 60)
    print("VAULT PHASE 3 — CHAOS ENGINEERING & SELF-HEALING VALIDATION")
    print("=" * 60)

    # Initialize cluster state
    await db.init_schema()
    node_manager.initialize()
    await node_manager.sync_database()
    for n in node_manager.list_nodes():
        await node_manager.set_node_online(n.node_id)

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://127.0.0.1:8000") as client:

        # ---------------------------------------------------------------------
        # STEP 1: Upload with replication factor 3
        # ---------------------------------------------------------------------
        print("\n[STEP 1] Uploading Object with Replication Factor = 3...")
        filename = "phase3_chaos_demo.txt"
        content = (
            b"Vault Phase 3 Mission-Critical Data Payload.\n"
            b"Demonstrating controlled chaos, bit-rot injection, and automatic self-healing repair.\n"
            b"Timestamp: 2026-09-26T04:25:00Z\n"
            b"Integrity Protected by SHA-256."
        )
        original_sha256 = hashlib.sha256(content).hexdigest()

        files = {"file": (filename, content, "text/plain")}
        up_res = await client.post("/api/objects?replication_factor=3", files=files)
        assert up_res.status_code == 201, f"Upload failed: {up_res.text}"
        obj_meta = up_res.json()
        object_id = obj_meta["object_id"]
        replica_nodes = obj_meta["replica_nodes"]

        print(f"  Object ID:        {object_id}")
        print(f"  Original SHA-256: {original_sha256}")
        print(f"  Assigned Nodes:   {replica_nodes}")

        # ---------------------------------------------------------------------
        # STEP 2: Verify 3 Healthy Replicas
        # ---------------------------------------------------------------------
        print("\n[STEP 2] Verifying initial cluster replica state...")
        v1_res = await client.get(f"/api/objects/{object_id}/verify")
        assert v1_res.status_code == 200
        v1 = v1_res.json()
        print(f"  Healthy: {v1['healthy_replicas']} | Corrupt: {v1['corrupt_replicas']} | Missing/Unavailable: {v1['missing_replicas']}")
        assert v1["healthy_replicas"] == 3
        assert v1["corrupt_replicas"] == 0
        assert v1["missing_replicas"] == 0

        # ---------------------------------------------------------------------
        # STEP 3: Corrupt exactly one physical replica
        # ---------------------------------------------------------------------
        victim_node = replica_nodes[1]
        print(f"\n[STEP 3] Injecting deterministic bit corruption into replica on {victim_node}...")
        corrupt_res = await client.post(f"/api/objects/{object_id}/replicas/{victim_node}/corrupt")
        assert corrupt_res.status_code == 200
        c_info = corrupt_res.json()
        print(f"  Corrupted Node:       {c_info['node_id']}")
        print(f"  Corrupted Checksum:   {c_info['corrupted_checksum']}")
        print(f"  Original Checksum:    {c_info['original_checksum']}")
        assert c_info["corrupted_checksum"] != original_sha256
        assert c_info["original_checksum"] == original_sha256

        # ---------------------------------------------------------------------
        # STEP 4: Verify corruption detected (2 Healthy, 1 Corrupt)
        # ---------------------------------------------------------------------
        print("\n[STEP 4] Verifying cluster detects the corrupted replica...")
        v2_res = await client.get(f"/api/objects/{object_id}/verify")
        assert v2_res.status_code == 200
        v2 = v2_res.json()
        print(f"  Healthy: {v2['healthy_replicas']} | Corrupt: {v2['corrupt_replicas']} | Missing/Unavailable: {v2['missing_replicas']}")
        for rep in v2["replicas"]:
            print(f"    Node: {rep['node_id']:<8} | State: {rep['node_state']:<7} | Healthy: {str(rep['healthy']):<5} | Status: {rep['status']}")
        assert v2["healthy_replicas"] == 2
        assert v2["corrupt_replicas"] == 1
        assert v2["missing_replicas"] == 0

        # ---------------------------------------------------------------------
        # STEP 5: Run Self-Healing Repair
        # ---------------------------------------------------------------------
        print("\n[STEP 5] Triggering Self-Healing Replica Repair...")
        repair_res = await client.post(f"/api/objects/{object_id}/repair")
        assert repair_res.status_code == 200
        repair_data = repair_res.json()
        print(f"  Source Replica:       {repair_data['source_replica']}")
        print(f"  Repaired Replicas:    {repair_data['repaired_replicas']}")
        print(f"  Failed Repairs:       {repair_data['failed_repairs']}")
        print(f"  Message:              {repair_data['message']}")
        assert victim_node in repair_data["repaired_replicas"]
        assert repair_data["source_replica"] != victim_node

        # ---------------------------------------------------------------------
        # STEP 6: Verify 3 Healthy, 0 Corrupt, 0 Missing after repair
        # ---------------------------------------------------------------------
        print("\n[STEP 6] Verifying cluster after self-healing repair...")
        v3_res = await client.get(f"/api/objects/{object_id}/verify")
        assert v3_res.status_code == 200
        v3 = v3_res.json()
        print(f"  Healthy: {v3['healthy_replicas']} | Corrupt: {v3['corrupt_replicas']} | Missing/Unavailable: {v3['missing_replicas']}")
        for rep in v3["replicas"]:
            print(f"    Node: {rep['node_id']:<8} | State: {rep['node_state']:<7} | Healthy: {str(rep['healthy']):<5} | Status: {rep['status']} | Checksum: {rep['checksum'][:16]}...")
        assert v3["healthy_replicas"] == 3
        assert v3["corrupt_replicas"] == 0
        assert v3["missing_replicas"] == 0

        # ---------------------------------------------------------------------
        # STEP 7: Download and verify content/checksum matches original
        # ---------------------------------------------------------------------
        print("\n[STEP 7] Downloading object and verifying integrity against original...")
        dl_res = await client.get(f"/api/objects/{object_id}/download")
        assert dl_res.status_code == 200
        assert dl_res.content == content
        assert dl_res.headers.get("X-Vault-Checksum") == original_sha256
        print("  Download status:  200 OK")
        print("  Content matched:  EXACT 100% BYTE-FOR-BYTE MATCH")
        print(f"  Header Checksum:  {dl_res.headers.get('X-Vault-Checksum')}")

        # ---------------------------------------------------------------------
        # STEP 8: Node failure demonstration (Offline -> Online -> Re-verify)
        # ---------------------------------------------------------------------
        offline_node = replica_nodes[0]
        print(f"\n[STEP 8] Node Failure Simulation: Taking {offline_node} OFFLINE...")
        off_res = await client.post(f"/api/nodes/{offline_node}/offline")
        assert off_res.status_code == 200
        print(f"  Node {offline_node} status changed from {off_res.json()['previous_state']} -> {off_res.json()['new_state']}")

        # Verify cluster reflects unavailable replica
        print("  Checking cluster verification with node offline:")
        v4_res = await client.get(f"/api/objects/{object_id}/verify")
        assert v4_res.status_code == 200
        v4 = v4_res.json()
        print(f"  Healthy: {v4['healthy_replicas']} | Unavailable: {v4.get('unavailable_replicas', 0)}")
        assert v4["healthy_replicas"] == 2
        assert v4.get("unavailable_replicas", 0) == 1

        # Bring node back ONLINE
        print(f"\n[STEP 9] Restoring Node: Bringing {offline_node} back ONLINE...")
        on_res = await client.post(f"/api/nodes/{offline_node}/online")
        assert on_res.status_code == 200
        print(f"  Node {offline_node} status changed from {on_res.json()['previous_state']} -> {on_res.json()['new_state']}")

        # Re-verify cluster health
        v5_res = await client.get(f"/api/objects/{object_id}/verify")
        assert v5_res.status_code == 200
        v5 = v5_res.json()
        print(f"  Final Healthy: {v5['healthy_replicas']} | Corrupt: {v5['corrupt_replicas']} | Missing/Unavailable: {v5['missing_replicas']}")
        assert v5["healthy_replicas"] == 3
        assert v5["corrupt_replicas"] == 0
        assert v5["missing_replicas"] == 0

        # Clean up demo object so environment remains clean
        print("\n[CLEANUP] Purging demo object...")
        del_res = await client.delete(f"/api/objects/{object_id}")
        assert del_res.status_code == 200
        print("  Demo object cleaned up successfully.")

    print("\n" + "=" * 60)
    print("PHASE 3 END-TO-END VALIDATION COMPLETED SUCCESSFULLY (10/10 STEPS)")
    print("=" * 60)

if __name__ == "__main__":
    asyncio.run(run_phase3_validation())
