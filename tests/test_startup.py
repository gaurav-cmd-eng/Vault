import pytest
from pathlib import Path
from app.config import settings
from app.database.db import db
from app.storage.node_manager import node_manager

@pytest.mark.asyncio
async def test_database_initialization():
    """Verify that database connects and initializes all required tables."""
    await db.init_schema()
    conn = await db.connect()
    
    # Verify tables exist
    cursor = await conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name IN ('nodes', 'objects', 'replicas', 'events')"
    )
    tables = [row[0] for row in await cursor.fetchall()]
    assert "nodes" in tables
    assert "objects" in tables
    assert "replicas" in tables
    assert "events" in tables

@pytest.mark.asyncio
async def test_node_creation_and_directories():
    """Verify that storage nodes are created and directories exist on disk."""
    node_manager.initialize()
    nodes = node_manager.list_nodes()
    
    assert len(nodes) == settings.num_nodes
    for node in nodes:
        assert node.directory_path.exists()
        assert node.directory_path.is_dir()
        assert node.is_online is True
        assert node.capacity_bytes == settings.node_capacity_bytes
        
    # Verify database sync
    await node_manager.sync_database()
    rows = await db.fetch_all("SELECT id, status FROM nodes")
    assert len(rows) == settings.num_nodes
