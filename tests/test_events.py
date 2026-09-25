import pytest
from app.database.db import db
from app.events.event_bus import event_bus
from app.events.types import EventType

@pytest.mark.asyncio
async def test_event_bus_emission_and_persistence():
    """Verify events are properly logged to database and subscribers notified."""
    await db.init_schema()
    
    received_events = []
    
    def on_event(event_type, target, message, details):
        received_events.append((event_type, target, message))
        
    event_bus.subscribe(on_event)
    
    await event_bus.emit(
        EventType.NODE_ONLINE,
        target="node_test",
        message="Test node came online",
        details={"speed": "fast"},
    )
    
    assert len(received_events) > 0
    assert received_events[-1][0] == EventType.NODE_ONLINE
    assert received_events[-1][1] == "node_test"
    
    # Verify persisted in database
    row = await db.fetch_one(
        "SELECT event_type, target, message FROM events WHERE target = ? ORDER BY id DESC LIMIT 1",
        ("node_test",),
    )
    assert row is not None
    assert row["event_type"] == EventType.NODE_ONLINE.value
    assert row["target"] == "node_test"
    assert row["message"] == "Test node came online"
