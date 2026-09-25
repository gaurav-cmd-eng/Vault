import json
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional
from app.events.types import EventType
from app.database.db import db
from app.utils.logger import logger

SubscriberFunc = Callable[[EventType, str, str, Optional[Dict[str, Any]]], Any]

class EventBus:
    """Asynchronous event bus and persistent audit logger."""
    def __init__(self):
        self._subscribers: List[SubscriberFunc] = []

    def subscribe(self, callback: SubscriberFunc) -> None:
        """Register a subscriber callback for published events."""
        self._subscribers.append(callback)

    async def emit(
        self,
        event_type: EventType,
        target: str,
        message: str,
        details: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Persists event to database and notifies all subscribers."""
        details_json = json.dumps(details) if details else None
        
        # Log to structured application logger
        logger.info(f"[EVENT] [{event_type.value}] Target: {target} | {message}")

        # Persist to SQLite
        try:
            query = """
                INSERT INTO events (event_type, target, message, details, timestamp)
                VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)
            """
            await db.execute(query, (event_type.value, target, message, details_json))
        except Exception as e:
            logger.error(f"Failed to persist event {event_type.value} to database: {e}")

        # Notify in-memory subscribers
        for subscriber in self._subscribers:
            try:
                res = subscriber(event_type, target, message, details)
                if hasattr(res, "__await__"):
                    await res
            except Exception as e:
                logger.error(f"Error in event subscriber {subscriber}: {e}")

# Global event bus singleton
event_bus = EventBus()
