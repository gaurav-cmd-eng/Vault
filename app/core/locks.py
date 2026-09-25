import asyncio
from contextlib import asynccontextmanager
from typing import Dict

class KeyLockManager:
    """Manages asynchronous per-key locks to serialize concurrent mutations on the same object."""
    
    def __init__(self):
        self._locks: Dict[str, asyncio.Lock] = {}
        self._master_lock = asyncio.Lock()

    @asynccontextmanager
    async def acquire(self, key: str):
        """Acquires a dedicated lock for the given object key."""
        async with self._master_lock:
            if key not in self._locks:
                self._locks[key] = asyncio.Lock()
            lock = self._locks[key]

        await lock.acquire()
        try:
            yield
        finally:
            lock.release()

# Global key lock manager singleton
key_lock_manager = KeyLockManager()
