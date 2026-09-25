import aiosqlite
from pathlib import Path
from typing import AsyncGenerator, Optional
from app.config import settings
from app.utils.logger import logger

SCHEMA_PATH = Path(__file__).parent / "schema.sql"

class Database:
    """Async SQLite database manager with WAL mode and foreign keys enabled."""
    def __init__(self, db_path: Path):
        self.db_path = db_path
        self._connection: Optional[aiosqlite.Connection] = None

    async def connect(self) -> aiosqlite.Connection:
        if self._connection is None:
            self.db_path.parent.mkdir(parents=True, exist_ok=True)
            self._connection = await aiosqlite.connect(self.db_path)
            self._connection.row_factory = aiosqlite.Row
            await self._connection.execute("PRAGMA journal_mode = WAL;")
            await self._connection.execute("PRAGMA busy_timeout = 5000;")
            await self._connection.execute("PRAGMA foreign_keys = ON;")
            await self._connection.commit()
            logger.info(f"Connected to SQLite database at {self.db_path} (WAL enabled)")
        return self._connection

    async def close(self) -> None:
        if self._connection is not None:
            await self._connection.close()
            self._connection = None
            logger.info("Closed SQLite database connection")

    async def init_schema(self) -> None:
        """Executes schema.sql to ensure all tables and indexes exist."""
        conn = await self.connect()
        if SCHEMA_PATH.exists():
            # Check if existing objects table has old schema
            cursor = await conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='objects'")
            if await cursor.fetchone():
                pragma_cursor = await conn.execute("PRAGMA table_info(objects)")
                columns = [col[1] for col in await pragma_cursor.fetchall()]
                if "object_id" not in columns:
                    logger.info("Migrating objects/replicas schema to Phase 2 structure...")
                    await conn.execute("DROP TABLE IF EXISTS replicas")
                    await conn.execute("DROP TABLE IF EXISTS objects")
                    await conn.commit()

            with open(SCHEMA_PATH, "r", encoding="utf-8") as f:
                schema_sql = f.read()
            await conn.executescript(schema_sql)
            await conn.commit()
            logger.info("Database schema initialized successfully")
        else:
            raise FileNotFoundError(f"Schema file not found at {SCHEMA_PATH}")

    async def execute(self, query: str, parameters: tuple = ()) -> aiosqlite.Cursor:
        conn = await self.connect()
        cursor = await conn.execute(query, parameters)
        await conn.commit()
        return cursor

    async def fetch_one(self, query: str, parameters: tuple = ()):
        conn = await self.connect()
        async with conn.execute(query, parameters) as cursor:
            return await cursor.fetchone()

    async def fetch_all(self, query: str, parameters: tuple = ()):
        conn = await self.connect()
        async with conn.execute(query, parameters) as cursor:
            return await cursor.fetchall()

# Global database instance
db = Database(settings.database_path)

async def get_db() -> AsyncGenerator[aiosqlite.Connection, None]:
    """Dependency helper to yield DB connection."""
    conn = await db.connect()
    yield conn
