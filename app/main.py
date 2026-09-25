from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.config import settings
from app.database.db import db
from app.storage.node_manager import node_manager
from app.api.routes import api_router
from app.utils.logger import logger

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifecycle manager: startup and shutdown logic."""
    logger.info(f"Starting {settings.app_name} v{settings.version}")
    
    # 1. Ensure storage and node directories exist
    settings.ensure_directories()
    
    # 2. Initialize database schema (WAL mode)
    await db.init_schema()
    
    # 3. Initialize simulated storage nodes and their directories
    node_manager.initialize()
    
    # 4. Synchronize nodes with SQLite registry
    await node_manager.sync_database()
    
    logger.info("Vault initialization complete. Storage cluster is ready.")
    
    yield
    
    # Shutdown
    logger.info("Shutting down Vault...")
    await db.close()
    logger.info("Vault shutdown complete.")

app = FastAPI(
    title=settings.app_name,
    version=settings.version,
    description="Fault-tolerant distributed object storage system running on local simulated nodes.",
    lifespan=lifespan,
)

# Enable CORS for local testing and dashboard
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Mount API routes
app.include_router(api_router, prefix="/api")

@app.get("/")
async def root():
    return {
        "app": settings.app_name,
        "version": settings.version,
        "status": "online",
        "docs_url": "/docs",
        "health_url": "/api/health",
        "nodes_url": "/api/nodes",
    }
