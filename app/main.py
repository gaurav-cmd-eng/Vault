from contextlib import asynccontextmanager
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
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

# Static files and interactive dashboard
static_dir = settings.base_dir / "app" / "static"
if static_dir.exists():
    app.mount("/static", StaticFiles(directory=static_dir), name="static")

@app.get("/")
async def root(request: Request):
    """Root endpoint serving metadata for API clients or Dashboard for browsers."""
    accept = request.headers.get("accept", "")
    static_index = static_dir / "index.html"
    if "text/html" in accept and static_index.exists():
        return FileResponse(static_index, media_type="text/html")
    return {
        "app": settings.app_name,
        "version": settings.version,
        "status": "online",
        "docs_url": "/docs",
        "health_url": "/api/health",
        "nodes_url": "/api/nodes",
        "dashboard_url": "/dashboard",
    }

@app.get("/dashboard")
async def dashboard():
    """Serves the interactive dashboard single-page application."""
    static_index = static_dir / "index.html"
    if static_index.exists():
        return FileResponse(static_index, media_type="text/html")
    return {"message": "Dashboard UI not found"}

