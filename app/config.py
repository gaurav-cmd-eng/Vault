import os
from pathlib import Path
from pydantic import BaseModel, Field

class Settings(BaseModel):
    """Vault system configuration settings."""
    app_name: str = "Vault Object Storage"
    version: str = "0.1.0"
    
    # Storage paths
    base_dir: Path = Field(default_factory=lambda: Path(os.getcwd()))
    storage_root: Path = Field(default_factory=lambda: Path(os.getcwd()) / "data")
    database_path: Path = Field(default_factory=lambda: Path(os.getcwd()) / "data" / "metadata.db")
    
    # Node cluster settings
    num_nodes: int = 5
    node_prefix: str = "node_"
    node_capacity_bytes: int = 10 * 1024 * 1024 * 1024  # 10 GiB per simulated node
    
    # Replication & Quorum settings
    default_replication_factor: int = 3
    write_quorum: int = 2
    read_quorum: int = 2
    
    # Limits & Timing
    max_upload_size: int = 50 * 1024 * 1024  # 50 MB
    health_check_interval: int = 10           # seconds
    
    # Server network bindings
    host: str = "127.0.0.1"
    port: int = 8000
    
    @property
    def nodes_dir(self) -> Path:
        return self.storage_root / "nodes"

    def ensure_directories(self) -> None:
        """Ensure base data directory and nodes root exist."""
        self.storage_root.mkdir(parents=True, exist_ok=True)
        self.nodes_dir.mkdir(parents=True, exist_ok=True)

# Global settings singleton
settings = Settings()
