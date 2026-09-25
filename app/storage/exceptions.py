class StorageError(Exception):
    """Base exception for storage operations."""
    pass

class NodeOfflineError(StorageError):
    """Raised when an operation is attempted on an offline node."""
    pass

class NodePartitionedError(StorageError):
    """Raised when an operation is attempted on a partitioned node."""
    pass
