import hashlib
from typing import BinaryIO, Union

def compute_sha256(data: Union[bytes, BinaryIO]) -> str:
    """Computes SHA-256 hex digest for bytes or binary stream."""
    hasher = hashlib.sha256()
    if isinstance(data, bytes):
        hasher.update(data)
    else:
        # Stream in 64KB chunks
        while chunk := data.read(65536):
            hasher.update(chunk)
    return hasher.hexdigest()

def verify_sha256(data: bytes, expected_hash: str) -> bool:
    """Verifies that bytes match expected SHA-256 hex digest."""
    return compute_sha256(data).lower() == expected_hash.strip().lower()
