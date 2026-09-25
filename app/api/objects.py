from typing import List, Optional
from fastapi import APIRouter, File, UploadFile, Query, Request, Response, HTTPException, status
from app.config import settings
from app.models.schemas import (
    ObjectMetadata,
    ObjectVerificationReport,
    ReplicaCorruptionResponse,
    ObjectRepairResponse,
)
from app.services.replication import (
    replication_service,
    ReplicationError,
    ObjectNotFoundError,
    ObjectCorruptedError,
)

router = APIRouter(tags=["Objects"])

@router.post(
    "/objects",
    response_model=ObjectMetadata,
    status_code=status.HTTP_201_CREATED,
    summary="Upload and replicate an object across storage nodes",
)
async def upload_object(
    file: UploadFile = File(...),
    replication_factor: int = Query(
        default=settings.default_replication_factor,
        description="Number of physical replicas to store (1-5)",
    ),
):
    """
    Uploads a file and replicates it across multiple storage nodes with SHA-256 integrity verification.
    """
    if not file.filename:
        raise HTTPException(status_code=400, detail="Missing filename")

    content = await file.read()
    content_type = file.content_type or "application/octet-stream"

    try:
        metadata = await replication_service.store_object(
            filename=file.filename,
            data=content,
            content_type=content_type,
            replication_factor=replication_factor,
        )
        return metadata
    except ReplicationError as re:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(re),
        )

@router.get(
    "/objects",
    response_model=List[ObjectMetadata],
    summary="List all stored objects and their replica placement",
)
async def list_objects():
    """Returns a list of all stored objects with replica metadata."""
    return await replication_service.list_objects()

@router.get(
    "/objects/{object_id}",
    response_model=ObjectMetadata,
    summary="Get object metadata",
)
async def get_object_metadata(object_id: str):
    """Returns complete metadata for a specific object."""
    meta = await replication_service.get_object_metadata(object_id)
    if not meta:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Object '{object_id}' not found",
        )
    return meta

@router.get(
    "/objects/{object_id}/download",
    summary="Download an object with integrity verification",
)
async def download_object(object_id: str):
    """
    Downloads the object content, automatically verifying SHA-256 integrity against metadata.
    """
    try:
        data, filename, content_type = await replication_service.retrieve_object(object_id)
        
        headers = {
            "Content-Disposition": f'attachment; filename="{filename}"',
            "Content-Length": str(len(data)),
            "ETag": f'"{compute_sha256_header(data)}"',
            "X-Vault-Checksum": compute_sha256_header(data),
        }
        return Response(content=data, media_type=content_type, headers=headers)
    except ObjectNotFoundError:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Object '{object_id}' not found",
        )
    except ObjectCorruptedError as ce:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=str(ce),
        )

@router.get(
    "/objects/{object_id}/verify",
    response_model=ObjectVerificationReport,
    summary="Verify all physical replicas on disk",
)
async def verify_object_replicas(object_id: str):
    """
    Inspects all known physical replicas on disk and checks their SHA-256 checksums.
    """
    try:
        return await replication_service.verify_object(object_id)
    except ObjectNotFoundError:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Object '{object_id}' not found",
        )

@router.post(
    "/objects/{object_id}/replicas/{node_id}/corrupt",
    response_model=ReplicaCorruptionResponse,
    status_code=status.HTTP_200_OK,
    summary="Inject byte corruption into exactly one physical replica on disk",
)
async def corrupt_replica(object_id: str, node_id: str):
    """
    Modifies only the selected physical replica on disk, leaving object metadata unchanged.
    """
    try:
        return await replication_service.corrupt_replica(object_id, node_id)
    except ObjectNotFoundError as oe:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(oe))
    except ReplicationError as re:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(re))

@router.post(
    "/objects/{object_id}/repair",
    response_model=ObjectRepairResponse,
    status_code=status.HTTP_200_OK,
    summary="Trigger explicit self-healing repair for an object",
)
async def repair_object(object_id: str):
    """
    Repairs corrupted or missing replicas using an existing healthy replica as source.
    """
    try:
        return await replication_service.repair_object(object_id)
    except ObjectNotFoundError as oe:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(oe))
    except ReplicationError as re:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(re))

@router.delete(
    "/objects/{object_id}",
    summary="Delete an object and all physical replicas",
)
async def delete_object(object_id: str):
    """
    Deletes an object's metadata and all physical replica files on storage nodes.
    """
    deleted = await replication_service.delete_object(object_id)
    if not deleted:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Object '{object_id}' not found",
        )
    return {"deleted": True, "object_id": object_id}

@router.put(
    "/objects/{key:path}",
    response_model=ObjectMetadata,
    status_code=status.HTTP_201_CREATED,
    summary="Backward-compatible raw upload endpoint",
)
async def put_object_raw(key: str, request: Request):
    """Raw byte upload for backward compatibility."""
    data = await request.body()
    content_type = request.headers.get("content-type", "application/octet-stream")
    try:
        return await replication_service.store_object(
            filename=key,
            data=data,
            content_type=content_type,
            custom_object_id=key,
        )
    except ReplicationError as re:
        raise HTTPException(status_code=400, detail=str(re))

def compute_sha256_header(data: bytes) -> str:
    import hashlib
    return hashlib.sha256(data).hexdigest()
