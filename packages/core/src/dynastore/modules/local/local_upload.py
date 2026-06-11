#    Copyright 2026 FAO
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
#
#    Author: Carlo Cancellieri (ccancellieri@gmail.com)
#    Company: FAO, Viale delle Terme di Caracalla, 00100 Rome, Italy
#    Contact: copyright@fao.org - http://fao.org/contact-us/terms/en/

"""
Local-disk upload backend — server-side file receive + asset registration.

Upload flow
-----------
1. Client calls ``POST /assets/catalogs/{id}/upload`` (or the collection variant).
2. ``assets_service.py`` resolves the upload backend via
   ``router.get_asset_upload_driver(catalog_id, collection_id)`` (which honours
   ``AssetRoutingConfig.operations[UPLOAD]`` and falls back to the first-
   registered backend) and calls ``initiate_upload`` → returns
   ``UploadTicket(backend="local", method="POST",
   upload_url="/local-upload/{ticket_id}")``.
3. Client POSTs the file (multipart) to ``/local-upload/{ticket_id}``.
4. The assets extension HTTP handler calls ``receive_upload(ticket_id, stream)``
   which streams bytes to ``STAGING_DIR/{ticket_id}/{filename}``, then calls
   ``complete_upload(ticket_id)`` synchronously:
   - moves the file to ``ASSET_DIR/{catalog_id}/{filename}``
   - registers the asset via ``AssetsProtocol.create_asset(owned_by="local")``
   - marks the ticket COMPLETED
5. Client calls ``GET /assets/catalogs/{id}/upload/{ticket_id}/status`` → COMPLETED.

Configuration (environment variables)
--------------------------------------
``LOCAL_UPLOAD_STAGING_DIR``
    Temporary directory for in-flight uploads.
    Default: ``/tmp/dynastore/uploads/staging``
``LOCAL_ASSET_DIR``
    Permanent storage root for registered local assets.
    Default: ``/tmp/dynastore/assets``
"""

import logging
import os
import shutil
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, AsyncIterator, Dict, Optional

from dynastore.modules.local.local_upload_store import (
    LocalUploadTicketStore,
    ensure_local_upload_tickets_table,
)
from dynastore.modules.protocols import ModuleProtocol

logger = logging.getLogger(__name__)

_TICKET_TTL_HOURS = 1


class LocalUploadModule(ModuleProtocol):
    """
    Implements ``AssetUploadProtocol`` for on-premise / local-disk deployments.

    The client delivers the file to a server-side endpoint (``POST
    /local-upload/{ticket_id}``); asset registration is synchronous — by the time
    the POST response is returned the asset exists in the catalog and the ticket
    status is already ``COMPLETED``.

    An instance of this class is registered as a plugin during ``lifespan`` so
    it appears in ``AssetRoutingConfig.operations[UPLOAD]`` (auto-augmented
    from discoverable ``AssetUploadProtocol`` impls). Operators select it for
    a given catalog by pinning ``"LocalUploadModule"`` in routing config.
    The upload endpoint is added directly to the FastAPI app during the same
    lifespan phase.
    """

    # AssetUploadProtocol surface
    driver_id: str = "local"
    supports_versioning: bool = False

    # Non-foundational: a missing python-multipart or path-write failure
    # should disable local upload but never abort the catalog.
    priority: int = 50

    def __init__(
        self,
        staging_dir: Optional[str] = None,
        asset_dir: Optional[str] = None,
    ) -> None:
        self._staging_root = Path(
            staging_dir
            or os.environ.get("LOCAL_UPLOAD_STAGING_DIR", "/tmp/dynastore/uploads/staging")
        )
        self._asset_root = Path(
            asset_dir
            or os.environ.get("LOCAL_ASSET_DIR", "/tmp/dynastore/assets")
        )

    # The ticket registry is PostgreSQL-backed (see ``local_upload_store``).
    # Tickets MUST survive across uvicorn workers and Cloud Run replicas: an
    # ``initiate_upload`` handled by one process stamps a ticket the
    # ``POST /local-upload/{ticket_id}`` receive — which may land on a
    # different process — has to resolve. A process-local dict 404s those
    # cross-replica receives. The store is keyed by ``ticket_id`` in the
    # global ``tasks`` schema so any process resolves any ticket.
    @property
    def _tickets(self) -> LocalUploadTicketStore:
        from dynastore.tools.protocol_helpers import get_engine

        engine = get_engine()
        if engine is None:
            raise RuntimeError(
                "LocalUploadModule: database engine unavailable; the upload "
                "ticket store requires PostgreSQL."
            )
        return LocalUploadTicketStore(engine)

    # -------------------------------------------------------------------------
    # Lifespan
    # -------------------------------------------------------------------------

    @asynccontextmanager
    async def lifespan(self, app_state: Any) -> AsyncIterator[None]:
        from dynastore.tools.discovery import register_plugin, unregister_plugin
        from dynastore.modules.local.asset_downloads import LocalAssetDownload

        self._staging_root.mkdir(parents=True, exist_ok=True)
        self._asset_root.mkdir(parents=True, exist_ok=True)
        logger.info(
            f"LocalUploadModule: staging={self._staging_root}, assets={self._asset_root}"
        )

        # Provision the shared ticket table. The DB module (priority 10) has
        # already installed the engine by the time this module (priority 50)
        # runs its lifespan. Table creation lives in a dedicated DDL helper —
        # never inlined here — and is idempotent (IF NOT EXISTS).
        from dynastore.tools.protocol_helpers import get_engine

        engine = get_engine()
        if engine is not None:
            try:
                await ensure_local_upload_tickets_table(engine)
            except Exception as exc:  # noqa: BLE001 — never abort the catalog
                logger.error(
                    "LocalUploadModule: failed to provision the upload ticket "
                    "table: %s. Local upload will be unavailable until the next "
                    "boot.", exc,
                )
        else:
            logger.warning(
                "LocalUploadModule: database engine unavailable at lifespan; "
                "skipping ticket-table provisioning (worker context?)."
            )

        # Register the local asset-download backend so the asset router
        # discovers it via get_protocols(AssetDownloadProtocol).
        self._download_process = LocalAssetDownload()
        register_plugin(self)
        register_plugin(self._download_process)
        try:
            yield
        finally:
            unregister_plugin(self._download_process)
            unregister_plugin(self)
            logger.info("LocalUploadModule: unregistered.")

    # -------------------------------------------------------------------------
    # Framework-free receive logic — called by the assets extension HTTP handler
    # -------------------------------------------------------------------------

    async def receive_upload(
        self,
        ticket_id: str,
        stream: AsyncIterator[bytes],
    ) -> Any:
        """Consume a byte stream and register the uploaded asset.

        The caller (the assets extension HTTP handler) provides an async byte
        iterator — typically obtained from ``UploadFile.read(chunk_size)`` — so
        this method remains framework-free and testable with any byte source.

        Returns an ``UploadStatusResponse`` with ``status=completed``.

        Raises:
            ValueError: ticket not found or expired (maps to HTTP 404 via the
                global exception registry's ``ValidationExceptionHandler``).
            RuntimeError: staging write failure or asset registration failure
                (maps to HTTP 500).
        """
        from dynastore.models.protocols import UploadStatusResponse, UploadStatus

        store = self._tickets
        ticket = await store.get(ticket_id)
        if not ticket:
            raise ValueError(
                f"Upload ticket '{ticket_id}' not found or expired."
            )

        ticket["status"] = UploadStatus.UPLOADING
        await store.update(ticket_id, {"status": UploadStatus.UPLOADING})

        # 1. Stream to staging
        staging_path = self._staging_root / ticket_id / ticket["filename"]
        staging_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            with staging_path.open("wb") as fh:
                async for chunk in stream:
                    if chunk:
                        fh.write(chunk)
        except Exception as exc:
            await store.update(
                ticket_id,
                {"status": UploadStatus.FAILED, "error": str(exc)},
            )
            logger.error(f"LocalUploadModule: write to staging failed: {exc}")
            raise RuntimeError(
                f"Failed to write upload to staging: {exc}"
            ) from exc

        # 2. Register asset (moves file + creates DB record)
        try:
            asset = await self._complete_upload(ticket_id, staging_path, ticket)
        except Exception as exc:
            await store.update(
                ticket_id,
                {"status": UploadStatus.FAILED, "error": str(exc)},
            )
            logger.error(f"LocalUploadModule: complete_upload failed: {exc}")
            raise RuntimeError(
                f"Asset registration failed: {exc}"
            ) from exc

        return UploadStatusResponse(
            ticket_id=ticket_id,
            status=UploadStatus.COMPLETED,
            asset_id=asset.asset_id,
        )

    # -------------------------------------------------------------------------
    # AssetUploadProtocol implementation
    # -------------------------------------------------------------------------

    def upload_available(self) -> bool:
        """AssetUploadProtocol upload-readiness signal.

        Local disk is always ready once the module is registered — staging /
        asset roots are created lazily during ``lifespan`` and on each receive.
        Always available so the upload router can fall through to it when an
        event-driven backend (e.g. GCS) reports its upload role unavailable.
        """
        return True

    async def initiate_upload(
        self,
        catalog_id: str,
        asset_def: Any,
        filename: str,
        content_type: Optional[str] = None,
        collection_id: Optional[str] = None,
        origin: Optional[str] = None,
    ) -> Any:
        """
        Reserves a staging slot and returns an ``UploadTicket`` directing the
        client to ``POST /local-upload/{ticket_id}``.

        Unlike GCS, no cloud storage is provisioned — the ticket is ready
        immediately. ``origin`` is accepted for protocol parity but ignored:
        the upload goes through the server itself, so no CORS stamping is
        needed.
        """
        from dynastore.models.protocols import UploadTicket

        ticket_id = str(uuid.uuid4())
        expires_at = datetime.now(timezone.utc) + timedelta(hours=_TICKET_TTL_HOURS)

        # Pre-create the staging directory so the receive endpoint can write
        staging_dir = self._staging_root / ticket_id
        staging_dir.mkdir(parents=True, exist_ok=True)

        from dynastore.models.protocols import UploadStatus

        await self._tickets.put(
            ticket_id,
            {
                "asset_id": asset_def.asset_id,
                "catalog_id": catalog_id,
                "collection_id": collection_id,
                "filename": filename,
                "content_type": content_type or "application/octet-stream",
                "asset_def": asset_def,
                "expires_at": expires_at,
                "status": UploadStatus.PENDING,
            },
        )

        logger.info(
            f"LocalUploadModule.initiate_upload: ticket '{ticket_id}' created "
            f"for asset '{asset_def.asset_id}' in catalog '{catalog_id}'."
        )

        headers: Dict[str, str] = {}
        if content_type:
            headers["Content-Type"] = content_type

        return UploadTicket(
            ticket_id=ticket_id,
            upload_url=f"/local-upload/{ticket_id}",
            method="POST",
            headers=headers,
            expires_at=expires_at,
            backend="local",
        )

    async def get_upload_status(
        self,
        ticket_id: str,
        catalog_id: str,
    ) -> Any:
        """
        Returns the current status of a local upload session.

        Because ``complete_upload`` is called synchronously inside the POST
        endpoint, the status is typically already ``COMPLETED`` by the time the
        client polls this endpoint.

        Raises:
            ValueError: ticket not found, expired, or belongs to a different
                catalog (maps to HTTP 404 via the global exception registry).
        """
        from dynastore.models.protocols import UploadStatus, UploadStatusResponse

        store = self._tickets
        ticket = await store.get(ticket_id)
        if not ticket:
            # ``get`` prunes an expired row before returning None, so missing
            # and expired both collapse to this single 404.
            raise ValueError(
                f"Upload ticket '{ticket_id}' not found or expired."
            )

        if ticket["catalog_id"] != catalog_id:
            raise ValueError(
                f"Upload ticket '{ticket_id}' not found in catalog '{catalog_id}'."
            )

        current_status = ticket.get("status", UploadStatus.PENDING)

        if current_status == UploadStatus.COMPLETED:
            asset_id = ticket.get("asset_id_result") or ticket["asset_id"]
            await store.delete(ticket_id)
            return UploadStatusResponse(
                ticket_id=ticket_id,
                status=UploadStatus.COMPLETED,
                asset_id=asset_id,
            )

        if current_status == UploadStatus.FAILED:
            return UploadStatusResponse(
                ticket_id=ticket_id,
                status=UploadStatus.FAILED,
                error=ticket.get("error", "Upload failed."),
            )

        return UploadStatusResponse(
            ticket_id=ticket_id,
            status=current_status,
        )

    # -------------------------------------------------------------------------
    # Internal — called synchronously from the receive endpoint
    # -------------------------------------------------------------------------

    async def _complete_upload(
        self,
        ticket_id: str,
        staging_path: Path,
        ticket: Dict[str, Any],
    ) -> Any:
        """
        Moves the staged file to permanent storage and registers the asset.

        Not part of ``AssetUploadProtocol`` — called by the upload endpoint
        immediately after streaming is complete.
        """
        from dynastore.tools.discovery import get_protocol
        from dynastore.models.protocols import AssetsProtocol, UploadStatus
        from dynastore.modules.catalog.asset_service import AssetCreate, AssetTypeEnum

        catalog_id: str = ticket["catalog_id"]
        collection_id: Optional[str] = ticket.get("collection_id")
        filename: str = ticket["filename"]
        asset_def = ticket["asset_def"]

        # Determine permanent path: {asset_root}/{catalog_id}/{collection or _catalog_tier}/{filename}
        # The on-disk segment for catalog-tier assets uses a stable internal
        # marker so two catalog-tier filenames don't collide; the marker
        # never reaches DTOs or the DB (which use NULL collection_id).
        scope = collection_id or "_catalog_tier"
        permanent_dir = self._asset_root / catalog_id / scope
        permanent_dir.mkdir(parents=True, exist_ok=True)
        permanent_path = permanent_dir / filename

        # If a file with the same name already exists, overwrite (idempotent re-upload)
        shutil.move(str(staging_path), str(permanent_path))

        # Clean up the empty staging subdirectory
        try:
            staging_path.parent.rmdir()
        except OSError:
            pass  # Non-empty or already gone — ignore

        uri = permanent_path.as_uri()  # file:///...

        assets = get_protocol(AssetsProtocol)
        if not assets:
            raise RuntimeError("AssetsProtocol not available for local asset registration.")

        try:
            size_bytes: Optional[int] = permanent_path.stat().st_size
        except OSError:
            size_bytes = None

        # The REST upload-create flow inserts a born-claimed PENDING row up
        # front (stamping ``metadata._upload.ticket_id``). Activate that row
        # in place rather than calling ``create_asset`` — a create would
        # collide with the pending row via the write-policy identity match,
        # echo the existing row, and leave it stuck status=pending / uri=NULL
        # (download then 404s). When no born-claimed row exists (policy path
        # skipped) ``finalize_pending_upload`` returns None and we fall back
        # to create_asset.
        asset = await assets.finalize_pending_upload(
            catalog_id=catalog_id,
            uri=uri,
            owned_by="local",
            ticket_id=ticket_id,
            asset_id=asset_def.asset_id,
            size_bytes=size_bytes,
            collection_id=collection_id,
        )

        if asset is None:
            asset_type = asset_def.asset_type if hasattr(asset_def, "asset_type") else AssetTypeEnum.ASSET
            metadata = dict(asset_def.metadata) if hasattr(asset_def, "metadata") and asset_def.metadata else {}

            asset_base = AssetCreate(
                asset_id=asset_def.asset_id,
                filename=filename,
                uri=uri,
                asset_type=asset_type,
                metadata=metadata,
                owned_by="local",
            )

            asset = await assets.create_asset(
                catalog_id=catalog_id,
                asset=asset_base,
                collection_id=collection_id,
            )

        ticket["status"] = UploadStatus.COMPLETED
        ticket["asset_id_result"] = asset.asset_id
        await self._tickets.update(
            ticket_id,
            {
                "status": UploadStatus.COMPLETED,
                "asset_id_result": asset.asset_id,
            },
        )
        logger.info(
            f"LocalUploadModule: asset '{asset.asset_id}' registered at {uri} "
            f"(catalog={catalog_id}, collection={collection_id})."
        )
        return asset
