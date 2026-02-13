#    Copyright 2025 FAO
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
Storage-related protocol definitions.
"""

from typing import Protocol, Optional, Any, Dict, runtime_checkable

@runtime_checkable
class StorageProtocol(Protocol):
    """
    Protocol for storage operations (e.g., GCS, S3), enabling decoupled
    access to buckets and files.
    """
    
    async def get_bucket_name_for_catalog(self, catalog_id: str) -> Optional[str]:
        """Returns the bucket name associated with a catalog."""
        ...

    async def get_or_create_bucket_for_catalog(self, catalog_id: str, conn: Optional[Any] = None) -> Optional[str]:
        """Retrieves the bucket name for a catalog, creating it if it doesn't exist."""
        ...

    async def get_catalog_storage_path(self, catalog_id: str) -> Optional[str]:
        """Returns the storage path (e.g., gs://...) for a catalog."""
        ...

    async def get_collection_storage_path(self, catalog_id: str, collection_id: str) -> Optional[str]:
        """Returns the storage path (e.g., gs://...) for a collection."""
        ...

    async def wait_for_bucket_ready(self, bucket_name: str, timeout_seconds: int = 30, interval_seconds: float = 1.0) -> bool:
        """Actively waits until the given bucket reports as existing."""
        ...

    async def prepare_upload_target(self, catalog_id: str, collection_id: Optional[str] = None) -> None:
        """Ensures that the target catalog and collection exist (JIT creation) before an upload."""
        ...

    async def upload_file(self, source_path: str, target_path: str, content_type: Optional[str] = None) -> str:
        """Uploads a local file to storage."""
        ...

    async def download_file(self, source_path: str, target_path: str) -> None:
        """Downloads a file from storage to local."""
        ...

    async def file_exists(self, path: str) -> bool:
        """Checks if a file exists in storage."""
        ...

    async def delete_file(self, path: str) -> None:
        """Deletes a file from storage."""
        ...
    async def upload_file_content(self, target_path: str, content: bytes, content_type: Optional[str] = None) -> str:
        """Uploads content (bytes) directly to storage."""
        ...

    async def apply_storage_config(self, catalog_id: str, config: Any) -> None:
        """Applies storage-related configuration changes to the live resource (e.g., CORS, Lifecycle)."""
        ...
