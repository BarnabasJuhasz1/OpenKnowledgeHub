"""What the ingestion use cases need from outside."""

from __future__ import annotations

from typing import Literal, Protocol

from openknowledge_contracts.datasets import DatasetName, ReleaseId, ReleaseMetadata
from openknowledge_contracts.manifests import ObjectPath, ObjectRef
from openknowledge_core.ingestion.models import (
    ByteStream,
    CheckedStream,
    DatasetListing,
    LoadObject,
    LoadRecord,
    ShardLink,
    StoredObject,
    TableState,
)


class ReleaseCatalog(Protocol):
    async def resolve_release(self, selector: ReleaseId | Literal["latest"]) -> ReleaseMetadata:
        """Resolves ``latest`` to the explicit release id it stands for."""
        ...

    async def list_dataset(self, release_id: ReleaseId, dataset: DatasetName) -> DatasetListing:
        """The dataset's metadata and a freshly signed link per shard."""
        ...


class ShardDownloader(Protocol):
    async def open(self, link: ShardLink) -> ByteStream:
        """Raises LinkExpired when the link no longer works."""
        ...


class ArtifactStore(Protocol):
    async def stat(self, path: ObjectPath) -> StoredObject | None: ...

    async def create(
        self, path: ObjectPath, data: bytes, *, content_type: str
    ) -> StoredObject | None:
        """Create a small object, or return None and leave an existing one alone."""
        ...

    async def create_from_stream(
        self, path: ObjectPath, body: CheckedStream, *, content_type: str
    ) -> StoredObject | None:
        """Finalize the object only once ``body.chunks`` ended with ``body.completed`` set."""
        ...

    async def open_read(self, ref: ObjectRef) -> ByteStream:
        """Read exactly ``ref.generation``, or raise ObjectChanged if it is gone."""
        ...


class TableLoader(Protocol):
    async def table(self, dataset: DatasetName) -> TableState: ...

    async def loaded(
        self, dataset: DatasetName, source_object: ObjectPath
    ) -> LoadRecord | None: ...

    async def load(self, request: LoadObject) -> int:
        """Insert every record of the object and return the rows written."""
        ...

    async def record(self, record: LoadRecord) -> None: ...

    async def live_rows(self, dataset: DatasetName, release: ReleaseId) -> int: ...
