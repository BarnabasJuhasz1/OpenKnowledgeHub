"""Google Cloud Storage. The only package that imports ``google.cloud``."""

from openknowledge_adapters.gcs.objects import (
    Client,
    ObjectDigest,
    SourceError,
    client,
    create_object,
    digest_object,
    is_transient,
    object_exists,
    stream_url_to_object,
)

__all__ = [
    "Client",
    "ObjectDigest",
    "SourceError",
    "client",
    "create_object",
    "digest_object",
    "is_transient",
    "object_exists",
    "stream_url_to_object",
]
