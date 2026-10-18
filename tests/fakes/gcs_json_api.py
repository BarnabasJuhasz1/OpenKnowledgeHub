"""The part of the GCS JSON API that gcloud-aio-storage uses, served on loopback by aiohttp."""

from __future__ import annotations

import asyncio
import secrets
from collections.abc import AsyncGenerator, Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass
from urllib.parse import unquote

from aiohttp import web
from aiohttp.test_utils import TestServer

from tests.fakes.checksums import crc32c_b64, md5_b64


@asynccontextmanager
async def running(fake: FakeGcs) -> AsyncGenerator[FakeGcs, None]:
    """Serve ``fake`` on loopback for the duration of the block."""
    server = TestServer(fake.app)
    await server.start_server()
    fake.base_url = str(server.make_url("")).rstrip("/")
    try:
        yield fake
    finally:
        await server.close()


@dataclass
class _Blob:
    data: bytes
    generation: int
    content_type: str


@dataclass
class _Session:
    bucket: str
    name: str
    length: int
    content_type: str
    create_only: bool


class FakeGcs:
    def __init__(self) -> None:
        self.objects: dict[tuple[str, str], _Blob] = {}
        self.sessions: dict[str, _Session] = {}
        self.received: list[int] = []
        self.put_statuses: list[int] = []
        self.before_put: Callable[[], None] | None = None
        self.after_media: Callable[[], None] | None = None
        self.before_media: Callable[[tuple[str, str]], None] | None = None
        self.media_stall_seconds = 0.0
        self.metadata_reply: bytes | None = None
        self.media_headers: list[dict[str, str]] = []
        self.media_preconditions: list[tuple[str, str | None]] = []
        self.base_url = ""
        self._generation = 1_000
        self.app = web.Application()
        self.app.router.add_post("/upload/storage/v1/b/{bucket}/o", self._upload)
        self.app.router.add_put("/upload/storage/v1/b/{bucket}/o", self._put)
        self.app.router.add_get("/storage/v1/b/{bucket}/o/{name:.+}", self._get)

    def put_object(self, bucket: str, name: str, data: bytes) -> int:
        self._generation += 1
        self.objects[(bucket, name)] = _Blob(data, self._generation, "application/octet-stream")
        return self._generation

    def data(self, bucket: str, name: str) -> bytes:
        return self.objects[(bucket, name)].data

    async def _upload(self, request: web.Request) -> web.Response:
        bucket = request.match_info["bucket"]
        create_only = request.query.get("ifGenerationMatch") == "0"
        if request.query.get("uploadType") == "media":
            name = request.query["name"]
            data = await request.read()
            if create_only and (bucket, name) in self.objects:
                return _error(412)
            return web.json_response(self._store(bucket, name, data, request.content_type))
        if request.query.get("uploadType") != "resumable":
            return _error(400)
        name = (await request.json())["name"]
        if create_only and (bucket, name) in self.objects:
            return _error(412)
        session = secrets.token_hex(8)
        self.sessions[session] = _Session(
            bucket,
            name,
            int(request.headers["X-Upload-Content-Length"]),
            request.headers.get("X-Upload-Content-Type", ""),
            create_only,
        )
        location = (
            f"{self.base_url}/upload/storage/v1/b/{bucket}/o"
            f"?uploadType=resumable&upload_id=SESSION{session}"
        )
        return web.Response(headers={"Location": location})

    async def _put(self, request: web.Request) -> web.Response:
        session = self.sessions.get(request.query.get("upload_id", "").removeprefix("SESSION"))
        if session is None:
            return _error(404)
        received = bytearray()
        try:
            while chunk := await request.content.read(64 * 1024):
                received += chunk
        except Exception:  # the client went away mid-body, so nothing is finalized
            self.received.append(len(received))
            raise
        self.received.append(len(received))
        if len(received) != session.length:
            return _error(400)
        if self.put_statuses:
            return _error(self.put_statuses.pop(0))
        if self.before_put is not None:
            self.before_put()
        if session.create_only and (session.bucket, session.name) in self.objects:
            return _error(412)
        return web.json_response(
            self._store(session.bucket, session.name, bytes(received), session.content_type)
        )

    async def _get(self, request: web.Request) -> web.StreamResponse:
        key = (request.match_info["bucket"], unquote(request.match_info["name"]))
        blob = self.objects.get(key)
        if blob is None:
            return _error(404)
        if request.query.get("alt") == "media":
            if self.before_media is not None:
                self.before_media(key)
            blob = self.objects.get(key)
            if blob is None:
                return _error(404)
            generation = request.query.get("ifGenerationMatch")
            self.media_preconditions.append((key[1], generation))
            if generation is not None and generation != str(blob.generation):
                return _error(412)
            self.media_headers.append(dict(request.headers))
            if self.media_stall_seconds:
                return await self._stall(request, blob.data)
            response = web.Response(body=blob.data, content_type="application/octet-stream")
            if self.after_media is not None:
                self.after_media()
            return response
        if self.metadata_reply is not None:
            return web.Response(body=self.metadata_reply, content_type="application/json")
        return web.json_response(self._metadata(key, blob))

    async def _stall(self, request: web.Request, data: bytes) -> web.StreamResponse:
        """Send half the body, then go quiet, as a stuck connection does."""
        response = web.StreamResponse(headers={"content-length": str(len(data))})
        await response.prepare(request)
        await response.write(data[: len(data) // 2])
        await asyncio.sleep(self.media_stall_seconds)
        return response

    def _store(self, bucket: str, name: str, data: bytes, content_type: str) -> dict[str, str]:
        self._generation += 1
        blob = _Blob(data, self._generation, content_type)
        self.objects[(bucket, name)] = blob
        return self._metadata((bucket, name), blob)

    def _metadata(self, key: tuple[str, str], blob: _Blob) -> dict[str, str]:
        return {
            "kind": "storage#object",
            "bucket": key[0],
            "name": key[1],
            "generation": str(blob.generation),
            "size": str(len(blob.data)),
            "md5Hash": md5_b64(blob.data),
            "crc32c": crc32c_b64(blob.data),
            "contentType": blob.content_type,
        }


def _error(status: int) -> web.Response:
    return web.json_response({"error": {"code": status, "message": "fake"}}, status=status)
