"""MD5 and CRC32C in the base64 form GCS reports them."""

import base64
import hashlib


def _crc32c_table() -> list[int]:
    table: list[int] = []
    for byte in range(256):
        crc = byte
        for _ in range(8):
            crc = (crc >> 1) ^ 0x82F63B78 if crc & 1 else crc >> 1
        table.append(crc)
    return table


_TABLE = _crc32c_table()


def crc32c_b64(data: bytes) -> str:
    crc = 0xFFFFFFFF
    for byte in data:
        crc = _TABLE[(crc ^ byte) & 0xFF] ^ (crc >> 8)
    return base64.b64encode((crc ^ 0xFFFFFFFF).to_bytes(4, "big")).decode()


def md5_b64(data: bytes) -> str:
    return base64.b64encode(hashlib.md5(data, usedforsecurity=False).digest()).decode()
