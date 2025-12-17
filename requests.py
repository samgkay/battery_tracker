"""Lightweight fallback implementation of a subset of the requests API.

This is *not* a full replacement for the real ``requests`` package but
provides enough functionality for simple GET requests with timeouts. It
exists because the execution environment may not allow installing third
party packages from PyPI.
"""
from __future__ import annotations

import json
import ssl
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, Dict, Optional


class RequestException(Exception):
    """Generic request exception mirroring ``requests`` behaviour."""


class Response:
    def __init__(self, status_code: int, headers: Dict[str, Any], content: bytes, url: str):
        self.status_code = status_code
        self.headers = headers
        self._content = content
        self.url = url

    @property
    def text(self) -> str:
        return self._content.decode("utf-8", errors="replace")

    def json(self) -> Any:
        return json.loads(self.text)

    @property
    def ok(self) -> bool:
        return 200 <= self.status_code < 400


def get(url: str, params: Optional[Dict[str, Any]] = None, timeout: Optional[float] = None) -> Response:
    query = urllib.parse.urlencode(params or {})
    full_url = f"{url}?{query}" if query else url

    try:
        with urllib.request.urlopen(full_url, timeout=timeout, context=ssl.create_default_context()) as resp:
            content = resp.read()
            return Response(resp.getcode(), dict(resp.getheaders()), content, full_url)
    except urllib.error.HTTPError as exc:  # pragma: no cover - best effort compatibility
        return Response(exc.code, dict(exc.headers or {}), exc.read() if exc.fp else b"", url)
    except urllib.error.URLError as exc:  # pragma: no cover
        raise RequestException(str(exc)) from exc


__all__ = ["RequestException", "Response", "get"]
