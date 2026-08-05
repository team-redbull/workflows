"""Shared FastAPI dependencies for the workflow-trigger API.

The Temporal client is created once in api.py's `lifespan` and stored on
`app.state`; routers reach it through this dependency instead of each opening
their own connection.
"""

from __future__ import annotations

from fastapi import Request
from temporalio.client import Client


def get_temporal_client(request: Request) -> Client:
    """The process-wide Temporal client established by api.py's lifespan."""
    return request.app.state.temporal_client
