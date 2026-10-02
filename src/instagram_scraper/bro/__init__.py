"""getbro transport layer: SDK client, command builders, managed session.

Every wire call goes through the official ``bro-api-sdk`` package; the
scraper has no HTTP client of its own.
"""

from __future__ import annotations

from typing import Any

from .client import BroClient, fetch_offloaded, last_data, step_data
from .sdk_client import SDK_VERSION, SdkBroClient
from .session import BroSession
from . import commands


def make_client(api_key: str, **kwargs: Any) -> SdkBroClient:
    """The getbro client: ``bro-api-sdk`` underneath, our recovery loops on
    top. ``kwargs`` are :class:`SdkBroClient` options (``base_url``,
    ``timeout``, ``dispatch_timeout``, ``create_timeout``)."""
    return SdkBroClient(api_key, **kwargs)


__all__ = ["BroClient", "SdkBroClient", "BroSession", "commands", "make_client",
           "step_data", "last_data", "fetch_offloaded", "SDK_VERSION"]
