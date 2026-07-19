"""Common graceful-shutdown wiring for worker processes.

On SIGTERM (pod termination during a rollout/restart) or SIGINT (Ctrl-C),
signal the returned event instead of letting the default handler raise/kill
the process — callers await it to stop polling and let in-flight work finish
before exiting.
"""

from __future__ import annotations

import asyncio
import signal


def install_shutdown_handler() -> asyncio.Event:
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)
    return stop
