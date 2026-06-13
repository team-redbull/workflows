"""Common logging setup for worker processes.

Gives a compact, timestamped, single-line format and quiets noisy libraries
(httpx logs every request at INFO; the Temporal SDK's activity logger dumps a
large metadata dict into every message by default).
"""

from __future__ import annotations

import logging
import re

# Matches the " ({'activity_id': ..., ...})" suffix that temporalio's
# activity/workflow LoggerAdapter appends to every message.
_TRAILING_INFO_DICT = re.compile(r" \(\{.*\}\)$")


class _ConciseActivityFilter(logging.Filter):
    """Drop the bulky Temporal activity-info dict from log records.

    temporalio's activity.logger passes activity context as `extra`, which the
    default formatter renders as a huge inline dict. We keep it accessible via
    record attributes but stop it from being appended to the message text by
    pre-formatting a short prefix instead.
    """

    def filter(self, record: logging.LogRecord) -> bool:
        info = getattr(record, "activity_info", None) or getattr(
            record, "temporal_activity", None
        )
        msg = record.getMessage()
        msg = _TRAILING_INFO_DICT.sub("", msg)
        if info:
            msg = (
                f"[{info.get('activity_type')} "
                f"wf={info.get('workflow_id')} attempt={info.get('attempt')}] {msg}"
            )
        record.msg = msg
        record.args = ()
        return True


def configure_logging(level: int = logging.INFO) -> None:
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )
    # httpx/httpcore log every single request/response at INFO — too chatty.
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)

    for logger_name in ("temporalio.activity", "temporalio.workflow"):
        logging.getLogger(logger_name).addFilter(_ConciseActivityFilter())
