"""
chive_shared/schemas/run.py
────────────────────────────
Minimal shared enums extracted from chive's schemas/run.py.

Only contains definitions that the shared storage layer (models.py) depends on,
so that chive_shared has no dependency on chive's full schema tree.
"""

from enum import Enum


class EventStatus(str, Enum):
    OK = "ok"          # agent call completed without error
    ERROR = "error"    # agent call raised an exception
    SKIPPED = "skipped"  # pipeline step intentionally did not run (no trigger, dryrun, etc.)
