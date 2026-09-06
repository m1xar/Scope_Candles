from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Optional


class WorkerState(str, Enum):
    starting = "starting"
    running = "running"
    failed = "failed"
    stopped = "stopped"


@dataclass(slots=True)
class Heartbeat:
    worker_id: str
    state: str = WorkerState.running.value
    at: Optional[datetime] = None
    detail: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class WorkerStatus:
    worker_id: str
    terminal_path: str
    state: WorkerState = WorkerState.starting
    pid: Optional[int] = None
    restarts: int = 0
    last_heartbeat_at: Optional[datetime] = None
    last_error: Optional[str] = None
    detail: dict[str, Any] = field(default_factory=dict)
