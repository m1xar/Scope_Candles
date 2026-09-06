from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv
from pydantic_settings import BaseSettings

_ENV_PREFIX = "MT5_CANDLES_"


class Settings(BaseSettings):
    clickhouse_host: str = "localhost"
    clickhouse_port: int = 8123
    clickhouse_user: str = "default"
    clickhouse_password: str = ""
    clickhouse_database: str = "candles"

    redis_url: str = "redis://localhost:6379/0"

    mt5_login: int = 0
    mt5_password: str = ""
    mt5_server: str = ""

    candles_terminal_path: str = ""
    price_terminal_path: str = ""
    terminal_portable: bool = True
    terminal_init_timeout_ms: int = 60000
    terminal_login_timeout_ms: int = 30000

    backfill_interval_minutes: int = 15
    backfill_chunk_days: int = 30
    backfill_start_date: str = ""
    symbol_history_retries: int = 3
    prune_cache: bool = False

    tick_poll_ms: int = 300
    price_ttl_seconds: int = 300

    symbol_suffixes: str = ""
    symbol_include_path_prefixes: str = ""

    max_rows: int = 50000
    freshness_lag_minutes: int = 15

    api_token: str = ""
    api_host: str = "0.0.0.0"
    api_port: int = 8040
    cors_allow_origin: str = "*"

    worker_start_timeout_seconds: float = 180.0
    worker_heartbeat_timeout_seconds: float = 900.0

    log_level: str = "INFO"
    log_json: bool = True

    @property
    def suffixes(self) -> list[str]:
        return [part.strip() for part in self.symbol_suffixes.split(",") if part.strip()]

    @property
    def path_prefixes(self) -> list[str]:
        return [part.strip() for part in self.symbol_include_path_prefixes.split(",") if part.strip()]

    @property
    def terminals(self) -> dict[str, str]:
        return {"candles": self.candles_terminal_path, "price": self.price_terminal_path}

    def problems(self) -> list[str]:
        issues: list[str] = []
        for role, path in self.terminals.items():
            if not path:
                issues.append(f"{_ENV_PREFIX}{role.upper()}_TERMINAL_PATH is empty; the {role} worker cannot start")
            elif not Path(path).is_file():
                issues.append(f"terminal not found for {role}: {path}")
        if self.candles_terminal_path and self.candles_terminal_path == self.price_terminal_path:
            issues.append("both workers point at the same terminal install; each needs its own")
        if not self.mt5_login or not self.mt5_server:
            issues.append("MT5 credentials are incomplete; no symbol data can be read")
        if not self.api_token:
            issues.append(f"{_ENV_PREFIX}API_TOKEN is empty, so every route is unauthenticated")
        return issues


def _load_settings() -> Settings:
    load_dotenv()
    values: dict[str, object] = {}
    for field in Settings.model_fields:
        for name in (f"{_ENV_PREFIX}{field.upper()}", field.upper()):
            if name in os.environ:
                values[field] = os.environ[name]
                break
    return Settings(**values)


settings = _load_settings()
