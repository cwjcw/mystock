"""Utility helpers for loading Flask API configuration."""
from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict

DEFAULT_CONFIG_PATH = Path(__file__).resolve().with_name("config.json")


@dataclass
class AppConfig:
    host: str = "0.0.0.0"
    port: int = 8800
    default_db: str = "fund_flow"
    databases: Dict[str, Path] = field(default_factory=dict)


def _expand_path(value: str) -> Path:
    expanded = os.path.expandvars(os.path.expanduser(value))
    path = Path(expanded).resolve()
    return path


def load_config(path: Path | None = None) -> AppConfig:
    config_path = path or Path(os.environ.get("FUND_FLOW_CONFIG", DEFAULT_CONFIG_PATH))
    config_path = config_path.resolve()

    data: Dict[str, object] = {}
    if config_path.exists():
        raw = config_path.read_text(encoding="utf-8")
        data = json.loads(raw)

    host = str(data.get("host", "0.0.0.0"))
    port = int(data.get("port", 8800))
    default_db = str(data.get("default_db", "fund_flow"))

    db_map_raw = data.get("databases", {})
    databases: Dict[str, Path] = {}
    if isinstance(db_map_raw, dict):
        for key, value in db_map_raw.items():
            if not isinstance(key, str) or not isinstance(value, str):
                continue
            databases[key] = _expand_path(value)

    if default_db not in databases:
        # fall back to default stocks database if nothing provided
        default_path = Path(__file__).resolve().parents[1] / "data" / "stocks.db"
        databases.setdefault("fund_flow", default_path)
        default_db = "fund_flow"

    return AppConfig(host=host, port=port, default_db=default_db, databases=databases)


__all__ = ["AppConfig", "load_config"]
