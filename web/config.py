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
    databases: Dict[str, str] = field(default_factory=dict)


def _load_json_config(config_path: Path) -> Dict[str, object]:
    if not config_path.exists():
        return {}
    raw = config_path.read_text(encoding="utf-8")
    return json.loads(raw)


def load_config(path: Path | None = None) -> AppConfig:
    config_path = path or Path(os.environ.get("FUND_FLOW_CONFIG", DEFAULT_CONFIG_PATH))
    config_path = config_path.resolve()

    data = _load_json_config(config_path)

    host = str(data.get("host", "0.0.0.0"))
    port = int(data.get("port", 8800))
    default_db = str(data.get("default_db", "fund_flow"))

    db_map_raw = data.get("databases", {})
    databases: Dict[str, str] = {}
    if isinstance(db_map_raw, dict):
        for key, value in db_map_raw.items():
            if not isinstance(key, str) or not isinstance(value, str):
                continue
            value_str = value.strip()
            if not value_str.startswith(("mysql://", "mysql+pymysql://")):
                raise ValueError(f"数据库配置 `{key}` 必须是 mysql DSN，例如 mysql://user:pwd@host:3306/db")
            databases[key] = value_str

    if not databases:
        env_dsn = os.environ.get("MYSQL_DSN")
        if not env_dsn:
            raise ValueError("未配置任何 MySQL 数据源，请在 web/config.json 或环境变量 MYSQL_DSN 中提供 DSN")
        databases[default_db] = env_dsn

    if default_db not in databases:
        raise ValueError(f"default_db `{default_db}` 未在 databases 中配置")

    return AppConfig(host=host, port=port, default_db=default_db, databases=databases)


__all__ = ["AppConfig", "load_config"]
