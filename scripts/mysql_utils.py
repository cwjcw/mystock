"""Helper utilities for connecting to MySQL using DSN strings."""
from __future__ import annotations

from typing import Any, Dict, Optional
from urllib.parse import parse_qs, unquote, urlparse

import pymysql


class MySQLConfigError(RuntimeError):
    """Raised when the MySQL DSN is invalid."""


def parse_mysql_dsn(dsn: str) -> Dict[str, Any]:
    parsed = urlparse(dsn)
    if parsed.scheme not in {"mysql", "mysql+pymysql"}:
        raise MySQLConfigError(f"Unsupported MySQL DSN: {dsn}")

    username = unquote(parsed.username or "")
    password = unquote(parsed.password or "")
    host = parsed.hostname or "127.0.0.1"
    port = parsed.port or 3306
    database = (parsed.path or "").lstrip("/")
    if not database:
        raise MySQLConfigError("MySQL DSN must include database name")

    query_params = parse_qs(parsed.query or "")
    charset = query_params.get("charset", ["utf8mb4"])[-1]

    connect_kwargs: Dict[str, Any] = {
        "host": host,
        "port": port,
        "user": username,
        "password": password,
        "database": database,
        "charset": charset,
    }

    def _maybe_float(value: str) -> Any:
        try:
            if "." in value:
                return float(value)
            return int(value)
        except ValueError:
            return value

    for key in ("connect_timeout", "read_timeout", "write_timeout"):
        if key in query_params:
            connect_kwargs[key] = _maybe_float(query_params[key][-1])

    ssl_params: Dict[str, Any] = {}
    if "ssl_ca" in query_params:
        ssl_params["ca"] = query_params["ssl_ca"][-1]
    if "ssl_cert" in query_params:
        ssl_params["cert"] = query_params["ssl_cert"][-1]
    if "ssl_key" in query_params:
        ssl_params["key"] = query_params["ssl_key"][-1]
    if ssl_params:
        connect_kwargs["ssl"] = ssl_params

    return connect_kwargs


def connect_mysql(
    dsn: str,
    *,
    autocommit: bool = True,
    cursorclass: Optional[type] = None,
) -> pymysql.connections.Connection:
    kwargs = parse_mysql_dsn(dsn)
    kwargs["autocommit"] = autocommit
    if cursorclass is not None:
        kwargs["cursorclass"] = cursorclass
    try:
        return pymysql.connect(**kwargs)
    except pymysql.MySQLError as exc:
        raise MySQLConfigError(f"Failed to connect MySQL: {exc}") from exc


__all__ = ["MySQLConfigError", "parse_mysql_dsn", "connect_mysql"]
