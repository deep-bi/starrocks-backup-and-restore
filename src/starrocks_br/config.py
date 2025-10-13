from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List

import yaml


@dataclass(frozen=True)
class DatabaseConfig:
    host: str
    port: int
    user: str
    password: str
    database: str
    tables: List[str]
    repository: str


REQUIRED_KEYS = {"host", "port", "user", "password", "database", "tables", "repository"}


def load_config(config_path: str | Path) -> DatabaseConfig:
    path = Path(config_path)
    if not path.exists():
        raise FileNotFoundError(
            f"error: config file not found at '{path}'\n"
            f"  help: ensure the file exists and the path is correct"
        )

    with path.open("r", encoding="utf-8") as f:
        raw: Dict[str, Any] = yaml.safe_load(f) or {}

    missing = REQUIRED_KEYS - set(raw.keys())
    if missing:
        missing_keys = ", ".join(sorted(missing))
        example_config = """
            Example config.yaml:

            host: "127.0.0.1"
            port: 9030
            user: "root"
            password: ""
            database: "teste"
            repository: "local_s3_repo"
            tables:
                - "teste.fact_sales"
                - "teste.dim_products"
        """
        raise ValueError(
            f"error: missing required configuration keys: {missing_keys}\n"
            f"  --> {path}\n"
            f"  help: ensure your config file has the following top-level keys:\n"
            f"        {', '.join(sorted(REQUIRED_KEYS))}\n"
            f"\n{example_config}\n"
            f"  note: do not use nested structures like 'starrocks.host' or 'backup.tables'\n"
            f"        all keys must be at the root level\n"
            f"\n"
            f"  then run: starrocks-br init --config config.yaml"
        )

    tables = raw["tables"] or []
    if not isinstance(tables, list):
        raise ValueError(
            f"error: 'tables' must be a list\n"
            f"  --> {path}\n"
            f"  help: format should be:\n"
            f"        tables:\n"
            f"          - \"database.table1\"\n"
            f"          - \"database.table2\""
        )
    
    if not all(isinstance(t, str) for t in tables):
        raise ValueError(
            f"error: all table entries must be strings in 'database.table' format\n"
            f"  --> {path}\n"
            f"  help: each table should be a simple string like:\n"
            f"        tables:\n"
            f"          - \"teste.fact_sales\"\n"
            f"          - \"teste.dim_products\"\n"
            f"  note: do not use objects with 'name' and 'type' fields"
        )

    return DatabaseConfig(
        host=str(raw["host"]),
        port=int(raw["port"]),
        user=str(raw["user"]),
        password=str(raw["password"]),
        database=str(raw["database"]),
        tables=tables,
        repository=str(raw["repository"]),
    )
