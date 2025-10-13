from pathlib import Path

import pytest
import textwrap

from starrocks_br.config import load_config, DatabaseConfig


def write_yaml(tmp_path: Path, content: str) -> Path:
    p = tmp_path / "config.yaml"
    p.write_text(textwrap.dedent(content).strip() + "\n", encoding="utf-8")
    return p


def test_load_config_success(tmp_path: Path):
    cfg_path = write_yaml(
        tmp_path,
        """
        host: localhost
        port: 9030
        user: root
        password: secret
        database: ops
        tables:
          - db1.tableA
          - db2.tableB
        repository: my_repo
        """,
    )

    cfg = load_config(cfg_path)
    assert isinstance(cfg, DatabaseConfig)
    assert cfg.host == "localhost"
    assert cfg.port == 9030
    assert cfg.tables == ["db1.tableA", "db2.tableB"]
    assert cfg.repository == "my_repo"


def test_load_config_missing_keys(tmp_path: Path):
    cfg_path = write_yaml(
        tmp_path,
        """
        host: localhost
        port: 9030
        user: root
        password: secret
        # database missing
        tables: []
        repository: my_repo
        """,
    )

    with pytest.raises(ValueError) as err:
        load_config(cfg_path)
        
    assert "error: missing required configuration keys: database" in err.value.args[0]


def test_load_config_invalid_tables_type(tmp_path: Path):
    cfg_path = write_yaml(
        tmp_path,
        """
        host: localhost
        port: 9030
        user: root
        password: secret
        database: ops
        tables: wrong
        repository: my_repo
        """,
    )

    with pytest.raises(ValueError):
        load_config(cfg_path)
