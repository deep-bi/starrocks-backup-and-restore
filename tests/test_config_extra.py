from pathlib import Path

import yaml
import pytest

from starrocks_bbr.config import load_config


def test_should_error_when_yaml_is_empty_or_none(tmp_path: Path, monkeypatch):
    p = tmp_path / "empty.yaml"
    p.write_text("", encoding="utf-8")

    # Explicitly make safe_load return None to exercise the branch
    monkeypatch.setattr(yaml, "safe_load", lambda f: None)

    with pytest.raises(ValueError):
        load_config(p)
