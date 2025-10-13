from click.testing import CliRunner

from starrocks_br.cli import cli, main


def test_should_print_help_and_exit_zero_when_no_args():
    """main([]) should print help and exit 0 to be CLI-friendly in library mode."""
    assert main([]) == 0


def test_should_print_only_headers_when_history_is_empty(monkeypatch, mocker):
    """list command prints headers even when there are no rows (README §3.d)."""
    runner = CliRunner()
    db_cls = mocker.patch("starrocks_br.cli.Database")
    db = db_cls.return_value
    db.query.return_value = []

    from tests.utils import write_cfg
    from pathlib import Path
    import tempfile

    with tempfile.TemporaryDirectory() as td:
        cfg = write_cfg(Path(td))
        result = runner.invoke(cli, ["list", "--config", str(cfg)])
        assert result.exit_code == 0
        assert "no backup history found" == result.output.strip()
