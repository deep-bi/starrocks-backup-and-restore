import logging
from unittest.mock import patch

from src.starrocks_br import logger


class TestLogger:
    """Test suite for the centralized logger module."""

    def test_logger_functions_call_correct_log_levels_with_symbols(self):
        """Test that each logger function calls the correct log level with proper symbols."""
        logger.setup_logging(level=logging.DEBUG)
        log = logger._get_logger()

        with patch.object(log, "info") as mock_info:
            logger.info("test message")
            mock_info.assert_called_once_with("test message")

        with patch.object(log, "info") as mock_info:
            logger.success("operation done")
            mock_info.assert_called_once_with("✓ operation done")

        with patch.object(log, "warning") as mock_warning:
            logger.warning("be careful")
            mock_warning.assert_called_once_with("⚠ be careful")

        with patch.object(log, "error") as mock_error:
            logger.error("something wrong")
            mock_error.assert_called_once_with("Error: something wrong")

        with patch.object(log, "critical") as mock_critical:
            logger.critical("system failure")
            mock_critical.assert_called_once_with("❌ CRITICAL: system failure")

        with patch.object(log, "info") as mock_info:
            logger.progress("processing")
            mock_info.assert_called_once_with("⏳ processing")

        with patch.object(log, "warning") as mock_warning:
            logger.tip("try this")
            mock_warning.assert_called_once_with("💡 try this")

        with patch.object(log, "debug") as mock_debug:
            logger.debug("debug info")
            mock_debug.assert_called_once_with("debug info")

    def test_setup_logging_with_info_level(self):
        """Test that setup_logging configures logger with INFO level and simple formatter."""
        logger.setup_logging(level=logging.INFO)

        log = logger._get_logger()
        assert log.level == logging.INFO
        assert len(log.handlers) == 1
        assert isinstance(log.handlers[0], logging.StreamHandler)

        formatter = log.handlers[0].formatter
        assert formatter._fmt == "%(message)s"

    def test_setup_logging_with_debug_level(self):
        """Test that setup_logging configures logger with DEBUG level and detailed formatter."""
        logger.setup_logging(level=logging.DEBUG)

        log = logger._get_logger()
        assert log.level == logging.DEBUG
        assert len(log.handlers) == 1

        formatter = log.handlers[0].formatter
        assert "%(asctime)s" in formatter._fmt
        assert "%(levelname)s" in formatter._fmt
        assert "%(name)s" in formatter._fmt

    def test_setup_logging_clears_existing_handlers(self):
        """Test that setup_logging clears existing handlers before adding new ones."""
        logger.setup_logging(level=logging.INFO)
        logger.setup_logging(level=logging.DEBUG)

        log = logger._get_logger()
        assert len(log.handlers) == 1
