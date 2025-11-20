import logging
import threading

_logger = None
_logger_lock = threading.Lock()


def setup_logging(level: int = logging.INFO) -> None:
    global _logger
    _logger = logging.getLogger("starrocks_br")
    _logger.setLevel(level)

    if _logger.handlers:
        _logger.handlers.clear()

    handler = logging.StreamHandler()

    if level == logging.DEBUG:
        formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(name)s - %(message)s")
    else:
        formatter = logging.Formatter("%(message)s")

    handler.setFormatter(formatter)
    _logger.addHandler(handler)
    _logger.propagate = False


def _get_logger() -> logging.Logger:
    global _logger
    if _logger is None:
        with _logger_lock:
            if _logger is None:
                setup_logging()
    return _logger


def info(message: str) -> None:
    _get_logger().info(message)


def success(message: str) -> None:
    _get_logger().info(f"✓ {message}")


def warning(message: str) -> None:
    _get_logger().warning(f"⚠ {message}")


def error(message: str) -> None:
    _get_logger().error(f"Error: {message}")


def critical(message: str) -> None:
    _get_logger().critical(f"❌ CRITICAL: {message}")


def progress(message: str) -> None:
    _get_logger().info(f"⏳ {message}")


def tip(message: str) -> None:
    _get_logger().warning(f"💡 {message}")


def debug(message: str) -> None:
    _get_logger().debug(message)
