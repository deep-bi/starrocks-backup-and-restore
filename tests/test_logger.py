from unittest.mock import call, patch

import pytest

from src.starrocks_br import logger


class TestLogger:
    """Test suite for the centralized logger module."""

    @patch("src.starrocks_br.logger.click.echo")
    def test_should_log_info_message_when_calling_info(self, mock_echo):
        """Test that info() logs a message without any prefix."""
        message = "Test info message"
        logger.info(message)

        mock_echo.assert_called_once_with(message)

    @patch("src.starrocks_br.logger.click.echo")
    def test_should_log_success_message_with_checkmark_when_calling_success(self, mock_echo):
        """Test that success() logs a message with checkmark prefix."""
        message = "Operation completed"
        logger.success(message)

        mock_echo.assert_called_once_with(f"✓ {message}")

    @patch("src.starrocks_br.logger.click.echo")
    def test_should_log_warning_message_with_warning_symbol_when_calling_warning(self, mock_echo):
        """Test that warning() logs a message with warning symbol and stderr."""
        message = "This is a warning"
        logger.warning(message)

        mock_echo.assert_called_once_with(f"⚠ {message}", err=True)

    @patch("src.starrocks_br.logger.click.echo")
    def test_should_log_error_message_with_error_prefix_when_calling_error(self, mock_echo):
        """Test that error() logs a message with error prefix and stderr."""
        message = "Something went wrong"
        logger.error(message)

        mock_echo.assert_called_once_with(f"Error: {message}", err=True)

    @patch("src.starrocks_br.logger.click.echo")
    def test_should_log_critical_message_with_critical_symbol_when_calling_critical(
        self, mock_echo
    ):
        """Test that critical() logs a message with critical symbol and stderr."""
        message = "System failure"
        logger.critical(message)

        mock_echo.assert_called_once_with(f"❌ CRITICAL: {message}", err=True)

    @patch("src.starrocks_br.logger.click.echo")
    def test_should_log_progress_message_with_hourglass_when_calling_progress(self, mock_echo):
        """Test that progress() logs a message with hourglass symbol."""
        message = "Processing data"
        logger.progress(message)

        mock_echo.assert_called_once_with(f"⏳ {message}")

    @patch("src.starrocks_br.logger.click.echo")
    def test_should_log_tip_message_with_lightbulb_when_calling_tip(self, mock_echo):
        """Test that tip() logs a message with lightbulb symbol and stderr."""
        message = "Consider using this approach"
        logger.tip(message)

        mock_echo.assert_called_once_with(f"💡 {message}", err=True)

    @patch("src.starrocks_br.logger.click.echo")
    def test_should_handle_empty_string_messages_when_calling_any_logger_function(self, mock_echo):
        """Test that all logger functions handle empty string messages correctly."""
        empty_message = ""

        logger.info(empty_message)
        logger.success(empty_message)
        logger.warning(empty_message)
        logger.error(empty_message)
        logger.critical(empty_message)
        logger.progress(empty_message)
        logger.tip(empty_message)

        expected_calls = [
            call(""),
            call("✓ "),
            call("⚠ ", err=True),
            call("Error: ", err=True),
            call("❌ CRITICAL: ", err=True),
            call("⏳ "),
            call("💡 ", err=True),
        ]
        mock_echo.assert_has_calls(expected_calls)

    @patch("src.starrocks_br.logger.click.echo")
    def test_should_handle_none_messages_when_calling_any_logger_function(self, mock_echo):
        """Test that all logger functions handle None messages correctly."""
        none_message = None

        logger.info(none_message)
        logger.success(none_message)
        logger.warning(none_message)
        logger.error(none_message)
        logger.critical(none_message)
        logger.progress(none_message)
        logger.tip(none_message)

        expected_calls = [
            call(None),
            call("✓ None"),
            call("⚠ None", err=True),
            call("Error: None", err=True),
            call("❌ CRITICAL: None", err=True),
            call("⏳ None"),
            call("💡 None", err=True),
        ]
        mock_echo.assert_has_calls(expected_calls)

    @patch("src.starrocks_br.logger.click.echo")
    def test_should_handle_multiline_messages_when_calling_any_logger_function(self, mock_echo):
        """Test that all logger functions handle multiline messages correctly."""
        multiline_message = "Line 1\nLine 2\nLine 3"

        logger.info(multiline_message)
        logger.success(multiline_message)
        logger.warning(multiline_message)
        logger.error(multiline_message)
        logger.critical(multiline_message)
        logger.progress(multiline_message)
        logger.tip(multiline_message)

        expected_calls = [
            call("Line 1\nLine 2\nLine 3"),
            call("✓ Line 1\nLine 2\nLine 3"),
            call("⚠ Line 1\nLine 2\nLine 3", err=True),
            call("Error: Line 1\nLine 2\nLine 3", err=True),
            call("❌ CRITICAL: Line 1\nLine 2\nLine 3", err=True),
            call("⏳ Line 1\nLine 2\nLine 3"),
            call("💡 Line 1\nLine 2\nLine 3", err=True),
        ]
        mock_echo.assert_has_calls(expected_calls)

    @patch("src.starrocks_br.logger.click.echo")
    def test_should_handle_special_characters_when_calling_any_logger_function(self, mock_echo):
        """Test that all logger functions handle special characters correctly."""
        special_message = "Special chars: !@#$%^&*()_+-=[]{}|;':\",./<>?"

        logger.info(special_message)
        logger.success(special_message)
        logger.warning(special_message)
        logger.error(special_message)
        logger.critical(special_message)
        logger.progress(special_message)
        logger.tip(special_message)

        expected_calls = [
            call("Special chars: !@#$%^&*()_+-=[]{}|;':\",./<>?"),
            call("✓ Special chars: !@#$%^&*()_+-=[]{}|;':\",./<>?"),
            call("⚠ Special chars: !@#$%^&*()_+-=[]{}|;':\",./<>?", err=True),
            call("Error: Special chars: !@#$%^&*()_+-=[]{}|;':\",./<>?", err=True),
            call("❌ CRITICAL: Special chars: !@#$%^&*()_+-=[]{}|;':\",./<>?", err=True),
            call("⏳ Special chars: !@#$%^&*()_+-=[]{}|;':\",./<>?"),
            call("💡 Special chars: !@#$%^&*()_+-=[]{}|;':\",./<>?", err=True),
        ]
        mock_echo.assert_has_calls(expected_calls)

    @patch("src.starrocks_br.logger.click.echo")
    def test_should_use_stderr_for_error_warning_critical_and_tip_functions(self, mock_echo):
        """Test that error, warning, critical, and tip functions use stderr."""
        message = "Test message"

        logger.warning(message)
        logger.error(message)
        logger.critical(message)
        logger.tip(message)

        # Check that err=True is passed for these functions
        warning_call = mock_echo.call_args_list[0]
        error_call = mock_echo.call_args_list[1]
        critical_call = mock_echo.call_args_list[2]
        tip_call = mock_echo.call_args_list[3]

        assert warning_call[1]["err"] is True
        assert error_call[1]["err"] is True
        assert critical_call[1]["err"] is True
        assert tip_call[1]["err"] is True

    @patch("src.starrocks_br.logger.click.echo")
    def test_should_not_use_stderr_for_info_success_and_progress_functions(self, mock_echo):
        """Test that info, success, and progress functions do not use stderr."""
        message = "Test message"

        logger.info(message)
        logger.success(message)
        logger.progress(message)

        # Check that err is not passed (defaults to False) for these functions
        info_call = mock_echo.call_args_list[0]
        success_call = mock_echo.call_args_list[1]
        progress_call = mock_echo.call_args_list[2]

        assert "err" not in info_call[1] or info_call[1]["err"] is False
        assert "err" not in success_call[1] or success_call[1]["err"] is False
        assert "err" not in progress_call[1] or progress_call[1]["err"] is False

    @patch("src.starrocks_br.logger.click.echo")
    def test_should_propagate_click_echo_exception_when_logging_messages(self, mock_echo):
        """Test that logger functions propagate click.echo exceptions."""
        mock_echo.side_effect = Exception("Click echo failed")

        with pytest.raises(Exception, match="Click echo failed"):
            logger.info("Test message")

        with pytest.raises(Exception, match="Click echo failed"):
            logger.success("Test success")

        with pytest.raises(Exception, match="Click echo failed"):
            logger.warning("Test warning")

        with pytest.raises(Exception, match="Click echo failed"):
            logger.error("Test error")

        with pytest.raises(Exception, match="Click echo failed"):
            logger.critical("Test critical")

        with pytest.raises(Exception, match="Click echo failed"):
            logger.progress("Test progress")

        with pytest.raises(Exception, match="Click echo failed"):
            logger.tip("Test tip")

    def test_should_have_correct_function_signatures_for_all_logger_functions(self):
        """Test that all logger functions have the correct signatures."""
        import inspect

        # Check that all functions take exactly one parameter (message)
        functions = [
            logger.info,
            logger.success,
            logger.warning,
            logger.error,
            logger.critical,
            logger.progress,
            logger.tip,
        ]

        for func in functions:
            sig = inspect.signature(func)
            params = list(sig.parameters.keys())

            assert len(params) == 1, f"{func.__name__} should have exactly one parameter"
            assert params[0] == "message", f"{func.__name__} parameter should be named 'message'"

            # Check that the parameter has no default value
            param = sig.parameters["message"]
            assert param.default == inspect.Parameter.empty, (
                f"{func.__name__} message parameter should not have a default value"
            )

    def test_should_have_correct_return_types_for_all_logger_functions(self):
        """Test that all logger functions have the correct return type annotation."""
        import inspect

        functions = [
            logger.info,
            logger.success,
            logger.warning,
            logger.error,
            logger.critical,
            logger.progress,
            logger.tip,
        ]

        for func in functions:
            sig = inspect.signature(func)
            return_annotation = sig.return_annotation

            assert return_annotation is None, f"{func.__name__} should return None"
