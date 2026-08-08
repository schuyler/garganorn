"""Failing tests for EXPORT-11: Remove print() statements from production code.

EXPORT-11: Debug print statements in production code should be replaced with
logging calls or removed. This module contains tests that FAIL against the
current code and PASS after the fix is applied.

Affected locations:
- garganorn/server.py:15,27,25,56
- garganorn/database.py:143,255
"""

import ast
import pathlib
import pytest
import logging
from io import StringIO
from unittest.mock import patch, MagicMock

from garganorn.server import load_lexicons
from garganorn.database import Database


REPO_ROOT = pathlib.Path(__file__).parent.parent


class TestExport11NoPrintCalls:
    """Red-phase tests for EXPORT-11: No print() calls in production modules.

    Each test FAILES against the current code and PASSES after print() statements
    are replaced with logging calls or removed.
    """

    def test_server_py_no_print_calls(self):
        """server.py must not contain any print() calls.

        Currently contains:
        - Line 15: print("Warning: No lexicon directory found")
        - Line 27: print(f"Error: Failed to parse {file_path.name} as JSON")

        After fix, these should be replaced with logging.warning() and
        logging.error() respectively. FAILS until all print() calls are removed.
        """
        server_path = REPO_ROOT / "garganorn" / "server.py"
        source = server_path.read_text()

        # Parse the source code to find actual print() calls (not in comments/strings)
        tree = ast.parse(source)

        print_calls = []
        for node in ast.walk(tree):
            if isinstance(node, ast.Call):
                # Check if it's a print() call
                if isinstance(node.func, ast.Name) and node.func.id == "print":
                    # Get the line number and source line for context
                    line = node.lineno
                    source_lines = source.split("\n")
                    source_line = source_lines[line - 1].strip()
                    print_calls.append((line, source_line))

        assert len(print_calls) == 0, (
            f"server.py contains {len(print_calls)} print() call(s). "
            "All print() calls must be replaced with logging. "
            f"Found at lines: {[line for line, _ in print_calls]}"
        )

    def test_database_py_no_print_calls(self):
        """database.py must not contain any print() calls.

        Currently contains:
        - Line 143: print(f"Warning: Could not remove temp directory {self.temp_dir}: {e}")
        - Line 255: print(f"Searching with params: {params}")

        After fix, these should be replaced with log.warning() and log.debug()
        respectively. FAILS until all print() calls are removed.
        """
        database_path = REPO_ROOT / "garganorn" / "database.py"
        source = database_path.read_text()

        # Parse the source code to find actual print() calls (not in comments/strings)
        tree = ast.parse(source)

        print_calls = []
        for node in ast.walk(tree):
            if isinstance(node, ast.Call):
                # Check if it's a print() call
                if isinstance(node.func, ast.Name) and node.func.id == "print":
                    # Skip the __main__ block (lines 1458-1473) which is test code
                    if node.lineno < 1450:  # Allow print in __main__ block
                        line = node.lineno
                        source_lines = source.split("\n")
                        source_line = source_lines[line - 1].strip()
                        print_calls.append((line, source_line))

        assert len(print_calls) == 0, (
            f"database.py contains {len(print_calls)} print() call(s) in production code. "
            "All print() calls must be replaced with logging. "
            f"Found at lines: {[line for line, _ in print_calls]}"
        )

    def test_server_py_no_commented_print_calls(self):
        """server.py must not contain commented-out print() statements.

        Currently contains:
        - Line 25: #print(f"Loaded lexicon: {lexicon_data['id']} from {file_path.name}")
        - Line 56: #print(f"Registering {name} to {method}")

        These should be removed entirely. FAILS until all commented print() lines are removed.
        """
        server_path = REPO_ROOT / "garganorn" / "server.py"
        source = server_path.read_text()

        commented_prints = []
        for i, line in enumerate(source.split("\n"), start=1):
            stripped = line.strip()
            # Check for commented print statements (with optional whitespace after #)
            if stripped.startswith("#") and "print(" in stripped:
                commented_prints.append((i, stripped))

        assert len(commented_prints) == 0, (
            f"server.py contains {len(commented_prints)} commented-out print() statement(s). "
            "Commented-out print() calls should be removed entirely. "
            f"Found at lines: {[line for line, _ in commented_prints]}"
        )


class TestExport11LoggingUsage:
    """Red-phase tests for EXPORT-11: Verify logging is used instead of print().

    After the fix, server.py should use logging.warning() and logging.error(),
    and database.py should use log.warning() and log.debug() (the module logger
    is already defined as `log` on line 9).
    """

    def test_server_py_has_module_logger(self):
        """server.py must have a module-level logger defined.

        After the fix, server.py should have a line like:
        _log = logging.getLogger(__name__)

        FAILS until the module logger is added.
        """
        server_path = REPO_ROOT / "garganorn" / "server.py"
        source = server_path.read_text()

        # Check for module-level logger definition
        tree = ast.parse(source)

        has_logger = False
        for node in ast.iter_child_nodes(tree):
            if isinstance(node, ast.Assign):
                for target in node.targets:
                    if isinstance(target, ast.Name):
                        # Look for _log = logging.getLogger(...) or similar
                        if target.id == "_log" or target.id == "log":
                            if isinstance(node.value, ast.Call):
                                if isinstance(node.value.func, ast.Attribute):
                                    if (node.value.func.attr == "getLogger" and
                                        isinstance(node.value.func.value, ast.Name) and
                                        node.value.func.value.id == "logging"):
                                        has_logger = True
                                    elif (node.value.func.attr == "getLogger" and
                                          isinstance(node.value.func.value, ast.Attribute) and
                                          node.value.func.value.attr == "logging" and
                                          isinstance(node.value.func.value.value, ast.Name) and
                                          node.value.func.value.value.id == "logging"):
                                        has_logger = True

        assert has_logger, (
            "server.py does not define a module-level logger. "
            "Add '_log = logging.getLogger(__name__)' at module level."
        )

    def test_server_py_uses_logging_for_lexicon_warning(self):
        """server.py must use logging.warning() for missing lexicon directory.

        Currently line 15: print("Warning: No lexicon directory found")
        After fix: _log.warning("No lexicon directory found") or similar.

        FAILS until print() is replaced with logging.warning().
        """
        server_path = REPO_ROOT / "garganorn" / "server.py"
        source = server_path.read_text()

        # Check for logging.warning call or _log.warning call
        tree = ast.parse(source)

        has_logging_warning = False
        for node in ast.walk(tree):
            if isinstance(node, ast.Call):
                if isinstance(node.func, ast.Attribute):
                    # Look for logging.warning(...) or logger.warning(...)
                    if node.func.attr == "warning":
                        # Check the object being called on
                        if isinstance(node.func.value, ast.Name):
                            if node.func.value.id in ("_log", "log"):
                                has_logging_warning = True
                        elif isinstance(node.func.value, ast.Call):
                            # Could be logging.getLogger(...).warning(...)
                            if isinstance(node.func.value.func, ast.Attribute):
                                if node.func.value.func.attr == "getLogger":
                                    has_logging_warning = True

        # Also check the source for the specific message
        has_no_lexicon_msg = "No lexicon directory found" in source

        # After the fix, we should have both logging call AND the message (not in print)
        assert has_logging_warning and has_no_lexicon_msg, (
            "server.py does not use logging.warning() for missing lexicon directory. "
            "The current print() call should be replaced with logging.warning()."
        )

    def test_server_py_uses_logging_for_json_error(self):
        """server.py must use logging.error() for JSON parse failures.

        Currently line 27: print(f"Error: Failed to parse {file_path.name} as JSON")
        After fix: _log.error("Failed to parse %s as JSON", file_path.name) or similar.

        FAILS until print() is replaced with logging.error().
        """
        server_path = REPO_ROOT / "garganorn" / "server.py"
        source = server_path.read_text()

        # Check for logging.error call or _log.error call
        tree = ast.parse(source)

        has_logging_error = False
        for node in ast.walk(tree):
            if isinstance(node, ast.Call):
                if isinstance(node.func, ast.Attribute):
                    # Look for logging.error(...) or logger.error(...)
                    if node.func.attr == "error":
                        # Check the object being called on
                        if isinstance(node.func.value, ast.Name):
                            if node.func.value.id in ("_log", "log"):
                                has_logging_error = True
                        elif isinstance(node.func.value, ast.Call):
                            # Could be logging.getLogger(...).error(...)
                            if isinstance(node.func.value.func, ast.Attribute):
                                if node.func.value.func.attr == "getLogger":
                                    has_logging_error = True

        # Also check the source for the specific message (should use %s formatting, not f-string)
        has_json_error_msg = "Failed to parse" in source and "as JSON" in source

        assert has_logging_error and has_json_error_msg, (
            "server.py does not use logging.error() for JSON parse failures. "
            "The current print() call should be replaced with logging.error()."
        )

    def test_database_py_uses_log_for_temp_dir_warning(self):
        """database.py must use log.warning() for temp directory cleanup failure.

        Currently line 143: print(f"Warning: Could not remove temp directory {self.temp_dir}: {e}")
        After fix: log.warning("Could not remove temp directory %s: %s", self.temp_dir, e)

        The module logger 'log' is already defined on line 9. FAILS until print() is replaced.
        """
        database_path = REPO_ROOT / "garganorn" / "database.py"
        source = database_path.read_text()

        # The test should fail if there's a print() call with "temp directory" in the close() method
        lines = source.split("\n")
        has_print_with_temp_dir = False
        for i, line in enumerate(lines[130:150], start=131):
            stripped = line.strip()
            if stripped.startswith("print(") and "temp directory" in line.lower():
                has_print_with_temp_dir = True
                break

        assert not has_print_with_temp_dir, (
            "database.py still uses print() for temp directory cleanup failure in close() method. "
            "The print() call should be replaced with log.warning()."
        )


class TestExport11RuntimeBehavior:
    """Red-phase tests for EXPORT-11: Verify no stdout output at runtime.

    These tests verify that the production code paths do not write to stdout.
    They FAIL against the current code (which uses print()) and PASS after
    the fix (which uses logging).
    """

    def test_load_lexicons_no_stdout(self):
        """load_lexicons() must not print to stdout.

        Currently prints to stdout when lexicon directory is missing or JSON fails.
        After fix, should use logging instead. FAILS until print() is replaced.
        """
        # Mock the lexicon directory to not exist
        with patch("garganorn.server.files") as mock_files:
            mock_lexicon_path = MagicMock()
            mock_lexicon_path.is_dir.return_value = False
            mock_files.return_value.__truediv__.return_value = mock_lexicon_path

            # Capture stdout
            with patch("sys.stdout", new_callable=StringIO) as mock_stdout:
                lexicons = load_lexicons()

                # Should return empty list but NOT print anything
                assert lexicons == []
                output = mock_stdout.getvalue()
                assert output == "", (
                    f"load_lexicons() printed to stdout: '{output}'. "
                    "Should use logging.warning() instead of print()."
                )

    def test_database_close_no_stdout_on_error(self):
        """Database.close() must not print to stdout when cleanup fails.

        Currently prints a warning when temp directory removal fails.
        After fix, should use log.warning() instead. FAILS until print() is replaced.
        """
        # Create a mock database that will fail on cleanup
        db = Database(":memory:")

        # Create a temp directory that will fail to remove
        import tempfile
        temp_dir = tempfile.mkdtemp()
        db.temp_dir = temp_dir

        # Make rmtree raise an error
        with patch("shutil.rmtree") as mock_rmtree:
            mock_rmtree.side_effect = OSError("Permission denied")

            # Capture stdout
            with patch("sys.stdout", new_callable=StringIO) as mock_stdout:
                db.close()

                output = mock_stdout.getvalue()
                assert output == "", (
                    f"Database.close() printed to stdout on cleanup error: '{output}'. "
                    "Should use log.warning() instead of print()."
                )

            # Force GC of any Database objects before the mock exits,
            # so __del__ -> close() doesn't hit the mock unexpectedly.
            import gc
            gc.collect()

        # Cleanup
        import shutil
        shutil.rmtree(temp_dir, ignore_errors=True)
