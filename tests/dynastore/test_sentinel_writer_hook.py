"""Pin the ``pytest_runtest_logstart`` sentinel writer (#541).

CI's integration-tests job hits its 30-min timeout periodically; when it
does, pytest's stdout buffer is dropped and ``junitxml`` is never
finalised, leaving no on-disk evidence of which test was last running.
The sentinel hook in ``tests/conftest.py`` writes the current nodeid to
``$DYNASTORE_TEST_SENTINEL`` atomically before each test starts; the
workflow uploads that file as an artifact so a cancelled run still
identifies the offending test.

These tests run the hook directly (no subprocess) and assert:
- no-op when the env var is unset;
- atomic overwrite (each call replaces the previous nodeid);
- writes to ``<path>.tmp`` then ``os.replace`` (verified by absence of
  the ``.tmp`` file post-call);
- silent on write errors (sentinel must never break a test).
"""

import importlib.util
from pathlib import Path

_CONFTEST_PATH = (
    Path(__file__).resolve().parents[2] / "tests" / "conftest.py"
)


def _load_hook():
    """Import the hook function out of the top-level tests/conftest.py."""
    spec = importlib.util.spec_from_file_location(
        "_tests_root_conftest", _CONFTEST_PATH
    )
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod.pytest_runtest_logstart


def test_noop_when_env_unset(monkeypatch, tmp_path):
    monkeypatch.delenv("DYNASTORE_TEST_SENTINEL", raising=False)
    hook = _load_hook()
    hook("tests/x.py::test_foo", ("tests/x.py", 1, "test_foo"))
    assert not list(tmp_path.iterdir())


def test_writes_nodeid_when_env_set(monkeypatch, tmp_path):
    sentinel = tmp_path / "last-test.txt"
    monkeypatch.setenv("DYNASTORE_TEST_SENTINEL", str(sentinel))
    hook = _load_hook()
    hook("tests/x.py::test_foo", ("tests/x.py", 1, "test_foo"))
    body = sentinel.read_text()
    assert "tests/x.py::test_foo" in body


def test_atomic_overwrite_no_tmp_left(monkeypatch, tmp_path):
    sentinel = tmp_path / "last-test.txt"
    monkeypatch.setenv("DYNASTORE_TEST_SENTINEL", str(sentinel))
    hook = _load_hook()
    hook("tests/x.py::test_a", ("tests/x.py", 1, "test_a"))
    hook("tests/y.py::test_b", ("tests/y.py", 2, "test_b"))
    body = sentinel.read_text()
    assert "test_b" in body and "test_a" not in body
    assert not (tmp_path / "last-test.txt.tmp").exists()


def test_includes_worker_id(monkeypatch, tmp_path):
    sentinel = tmp_path / "last-test.txt"
    monkeypatch.setenv("DYNASTORE_TEST_SENTINEL", str(sentinel))
    monkeypatch.setenv("PYTEST_XDIST_WORKER", "gw3")
    hook = _load_hook()
    hook("tests/x.py::test_foo", ("tests/x.py", 1, "test_foo"))
    assert "gw3" in sentinel.read_text()


def test_silent_on_write_error(monkeypatch):
    # Point at an unwritable path; hook must swallow the error.
    monkeypatch.setenv(
        "DYNASTORE_TEST_SENTINEL", "/nonexistent/dir/last-test.txt"
    )
    hook = _load_hook()
    hook("tests/x.py::test_foo", ("tests/x.py", 1, "test_foo"))  # no raise
