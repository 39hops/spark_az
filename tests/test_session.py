"""Tests for spark_az.session."""
from __future__ import annotations

from typing import Any

import pytest

from spark_az import session


def test_get_spark_raises_when_nothing_registered_and_no_active(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """get_spark must not call SparkSession.builder.getOrCreate()."""
    monkeypatch.setattr(session, "_spark", None, raising=False)
    monkeypatch.setattr(session, "_active_spark_session", lambda: None)
    with pytest.raises(RuntimeError, match="No active SparkSession"):
        session.get_spark()


def test_get_spark_returns_registered(monkeypatch: pytest.MonkeyPatch) -> None:
    sentinel: Any = object()
    monkeypatch.setattr(session, "_spark", sentinel, raising=False)
    assert session.get_spark() is sentinel


def test_set_spark_round_trips(monkeypatch: pytest.MonkeyPatch) -> None:
    sentinel: Any = object()
    monkeypatch.setattr(session, "_spark", None, raising=False)
    session.set_spark(sentinel)
    assert session.get_spark() is sentinel


def test_get_spark_falls_back_to_active_session(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sentinel: Any = object()
    monkeypatch.setattr(session, "_spark", None, raising=False)
    monkeypatch.setattr(session, "_active_spark_session", lambda: sentinel)
    assert session.get_spark() is sentinel
