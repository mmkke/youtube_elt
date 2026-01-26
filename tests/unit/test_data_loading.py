import json
import builtins
import pytest
from datetime import date
from pathlib import Path

import datawarehouse.data_loading as dl

def test_load_data_missing_channel_handle_raises(monkeypatch):
    monkeypatch.setattr(dl.Variable, "get", lambda *args, **kwargs: None)
    with pytest.raises(RuntimeError, match="CHANNEL_HANDLE not set"):
        dl.load_data()

def test_load_data_file_not_found_raises(monkeypatch):
    monkeypatch.setattr(dl.Variable, "get", lambda *args, **kwargs: "channel_handle")
    monkeypatch.setattr(Path, "is_file", lambda self: False)
    with pytest.raises(FileNotFoundError):
        dl.load_data()

def test_load_data_invalid_json_raises(monkeypatch):
    monkeypatch.setattr(dl.Variable, "get", lambda *args, **kwargs: "channel_handle")
    monkeypatch.setattr(Path, "is_file", lambda self: True)
    
    def fake_json_load(_file_obj):
        raise json.JSONDecodeError("Expecting value", doc="", pos=0)
    monkeypatch.setattr(dl.json, "load", fake_json_load)

    class DummyFile:
        def __enter__(self): return self
        def __exit__(self, exc_type, exc, tb): return False
    monkeypatch.setattr(builtins, "open", lambda *args, **kwargs: DummyFile())

    with pytest.raises(json.JSONDecodeError):
        dl.load_data()

def test_load_data_success_returns_data(monkeypatch):
    monkeypatch.setattr(dl.Variable, "get", lambda *args, **kwargs: "channel_handle")
    monkeypatch.setattr(Path, "is_file", lambda self: True)

    expected = [{"video_id": "abc123"}]
    monkeypatch.setattr(dl.json, "load", lambda _file_obj: expected)

    class DummyFile:
        def __enter__(self): return self
        def __exit__(self, exc_type, exc, tb): return False
    monkeypatch.setattr(builtins, "open", lambda *args, **kwargs: DummyFile())

    out = dl.load_data()
    assert out == expected, f"Loaded data does not match expected. \nLoaded: {out} \nExpected: {expected}"