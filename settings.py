# settings.py
from __future__ import annotations

import json
import os
import threading
from types import SimpleNamespace
from typing import Any, Dict, Optional

# ---- Дефолты и типы ----
_DEFAULTS: Dict[str, Any] = {
    "BUFFER_TIME": 6.0,
    "DELAY": 5.0,
    "TYPING_DELAY": 0.3,
    "INACTIVITY_TIMEOUT": 50,
    "GREET_PERIOD": 300,
    "UPDATE_BD_PERIOD": 100,
    "FLOOD_WAIT": 1000,
    "TIMEZONE": "Europe/Moscow",
    "MORNING": 9,
    "NIGHT": 21,
}
_TYPES: Dict[str, type] = {
    "BUFFER_TIME": float,
    "DELAY": float,
    "TYPING_DELAY": float,
    "INACTIVITY_TIMEOUT": int,
    "GREET_PERIOD": int,
    "UPDATE_BD_PERIOD": int,
    "FLOOD_WAIT": int,
    "TIMEZONE": str,
    "MORNING": int,
    "NIGHT": int,
}

# ---- Состояние ----
_state = {
    "path": "config.json",
    "data": dict(_DEFAULTS),
    "mtime": 0.0,
    "lock": threading.RLock(),
}

# ---- Утилиты ----
def _atomic_write(path: str, content: str) -> None:
    tmp = f"{path}.tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        f.write(content)
    os.replace(tmp, path)

def _validate_pair(k: str, v: Any) -> Any:
    if k not in _TYPES:
        raise KeyError(f"Unknown setting: {k}")
    t = _TYPES[k]
    if t is float:
        return float(v)
    if t is int:
        fv = float(v)
        if not fv.is_integer():
            raise ValueError(f"{k} must be an integer, got {v!r}")
        return int(fv)
    if t is str:
        return str(v)
    return v

def _merge_defaults(d: Dict[str, Any]) -> Dict[str, Any]:
    merged = dict(_DEFAULTS)
    merged.update(d)
    return {k: _validate_pair(k, merged[k]) for k in _TYPES.keys()}

def _load_from_disk_unlocked() -> None:
    path = _state["path"]
    try:
        with open(path, "r", encoding="utf-8") as f:
            raw = json.load(f)
        if not isinstance(raw, dict):
            raise ValueError("config.json must contain a JSON object")
    except FileNotFoundError:
        raw = {}
    data = _merge_defaults(raw)
    _state["data"] = data
    _state["mtime"] = os.path.getmtime(path) if os.path.exists(path) else 0.0

def _maybe_reload_unlocked() -> None:
    path = _state["path"]
    try:
        mtime = os.path.getmtime(path)
    except FileNotFoundError:
        mtime = 0.0
    if mtime != _state["mtime"]:
        _load_from_disk_unlocked()

# ---- API ----
def init_config(path: str = "config.json") -> None:
    """Задать путь к конфигу и загрузить его."""
    with _state["lock"]:
        _state["path"] = path
        _load_from_disk_unlocked()

def get_settings() -> SimpleNamespace:
    """Снимок настроек как объект-namespace."""
    with _state["lock"]:
        _maybe_reload_unlocked()
        return SimpleNamespace(**_state["data"])

def get(key: str, default: Optional[Any] = None) -> Any:
    """Достать одно значение."""
    with _state["lock"]:
        _maybe_reload_unlocked()
        return _state["data"].get(key, default)

def get_all() -> Dict[str, Any]:
    """Снимок словаря всех настроек."""
    with _state["lock"]:
        _maybe_reload_unlocked()
        return dict(_state["data"])

def update_settings(updates: Dict[str, Any]) -> Dict[str, Any]:
    """Обновить файл и память атомарно."""
    if not updates:
        return get_all()

    with _state["lock"]:
        _maybe_reload_unlocked()
        new_data = dict(_state["data"])
        # валидация + применение
        for k, v in updates.items():
            new_data[k] = _validate_pair(k, v)
        # запись
        content = json.dumps(new_data, ensure_ascii=False, indent=4)
        _atomic_write(_state["path"], content)
        _state["data"] = new_data
        _state["mtime"] = os.path.getmtime(_state["path"])
        return dict(new_data)

def set(key: str, value: Any) -> Any:
    """Одиночное изменение."""
    return update_settings({key: value})[key]
