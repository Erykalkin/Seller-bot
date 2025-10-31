from __future__ import annotations

import asyncio
import random
import time
from typing import Optional, Sequence, Union

from zoneinfo import ZoneInfo
from pyrogram import Client
from pyrogram.enums import ChatAction
from pyrogram.errors import FloodWait, PeerFlood, ChatWriteForbidden, UserBannedInChannel, ChannelPrivate

from db_modules.controller import DatabaseController
from telegram.botpool import BotPool
from settings import get


_stop_flag = asyncio.Event()
_tasks: dict[int, asyncio.Task] = {}  # executor_id -> task


_DEFAULT_MESSAGES: Sequence[str] = (
    "На связи ✅",
    "Жив-здоров 👋",
    "Пульс есть 💓",
    "Все ок, работаю.",
    "Проверка присутствия.",
)


def _now_tz():
    tzname = get("TIMEZONE") or "Europe/Moscow"
    try:
        return time.time(), ZoneInfo(tzname)
    except Exception:
        return time.time(), None


def _in_awake_window() -> bool:
    """Дневное окно по настройкам MORNING..NIGHT."""
    _, tz = _now_tz()
    try:
        import datetime as dt
        now = dt.datetime.now(tz or dt.timezone.utc)
    except Exception:
        return True

    morning = int(get("MORNING") or 9)
    night = int(get("NIGHT") or 21)
    return morning <= now.hour <= night


def _pick_msg(custom: Optional[Sequence[str]]) -> str:
    pool = [s.strip() for s in (custom or _DEFAULT_MESSAGES) if s and s.strip()]
    return random.choice(pool) if pool else "На связи"


async def _idle_wait(min_sec: int, max_sec: int) -> None:
    delay = random.randint(max(1, min_sec), max(2, max_sec))
    await asyncio.sleep(delay)


async def _heartbeat_worker(
    executor_id: int,
    pool: BotPool,
    db: DatabaseController,
    *,
    group: Union[int, str],
    messages: Optional[Sequence[str]],
    min_interval: int,
    max_interval: int,
    typing_max: int,
    set_active_on_success: bool,
) -> None:
    """
    Один исполнитель периодически отправляет heartbeat в общий чат.
    """
    await asyncio.sleep(random.uniform(0.5, 3.0))

    client = await pool.ensure_client(executor_id)
    if not client:
        return

    while not _stop_flag.is_set():
        if typing_max > 0:
            try:
                await client.send_chat_action(group, ChatAction.TYPING)
                await asyncio.sleep(random.randint(1, max(1, typing_max)))
            except Exception:
                pass

        text = _pick_msg(messages)
        try:
            await client.send_message(group, text)

            if set_active_on_success:
                try:
                    await db.update_executor_param(executor_id, "status", "active")
                    await db.executor_timestamp(executor_id)
                except Exception:
                    pass

        except FloodWait as e:
            await asyncio.sleep(float(getattr(e, "value", 1)) or 1)

        except PeerFlood:
            try:
                await db.update_executor_param(executor_id, "status", "limited")
            except Exception:
                pass
            await asyncio.sleep(3600)

        except (ChatWriteForbidden, UserBannedInChannel, ChannelPrivate):
            try:
                await db.update_executor_param(executor_id, "status", "forbidden")
            except Exception:
                pass
            await asyncio.sleep(1800)

        except Exception:
            try:
                await db.update_executor_param(executor_id, "status", "error")
            except Exception:
                pass
            await asyncio.sleep(60)


        await _idle_wait(min_interval, max_interval)


async def start_heartbeat(
    db: DatabaseController,
    pool: BotPool,
    *,
    group: Optional[Union[int, str]] = None,
    messages: Optional[Sequence[str]] = None,
    min_interval: Optional[int] = None,
    max_interval: Optional[int] = None,
    typing_max: Optional[int] = None,
    set_active_on_success: bool = True,
) -> None:
    """
    Запустить heartbeat-воркеры для всех исполнителей.
    Параметры можно не указывать — возьмутся из config.json (см. ниже).
    """
    if _stop_flag.is_set():
        _stop_flag.clear()

    group = group or (get("HEARTBEAT_GROUP") or "").strip()
    if not group:
        return

    min_interval = int(min_interval if min_interval is not None else (get("HEARTBEAT_MIN_SEC") or 1200))
    max_interval = int(max_interval if max_interval is not None else (get("HEARTBEAT_MAX_SEC") or 3600))
    typing_max = int(typing_max if typing_max is not None else (get("HEARTBEAT_TYPING_MAX") or 3))

    async with db.executors() as executors_repo:
        exec_ids = await executors_repo.get_ids()

    for ex_id in exec_ids:
        if ex_id in _tasks and not _tasks[ex_id].done():
            continue
        _tasks[ex_id] = asyncio.create_task(
            _heartbeat_worker(
                ex_id,
                pool,
                db,
                group=group,
                messages=messages,
                min_interval=min_interval,
                max_interval=max_interval,
                typing_max=typing_max,
                set_active_on_success=set_active_on_success,
            ),
            name=f"heartbeat:{ex_id}",
        )


async def stop_heartbeat_tasks() -> None:
    """Остановить все heartbeat-воркеры."""
    _stop_flag.set()
    tasks = list(_tasks.values())
    _tasks.clear()
    if tasks:
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
    _stop_flag.clear()
