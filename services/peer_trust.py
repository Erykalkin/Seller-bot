# services/keepalive.py
from __future__ import annotations

import asyncio
import random
import time
from typing import Iterable, List, Optional, Sequence, Union
from zoneinfo import ZoneInfo

from pyrogram.errors import FloodWait, PeerFlood
from pyrogram.enums import ChatAction

from settings import get
from db_modules.controller import DatabaseController
from telegram.botpool import BotPool

# --- управление жизненным циклом ---
stop_keepalive = asyncio.Event()
_keepalive_tasks: dict[int, asyncio.Task] = {}  # executor_id -> task


# === настройки (читаются через settings.get()) ===
# KEEPALIVE_ENABLED: bool            (default True)
# KEEPALIVE_GROUP: str | int         (username "@mygroup" или id "-100...")
# KEEPALIVE_MIN_SEC: int             (минимальный интервал, сек, default 600 = 10 минут)
# KEEPALIVE_MAX_SEC: int             (максимальный интервал, сек, default 2400 = 40 минут)
# KEEPALIVE_TYPING_MAX: int          (максимум "печатает..." перед сообщением, сек, default 5)
# TIMEZONE, MORNING, NIGHT          (у тебя уже есть в settings.py)

_DEFAULT_MESSAGES: Sequence[str] = (
    "Здорова народ 👋",
    "Как дела у всех?",
    "Проверка связи…",
    "Кофе зашёл как надо ☕",
    "Что нового?",
    "Поймал вдохновение работать 🧠",
    "Погнали!",
    "Минутка активности 🙂",
)

def _now_tz():
    tz = get("TIMEZONE") or "Europe/Moscow"
    try:
        return time.time(), ZoneInfo(tz)
    except Exception:
        return time.time(), None


def _in_awake_window() -> bool:
    """Проверка, что сейчас между MORNING и NIGHT в локальном TZ."""
    _, tz = _now_tz()
    try:
        import datetime as dt
        now = dt.datetime.now(tz or dt.timezone.utc)
    except Exception:
        return True  # если TZ сломан — не блокируем

    morning = int(get("MORNING") or 9)
    night = int(get("NIGHT") or 21)
    return morning <= now.hour <= night


def _pick_message(custom: Optional[Sequence[str]]) -> str:
    pool = [s.strip() for s in (custom or _DEFAULT_MESSAGES) if s and s.strip()]
    return random.choice(pool) if pool else "✌️"


async def _idle_sleep(min_sec: int, max_sec: int) -> None:
    """Случайная пауза между событиями."""
    span = max(min_sec, 1), max(max_sec, 2)
    delay = random.randint(*span)
    await asyncio.sleep(delay)


async def _worker_for_executor(
    executor_id: int,
    pool: BotPool,
    group: Union[int, str],
    messages: Optional[Sequence[str]],
    min_interval: int,
    max_interval: int,
    typing_max: int,
) -> None:
    """
    Один исполнитель периодически шлёт сообщение в общий чат.
    """
    jitter = random.uniform(0.0, 1.0)  # немного разнести запуск
    await asyncio.sleep(3.0 + jitter * 5.0)

    client = await pool.ensure_client(executor_id)
    if not client:
        return  # не удалось подключить клиента — завершаем спокойно

    while not stop_keepalive.is_set():
        # окно бодрствования
        if not _in_awake_window():
            await asyncio.sleep(300)  # 5 мин, потом проверим окно снова
            continue

        # имитация "печатает..." для групп
        try:
            if typing_max > 0:
                typing_time = random.randint(1, max(1, typing_max))
                # ChatAction.TYPING в группе выглядит как "печатает..."
                await client.send_chat_action(group, ChatAction.TYPING)
                await asyncio.sleep(typing_time)
        except Exception:
            # не критично, просто пропустим typing
            pass

        # отправка
        text = _pick_message(messages)
        try:
            await client.send_message(group, text)
        except FloodWait as e:
            # аккуратно уходим спать ровно на требование API
            await asyncio.sleep(float(e.value))
        except PeerFlood:
            # сигнал, что мы слишком активны → пауза подлиннее
            await asyncio.sleep(3600)
        except Exception:
            # любые прочие проблемы — просто подождём, чтобы не лупить в цикл
            await asyncio.sleep(60)

        # рандомная пауза до следующего сообщения
        await _idle_sleep(min_interval, max_interval)


async def start_keepalive(
    db: DatabaseController,
    pool: BotPool,
    *,
    group: Optional[Union[int, str]] = None,
    messages: Optional[Sequence[str]] = None,
    min_interval: Optional[int] = None,
    max_interval: Optional[int] = None,
    typing_max: Optional[int] = None,
) -> None:
    """
    Запускает фоновые задачи keepalive для всех известных исполнителей.
    Параметры можно не передавать — возьмутся из settings.json.
    """
    if str(get("KEEPALIVE_ENABLED") or "true").lower() in ("0", "false", "no"):
        return

    group = group or (get("KEEPALIVE_GROUP") or "").strip()
    if not group:
        # без чата работать не будем
        return

    min_interval = int(min_interval if min_interval is not None else (get("KEEPALIVE_MIN_SEC") or 600))
    max_interval = int(max_interval if max_interval is not None else (get("KEEPALIVE_MAX_SEC") or 2400))
    typing_max = int(typing_max if typing_max is not None else (get("KEEPALIVE_TYPING_MAX") or 5))

    # список исполнителей
    async with db.executors() as executors_repo:
        exec_ids = await executors_repo.get_ids()

    # по одному воркеру на каждого
    for ex_id in exec_ids:
        if ex_id in _keepalive_tasks and not _keepalive_tasks[ex_id].done():
            continue
        _keepalive_tasks[ex_id] = asyncio.create_task(
            _worker_for_executor(
                ex_id,
                pool,
                group=group,
                messages=messages,
                min_interval=min_interval,
                max_interval=max_interval,
                typing_max=typing_max,
            )
        )


async def stop_keepalive_tasks() -> None:
    """Останавливает все keepalive-воркеры корректно."""
    stop_keepalive.set()
    tasks = list(_keepalive_tasks.values())
    _keepalive_tasks.clear()
    if tasks:
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
    stop_keepalive.clear()
