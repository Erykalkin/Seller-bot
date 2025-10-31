from __future__ import annotations
import asyncio
import datetime as dt
from zoneinfo import ZoneInfo
from contextlib import suppress
from typing import Any, Optional
import time
import random

from settings import get
from state import stop_greeter
from telegram.botpool import BotPool
from db_modules.controller import DatabaseController
from .start_messages import generate_intro_message


UserRow = tuple[int, int, int]

def _now_tz():
    tz = get("TIMEZONE") or "Europe/Moscow"
    try:
        return dt.datetime.now(ZoneInfo(tz))
    except Exception:
        return dt.datetime.utcnow()


def _in_awake_window() -> bool:
    now = _now_tz()
    morning = int(get("MORNING") or 9)
    night   = int(get("NIGHT")   or 21)
    return morning <= now.hour <= night
    # return True
    # return False


def _clamped_normal_in_window(window_sec: float, lo_frac: float = 0.2, hi_frac: float = 0.8, std_frac: float = 0.1) -> float:
    """
    Возвращает offset в секундах внутри [lo_frac*window, hi_frac*window],
    распределённый нормально вокруг середины окна.
    """
    mean = window_sec * 0.5
    std  = max(1.0, window_sec * std_frac)
    lo = window_sec * lo_frac
    hi = window_sec * hi_frac
    x = random.gauss(mean, std)
    return max(lo, min(hi, x))


def _build_schedule(n: int, window_sec: float, min_gap: float = 2.0) -> list[float]:
    """
    Формирует отсортированный список временных оффсетов с гарантированным зазором между исполнителями.
    """
    if n <= 0:
        return []
    pts = sorted(_clamped_normal_in_window(window_sec) for _ in range(n))
    adjusted = []
    last = None
    for t in pts:
        if last is None:
            adjusted.append(t)
            last = t
        else:
            t = max(t, last + min_gap)
            if t > window_sec:
                t = min(window_sec, last + min_gap)
            adjusted.append(t)
            last = t
    return adjusted


async def _greet_one_user(db: DatabaseController, pool: BotPool, handle_assistant_response, item: UserRow) -> None:
    user_id, executor_id, access_hash = item

    if await db.get_user_param(user_id, "banned"):
        return
    if await db.get_user_param(user_id, "problem"):
        return
    print(f"\n[GREETER] Начинаю приветствие user {user_id} через executor {executor_id}")

    bot = await pool.ensure_client(executor_id)
    if not bot:
        print(f"[GREETER] Не удалось подключить executor {executor_id}")
        return
    
    me = await bot.get_me()
    name = me.username

    user = await pool.connect_user(bot, user_id, access_hash)

    if user is None:
        await db.rotate_user_down(user_id)
        print(f"[GREETER] Ошибка при приветствии user {user_id}: connect_user вернул None")
        return

    info = await db.get_user_param(user_id, "info") or ""

    ok = await handle_assistant_response(
        bot,
        user,
        f"CLIENT_INFO: {info}\n\nSTART_MESSAGE: {generate_intro_message()}",
        first=get("SECOND_GREET"),
    )
    if ok:
        await db.update_user_param(user_id, "contact", True)
        await db.user_timestamp(user_id)
        print(f"[GREETER] Привет отправлен user {(user_id, user.username)} через executor {(executor_id, name)}")
    else:
        await db.rotate_user_down(user_id)
        print(f"[GREETER] Ошибка при приветствии user {(user_id, user.username)} через executor {(executor_id, name)}")


async def _pick_batch(db: DatabaseController) -> list[UserRow]:
    """
    Выбираем пачку (по одному на исполнителя).
    """
    async with db.users() as users_repo:
        items = await users_repo.pop_users_to_greet()

    if len(items) > 0:
        print(f"\n\n[GREETER] Подобрано {len(items)} пользователей для приветствия.")
    return items or []


async def periodic_greeting(db: DatabaseController, pool: BotPool, handle_assistant_response) -> None:
    """
    Цикл:
      1. ждёт дневное окно;
      2. выбирает пачку;
      3. планирует оффсеты по нормальному распределению;
      4. последовательно шлёт;
      5. доспит остаток окна.
    """
    WINDOW_SEC = float(get("GREET_PERIOD") or 300)
    MIN_GAP = 2.0
    IDLE_SLEEP = 5.0

    await asyncio.sleep(200)

    print(f"[GREETER] Старт сервиса приветствий")

    while True:
        if not _in_awake_window():
            await asyncio.sleep(300)
            continue

        batch = await _pick_batch(db)
        if not batch:
            await asyncio.sleep(IDLE_SLEEP)
            continue

        offsets = _build_schedule(len(batch), WINDOW_SEC, min_gap=MIN_GAP)
        start = time.monotonic()

        for (item, target_offset) in zip(batch, offsets):
            now = time.monotonic()
            elapsed = now - start
            delay = max(0.0, target_offset - elapsed)
            if delay:
                await asyncio.sleep(delay)

            await _greet_one_user(db, pool, handle_assistant_response, item)

        elapsed = time.monotonic() - start
        tail = max(0.0, WINDOW_SEC - elapsed)
        if tail:
            await asyncio.sleep(tail)
