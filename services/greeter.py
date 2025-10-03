from __future__ import annotations
import asyncio
import datetime as dt
from zoneinfo import ZoneInfo
from contextlib import suppress
from typing import Any, Optional
import time

from settings import get
from state import stop_greeter
from botpool import BotPool
from db_modules.controller import DatabaseController
from telegram.logic import handle_assistant_response


async def greet_new_users(db: DatabaseController, pool: BotPool) -> None:
    """
    Взять первого кандидата на привет (pop_first_user_to_greet),
    выполнить 'first contact' через connect_user + handle_assistant_response,
    отметить контакт/время в БД.
    Возвращает None. Без логирования (print) — исключения подавляются локально,
    но можно расширить обработку ошибок по типу FloodWait/PeerFlood в пуле.
    """
    try:
        async with db.users() as users_repo:
            item = await users_repo.pop_first_user_to_greet()
            if not item:
                return

            user_id, executor_id, access_hash = item    # tuple (user_id, executor_id, access_hash)

            if await users_repo.get_user_param(user_id, 'banned'):
                return

            bot = await pool.ensure_client(executor_id)

            user_obj = await users_repo.connect_user(bot, user_id, access_hash)

            info = await users_repo.get_user_param(user_id, 'info')
            await handle_assistant_response(user_obj, f"CLIENT_INFO: {info}", first=True)

            await users_repo.update_user_param(user_id, "contact", True)
            await users_repo.update_user_param(user_id, "last_message",  int(time.time()))

    except Exception:
        return


async def periodic_greeting(db: DatabaseController, pool: BotPool) -> None:
    """
    Фоновый цикл приветствий.
    - Раз в GREET_PERIOD секунд пытается выполнить одно приветствие (greet_new_users).
    - Проверяет окно MORNING-NIGHT в TIMEZONE.
    """
    while not stop_greeter.is_set():
        tzname = get("TIMEZONE") or "Europe/Moscow"
        try:
            now = dt.datetime.now(ZoneInfo(tzname))
        except Exception:
            now = dt.datetime.now()

        morning = int(get("MORNING") or 9)
        night = int(get("NIGHT") or 21)

        if morning <= now.hour <= night:
            await greet_new_users(db, pool)

        period = int(get("GREET_PERIOD") or 300)
        await asyncio.sleep(max(5, period))