from __future__ import annotations
import asyncio
import sqlite3
import datetime as dt
from zoneinfo import ZoneInfo
from typing import Optional, Tuple

from settings import get
from state import stop_group_parser
from db_modules.controller import DatabaseController
from telegram.botpool import BotPool

# (user_id, username, telephone, info, source_link)
ExtRow = Tuple[int, Optional[str], Optional[str], Optional[str], Optional[str]]


def _now_tz():
    tz = get("TIMEZONE") or "Europe/Moscow"
    try:
        return dt.datetime.now(ZoneInfo(tz))
    except Exception:
        return dt.datetime.utcnow()


def _is_night() -> bool:
    """
    Парсим ночью.
    """
    now = _now_tz()
    morning = int(get("MORNING") or 9)
    night = int(get("NIGHT") or 21)
    return not (morning <= now.hour <= night)
    # return (morning <= now.hour <= night)
    # return True


def _fetch_targets_with_last_link(external_db_path: str) -> list[ExtRow]:
    """
    Достаем всех пользователей из внешней БД с target=1 и к каждому — последний source_link из messages.
    """
    conn = sqlite3.connect(external_db_path)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    cur.execute("""
        SELECT u.user_id,
               u.username,
               u.telephone,
               u.name,
               u.info,
               (
                    SELECT m.source_link
                    FROM messages m
                    WHERE m.user_id = u.user_id
                        AND m.source_link IS NOT NULL
                        AND TRIM(m.source_link) <> ''
                    ORDER BY m.created_at DESC
                    LIMIT 1
                ) AS source_link
        FROM users u
        WHERE u.target = 1
    """)
    rows = cur.fetchall()
    conn.close()
    return [(r["user_id"], r["username"], r["telephone"], r['name'], r["info"], r["source_link"]) for r in rows]


async def group_parser(db: DatabaseController, pool: BotPool, *, external_db_path: str) -> None:
    """
    Берёт пользователей из внешней БД и заносит их в основную.
    1. pool.add_user(user_id, info, phone, username)
    2. если есть source_link — добываем access_hash внутри add_user
    """
    period = int(get("UPDATE_BD_PERIOD") or 100)

    await asyncio.sleep(200)

    print(f"[PARSER] Старт сервиса парсинга внешней БД")

    while not stop_group_parser.is_set():
        if not _is_night():
            await asyncio.sleep(3600)
            continue

        try:
            ext_rows = _fetch_targets_with_last_link(external_db_path)
            print(f"[PARSER] Найдено {len(ext_rows)} пользователей во внешней БД")
        except Exception as e:
            print(f"[PARSER] external DB read error: {e}")
            await asyncio.sleep(period)
            continue

        for user_id, username, telephone, name, info, source_link in ext_rows:
            try:
                async with db.users() as users_repo:
                    if await users_repo.has_user(user_id):
                        continue

                eid = await pool.add_user(
                    user_id = user_id,
                    username = username or None,
                    phone = telephone or None,
                    info = info or "",
                    name = name or None,
                    link = source_link,
                )

                ename = (await db.get_executor(executor_id=eid))['name']

                print(f"[PARSER] Добавлен пользоваатель {user_id, username} c исполнителем {eid, ename}")

            except Exception as e:
                print(f"[PARSER] failed uid={user_id}: {e}")

        await asyncio.sleep(period)
