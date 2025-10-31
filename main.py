import asyncio
import os
from pyrogram.handlers import MessageHandler
from pyrogram import filters
from contextlib import suppress

from db_modules.controller import DatabaseController
from telegram.botpool import BotPool
from telegram.logic import build_logic
from services.parser import group_parser
from services.greeter import periodic_greeting
from assistant.gpt import Assistant
import settings
import state


async def _cancel(tasks: list[asyncio.Task]) -> None:
    if not tasks:
        return
    for t in tasks:
        t.cancel()
    with suppress(asyncio.CancelledError):
        await asyncio.gather(*tasks, return_exceptions=False)


async def main():

    print("\nStart\n")

    settings.init_config("config.json")

    db = DatabaseController("sqlite+aiosqlite:///data/new.db")
    await db.init_db()

    assistant = Assistant('gpt-4.1', db)

    pool = BotPool(db=db)
    handlers = build_logic(pool, db, assistant, state, settings)
    pool.add_handler(handlers['handle_message'])

    tasks = []

    external_db_path = "/home/appuser/parser/data/users.db"
    parser_task = asyncio.create_task(group_parser(db, pool, external_db_path=external_db_path), name="group_parser")
    tasks.append(parser_task)

    await asyncio.sleep(30)

    greeter_task = asyncio.create_task(periodic_greeting(db, pool, handlers['handle_assistant_response']), name="periodic_greeting")
    tasks.append(greeter_task)

    try:
        await pool.activate()
    finally:
        await _cancel(tasks)

        with suppress(Exception):
            await pool.shutdown()

        with suppress(Exception):
            await db.engine.dispose()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nЗавершение по Ctrl+C (launcher)э.")