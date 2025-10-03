import asyncio
import os

from db_modules.controller import DatabaseController
from pyrogram.handlers import MessageHandler
from pyrogram import filters
from telegram.botpool import BotPool
from telegram.logic import make_handlers
import settings
import state


async def attach_handlers(pool: BotPool, db: DatabaseController):
    """
    Вешаем единый MessageHandler на всех клиентов пула.
    Если клиенты подключаются лениво через ensure_client — pool.add_handler
    гарантирует навешивание и на будущих клиентов.
    """
    handlers = make_handlers(db=db, pool=pool, state=state)
    msg_handler = MessageHandler(handlers["handle_message"], filters.private & filters.text)
    pool.add_handler(msg_handler)  # навесит на уже подключённых и на будущих



async def main():
    # 1) инициализация конфига и БД
    settings.init_config("config.json")
    db = DatabaseController("sqlite+aiosqlite:///data/test.db")
    await db.init_db()

    # 2) создаём пул ботов
    pool = BotPool(db=db)

    # 3) вешаем хэндлеры
    await attach_handlers(pool, db)

    await pool.activate()

    print("✅ Боты запущены. Жду сообщений... (Ctrl+C для выхода)")
    try:
        while True:
            await asyncio.sleep(3600)
    except KeyboardInterrupt:
        print("⏹ Остановка...")

if __name__ == "__main__":
    asyncio.run(main())