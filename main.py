# app/main.py
import asyncio
import json
import signal
from pathlib import Path

# --- твои импорты (проверь пути под свой проект) ---
from db_modules.controller import DatabaseController           # твой контроллер БД (SQLAlchemy + репозитории)
from botpool import BotPool                      # BotPool с ensure_client/sleep/очередями
from telegram.logic import handle_message             # фабрика с handle_message/...
from pyrogram.handlers import MessageHandler
from pyrogram import filters
import state


# ------------------ утилиты ------------------

def load_settings(path: str = "config.json") -> dict:
    p = Path(path)
    if not p.exists():
        # минимальные дефолты, если файла нет
        return dict(
            BUFFER_TIME=1.0,
            DELAY=1.0,
            TYPING_DELAY=0.1,
            INACTIVITY_TIMEOUT=1000,
        )
    with p.open("r", encoding="utf-8") as f:
        data = json.load(f)
    # подстрахуем дефолтами
    return {
        "BUFFER_TIME": float(data.get("BUFFER_TIME", 1.0)),
        "DELAY": float(data.get("DELAY", 1.0)),
        "TYPING_DELAY": float(data.get("TYPING_DELAY", 0.1)),
        "INACTIVITY_TIMEOUT": int(data.get("INACTIVITY_TIMEOUT", 1000)),
    }


async def attach_handlers(pool: BotPool, db: DatabaseController, settings: dict):
    """
    Вешаем единый MessageHandler на всех клиентов пула.
    Если клиенты подключаются лениво через ensure_client — pool.add_handler
    гарантирует навешивание и на будущих клиентов.
    """
    handlers = make_handlers(db=db, pool=pool, state=state, settings=settings)
    msg_handler = MessageHandler(handlers["handle_message"], filters.private & filters.text)
    pool.add_handler(msg_handler)  # навесит на уже подключённых и на будущих


async def warm_up_executors(pool: BotPool, db: DatabaseController):
    """
    Прогреваем активных исполнителей, чтобы сразу получать входящие.
    Ожидается метод репозитория: get_active_executor_ids() -> list[int].
    При необходимости замени на свой (например, get_all_ids(status='active')).
    """
    async with db.executors() as execs_repo:
        try:
            active_ids = await execs_repo.get_active_executor_ids()
        except AttributeError:
            # запасной вариант — подстрой под свой интерфейс
            active_ids = await execs_repo.get_all_ids(status="active")

    for ex_id in active_ids:
        try:
            await pool.ensure_client(ex_id)  # клиент подключится и получит хэндлеры
        except Exception as e:
            print(f"[WARMUP] executor {ex_id} connect failed: {e}")


# ------------------ graceful shutdown ------------------

class _Stopper:
    def __init__(self):
        self._evt = asyncio.Event()

    def set(self, *_):
        self._evt.set()

    async def wait(self):
        await self._evt.wait()


async def shutdown(pool: BotPool):
    # отменяем пользовательские таски
    await state.cancel_all_tasks()
    # мягко останавливаем клиентов (если есть метод в пуле — используй его)
    try:
        # если у тебя есть pool.stop_all() — вызови его
        for cli in list(pool._clients.values()):
            try:
                await cli.stop()
            except Exception:
                pass
    except Exception:
        pass


# ------------------ main ------------------

async def main():
    settings = load_settings("config.json")

    # 1) БД
    # пример: SQLite файл
    db = DatabaseController("sqlite+aiosqlite:///data/users.db")
    await db.init_models()  # если требуется миграция/создание

    # 2) Пул (ленивое подключение клиентов через db.connect_executor)
    pool = BotPool(db)

    # 3) Хэндлеры
    await attach_handlers(pool, db, settings)

    # 4) Прогрев активных исполнителей (чтобы сразу ловить входящие)
    await warm_up_executors(pool, db)

    # 5) ожидание сигналов завершения
    stopper = _Stopper()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stopper.set)
        except NotImplementedError:
            # Windows
            pass

    print("[MAIN] started. Press Ctrl+C to stop.")
    await stopper.wait()

    print("[MAIN] shutting down...")
    await shutdown(pool)
    print("[MAIN] bye.")


if __name__ == "__main__":
    asyncio.run(main())