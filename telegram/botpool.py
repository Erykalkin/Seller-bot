import asyncio, time
from typing import Optional, Dict, List, Tuple
from pydantic import BaseModel
from pyrogram import Client
from pyrogram.errors import FloodWait, PeerFlood, UserIsBlocked, RPCError
from pyrogram.handlers import MessageHandler
from pyrogram import filters
from contextlib import suppress

from db_modules.controller import DatabaseController
from telegram.senders import send_message, send_document
from .basepool import BasePool
from .botpool_users import add_user, connect_user, get_access_hash, get_username_by_id, get_phone_by_id
from .botpool_executors import connect_executor, create_session, add_executor, reload_executor, delete_executor


class BotSlot:
    def __init__(self, name: str, client: Client):
        self.name = name
        self._client = client
        self._lock: Optional[asyncio.Lock] = None
        self.disabled_until: float = 0.0  # timestamp, когда слот снова доступен

    @property
    def lock(self) -> asyncio.Lock:
        if self._lock is None:
            self._lock = asyncio.Lock()
        return self._lock

    @property
    def available(self) -> bool:
        return time.time() >= (self.disabled_until or 0.0)


class BotPool(BasePool):
    def __init__(self, db: DatabaseController, *, main_executor: int = None, initial_backoff: float = 60.0,
                 backoff_factor: float = 2.0, max_backoff: float = 24*3600.0):
        
        super().__init__(initial_backoff=initial_backoff, backoff_factor=backoff_factor, max_backoff=max_backoff)
        
        self.db = db

        self.main_executor = main_executor

        self._clients: Dict[int, Client] = {}      # кеш клиентов: executor_id -> Client
        self._handlers: List = []                  # общие хэндлеры (навешиваются на каждый клиент при connect_executor)

    add_user = add_user
    connect_user = connect_user
    get_access_hash = get_access_hash
    get_username_by_id = get_username_by_id
    get_phone_by_id = get_phone_by_id

    connect_executor = connect_executor
    create_session = create_session
    add_executor = add_executor
    reload_executor = reload_executor
    delete_executor = delete_executor


    async def activate(self):
        self._install_signal_handlers()
        
        async with self.db.executors() as executors_repo:
            executors = await executors_repo.get_ids()
        for executor_id in executors:
            await self.ensure_client(executor_id)
        
        print("Все клиенты активированы.")

        await self._stop.wait()

        await self.shutdown()

    
    async def shutdown(self):
        """
        Полное завершение пула:
        - сигнализируем базовому классу о завершении (просыпаются все executors)
        - отменяем фоновые задачи
        - отключаем клиентов
        - закрываем БД
        """

        print("\nЗавершение пула")

        self.request_stop()
        await self.aclose(drain_queues=False)

        if hasattr(self, "_bg_tasks") and self._bg_tasks:
            for t in list(self._bg_tasks):
                t.cancel()
            with suppress(asyncio.CancelledError):
                await asyncio.gather(*self._bg_tasks, return_exceptions=False)
            self._bg_tasks.clear()

        for executor_id, client in list(self._clients.items()):
            try:
                if hasattr(client, "stop") and asyncio.iscoroutinefunction(client.stop):
                    await client.stop()
                elif hasattr(client, "stop"):
                    await client.stop()
            except Exception as e:
                print(f"Ошибка при отключении клиента {executor_id}: {e}")

        try:
            await self.db.close()
        except Exception as e:
            print(f"Ошибка при закрытии БД: {e}")

        print("\nЗавершение выполнено.")

            
    def add_handler(self, handler: MessageHandler, rules = filters.private & filters.text) -> None:
        """
        Регистрирует общий хэндлер на все будущие клиенты + уже подключённых.
        """
        handler = MessageHandler(handler, rules)
        self._handlers.append(handler)
        for cli in self._clients.values():
            cli.add_handler(handler)


    async def ensure_client(self, executor_id: int) -> Optional[Client]:
        """
        Возвращает подключённый Client для executor_id.
        Берёт через db.connect_executor(executor_id) и кеширует.
        Подвешивает заранее добавленные хэндлеры.
        """
        if executor_id in self._clients:
            return self._clients[executor_id]

        async with self._lock_for(executor_id):
            if executor_id in self._clients:
                return self._clients[executor_id]

            cli = await self.connect_executor(executor_id=executor_id)
            if not cli:
                return None
            
            if not cli.is_connected:
                try:
                    await cli.start()
                except Exception as e:
                    print(f"[POOL] [ensure_client] [executor_id = {executor_id}] {e}")

            if cli.is_connected:
                await self.db.update_executor_param(executor_id, 'status', 'active')
            else :
                await self.db.update_executor_param(executor_id, 'status', 'disconected')

            # навесим все сохранённые хэндлеры на только что подключённого клиента
            for h in self._handlers:
                cli.add_handler(h)

            self._clients[executor_id] = cli
            return cli


    def get_client_cached(self, executor_id: int) -> Optional[Client]:
        """Только из кеша, без подключения"""
        return self._clients.get(executor_id)
    

    async def send_text(self, user_id: int, text: str, reply_to: int = None, first: bool = False, bot: Client = None) -> bool:
        """
        Шлёт текст через закреплённого за пользователем исполнителя.
        Явного исполнителя может указывать хэндлер.
        Использует готовую функцию send_message(bot, user, ...).
        """
        if bot is None:
            executor_id = await self.db.get_user_param(user_id, 'executor_id')
            if executor_id is None:
                print(f"[POOL] [send_text] [user {user_id}] has no executor_id")
                return False

            bot = await self.ensure_client(executor_id)
            if not bot:
                print(f"[POOL] executor '{executor_id}' not connected")
                return False
        else:
            me = await bot.get_me()
            executor_id = me.id

        if self.is_sleeping(executor_id):
            self.defer_for_executor(executor_id, self.send_text(user_id, text, reply_to, first=first))
            return False

        user = await self.connect_user(bot, user_id)

        try:
            ok = await send_message(bot, user, text=text, reply=reply_to, first=first)
            await self.db.executor_timestamp(executor_id)
            return ok
        
        except FloodWait as e:
            await self.sleep_executor(executor_id, float(e.value))
            self.defer_for_executor(executor_id, self.send_text(user_id, text, reply_to, first=first))
            print(f"[POOL] [send_text] [executor {executor_id} -> user {user_id}] FloodWait: ждём {e.value} сек")
            return False
        
        except PeerFlood as e:
            await self.sleep_executor(executor_id, self._current_backoff(executor_id))
            self._increase_backoff(executor_id)
            self.defer_for_executor(executor_id, self.send_text(user_id, text, reply_to, first=first))
            print(f"[POOL] [send_text] [executor {executor_id} -> user {user_id}] Telegram ограничил отправку: {e}")
            return False

        except UserIsBlocked as e:
            await self.db.update_user_param(user_id, 'banned', True)
            print(f"[POOL] [send_text] [executor {executor_id} -> user {user_id}] Исполнитель заблокирован пользователем: {e}")
            return False
        
        except RPCError as e:
            if e.ID == "PRIVACY_PREMIUM_REQUIRED" or "PRIVACY_PREMIUM_REQUIRED" in e.MESSAGE:
                await self.db.rotate_user_down(user_id)
                print(f"[POOL] [send_text] [executor {executor_id} -> user {user_id}] Telegram требует от исполнителя Telegram Premium для действия: {e}")
        
        except Exception as e:
            await self.db.rotate_user_down(user_id)
            print(f"[POOL] [send_text] [executor {executor_id} -> user {user_id}]: {e}")
            return False


    async def send_document(self, user_id: int, path: str, caption: str = "", first: bool = False, bot: Client = None) -> bool:
        """
        Шлёт документ через закреплённого за пользователем исполнителя.
        Использует готовую функцию send_document(bot, user, ...).
        """
        if bot is None:
            executor_id = await self.db.get_user_param(user_id, 'executor_id')
            if executor_id is None:
                print(f"[POOL] [send_document] [user {user_id}] has no executor_id")
                return False

            bot = await self.ensure_client(executor_id)
            if not bot:
                print(f"[POOL] [send_document] [user {user_id}] executor '{executor_id}' not connected")
                return False
        else:
            me = await bot.get_me()
            executor_id = me.id

        if self.is_sleeping(executor_id):
            self.defer_for_executor(executor_id, self.send_document(user_id, path, caption, first=first))
            return False

        user = await self.connect_user(bot, user_id)

        try:
            ok = await send_document(bot, user, path=path, caption=caption, first=first)
            await self.db.executor_timestamp(executor_id)
            return ok
        
        except FloodWait as e:
            await self.sleep_executor(executor_id, float(e.value))
            self.defer_for_executor(executor_id, self.send_document(user_id, path, caption, first=first))
            print(f"[POOL] [send_document] [executor {executor_id} -> user {user_id}] FloodWait: ждём {e.value} сек")
            return False
        
        except PeerFlood as e:
            await self.sleep_executor(executor_id, self._current_backoff(executor_id))
            self._increase_backoff(executor_id)
            self.defer_for_executor(executor_id, self.send_document(user_id, path, caption, first=first))
            print(f"[POOL] [send_document] [executor {executor_id} -> user {user_id}] Telegram ограничил отправку: {e}")
            return False

        except UserIsBlocked as e:
            await self.db.update_user_param(user_id, 'banned', True)
            print(f"[POOL] [send_document] [executor {executor_id} -> user {user_id}] Исполнитель заблокирован пользователем: {e}")
            return False
        
        except RPCError as e:
            if e.ID == "PRIVACY_PREMIUM_REQUIRED" or "PRIVACY_PREMIUM_REQUIRED" in e.MESSAGE:
                await self.db.rotate_user_down(user_id)
                print(f"[POOL] [send_document] [executor {executor_id} -> user {user_id}] Telegram требует от исполнителя Telegram Premium для действия: {e}")

        except Exception as e:
            await self.db.rotate_user_down(user_id)
            print(f"[POOL] [send_document] [executor {executor_id} -> user {user_id}]: {e}")
            return False
