import asyncio, json, random, time
from typing import Optional, Dict, List, Tuple
from pydantic import BaseModel
from decouple import config
from pyrogram import Client
from pyrogram.errors import FloodWait, PeerFlood, SessionPasswordNeeded
from telegram.senders import*
from typing import Awaitable
from contextlib import suppress
import uuid
from decouple import Config, RepositoryEnv
from .basepool import BasePool
from pyrogram.types import User as PyroUser
from pyrogram.raw.types import User as RawUser

from db_modules.controller import DatabaseController
db = DatabaseController("sqlite+aiosqlite:///./data/app.db", echo=False)

config=Config(RepositoryEnv('/home/appuser/Seller/.env'))


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
    def __init__(self, db: DatabaseController, *, initial_backoff: float = 60.0,
                 backoff_factor: float = 2.0, max_backoff: float = 24*3600.0):
        
        super().__init__(initial_backoff=initial_backoff, backoff_factor=backoff_factor, max_backoff=max_backoff)
        
        self.db = db

        self._clients: Dict[int, Client] = {}      # кеш клиентов: executor_id -> Client
        self._handlers: List = []                  # общие хэндлеры (навешиваются на каждый клиент при connect_executor)


    async def executors_table(self, limit: int = 20, order_by: str = None, asc: bool = True, columns: List[str] = None) -> str:
        async with self.db.executors() as executors_repo:
            table = await executors_repo.show_table(limit, order_by, asc, columns)
            return table
        
    
    async def users_table(self, limit: int = 20, order_by: str = None, asc: bool = True, columns: List[str] = None) -> str:
        async with self.db.users() as users_repo:
            table = await users_repo.show_table(limit, order_by, asc, columns)
            return table

    
    async def activate(self):
        async with self.db.executors() as executors_repo:
            executors = await executors_repo.get_ids()
        for executor_id in executors:
            await self.ensure_client(executor_id)
        
        print("Все клиенты активированы")
        while True:
            await asyncio.sleep(3600)

    # ===========================
    # Executors
    # ===========================
    
    async def connect_executor(self, *, executor_id: int = None, name: str = None, **kwargs) -> Client:
        """
        Возвращает pyrogram.Client
        """
        async with self.db.executors() as executors_repo:
            obj = await executors_repo.get_one_by_one_of(executor_id=executor_id, name=name)

            if obj:
                if obj.proxy_ip and obj.proxy_port:
                    proxy = {
                        "hostname": obj.proxy_ip,
                        "port": kwargs.get('proxy_port') or obj.proxy_port,
                        "scheme": obj.proxy_type,
                        "username": obj.proxy_user,
                        "password": obj.proxy_pass,
                    }
                bot = Client(
                    name=obj.name,
                    api_id=obj.api_id,
                    api_hash=obj.api_hash,
                    session_string=obj.session_string,
                    proxy=proxy
                )
                return bot

            if kwargs.get('api_id') is None or kwargs.get('api_hash') is None:
                    return None
            
            proxy = {
                "hostname": kwargs.get('proxy_ip') or config('PROXY_IP'),
                "port": kwargs.get('proxy_port') or 10001,
                "scheme": kwargs.get('proxy_type') or 'http',
                "username": kwargs.get('proxy_user') or config('PROXY_USER'),
                "password": kwargs.get('proxy_pass') or config('PROXY_PASS'),
            }

            if kwargs.get('session_string'):
                bot = Client(
                    name = name,
                    api_id = kwargs.get('api_id'),
                    api_hash = kwargs.get('api_hash'),
                    session_string = kwargs.get('session_string'),
                    proxy = proxy
                )
                return bot

            if kwargs.get('phone'):
                bot = Client(
                    name = name,
                    api_id = kwargs.get('api_id'),
                    api_hash = kwargs.get('api_hash'),
                    phone_number = kwargs.get('phone'),
                    proxy = proxy
                )
                return bot
            
            return None

    
    async def create_session(self, *, phone: str, api_id: int, api_hash: str, name: str = None, **kwargs) -> tuple[int, str]:
        """
        Создаёт временный клиент, вызывает send_code, ждёт ручной ввод кода и (если нужно) 2FA-пароля,
        затем экспортирует session_string.
        Возвращает пару (executor_id, session_string).
        """
        tmp_name = f"tmp_session_{uuid.uuid4().hex}"
        bot = await self.connect_executor(name=':memory:', api_id=api_id, api_hash=api_hash, phone=phone, **kwargs)
        await bot.connect()

        try:
            sent = await bot.send_code(phone)
            loop = asyncio.get_running_loop()
            code_prompt = f"Введите код для {phone}: "
            code = await loop.run_in_executor(None, input, code_prompt)
            code = code.strip()

            try:
                await bot.sign_in(phone_number=phone, phone_code_hash=sent.phone_code_hash, phone_code=code)
            except SessionPasswordNeeded:
                pwd_prompt = "Введите пароль двухфакторной аутентификации: "
                password = await loop.run_in_executor(None, input, pwd_prompt)
                await bot.check_password(password)

            session_string = await bot.export_session_string()

            me = await bot.get_me()

            return me.id, session_string
        finally:
            await bot.disconnect()


    async def add_executor(self, *, name: str, api_id: int, api_hash: str, phone: str = None, session_string: str = None, **kwargs) -> int:
        """
        Записывает исполнителя в БД.
        Если передан session_string — пробует применить его и подтянуть недостающие поля (executor_id/phone).
        Если session_string не передан — вызывает create_session (ждёт код подтверждения).
        Возвращает executor_id.
        """
        async with self.db.executors() as executors_repo:
            port = kwargs.get('proxy_port') or await executors_repo.get_free_port()

        if session_string:
            try:
                bot = await self.connect_executor(name=name, api_id=api_id, api_hash=api_hash, session_string=session_string, proxy_port=port, **kwargs)
                async with bot:
                    me = await bot.get_me()
                    eid = me.id
                    phone = me.phone_number
                
                async with self.db.executors() as executors_repo:
                    await executors_repo.add_executor(name, api_id, api_hash, executor_id=eid, phone=phone, session_string=session_string, proxy_port=port, **kwargs)

            except Exception as e:
                print("Failed to connect with provided session_string:", e)
                return

            return eid

        if not phone:
            print("Для активации необходим session_string или phone.")
            return

        eid, session_string = await self.create_session(phone=phone, api_id=api_id, api_hash=api_hash, name=name, proxy_port=port)

        async with self.db.executors() as executors_repo:
            await executors_repo.add_executor(name, api_id, api_hash, executor_id=eid, phone=phone, session_string=session_string, proxy_port=port, **kwargs)

        return eid
    

    async def delete_executor(self, *, executor_id=None, name=None) -> bool:
        if executor_id is None and name is not None:
            async with self.db.executors() as executors_repo:
                row = await executors_repo.get_one_by(name=name)
                executor_id = getattr(row, "executor_id", None)
                if executor_id is None:
                    print("Такого исполнителя нет")
                    return 0

        if executor_id is None:
            return 0

        client = self._clients.pop(executor_id, None)
        if client is not None:
            with suppress(Exception):
                if getattr(client, "is_connected", False) or getattr(client, "is_initialized", False):
                    await client.stop()

        drainer = self._drainers.pop(executor_id, None)
        if drainer is not None:
            with suppress(Exception):
                drainer.cancel()
                await asyncio.gather(drainer, return_exceptions=True)

        q = self._queues.pop(executor_id, None)
        if q is not None:
            with suppress(Exception):
                while not q.empty():
                    _ = q.get_nowait()
                    if isinstance(_, asyncio.Task):
                        _.cancel()

        evt = self._sleep_events.pop(executor_id, None)
        if evt is not None:
            with suppress(Exception):
                evt.set()
                
        self._sleep_until.pop(executor_id, None)

        self._backoffs.pop(executor_id, None)

        self._locks.pop(executor_id, None)

        async with self.db.executors() as executors_repo:
            deleted = await executors_repo.delete_executor(executor_id=executor_id)
    
            
    def add_handler(self, handler) -> None:
        """Регистрирует общий хэндлер на все будущие клиенты + уже подключённых."""
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
                    print(e)

            if cli.is_connected:
                async with self.db.executors() as executors_repo:
                    await executors_repo.update_param(key='executor_id', target=executor_id, column='status', value='active')

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
        async with self.db.users() as users_repo:
            if bot is None:
                executor_id = await users_repo.get_user_param(user_id, 'executor_id')
                if executor_id is None:
                    print(f"[POOL] user {user_id} has no executor_id")
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
            await db.executor_timestamp(executor_id)
            return ok
        
        except FloodWait as e:
            await self.sleep_executor(executor_id, float(e.value))
            self.defer_for_executor(executor_id, self.send_text(user_id, text, reply_to, first=first))
            return False
        
        except PeerFlood:
            await self.sleep_executor(executor_id, self._current_backoff(executor_id))
            self._increase_backoff(executor_id)
            self.defer_for_executor(executor_id, self.send_text(user_id, text, reply_to, first=first))
            return False


    async def send_document(self, user_id: int, path: str, caption: str = "", first: bool = False, bot: Client = None) -> bool:
        """
        Шлёт документ через закреплённого за пользователем исполнителя.
        Использует готовую функцию send_document(bot, user, ...).
        """
        async with self.db.users() as users_repo:
            if bot is None:
                executor_id = await users_repo.get_user_param(user_id, 'executor_id')
                if executor_id is None:
                    print(f"[POOL] user {user_id} has no executor_id")
                    return False

                bot = await self.ensure_client(executor_id)
                if not bot:
                    print(f"[POOL] executor '{executor_id}' not connected")
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
            await db.executor_timestamp(executor_id)
            return ok
        
        except FloodWait as e:
            await self.sleep_executor(executor_id, float(e.value))
            self.defer_for_executor(executor_id, self.send_document(user_id, path, caption, first=first))
            return False
        
        except PeerFlood:
            await self.sleep_executor(executor_id, self._current_backoff(executor_id))
            self._increase_backoff(executor_id)
            self.defer_for_executor(executor_id, self.send_document(user_id, path, caption, first=first))
            return False
        

    # ===========================
    # Users
    # ===========================

    async def add_user(self, user_id: int, executor_id: int = None, access_hash: int = None, info: str = None, **kwargs) -> int:
        """
        Добавляет пользователя в БД (через UsersRepo.add_user),
        назначает executor (если нужно), получает username/phone/access_hash через Pyrogram
        и обновляет запись в БД.
        Возвращает user_id.
        """
        async with self.db.users() as users_repo:
            await users_repo.add_user(user_id=user_id, executor_id=executor_id, access_hash=access_hash, info=info, **kwargs)

        assigned_executor = executor_id
        async with self.db.users() as users_repo:
            assigned_executor = await users_repo.assign_executor(user_id, executor_id)

        if assigned_executor is None:
            return user_id

        bot = await self.ensure_client(assigned_executor)
        if not bot:
            async with self.db.users() as users_repo:
                await users_repo.update_param(key='user_id', target=user_id, column='executor_id', value=assigned_executor)
            return user_id

        try:
            access_hash = await self.get_access_hash(bot, user_id)
            username = await self.get_username_by_id(bot, user_id, access_hash)
            phone = await self.get_phone_by_id(bot, user_id, access_hash)

            async with self.db.users() as users_repo:
                await users_repo.update_param(key='user_id', target=user_id, column='executor_id', value=assigned_executor)
                if username:
                    await users_repo.update_param(key='user_id', target=user_id, column='username', value=username)
                if phone:
                    await users_repo.update_param(key='user_id', target=user_id, column='phone', value=phone)
                if access_hash:
                    await users_repo.update_param(key='user_id', target=user_id, column='access_hash', value=access_hash)

            if phone:
                more = f"\n\nTG phone NUMBER: {phone}"
                async with self.db.users() as users_repo:
                    cur_info = await users_repo.get_param(key='user_id', target=user_id, column='info') or ""
                    await users_repo.update_param(key='user_id', target=user_id, column='info', value=(cur_info + more))

        except Exception as e:
            print(f"[BotPool.add_user] warning: {user_id}: {e}")

        return user_id
    

    async def connect_user(self, bot: Client, user_id: int, access_hash: int = None) -> PyroUser | RawUser:
        """
        Возвращает pyrogram.types.User при наличии доступа.
        Возвращает pyrogram.raw.types.User при отсутствии доступа, если есть access_hash.
        """
        if access_hash is None:
            async with self.db.users() as users_repo:
                access_hash = await users_repo.get_param_one_of(column='access_hash', user_id=user_id)

        try:
            user = await bot.get_users(user_id)
            if isinstance(user, PyroUser):
                return user
        except Exception:
            pass

        if access_hash:
            try:
                input_user = types.InputUser(user_id=user_id, access_hash=access_hash)
                res = await bot.invoke(functions.users.GetUsers(id=[input_user]))
                return res[0] if res else None
            except Exception:
                return None
        return None


    async def get_access_hash(self, bot: Client, user_id: int) -> int | None:
        """
        Возвращает access_hash из БД, если он там есть.
        Иначе — получает его через resolve_peer после того, как пользователь написал боту.
        """
        async with self.db.users() as users_repo:
            access_hash = await users_repo.get_param_one_of(column='access_hash', user_id=user_id)
            if access_hash:
                return access_hash

        try:
            input_peer = await bot.resolve_peer(user_id)
            if isinstance(input_peer, types.InputPeerUser):
                access_hash = input_peer.access_hash
                async with self.db.users() as users_repo:
                    await users_repo.update_param(key='user_id', target=user_id, column='access_hash', value=access_hash)
                return access_hash
        except Exception:
            return None
        return None


    async def get_username_by_id(self, bot: Client, user_id: int, access_hash: int = None) -> str:
        """
        Получает username по user_id. Если его нет, вернёт телефон или "User_id_xxx"
        """
        try:
            user = await self.connect_user(bot, user_id, access_hash)
            if not user:
                return f"User_id_{user_id}"
            if isinstance(user, PyroUser):
                return user.username or (f"+{user.phone_number}" if user.phone_number else f"User_id_{user.id}")
            if isinstance(user, RawUser):
                return user.username or (f"+{user.phone}" if user.phone else f"User_id_{user.id}")
        except Exception:
            return f"User_id_{user_id}"


    async def get_phone_by_id(self, bot: Client, user_id: int, access_hash: int = None) -> str | None:
        try:
            user = await self.connect_user(bot, user_id, access_hash)
            if not user:
                return None
            if isinstance(user, PyroUser):
                return f"+{user.phone_number}" if user.phone_number else None
            if isinstance(user, RawUser):
                return f"+{user.phone}" if user.phone else None
        except Exception:
            return None
        