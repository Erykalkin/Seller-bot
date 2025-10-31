# Методы botpool для работы с исполнителями

import asyncio
from decouple import config
from pyrogram import Client
from pyrogram.errors import SessionPasswordNeeded
from contextlib import suppress
import contextlib


async def connect_executor(self, *, executor_id: int = None, name: str = None, **kwargs) -> Client:
    """
    Возвращает pyrogram.Client
    """
    async with self.db.executors() as executors_repo:
        obj = await executors_repo.get_one_by_one_of(executor_id=executor_id, name=name)

        proxy = None

        if obj:
            if obj.proxy_ip and obj.proxy_port != 0:
                proxy = {
                    "hostname": obj.proxy_ip,
                    "port": obj.proxy_port,
                    "scheme": obj.proxy_type,
                    "username": obj.proxy_user,
                    "password": obj.proxy_pass,
                }
            bot = Client(
                name = f"session_{name}_{obj.proxy_port  or 'noproxy'}",
                api_id=obj.api_id,
                api_hash=obj.api_hash,
                session_string=obj.session_string,
                **({"proxy": proxy} if proxy else {}),
            )
            return bot

        if kwargs.get('api_id') is None or kwargs.get('api_hash') is None:
            return None
        
        port = kwargs.get('proxy_port') or 10001
        
        if port != 0:
            proxy = {
                "hostname": kwargs.get('proxy_ip') or config('PROXY_IP'),
                "port": port,
                "scheme": kwargs.get('proxy_type') or 'http',
                "username": kwargs.get('proxy_user') or config('PROXY_USER'),
                "password": kwargs.get('proxy_pass') or config('PROXY_PASS'),
            }

        if kwargs.get('session_string'):
            bot = Client(
                api_id = kwargs.get('api_id'),
                api_hash = kwargs.get('api_hash'),
                session_string = kwargs.get('session_string'),
                **({"proxy": proxy} if proxy else {}),
            )
            return bot

        if kwargs.get('phone'):
            bot = Client(
                name = f"session_{name}_{port}",
                api_id = kwargs.get('api_id'),
                api_hash = kwargs.get('api_hash'),
                phone_number = kwargs.get('phone'),
                **({"proxy": proxy} if proxy else {}),
            )
            return bot
        
        return None


async def create_session(self, *, phone: str, api_id: int, api_hash: str, name: str = None, **kwargs) -> tuple[int, str]:
    """
    Создаёт временный клиент, вызывает send_code, ждёт ручной ввод кода и (если нужно) 2FA-пароля,
    затем экспортирует session_string.
    Возвращает пару (executor_id, session_string).
    """
    bot = await self.connect_executor(name=name, api_id=api_id, api_hash=api_hash, phone=phone, **kwargs)
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


async def reload_executor(
        self,
        *,
        executor_id: int = None,
        name: str  = None,
        proxy_port: int = None,
        test_connection: bool = True,
    ) -> bool:
        """
        Перезагружает исполнителя: обновляет session_string.
        Меняет прокси, если указан proxy_port.
        """
        async with self.db.executors() as executors_repo:
            row = await executors_repo.get_one_by_one_of(executor_id=executor_id, name=name)
            if row is None:
                print("[POOL][reload_executor] Исполнитель не найден")
                return False
            
            executor_id = row.executor_id
            name = name or row.name
            phone = row.phone
            api_id  = row.api_id
            api_hash = row.api_hash

            new_proxy_port = row.proxy_port if proxy_port is None else proxy_port

        eid, new_session = await self.create_session(
            phone=phone, api_id=api_id, api_hash=api_hash, name=name, proxy_port=new_proxy_port
        )

        if test_connection:
            try:
                test_bot = await self.connect_executor(
                    name=name,
                    api_id=api_id,
                    api_hash=api_hash,
                    session_string=new_session,
                    proxy_port=new_proxy_port,
                )
                async with test_bot:
                    me = await test_bot.get_me()
                    print(f"[POOL] [reload_executor] Проверка новой сессии для execotor_id = {me.id} прошла")
            except Exception as e:
                print(f"[POOL] [reload_executor] Проверка новой сессии не прошла: {e}")
                await self.db.update_executor_param(executor_id, "status", "proxy_or_auth_failed")
                return False

        old_cli = self._clients.pop(executor_id, None)
        if old_cli is not None:
            with contextlib.suppress(Exception):
                if getattr(old_cli, "is_connected", False) or getattr(old_cli, "is_initialized", False):
                    await old_cli.stop()

        await self.db.update_executor_param(executor_id, "session_string", new_session)
        if proxy_port is not None: await self.db.update_executor_param(executor_id, "proxy_port", new_proxy_port)

        cli = await self.ensure_client(executor_id)
        if cli and getattr(cli, "is_connected", False):
            await self.db.update_executor_param(executor_id, "status", "active")
            return True

        await self.db.update_executor_param(executor_id, "status", "disconnected")
        return False


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