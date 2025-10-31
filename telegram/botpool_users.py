# Методы botpool для работы с пользователями

import asyncio
from pyrogram import Client
from pyrogram.types import User as PyroUser
from pyrogram.raw.types import User as RawUser
from pyrogram.raw.types import InputUser
from pyrogram.raw.functions.users import GetUsers
from .botpool_utils import get_hash_via_discussion, get_access_hash_from_user_id


async def add_user(self, *, user_id: int, executor_id: int = None, 
                    access_hash: int = None, link: str = None,
                    info: str = None, **kwargs) -> int:
    """
    Добавляет пользователя в БД (через UsersRepo.add_user),
    назначает исполнителя, получает username/phone/access_hash через Pyrogram
    и обновляет запись в БД.
    Возвращает executor_id назначенного исполнителя.
    """
    await self.db.add_user(user_id=user_id, executor_id=executor_id, access_hash=access_hash, info=info, **kwargs)

    assigned_executor = executor_id
    assigned_executor = await self.db.assign_executor(user_id, executor_id)

    if assigned_executor is None:
        return user_id

    bot = await self.ensure_client(assigned_executor)
    if not bot:
        await self.db.update_user_param(target=user_id, column='executor_id', value=assigned_executor)
        return user_id

    try:
        access_hash = access_hash or await self.get_access_hash(bot, user_id, link=link)
        username = kwargs.get('username') or await self.get_username_by_id(bot, user_id, access_hash)
        phone = kwargs.get('phone') or await self.get_phone_by_id(bot, user_id, access_hash)

        await self.db.update_user_param(user_id, 'executor_id', assigned_executor)
        if username:
            await self.db.update_user_param(user_id, 'username', username)
        if phone:
            await self.db.update_user_param(user_id, 'phone', phone)
        if access_hash:
            await self.db.update_user_param(user_id, 'access_hash', access_hash)

        if phone:
            more = f"\n\nTG phone NUMBER: {phone}"
            cur_info = await self.db.get_user_param(user_id, 'info') or ""
            await self.db.update_user_param(user_id, 'info', (cur_info + more))

    except Exception as e:
        print(f"[POOL] [add_user] [user {user_id}] {e}")

    return assigned_executor


async def connect_user(self, bot: Client, user_id: int, access_hash: int = None) -> PyroUser | RawUser:
    """
    Возвращает pyrogram.types.User при наличии доступа.
    Возвращает pyrogram.raw.types.User при отсутствии доступа, если есть access_hash.
    """
    if access_hash is None:
        access_hash = await self.db.get_user_param(user_id, 'access_hash')

    try:
        user = await bot.get_users(user_id)
        if isinstance(user, PyroUser):
            return user
    except Exception as e:
        if access_hash is None:
            print(f"[POOL] [connect_user] [user {user_id}]: {e}")
        pass

    if access_hash:
        try:
            input_user = InputUser(user_id=user_id, access_hash=access_hash)
            res = await bot.invoke(GetUsers(id=[input_user]))
            return res[0] if res else None

        except Exception as e:
            print(f"[POOL] [connect_user raw] [user {user_id}]: {e}")
            return None
    return None


async def get_access_hash(self, bot: Client, user_id: int, *, link: str | None = None) -> int | None:
    """
    Возвращает access_hash.
    Порядок получения:
    1. Если указана ссылка на сообщение в чате, то получает через нее.
    2. Поиск в БД
    3. resolve_peer, если пользователь написал боту
    """
    if link:
        uid, access_hash = await get_hash_via_discussion(bot, link)

    if uid != user_id:
        print(f"[POOL] [get_access_hash] [user {user_id}] не совпали требуемый и найденный user_id = {uid}")
        return None
    if access_hash:
        return access_hash

    access_hash = await self.db.get_user_param(user_id, 'access_hash')
    if access_hash:
        return access_hash

    access_hash = await get_access_hash_from_user_id(bot, user_id)
    return access_hash


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