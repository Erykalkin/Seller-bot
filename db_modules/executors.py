import asyncio
import time
from typing import Optional, List, Dict, Any
from sqlalchemy import (
    Column, Integer, String, Text, UniqueConstraint, select, update, delete
)
from sqlalchemy.ext.asyncio import (
    create_async_engine, async_sessionmaker, AsyncSession
)
from sqlalchemy.orm import declarative_base, relationship
from .base import BaseRepo, Base 
from decouple import Config, RepositoryEnv
from pyrogram import Client
from pyrogram.types import User as PyroUser
from typing import TYPE_CHECKING
import random

if TYPE_CHECKING:
    from .users import User

config=Config(RepositoryEnv('/home/appuser/Seller/.env'))


class Executor(Base):
    __tablename__ = "executors"

    executor_id    = Column(Integer, primary_key=True)
    name           = Column(String, nullable=False, unique=True)
    phone          = Column(String)
    api_id         = Column(Integer, nullable=False)
    api_hash       = Column(String,  nullable=False)
    session_string = Column(Text,    nullable=False, unique=True)
    status         = Column(String)  
    users          = Column(Integer, default=0)
    active_users   = Column(Integer, default=0)
    last_message   = Column(Integer, default=time.time)
    proxy_ip       = Column(String, default=config('PROXY_IP'))
    proxy_port     = Column(Integer)
    proxy_type     = Column(String, default='http')
    proxy_user     = Column(String, default=config('PROXY_USER'))
    proxy_pass     = Column(String, default=config('PROXY_PASS'))

    # Уникальность пары api_id + api_hash
    __table_args__ = (UniqueConstraint("api_id", "api_hash", name="ux_executors_api"),)

    users_list = relationship("User", back_populates="executor")



class ExecutorsRepo(BaseRepo):
    PROXY_MIN = 10001
    PROXY_MAX = 19999
    
    def __init__(self, session):
        super().__init__(session, Executor)

    # ===========================
    # Telegram & Proxy
    # ===========================

    async def get_used_ports(self) -> set[int]:
        stmt = select(self.model.proxy_port).where(self.model.proxy_port.isnot(None))
        res = await self.session.execute(stmt)
        return {row for (row,) in res.all() if row}


    async def get_free_port(self, mode: str = "random") -> int:
        """
        Возвращает свободный порт из диапазона [PROXY_MIN, PROXY_MAX].
        mode:
        - "sequential": первый свободный по порядку (по умолчанию)
        - "random": случайный свободный (без повторов)
        """
        used = await self.get_used_ports()
        all_ports = list(range(self.PROXY_MIN, self.PROXY_MAX + 1))
        free_ports = [p for p in all_ports if p not in used]

        if not free_ports:
            return self.PROXY_MIN

        if mode == "random":
            return random.choice(free_ports)
        elif mode == "sequential":
            return free_ports[0]
        else:
            print(f"Unknown mode: {mode!r}. Use 'sequential' or 'random'.")
            return self.PROXY_MIN


    # async def connect_executor(self, *, executor_id: int = None, name: str = None) -> Client:
    #     """
    #     Возвращает pyrogram.Client
    #     """
    #     cond = self._where_one_of(executor_id=executor_id, name=name)
    #     row = await self.session.scalar(select(self.model).where(cond))
    #     if not row:
    #         print("Исполнитель не найден")

    #     bot = Client(
    #         name=row.name,
    #         api_id=row.api_id,
    #         api_hash=row.api_hash,
    #         session_string=row.session_string,
    #         proxy={
    #             "scheme": row.proxy_type,
    #             "hostname": row.proxy_ip,
    #             "port": row.proxy_port,
    #             "username": row.proxy_user,
    #             "password": row.proxy_pass,
    #         }
    #     )
    #     return bot

    # ===========================
    # CRUD
    # ===========================

    async def add_executor(self, name: str, api_id: int, api_hash: str, **kwargs) -> int:
        if not name or api_id is None or not api_hash:
            print("Параметры 'name', 'api_id' и 'api_hash' обязательны")
            return
        
        session_string = kwargs.get("session_string")

        # проверки на дубликаты
        if await self.exists_by(name=name):
            print(f"Исполнитель '{name}' уже существует")
            return
        if session_string and await self.exists_by(session_string=session_string):
            print("Такой session_string уже существует")
            return

        q = select(self.model.executor_id).where(
            (self.model.api_id == api_id) &
            (self.model.api_hash == api_hash)
        ).limit(1)
        if await self.session.scalar(q):
            print("Пара api_id+api_hash уже существует")
            return
        
        if not kwargs.get("proxy_port"):
            kwargs["proxy_port"] = await self.get_free_port()
        
        obj = self.model(name=name, api_id=api_id, api_hash=api_hash, **kwargs)
        self.session.add(obj)
        await self.session.commit()
        await self.session.refresh(obj)
        return int(obj.executor_id)

        # if not kwargs.get("session_string"):
            

        # if not kwargs.get("executor_id"):
        #     bot = await self.connect_executor(name=kwargs["name"])
        #     async with bot:
        #         me = await bot.get_me()
        #     await self.update_param(key='name', target=kwargs["name"], column='executor_id', value=me.id)
        #     await self.update_param(key='name', target=kwargs["name"], column='phone', value=me.phone_number)
        # return int(obj.executor_id)


    async def delete_executor(self, *, executor_id=None, name=None) -> bool:
        return (await self.delete_by_one_of(executor_id=executor_id, name=name)) > 0

    # ===========================
    # Queries
    # ===========================

    async def has_executor(self, *, executor_id=None, name=None) -> bool:
        return await self.exists_by_one_of(executor_id=executor_id, name=name)


    async def get_executor(self, *, executor_id=None, name=None) -> dict:
        obj = await self.get_one_by_one_of(executor_id=executor_id, name=name)
        return self.to_dict(obj)


    async def get_executors(self) -> list[dict]:
        return await self.get_all(order_by="name", asc=True)
    

    async def get_ids(self) -> List[int]:
        """
        озвращает список всех executor_id
        """
        stmt = select(self.model.executor_id)
        res = await self.session.execute(stmt)
        return [exid for (exid,) in res.all()]

    # ===========================
    # Users
    # ===========================
    
    async def pick_least_loaded(self) -> Executor:
        stmt = (
            select(Executor)
            .where(Executor.status == "active")
            .order_by(Executor.active_users.asc(), Executor.executor_id.asc())
            .limit(1)
        )
        res = await self.session.execute(stmt)
        return res.scalars().first()

    # безопасно увеличить active_users на 1 (с защитой от гонок)
    async def try_inc_active(self, executor_id: int, expected_active: int) -> bool:
        stmt = (
            update(Executor)
            .where(
                (Executor.executor_id == executor_id) &
                (Executor.active_users == expected_active)
            )
            .values(active_users=Executor.active_users + 1, users=Executor.users + 1)
        )
        res = await self.session.execute(stmt)
        return bool(res.rowcount and res.rowcount > 0)

    # симметричное уменьшение
    async def dec_active(self, executor_id: int) -> None:
        stmt = (
            update(Executor)
            .where(Executor.executor_id == executor_id, Executor.active_users > 0, Executor.users > 0)
            .values(active_users=Executor.active_users - 1, users=Executor.users - 1)
        )
        await self.session.execute(stmt)