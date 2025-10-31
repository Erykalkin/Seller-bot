from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
from typing import Optional, Dict, List, Tuple
from sqlalchemy.orm import declarative_base
from contextlib import asynccontextmanager
import time
from .base import Base
from .users import UsersRepo
from .executors import ExecutorsRepo


class DatabaseController:
    def __init__(self, db_url: str, echo: bool = False):
        self.engine = create_async_engine(db_url, echo=echo, future=True)
        self.Session = async_sessionmaker(self.engine, expire_on_commit=False, class_=AsyncSession)

    async def init_db(self):
        async with self.engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

    async def close(self):
        await self.engine.dispose() 

    @asynccontextmanager
    async def session(self):
        s = self.Session()
        try:
            yield s
        finally:
            await s.close()

    @asynccontextmanager
    async def users(self):
        async with self.session() as s:
            yield UsersRepo(s)

    @asynccontextmanager
    async def executors(self):
        async with self.session() as s:
            yield ExecutorsRepo(s)

    # ===========================
    # Executors
    # ===========================

    async def get_executor(self, *, executor_id=None, name=None):
        async with self.executors() as executors_repo:
            return await executors_repo.get_executor(executor_id=executor_id, name=name)
        

    async def update_executor_param(self, executor_id: int, column: str, value):
        async with self.executors() as executors_repo:
            await executors_repo.update_param(key='executor_id', target=executor_id, column=column, value=value)


    async def executors_table(self, limit: int = 20, order_by: str = None, asc: bool = True, columns: List[str] = None) -> str:
        async with self.executors() as executors_repo:
            table = await executors_repo.show_table(limit, order_by, asc, columns)
            return table


    async def executor_timestamp(self, executor_id: int, *, column: str = "last_message", ts: int = None) -> int:
        """
        Поставить исполнителю временную метку в колонку `column` (по умолчанию last_message).
        Возвращает установленное значение (UNIX ts).
        """
        value = int(time.time()) if ts is None else int(ts)
        async with self.executors() as executors_repo:
            await executors_repo.update_param(key="executor_id", target=executor_id, column=column, value=value)
        return value

    # ===========================
    # Users
    # ===========================

    async def add_user(self, **kwargs) -> int:
        async with self.users() as users_repo:
            uid = await users_repo.add_user(**kwargs)
        return uid


    async def delete_user(self, *, user_id: int = None, username: str = None) -> bool:
        async with self.users() as users_repo:
            return await users_repo.delete_user(user_id=user_id, username=username)


    async def update_user_param(self, user_id: int, column: str, value):
        async with self.users() as users_repo:
            await users_repo.update_param(key='user_id', target=user_id, column=column, value=value)

    
    async def get_user_param(self, user_id: int, column: str):
        async with self.users() as users_repo:
            return await users_repo.get_param(key='user_id', target=user_id, column=column)


    async def assign_executor(self, user_id: int, executor_id: int = None, max_retries: int = 5, sleep: float = 0.5) -> int:
        async with self.users() as users_repo:
            return await users_repo.assign_executor(user_id, executor_id, max_retries, sleep)

    
    async def users_table(self, limit: int = 20, order_by: str = None, asc: bool = True, columns: List[str] = None) -> str:
        async with self.users() as users_repo:
            table = await users_repo.show_table(limit, order_by, asc, columns)
            return table


    async def user_timestamp(self, user_id: int, *, column: str = "last_message", ts: int = None,) -> int:
        """
        Поставить пользователю временную метку в колонку `column` (по умолчанию last_message).
        Возвращает установленное значение (UNIX ts).
        """
        value = int(time.time()) if ts is None else int(ts)
        async with self.users() as users_repo:
            await users_repo.update_param(key="user_id", target=user_id, column=column, value=value)
        return value


    async def rotate_user_down(self, user_id: int):
        async with self.users() as users_repo:
            await users_repo.rotate_user_down(user_id)
