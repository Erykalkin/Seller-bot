from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
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


    async def user_timestamp(self, user_id: int, *, column: str = "last_message", ts: int = None,) -> int:
        """
        Поставить пользователю временную метку в колонку `column` (по умолчанию last_message).
        Возвращает установленное значение (UNIX ts).
        """
        value = int(time.time()) if ts is None else int(ts)
        async with self.users() as users_repo:
            await users_repo.update_param(key="user_id", target=user_id, column=column, value=value)
        return value

    async def executor_timestamp(self, executor_id: int, *, column: str = "last_message", ts: int = None) -> int:
        """
        Поставить исполнителю временную метку в колонку `column` (по умолчанию last_message).
        Возвращает установленное значение (UNIX ts).
        """
        value = int(time.time()) if ts is None else int(ts)
        async with self.executors() as executors_repo:
            await executors_repo.update_param(key="executor_id", target=executor_id, column=column, value=value)
        return value