from sqlalchemy import (
    Column, Integer, String, Text, Boolean, ForeignKey, UniqueConstraint, select, update, case, text
)
from sqlalchemy.orm import declarative_base, relationship
from pyrogram import Client, types
from pyrogram.raw import functions
from .base import BaseRepo, Base
from .executors import ExecutorsRepo, Executor
import asyncio
import time
# from gpt import get_or_create_thread

from typing import TYPE_CHECKING

import logging
import re
import asyncio
import time
from pyrogram import Client, errors
from pyrogram.raw.types import InputPeerUser, InputChannel, Message
from pyrogram.raw.functions.messages import GetDiscussionMessage
from pyrogram.raw.functions.channels import GetParticipant, GetFullChannel
from sqlalchemy import select, update
from .executors import Executor


class User(Base):
    __tablename__ = "users"

    user_id        = Column(Integer, primary_key=True)
    executor_id    = Column(Integer, ForeignKey("executors.executor_id"))
    access_hash    = Column(Integer)
    username       = Column(String, unique=True)
    phone          = Column(String)
    name           = Column(String)
    contact        = Column(Boolean, default=False)
    banned         = Column(Boolean, default=False)
    crm            = Column(Boolean, default=False)
    conversation   = Column(String)
    info           = Column(Text, default="")
    summary        = Column(Text)
    last_message   = Column(Integer, default=time.time)
    problems_count = Column(Integer, default=0)
    problem        = Column(Boolean, default=False)

    executor = relationship("Executor", back_populates="users_list")
    
    

class UsersRepo(BaseRepo):
    def __init__(self, session):
        super().__init__(session, User)
        self.execs = ExecutorsRepo(session)
        
    # ===========================
    # CRUD
    # ===========================

    async def add_user(self, **kwargs) -> int:
        """
        Чистая вставка пользователя в БД.
        """
        user_id = kwargs['user_id']

        if await self.exists_by(user_id=user_id):
            return int(await self.get_param(key='user_id', target=user_id, column='user_id'))

        obj = self.model(**{k: v for k, v in kwargs.items() if k in self._columns})
        self.session.add(obj)
        try:
            await self.session.commit()
            await self.session.refresh(obj)
            return int(obj.user_id)
        except Exception:
            await self.session.rollback()
            existing = await self.session.scalar(select(self.model.user_id).where(self.model.user_id == user_id))
            if existing:
                return int(existing)
            raise
    
    
    async def delete_user(self, *, user_id: int = None, username: str = None) -> bool:
        await self.unassign_executor(user_id)
        return (await self.delete_by_one_of(user_id=user_id, username=username)) > 0


    async def delete_user_by_name(self, username: str) -> bool:
        return (await self.delete_by_one_of(username=username)) > 0


    async def delete_all_users(self) -> int:
        """
        Удаляет всех пользователей из таблицы users с отвязкой исполнителей.
        Возвращает количество удалённых записей.
        """
        stmt = select(self.model.user_id)
        res = await self.session.execute(stmt)
        user_ids = [uid for (uid,) in res.all()]

        count = 0
        for uid in user_ids:
            try:
                ok = await self.delete_user(user_id=uid)
                if ok:
                    count += 1
            except Exception as e:
                print(f"[UsersRepo] Ошибка при удалении user_id={uid}: {e}")

        print(f"[UsersRepo] Удалено {count} пользователей (через delete_user).")
        return count


    async def forget_user(self, user_id: int) -> None:
        """
        Очищает все «рабочие» данные о пользователе, кроме user_id, executor_id, 
        access_hash, username, phone и info.
        """
        stmt = (
            update(self.model)
            .where(self.model.user_id == user_id)
            .values(
                contact=False,
                banned=False,
                crm=False,
                conversation=None,
                summary=None,
                last_message=int(time.time()),
                problems_count=0,
            )
        )
        await self.session.execute(stmt)
        await self.session.commit()

    
    async def update_user_param(self, user_id: int, column: str, value):
        await self.update_param(key='user_id', target=user_id, column=column, value=value)

    
    async def rotate_user_down(self, user_id: int) -> None:
        stmt = (
            update(self.model)
            .where(self.model.user_id == user_id)
            .values(
                problems_count=self.model.problems_count + 1,
                problem=case(
                    (self.model.problems_count + 1 >= 5, True),
                    else_=self.model.problem
                )
            )
        )
        await self.session.execute(stmt)
        await self.session.commit()

    # ===========================
    # Queries
    # ===========================

    async def get_user(self, user_id: int) -> dict:
        """
        озвращает словарь пользователя
        """
        obj = await self.get_one_by(user_id=user_id)
        return self.to_dict(obj) if obj else None


    async def get_users(self) -> list[tuple[int, int, int]]:
        """
        озвращает список (user_id, executor_id, access_hash)
        """
        stmt = select(self.model.user_id, self.model.executor_id, self.model.access_hash)
        res = await self.session.execute(stmt)
        return [(uid, eid, ah) for (uid, eid, ah) in res.all()]


    async def get_ids(self) -> list[int]:
        """
        озвращает список всех user_id
        """
        stmt = select(self.model.user_id)
        res = await self.session.execute(stmt)
        return [uid for (uid,) in res.all()]


    async def has_user(self, user_id: int) -> bool:
        return await self.exists_by(user_id=user_id)
    

    async def get_user_param(self, user_id: int, column: str):
        return await self.get_param(key='user_id', target=user_id, column=column)

    
    async def pop_users_to_greet(self, limit: int = 100) -> list[tuple[int, int, int]]:
        """
        Возвращает до `limit` пользователей на приветствие в формате
        (user_id, executor_id, access_hash), причём у каждого пользователя
        уникальный executor_id (не более одного пользователя на одного исполнителя).
        """
        stmt = (
            select(self.model.user_id, self.model.executor_id, self.model.access_hash)
            .where(
                self.model.contact.is_(False),
                self.model.problem.is_(False),
                self.model.access_hash.is_not(None),
                self.model.executor_id.is_not(None),
            )
            .order_by(self.model.problems_count.asc(), self.model.user_id.asc())
        )
        res = await self.session.execute(stmt)

        picked: list[tuple[int, int, int]] = []
        seen_execs: set[int] = set()

        for uid, eid, ah in res.all():
            if eid in seen_execs:
                continue
            picked.append((uid, eid, ah))
            seen_execs.add(eid)
            if len(picked) >= limit:
                break

        return picked


    async def get_inactive_users(self, interval_seconds: int) -> list[User]:
        """
        Возвращает список пользователей, у которых last_message старше заданного интервала
        """
        cutoff = int(time.time()) - interval_seconds
        stmt = select(self.model).where(
            (self.model.last_message.is_(None)) | (self.model.last_message < cutoff)
        )
        res = await self.session.execute(stmt)
        return res.scalars().all()

    # ===========================
    # Executors
    # ===========================

    async def assign_executor(self, user_id: int, executor_id: int = None, max_retries: int = 5, sleep: float = 0.5) -> int:
        """
        Назначает пользователю исполнителя с минимальным active_users, если не указан конкретный.
        Возвращает executor_id.
        Гонку за «самого свободного» решаем оптимистически (CAS через WHERE active_users=expected).
        """
        user = await self.session.get(self.model, user_id)
        if not user:
            print(f"Пользователь {user_id} не найден")
            return None

        # Явное назначение конкретного исполнителя
        if executor_id is not None:
            if not await self.execs.has_executor(executor_id=executor_id):
                print(f"Исполнитель с id={executor_id} не найден")
                return None

            # «Резервируем» исполнителя через CAS
            expected = await self.session.scalar(
                select(Executor.active_users).where(Executor.executor_id == executor_id)
            )
            if expected is None or not await self.execs.try_inc_active(executor_id, expected_active=expected):
                print("Исполнитель уже занят (гонка).")
                return None

            # Назначаем через связь (ORM сам проставит FK)
            user.executor_id = executor_id
            return executor_id

        # Автовыбор наименее загруженного (CAS-петля)
        for _ in range(max_retries):
            ex = await self.execs.pick_least_loaded()
            if not ex:
                print("Нет доступных исполнителей со статусом 'active'")
                return None

            expected = ex.active_users
            if not await self.execs.try_inc_active(ex.executor_id, expected_active=expected):
                await asyncio.sleep(sleep)
                continue

            user.executor_id = ex.executor_id
            return ex.executor_id

        print("Не удалось назначить исполнителя из-за гонки.")
        return None


    async def unassign_executor(self, user_id: int) -> None:
        """
        Отвязка пользователя от исполнителя: уменьшает users и active_users у текущего исполнителя,
        зануляет executor_id у пользователя.
        """
        # узнаём текущего исполнителя
        stmt = select(self.model.executor_id).where(self.model.user_id == user_id).limit(1)
        current_eid = await self.session.scalar(stmt)
        if not current_eid:
            return  # ничего не делаем

        # уменьшаем active_users, executor.users при желании тоже можно уменьшать
        await self.execs.dec_active(current_eid)

        # отвязываем
        await self.session.execute(
            update(self.model).where(self.model.user_id == user_id).values(executor_id=None)
        )
        await self.session.commit()
