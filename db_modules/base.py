from typing import Any, Iterable, List, Dict, Optional
from tabulate import tabulate
import os
from sqlalchemy.orm import DeclarativeMeta
from sqlalchemy import select, update, delete
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession
from sqlalchemy import Column, Integer, String, Text, UniqueConstraint
from sqlalchemy.orm import declarative_base
from sqlalchemy import and_

Base = declarative_base()

class BaseRepo:
    def __init__(self, session: AsyncSession, model: DeclarativeMeta):
        self.session = session
        self.model = model

    # ===========================
    # Utils
    # ===========================

    @property
    def _columns(self) -> List[str]:
        return list(self.model.__table__.columns.keys())


    def to_dict(self, obj) -> Dict[str, Any]:
        if obj is None:
            return {}
        return {c: getattr(obj, c) for c in self._columns}


    def to_dict_many(self, objs: Iterable) -> List[Dict[str, Any]]:
        return [self.to_dict(o) for o in objs]


    def _where_by(self, **filters):
        """
        Строит where-условие по переданным колонкам
        """
        if not filters:
            raise ValueError("Нужно передать хотя бы один фильтр")
        conds = []
        for k, v in filters.items():
            if k not in self._columns:
                raise ValueError(f"Недопустимое имя колонки: {k}")
            conds.append(getattr(self.model, k) == v)
        return and_(*conds)


    def _where_one_of(self, **filters):
        """
        Ровно одно непустое условие из набора
        """
        filled = {k: v for k, v in filters.items() if v is not None}
        if len(filled) != 1:
            names = ", ".join(filters.keys())
            raise ValueError(f"Можно использовать ровно один из параметров: {names}")
        return self._where_by(**filled)
    

    async def show_table(self, limit: int = 20, order_by: str = None, asc: bool = True, columns: List[str] = None) -> str:
        """
        Возвращает табличку (string) с первыми `limit` строками таблицы.
        """
        rows = await self.get_all(order_by=order_by, asc=asc)
        if not rows:
            return f"Table {self.model.__tablename__} is empty"

        headers = list(rows[0].keys())
        if columns:
            bad = [c for c in columns if c not in headers]
            if bad:
                raise ValueError(f"Unknown columns: {bad}. Available: {headers}")
            headers = columns

        data = [[row[h] for h in headers] for row in rows[:limit]]
        return tabulate(data, headers=headers, tablefmt="grid")
        
    # ===========================
    # CRUD
    # ===========================
    
    async def update_param(self, *, key: str, target, column: str, value):
        """
        Обновляет значение определённого параметра у key = target.
        """
        if key not in self._columns:
            raise ValueError(f"Недопустимое имя колонки: {key}")
        if column not in self._columns:
            raise ValueError(f"Недопустимое имя колонки: {column}")

        stmt = (
            update(self.model)
            .where(getattr(self.model, key) == target)
            .values({column: value})
        )
        await self.session.execute(stmt)
        await self.session.commit()


    async def delete_by(self, **filters) -> int:
        """
        Возвращает кол-во удалённых строк
        """
        stmt = delete(self.model).where(self._where_by(**filters))
        res = await self.session.execute(stmt)
        await self.session.commit()
        return int(res.rowcount or 0)
    

    async def delete_by_one_of(self, **one_filter) -> int:
        stmt = delete(self.model).where(self._where_one_of(**one_filter))
        res = await self.session.execute(stmt)
        await self.session.commit()
        return int(res.rowcount or 0)

    # ===========================
    # Queries
    # ===========================

    async def get_one_by(self, **filters):
        stmt = select(self.model).where(self._where_by(**filters))
        res = await self.session.execute(stmt)
        print(res)
        return res.scalar_one_or_none()
    

    async def get_one_by_one_of(self, **one_filter):
        stmt = select(self.model).where(self._where_one_of(**one_filter))
        res = await self.session.execute(stmt)
        return res.scalar_one_or_none()
    

    async def get_param(self, *, key: str, target, column: str):
        """
        Возвращает значение поля column у записи, где key == target
        """
        if key not in self._columns:
            raise ValueError(f"Недопустимое имя колонки (key): {key}")
        if column not in self._columns:
            raise ValueError(f"Недопустимое имя колонки (column): {column}")

        stmt = select(getattr(self.model, column)).where(self._where_by(**{key: target})).limit(1)
        return await self.session.scalar(stmt)
    

    async def get_param_one_of(self, *, column: str, **filters):
        """
        Возвращает значение поля column у записи, где совпадает ровно один фильтр
        """
        if column not in self._columns:
            raise ValueError(f"Недопустимое имя колонки (column): {column}")

        cond = self._where_one_of(**filters)
        stmt = select(getattr(self.model, column)).where(cond).limit(1)
        return await self.session.scalar(stmt)


    async def get_all(self, order_by: str = None, asc: bool = True) -> List[Dict[str, Any]]:
        if order_by and order_by not in self._columns:
            raise ValueError(f"Недопустимое имя колонки для сортировки: {order_by}")
        stmt = select(self.model)
        if order_by:
            col = getattr(self.model, order_by)
            stmt = stmt.order_by(col.asc() if asc else col.desc())
        res = await self.session.execute(stmt)
        return self.to_dict_many(res.scalars().all())
    

    async def exists_by(self, **filters) -> bool:
        stmt = select(self.model.__table__.c[next(iter(filters))]).where(self._where_by(**filters)).limit(1)
        return (await self.session.scalar(stmt)) is not None


    async def exists_by_one_of(self, **one_filter) -> bool:
        cond = self._where_one_of(**one_filter)
        first_col_name = self._columns[0]
        stmt = select(getattr(self.model, first_col_name)).where(cond).limit(1)
        return (await self.session.scalar(stmt)) is not None
