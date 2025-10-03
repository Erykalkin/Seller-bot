import sqlite3
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


class Logs(Base):
    __tablename__ = "logs"

    id = Column(Integer, primary_key=True)
    role = Column(String)
    tymestamp = Column(Integer)
    type = Column(String) # Error/log/Message
    content = Column(String)



class LogsRepo:
    def __init__(self, session: AsyncSession, model: DeclarativeMeta):
        self.session = session
        self.model = model