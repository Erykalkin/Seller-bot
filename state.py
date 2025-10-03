"""
Глобальное состояние рантайма бота:
- буферы сообщений и отметки времени
- задачи обработки пользователя и задачи «пуша при неактивности»
- флаги остановки фоновых воркеров
"""

from __future__ import annotations
import asyncio
import time
from collections import defaultdict
from typing import DefaultDict, Dict, List, Optional


# --- Флаги остановки фоновых циклов ---
stop_group_parser = asyncio.Event()
stop_greeter = asyncio.Event()

# Активные задачи
_group_parser_task: Optional[asyncio.Task] = None
_greeter_task: Optional[asyncio.Task] = None

# --- Буферы и задачи по пользователям ---
message_buffers: DefaultDict[int, List[str]] = defaultdict(list)
last_message_times: Dict[int, float] = {}

# Задача «собрать буфер и ответить» для пользователя
user_tasks: Dict[int, asyncio.Task] = {}

# Задача «пинг при неактивности» для пользователя
inactivity_tasks: Dict[int, asyncio.Task] = {}


# --- Утилиты ---

def touch_user(uid: int) -> None:
    """Обновить отметку последнего сообщения пользователя."""
    last_message_times[uid] = time.time()


def append_to_buffer(uid: int, text: str) -> None:
    """Добавить строку в буфер пользователя."""
    message_buffers[uid].append(text)


def cancel_task_safe(task: Optional[asyncio.Task]) -> None:
    """Отменить таск без исключений наружу."""
    if task and not task.done():
        task.cancel()


def cancel_user_task(uid: int) -> None:
    """Отменить и убрать задачу обработки буфера конкретного пользователя."""
    task = user_tasks.pop(uid, None)
    cancel_task_safe(task)


def cancel_inactivity_task(uid: int) -> None:
    """Отменить и убрать задачу неактивности конкретного пользователя."""
    task = inactivity_tasks.pop(uid, None)
    cancel_task_safe(task)


def last_gap(uid: int) -> float:
    """
    Возвращает время (в секундах), прошедшее с момента последнего сообщения пользователя.
    Если данных нет — вернёт большое число (считаем, что давно не писал).
    """
    import time
    return time.time() - last_message_times.get(uid, 0)


def set_inactivity_task(uid: int, task: asyncio.Task) -> None:
    """Ставит новый таймер неактивности для пользователя, отменяя старый."""
    cancel_inactivity_task(uid)
    inactivity_tasks[uid] = task


def pop_buffer(uid: int) -> str:
    """
    Извлекает и очищает буфер сообщений пользователя.
    Возвращает все накопленные строки, объединённые через '=========='
    или пустую строку, если буфера нет.
    """
    lines = message_buffers.pop(uid, [])
    return "\n==========\n".join(lines)


def clear_user_state(uid: int, *, cancel_tasks: bool = True) -> None:
    """
    Полностью очистить состояние пользователя:
    буфер, таймстемп, задачи (опционально).
    """
    if cancel_tasks:
        cancel_user_task(uid)
        cancel_inactivity_task(uid)
    message_buffers.pop(uid, None)
    last_message_times.pop(uid, None)


async def cancel_all_tasks() -> None:
    """Отменить все пользовательские задачи (аккуратно пройтись по копии словарей)."""
    for task in list(user_tasks.values()):
        cancel_task_safe(task)
    for task in list(inactivity_tasks.values()):
        cancel_task_safe(task)
    user_tasks.clear()
    inactivity_tasks.clear()


def set_group_parser_task(task: asyncio.Task) -> None:
    """Сохраняет задачу для group_parser."""
    global _group_parser_task
    _group_parser_task = task

def get_group_parser_task() -> Optional[asyncio.Task]:
    """Возвращает текущую задачу для group_parser."""
    return _group_parser_task

def set_greeter_task(task: asyncio.Task) -> None:
    """Сохраняет задачу для greeter."""
    global _greeter_task
    _greeter_task = task

def get_greeter_task() -> Optional[asyncio.Task]:
    """Возвращает текущую задачу для greeter."""
    return _greeter_task

def stop_group_parser_task() -> None:
    """Останавливает задачу group_parser."""
    stop_group_parser.set()
    task = get_group_parser_task()
    if task and not task.done():
        task.cancel()

def stop_greeter_task() -> None:
    """Останавливает задачу greeter."""
    stop_greeter.set()
    task = get_greeter_task()
    if task and not task.done():
        task.cancel()