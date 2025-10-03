# basepool.py
from __future__ import annotations
import asyncio
import time
from typing import Dict, Optional, Awaitable, Callable

class BasePool:
    def __init__(self, *,
                 initial_backoff: float = 60.0,
                 backoff_factor: float = 2.0,
                 max_backoff: float = 24*3600.0):
        # backoff state
        self._backoffs: Dict[int, float] = {}
        self._initial_backoff = initial_backoff
        self._backoff_factor = backoff_factor
        self._max_backoff = max_backoff

        # sleep/queue/drainer state
        self._sleep_until: Dict[int, float] = {}           # executor_id -> timestamp until (sleep)
        self._sleep_events: Dict[int, asyncio.Event] = {}  # executor_id -> Event (set when awake)
        self._queues: Dict[int, asyncio.Queue] = {}        # executor_id -> Queue[Awaitable]
        self._drainers: Dict[int, asyncio.Task] = {}       # executor_id -> background drainer task

        # per-executor locks
        self._locks: Dict[int, asyncio.Lock] = {}

    # ---- backoff helpers ----
    def _current_backoff(self, executor_id: int) -> float:
        return self._backoffs.get(executor_id, self._initial_backoff)

    def _increase_backoff(self, executor_id: int) -> None:
        cur = self._backoffs.get(executor_id, self._initial_backoff)
        nxt = min(cur * self._backoff_factor, self._max_backoff)
        self._backoffs[executor_id] = nxt

    def _reset_backoff(self, executor_id: int) -> None:
        self._backoffs.pop(executor_id, None)

    # ---- time / event / queue utils ----
    def _now(self) -> float:
        return time.time()

    def _event_for(self, executor_id: int) -> asyncio.Event:
        ev = self._sleep_events.get(executor_id)
        if ev is None:
            ev = asyncio.Event()
            ev.set()  # по умолчанию — не спит
            self._sleep_events[executor_id] = ev
        return ev
    
    def _queue_for(self, executor_id: int) -> asyncio.Queue:
        q = self._queues.get(executor_id)
        if q is None:
            q = asyncio.Queue()
            self._queues[executor_id] = q
        return q

    def _lock_for(self, executor_id: int) -> asyncio.Lock:
        lock = self._locks.get(executor_id)
        if lock is None:
            lock = asyncio.Lock()
            self._locks[executor_id] = lock
        return lock

    def is_sleeping(self, executor_id: int) -> bool:
        ts = self._sleep_until.get(executor_id, 0.0)
        return self._now() < ts

    def defer_for_executor(self, executor_id: int, coro: Awaitable) -> None:
        """
        Кладём отложенную корутину (awaitable) в очередь исполнителя,
        которая будет выполнена после пробуждения.
        """
        q = self._queue_for(executor_id)
        q.put_nowait(coro)

    async def sleep_executor(self, executor_id: int, seconds: float) -> None:
        """
        Переводит исполнителя в спячку на seconds (обновляет until, ставит ev.clear()).
        Запускает дрейнер, если он не поднят.
        """
        until = max(self._sleep_until.get(executor_id, 0.0), self._now() + float(seconds))
        self._sleep_until[executor_id] = until
        ev = self._event_for(executor_id)
        ev.clear()

        # поднимаем дрейнер, если ещё нет
        if executor_id not in self._drainers or self._drainers[executor_id].done():
            self._drainers[executor_id] = asyncio.create_task(self._drain_after_wakeup(executor_id))

    async def _drain_after_wakeup(self, executor_id: int) -> None:
        """
        Фоновая задача: ждёт конца сна и вычищает очередь отложенных задач.
        """
        try:
            while True:
                ts = self._sleep_until.get(executor_id, 0.0)
                wait = max(0.0, ts - self._now())
                if wait > 0:
                    await asyncio.sleep(wait)

                # проснулись
                self._sleep_until[executor_id] = 0.0
                self._event_for(executor_id).set()

                q = self._queue_for(executor_id)
                # выполняем очередь пока не введён новый сон
                while not q.empty() and not self.is_sleeping(executor_id):
                    coro = await q.get()
                    try:
                        await coro
                    except Exception as e:
                        # Здесь не знаем логгер; просто print или переопредели в подклассе
                        print(f"[BasePool] deferred task error exec={executor_id}: {e}")

                # если снова не спим и очередь пуста — прекращаем дрейнер
                if not self.is_sleeping(executor_id) and q.empty():
                    break
        finally:
            self._drainers.pop(executor_id, None)
