# basepool.py
from __future__ import annotations
import asyncio
import time
import signal
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

        # ---- stop ----
        self._stop = asyncio.Event()     # общий флаг остановки для всех фоновых задач пула
        self._closed = False


    def _install_signal_handlers(self) -> None:
        loop = asyncio.get_running_loop()

        def request_shutdown(sig_name: str) -> None:
            if not self._stop.is_set():
                print(f"\nПолучен {sig_name}. Завершаемся…")
                self.request_stop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, request_shutdown, sig.name)
            except NotImplementedError:
                signal.signal(sig, lambda *_: loop.call_soon_threadsafe(request_shutdown, sig.name))


    # ---- stop API ----
    def request_stop(self) -> None:
        """Сигнализировать всем фоновым задачам пула, что нужно завершаться."""
        self._stop.set()


    async def aclose(self, *, drain_queues: bool = False) -> None:
        """
        Закрыть пул:
        1) Ставим флаг остановки.
        2) Будим всех спящих (set Event + сбрасываем sleep_until).
        3) Останавливаем дрейнеры.
        4) Чистим очереди (или, опционально, доисполняем — drain_queues=True).
        """
        if self._closed:
            return
        self._closed = True
        self._stop.set()

        # 2) будим всех и снимаем sleep
        for exec_id in list(self._sleep_events.keys()):
            self._sleep_until[exec_id] = 0.0
            self._event_for(exec_id).set()

        # 3) отменяем дрейнеры
        drainers = [t for t in self._drainers.values() if t and not t.done()]
        for t in drainers:
            t.cancel()
        if drainers:
            from contextlib import suppress
            with suppress(asyncio.CancelledError):
                await asyncio.gather(*drainers, return_exceptions=False)
        self._drainers.clear()

        # 4) очереди
        if drain_queues:
            # аккуратно доисполнить, игнорируя новые слипы
            for exec_id, q in list(self._queues.items()):
                while not q.empty():
                    coro = await q.get()
                    try:
                        await coro
                    except Exception as e:
                        print(f"[BasePool] error draining queue exec={exec_id}: {e}")
        else:
            # просто очистить
            for q in self._queues.values():
                while not q.empty():
                    _ = q.get_nowait()


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
            while not self._stop.is_set():
                ts = self._sleep_until.get(executor_id, 0.0)
                wait = max(0.0, ts - self._now())
                if wait > 0:
                    sleep_task = asyncio.create_task(asyncio.sleep(wait))
                    stop_task = asyncio.create_task(self._stop.wait())
                    done, pending = await asyncio.wait({sleep_task, stop_task}, return_when=asyncio.FIRST_COMPLETED)
                    for p in pending:
                        p.cancel()
                    if stop_task in done:
                        break

                # проснулись
                self._sleep_until[executor_id] = 0.0
                self._event_for(executor_id).set()

                q = self._queue_for(executor_id)
                # выполняем очередь пока не введён новый сон
                while not q.empty() and not self.is_sleeping(executor_id) and not self._stop.is_set():
                    coro = await q.get()
                    try:
                        await coro
                    except Exception as e:
                        print(f"[BasePool] deferred task error exec={executor_id}: {e}")

                # если снова не спим и очередь пуста — прекращаем дрейнер
                if not self.is_sleeping(executor_id) and q.empty():
                    break
        finally:
            self._drainers.pop(executor_id, None)
