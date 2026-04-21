"""Stress-test engine with round-robin server distribution and live metrics."""

import itertools
import random
import struct
import threading
import time
import pyodbc
from collections import deque

SQL_COPT_SS_ACCESS_TOKEN = 1256


class Metrics:
    """Thread-safe sliding-window (60 s) metrics collector."""

    def __init__(self):
        self._lock = threading.Lock()
        self.total_success = 0
        self.total_errors = 0
        self.active_workers = 0
        self._ok_times = deque()
        self._err_times = deque()
        self.start_time = None
        self.error_messages = deque(maxlen=50)

    def record_success(self):
        now = time.time()
        with self._lock:
            self.total_success += 1
            self._ok_times.append(now)

    def record_error(self, msg: str):
        now = time.time()
        with self._lock:
            self.total_errors += 1
            self._err_times.append(now)
            self.error_messages.append((now, msg))

    def inc_workers(self):
        with self._lock:
            self.active_workers += 1

    def dec_workers(self):
        with self._lock:
            self.active_workers -= 1

    def _purge(self, now):
        cutoff = now - 60
        while self._ok_times and self._ok_times[0] < cutoff:
            self._ok_times.popleft()
        while self._err_times and self._err_times[0] < cutoff:
            self._err_times.popleft()

    def snapshot(self) -> dict:
        now = time.time()
        with self._lock:
            self._purge(now)
            total = self.total_success + self.total_errors
            return {
                "active_workers": self.active_workers,
                "total_success": self.total_success,
                "total_errors": self.total_errors,
                "success_per_min": len(self._ok_times),
                "errors_per_min": len(self._err_times),
                "connections_per_min": len(self._ok_times) + len(self._err_times),
                "success_rate": (self.total_success / total * 100) if total else 0.0,
                "elapsed": now - self.start_time if self.start_time else 0.0,
            }


class StressEngine:
    """Spawns concurrent workers that execute SQL queries in round-robin."""

    def __init__(self, servers, auth_provider, query, on_log=None,
                 delay_min=0.2, delay_max=1.0):
        self.servers = list(servers)        # [(server, database, display_name), ...]
        self.auth = auth_provider
        self.query = query
        self.delay_min = delay_min               # seconds, lower bound
        self.delay_max = delay_max               # seconds, upper bound
        self._on_log = on_log or (lambda _: None)
        self.running = False
        self.metrics = Metrics()
        self._counter = itertools.count()
        self._threads: list[threading.Thread] = []

    # ── internals ──────────────────────────────────────────

    def _next_server(self):
        idx = next(self._counter) % len(self.servers)
        return self.servers[idx]

    @staticmethod
    def _token_struct(access_token: str) -> bytes:
        raw = access_token.encode("UTF-16-LE")
        return struct.pack(f"<I{len(raw)}s", len(raw), raw)

    def _connect(self, server, database):
        token = self.auth.get_token()
        conn_str = (
            f"DRIVER={{ODBC Driver 18 for SQL Server}};"
            f"SERVER={server};DATABASE={database};"
        )
        return pyodbc.connect(
            conn_str,
            attrs_before={SQL_COPT_SS_ACCESS_TOKEN: self._token_struct(token)},
        )

    def _worker(self, wid: int, ramp_delay: float = 0.0):
        # stagger the initial start so load ramps up gradually
        if ramp_delay > 0:
            time.sleep(ramp_delay)
        self.metrics.inc_workers()
        self._on_log(f"Worker {wid} started")
        try:
            while self.running:
                server, database, display_name = self._next_server()
                try:
                    conn = self._connect(server, database)
                    try:
                        cur = conn.cursor()
                        cur.execute(self.query)
                        rows = cur.fetchall()
                        cur.close()
                    finally:
                        conn.close()
                    self.metrics.record_success()
                    self._on_log(
                        f"Worker {wid} | {display_name} | OK | {len(rows)} rows"
                    )
                except Exception as exc:
                    self.metrics.record_error(str(exc))
                    self._on_log(
                        f"Worker {wid} | {display_name} | ERROR | {exc}"
                    )
                    time.sleep(0.1)  # back off briefly on error

                if self.delay_max > 0:
                    time.sleep(random.uniform(self.delay_min, self.delay_max))
        finally:
            self.metrics.dec_workers()
            self._on_log(f"Worker {wid} stopped")

    # ── public API ─────────────────────────────────────────

    def start(self, num_workers: int):
        self.running = True
        self.metrics = Metrics()
        self.metrics.start_time = time.time()
        self._threads = []
        self._counter = itertools.count()

        for i in range(num_workers):
            ramp = i * random.uniform(self.delay_min, self.delay_max)
            t = threading.Thread(target=self._worker, args=(i + 1, ramp), daemon=True)
            t.start()
            self._threads.append(t)
        self._on_log(
            f"Started {num_workers} worker(s) across {len(self.servers)} server(s)"
        )

    def add_workers(self, count: int):
        base = len(self._threads)
        for i in range(count):
            wid = base + i + 1
            t = threading.Thread(target=self._worker, args=(wid,), daemon=True)
            t.start()
            self._threads.append(t)
        self._on_log(f"Added {count} worker(s) — total {len(self._threads)}")

    def stop(self):
        self._on_log("Stopping all workers …")
        self.running = False
        for t in self._threads:
            t.join(timeout=15)
        self._on_log("All workers stopped")
