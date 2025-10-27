import os
import sys
import json
import time
import asyncio
import signal
import random
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, List, Tuple

# --- Windows event loop (needed for websockets on Windows) ---
if sys.platform.startswith("win"):
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

import ccxt.pro as ccxtpro  # websocket streaming
from aiokafka import AIOKafkaProducer

# ====== ENV ======
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "127.0.0.1:9092").strip()
RAW_TOPIC = os.getenv("RAW_TOPIC", "trades.raw")

DEFAULT_TYPE = os.getenv("BITGET_DEFAULT_TYPE", "swap")           # "spot"|"swap"
DEFAULT_SUBTYPE = os.getenv("BITGET_DEFAULT_SUBTYPE", "linear")   # for swaps: "linear"|"inverse"

# Concurrency / throttling
MAX_SYMBOLS = int(os.getenv("MAX_SYMBOLS", "500"))               # overall cap
SUB_DELAY_S = float(os.getenv("SUB_DELAY_S", "0.10"))            # delay between starting tasks (per client)
MAX_IN_FLIGHT = int(os.getenv("MAX_IN_FLIGHT", "2000"))          # queue size before backpressure
WORKERS_JSON = int(os.getenv("WORKERS_JSON", "4"))               # threads for JSON encoding

# WS sharding
MAX_SUBS_PER_CLIENT = int(os.getenv("MAX_SUBS_PER_CLIENT", "25"))

# Kafka tuning
KAFKA_ACKS = os.getenv("KAFKA_ACKS", "1")                        # 0, 1, or "all"
REQUEST_TIMEOUT_MS = int(os.getenv("REQUEST_TIMEOUT_MS", "30000"))
LINGER_MS = int(os.getenv("LINGER_MS", "10"))
ENABLE_IDEMPOTENCE = os.getenv("ENABLE_IDEMPOTENCE", "false").strip().lower() in ("1", "true", "yes")

PRINT_EVERY = int(os.getenv("PRINT_EVERY", "500"))               # progress log cadence

# Bitget rate-limit backoff for WS
BACKOFF_BASE = float(os.getenv("BACKOFF_BASE", "1.0"))           # seconds
BACKOFF_MAX = float(os.getenv("BACKOFF_MAX", "30.0"))            # max backoff seconds

# Metrics
METRICS_EVERY = float(os.getenv("METRICS_EVERY", "5"))           # seconds
METRICS_TOP_N = int(os.getenv("METRICS_TOP_N", "10"))

# Pairs refresh (48h)
REFRESH_SECONDS = float(os.getenv("REFRESH_SECONDS", "172800"))  # 48h

# ====== JSON encoder offloaded to threads ======
_executor: ThreadPoolExecutor | None = None


def encode_json(obj: dict) -> bytes:
    return json.dumps(obj, separators=(",", ":"), ensure_ascii=False).encode("utf-8")


def _parse_acks(s: str):
    s = (s or "").strip().lower()
    if s in ("all", "-1"):
        return "all"
    try:
        return int(s)
    except ValueError:
        return 1


# ====== Kafka sink with backpressure ======
class KafkaSink:
    def __init__(self, bootstrap: str, topic: str, queue_max: int):
        self.bootstrap = bootstrap
        self.topic = topic
        self.queue: asyncio.Queue[dict | None] = asyncio.Queue(maxsize=queue_max)
        self._producer: AIOKafkaProducer | None = None
        self._drain_task: asyncio.Task | None = None
        self._sent = 0

    async def start(self):
        print(f"[kafka] starting -> {self.bootstrap}")
        self._producer = AIOKafkaProducer(
            bootstrap_servers=self.bootstrap,
            security_protocol="PLAINTEXT",
            acks=_parse_acks(KAFKA_ACKS),
            request_timeout_ms=REQUEST_TIMEOUT_MS,
            linger_ms=LINGER_MS,
            enable_idempotence=ENABLE_IDEMPOTENCE,
        )
        await self._producer.start()
        self._drain_task = asyncio.create_task(self._drain_loop())
        print("[kafka] ready")

    async def stop(self):
        if self._drain_task:
            await self.queue.put(None)  # sentinel to stop the loop
            try:
                await self._drain_task
            finally:
                self._drain_task = None
        if self._producer:
            await self._producer.stop()
            self._producer = None
        print(f"[kafka] stopped (sent={self._sent})")

    async def send(self, obj: dict):
        await self.queue.put(obj)

    async def _drain_loop(self):
        assert self._producer is not None
        producer = self._producer
        n = 0
        while True:
            item = await self.queue.get()
            if item is None:
                break
            payload = await asyncio.get_running_loop().run_in_executor(_executor, encode_json, item)
            try:
                await producer.send_and_wait(self.topic, payload)
                n += 1
                self._sent += 1
                if n % PRINT_EVERY == 0:
                    print(f"[kafka] -> {self.topic} sent {n} (total {self._sent})")
            except Exception as e:
                print(f"[kafka] send error -> {self.topic}: {type(e).__name__}: {e}")


# ====== Global metrics counters ======
_symbol_counts: Dict[str, int] = {}   # total messages per symbol since start/reset
_symbol_counts_lock = asyncio.Lock()  # guarding concurrent updates


# ====== Bitget stream per symbol with smart retry ======
async def stream_symbol(ex, symbol: str, sink: KafkaSink):
    print(f"[ws] start {symbol}")
    backoff = BACKOFF_BASE
    while True:
        try:
            trades = await ex.watch_trades(symbol)
            iterable = trades if isinstance(trades, list) else [trades]

            now_ms = int(time.time() * 1000)
            local_inc = 0
            for t in iterable:
                ts = int(t.get("timestamp", now_ms))
                price = float(t.get("price"))
                amount = float(t.get("amount") or t.get("size") or 0.0)

                msg = {
                    "source": "bitget",
                    "symbol": symbol,
                    "ts": ts,
                    "price": price,
                    "amount": amount,
                    "type": "trade",
                }
                await sink.send(msg)
                local_inc += 1

            if local_inc:
                async with _symbol_counts_lock:
                    _symbol_counts[symbol] = _symbol_counts.get(symbol, 0) + local_inc

            backoff = BACKOFF_BASE  # success path: reset backoff

        except asyncio.CancelledError:
            raise
        except Exception as e:
            msg = str(e)
            if "does not have market symbol" in msg:
                print(f"[{symbol}] permanent error: {msg} -> stopping")
                return
            if '"code":30006' in msg or "request too many" in msg:
                sleep_s = min(backoff * (1.5 + random.random()), BACKOFF_MAX)
                print(f"[{symbol}] rate-limited (30006). Backing off {sleep_s:.2f}s")
                await asyncio.sleep(sleep_s)
                backoff = min(backoff * 2, BACKOFF_MAX)
                continue
            print(f"[{symbol}] ws error: {msg}; retrying in 2s")
            await asyncio.sleep(2.0)


# ====== helpers ======
def chunked(items: List[str], n: int):
    for i in range(0, len(items), n):
        yield items[i:i + n]


def filter_pairs_for_bitget(all_pairs: List[str], markets: dict, want_type: str, want_subtype: str) -> Tuple[List[str], List[str]]:
    ok, bad = [], []
    for s in all_pairs:
        m = markets.get(s)
        if not m:
            bad.append(s)
            continue
        mtype = m.get("type")
        msub = m.get("subType") or m.get("linear", None)
        is_linear = (msub == "linear") or (msub is True if isinstance(msub, bool) else False)
        sub_ok = (want_subtype == "linear" and is_linear) or (want_subtype != "linear")
        if mtype == want_type and sub_ok:
            ok.append(s)
        else:
            bad.append(s)
    return ok, bad


async def load_and_filter_pairs(limit: int) -> List[str]:
    # Load pairs.json
    with open("pairs.json", "r", encoding="utf-8") as f:
        pairs = json.load(f)
    if not isinstance(pairs, list) or not pairs:
        raise RuntimeError("pairs.json must be a non-empty JSON array of symbols")
    if len(pairs) > limit:
        print(f"[warn] limiting pairs from {len(pairs)} -> {limit}")
        pairs = pairs[:limit]

    # Use a temp client to load markets and filter
    temp_ex = ccxtpro.bitget({
        "enableRateLimit": True,
        "options": {"defaultType": DEFAULT_TYPE, "defaultSubType": DEFAULT_SUBTYPE},
    })
    try:
        await temp_ex.load_markets()
        markets = temp_ex.markets
        ok_pairs, bad_pairs = filter_pairs_for_bitget(pairs, markets, DEFAULT_TYPE, DEFAULT_SUBTYPE)
        if bad_pairs:
            print(f"[filter] skipping {len(bad_pairs)} unknown/unsupported symbols (first 10): {bad_pairs[:10]}")
        if not ok_pairs:
            print("[filter] no valid pairs after filtering")
        return ok_pairs
    finally:
        try:
            await temp_ex.close()
        except Exception:
            pass


async def start_streams(pairs: List[str], sink: KafkaSink):
    """Start shards and stream tasks. Returns (clients, tasks)."""
    shards = list(chunked(pairs, MAX_SUBS_PER_CLIENT))
    num_clients = len(shards)
    print(f"[ws] starting {len(pairs)} pairs across {num_clients} client(s) (<= {MAX_SUBS_PER_CLIENT} subs/client)")

    clients = []
    tasks: List[asyncio.Task] = []

    # Build clients
    for _ in range(num_clients):
        ex = ccxtpro.bitget({
            "enableRateLimit": True,
            "options": {"defaultType": DEFAULT_TYPE, "defaultSubType": DEFAULT_SUBTYPE},
        })
        await ex.load_markets()
        clients.append(ex)

    # Start streams with per-client ramp
    for shard, ex in zip(shards, clients):
        for s in shard:
            tasks.append(asyncio.create_task(stream_symbol(ex, s, sink)))
            await asyncio.sleep(SUB_DELAY_S)

    return clients, tasks


async def stop_streams(clients: List, tasks: List[asyncio.Task]):
    for t in tasks:
        t.cancel()
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)
    for ex in clients:
        try:
            await ex.close()
        except Exception:
            pass


async def metrics_loop(stop_event: asyncio.Event):
    """Periodically prints per-symbol msg/s (top N) and overall rate."""
    prev_counts: Dict[str, int] = {}
    prev_time = time.time()
    while not stop_event.is_set():
        await asyncio.sleep(METRICS_EVERY)
        now = time.time()
        dt = max(1e-6, now - prev_time)

        async with _symbol_counts_lock:
            # snapshot
            curr = dict(_symbol_counts)

        # deltas
        deltas = {sym: curr.get(sym, 0) - prev_counts.get(sym, 0) for sym in curr.keys()}
        total_delta = sum(deltas.values())
        overall_rate = total_delta / dt

        # top N by delta
        top = sorted(deltas.items(), key=lambda kv: kv[1], reverse=True)[:METRICS_TOP_N]
        top_str = ", ".join(f"{sym}:{(cnt/dt):.1f}/s" for sym, cnt in top if cnt > 0)

        print(f"[metrics] window={dt:.1f}s overall={overall_rate:.1f}/s total_msgs={sum(curr.values())} top{METRICS_TOP_N}=[{top_str}]")

        prev_counts = curr
        prev_time = now


async def refresh_pairs_loop(stop_event: asyncio.Event, sink: KafkaSink):
    """Reload pairs.json every REFRESH_SECONDS, rebuild shards/streams cleanly."""
    # First build
    pairs = await load_and_filter_pairs(MAX_SYMBOLS)
    if not pairs:
        print("[refresh] nothing to stream; sleeping until next refresh")
        clients, tasks = [], []
    else:
        clients, tasks = await start_streams(pairs, sink)

    # Start metrics task
    metrics_task = asyncio.create_task(metrics_loop(stop_event))

    try:
        while not stop_event.is_set():
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=REFRESH_SECONDS)
                break  # stop requested
            except asyncio.TimeoutError:
                pass  # time to refresh

            print("[refresh] refreshing pairs.json and WS shards...")
            new_pairs = await load_and_filter_pairs(MAX_SYMBOLS)

            # Rebuild only if changed
            if set(new_pairs) != set(pairs):
                print(f"[refresh] set changed: {len(pairs)} -> {len(new_pairs)} symbols")
                await stop_streams(clients, tasks)
                if new_pairs:
                    clients, tasks = await start_streams(new_pairs, sink)
                else:
                    clients, tasks = [], []
                # reset metrics base to avoid a single huge delta on next tick
                async with _symbol_counts_lock:
                    # keep totals, metrics loop uses deltas from snapshot
                    pass
                pairs = new_pairs
            else:
                print("[refresh] no changes in pairs; keeping current streams")
    finally:
        metrics_task.cancel()
        await asyncio.gather(metrics_task, return_exceptions=True)
        await stop_streams(clients, tasks)


# ====== main ======
async def main():
    global _executor
    _executor = ThreadPoolExecutor(max_workers=WORKERS_JSON)

    sink = KafkaSink(KAFKA_BOOTSTRAP, RAW_TOPIC, queue_max=MAX_IN_FLIGHT)
    await sink.start()

    stop_event = asyncio.Event()

    def _handle_sig(*_):
        stop_event.set()

    # Register signals where supported
    try:
        loop = asyncio.get_running_loop()
        loop.add_signal_handler(signal.SIGINT, _handle_sig)
        try:
            loop.add_signal_handler(signal.SIGTERM, _handle_sig)
        except (AttributeError, NotImplementedError):
            pass
    except (NotImplementedError, RuntimeError):
        pass

    # Run the refresher (which also does the initial start) until stop
    refresher = asyncio.create_task(refresh_pairs_loop(stop_event, sink))

    try:
        await stop_event.wait()
    except KeyboardInterrupt:
        pass
    finally:
        refresher.cancel()
        await asyncio.gather(refresher, return_exceptions=True)

        try:
            await sink.stop()
        except Exception:
            pass

        if _executor:
            _executor.shutdown(wait=True)

        print("[main] bye")


if __name__ == "__main__":
    asyncio.run(main())
