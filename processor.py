import os, asyncio, json, time, statistics
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
RAW_TOPIC = os.getenv("RAW_TOPIC", "trades.raw")
BARS_TOPIC = os.getenv("BARS_TOPIC", "bars.250ms")
ALERT_TOPIC = os.getenv("ALERTS_TOPIC", "alerts.breakout")

WINDOW_MS = int(os.getenv("WINDOW_MS", "250"))
HIST_WINDOWS = int(os.getenv("HIST_WINDOWS", "120"))
Z_THRESHOLD = float(os.getenv("Z_THRESHOLD", "3.0"))
COOLDOWN_WINDOWS = int(os.getenv("COOLDOWN_WINDOWS", "6"))

PRINT_BARS = os.getenv("PRINT_BARS", "1") == "1"
PRINT_ALERTS = os.getenv("PRINT_ALERTS", "1") == "1"

def floor_window(ts_ms: int, size_ms: int) -> int:
    return (ts_ms // size_ms) * size_ms

class SymState:
    def __init__(self):
        self.win = {}         # w_start -> dict(open, high, low, close, volume, vel)
        self.hist = []        # velocity history
        self.cooldown_left = 0

    def add(self, ts: int, price: float, amount: float):
        w = floor_window(ts, WINDOW_MS)
        b = self.win.get(w)
        if b is None:
            b = {"open": price, "high": price, "low": price, "close": price,
                 "volume": amount, "vel": price*amount}
            self.win[w] = b
        else:
            b["high"] = max(b["high"], price)
            b["low"]  = min(b["low"], price)
            b["close"] = price
            b["volume"] += amount
            b["vel"]    += price*amount

    def finalize(self, now_ms: int):
        out = []
        for w in sorted(list(self.win.keys())):
            if w + WINDOW_MS <= now_ms:
                b = self.win.pop(w)
                v = b["vel"]
                z = None
                br = False
                if len(self.hist) >= max(10, HIST_WINDOWS // 4):
                    mu = statistics.fmean(self.hist)
                    sigma = statistics.pstdev(self.hist) or 1e-9
                    z = (v - mu) / sigma
                    if self.cooldown_left > 0:
                        self.cooldown_left -= 1
                    else:
                        br = z >= Z_THRESHOLD
                        if br:
                            self.cooldown_left = COOLDOWN_WINDOWS
                self.hist.append(v)
                if len(self.hist) > HIST_WINDOWS:
                    self.hist.pop(0)
                out.append((w, b, z, br))
        return out

async def main():
    consumer = AIOKafkaConsumer(
        RAW_TOPIC,
        bootstrap_servers=BOOTSTRAP,
        enable_auto_commit=True,
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
        auto_offset_reset="latest",
        group_id="velocity-processor",
        max_poll_records=1000,
    )
    producer = AIOKafkaProducer(
        bootstrap_servers=BOOTSTRAP,
        linger_ms=10,
        value_serializer=lambda o: json.dumps(o, separators=(",", ":"), ensure_ascii=False).encode("utf-8"),
    )

    await consumer.start()
    await producer.start()
    print(f"[processor] listening={RAW_TOPIC} -> {BARS_TOPIC}, {ALERT_TOPIC}")

    state = {}   # symbol -> SymState
    try:
        while True:
            msgs = await consumer.getmany(timeout_ms=500, max_records=5000)
            now = int(time.time()*1000)
            for tp, batch in msgs.items():
                for r in batch:
                    m = r.value
                    sym = m.get("symbol")
                    ts  = int(m.get("ts", now))
                    price = float(m.get("price"))
                    amount = float(m.get("amount", 0.0))
                    st = state.get(sym)
                    if st is None:
                        st = state[sym] = SymState()
                    st.add(ts, price, amount)

            # finalize closed windows and emit
            for sym, st in state.items():
                for w, b, z, br in st.finalize(now):
                    bar = {
                        "symbol": sym, "t_start": w, "t_end": w + WINDOW_MS,
                        "open": b["open"], "high": b["high"], "low": b["low"], "close": b["close"],
                        "volume": b["volume"], "velocity": b["vel"], "z": z
                    }
                    if PRINT_BARS:
                        if z is None:
                            print(f"{sym} {w}: vel={b['vel']:.4f}")
                        else:
                            print(f"{sym} {w}: vel={b['vel']:.4f} z={z:.2f}")
                    await producer.send_and_wait(BARS_TOPIC, bar)

                    if br:
                        alert = {"symbol": sym, "t_window": w, "velocity": b["vel"], "z": z, "note": "BREAKOUT"}
                        if PRINT_ALERTS:
                            print(f"🚨 BREAKOUT {sym} {w}: z={z:.2f} vel={b['vel']:.4f}")
                        await producer.send_and_wait(ALERT_TOPIC, alert)
    finally:
        await producer.stop()
        await consumer.stop()

if __name__ == "__main__":
    asyncio.run(main())
