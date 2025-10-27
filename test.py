
import asyncio, json, time
from aiokafka import AIOKafkaProducer

async def main():
    p = AIOKafkaProducer(bootstrap_servers="127.0.0.1:9092", linger_ms=10)
    await p.start()
    try:
        msg = {"ping":"hello","ts":int(time.time()*1000)}
        md = await p.send_and_wait("trades.raw", json.dumps(msg).encode())
        print("sent to", md.topic, "partition", md.partition, "offset", md.offset)
    finally:
        await p.stop()

asyncio.run(main())

