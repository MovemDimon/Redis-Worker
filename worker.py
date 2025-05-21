import asyncio, json, time, os
from app.services.verifier import verify
import aioredis

async def worker_loop():
    redis = aioredis.from_url(
        os.environ["REDIS_URL"],
        decode_responses=True,
        ssl=True
    )

    while True:
        try:
            result = await redis.brpop("tx_queue", timeout=5)
            if not result:
                await asyncio.sleep(1)
                continue
            _, item = result
            payload = json.loads(item)
            result = await verify(payload)

            if result['status'] == 'pending':
                await asyncio.sleep(5)
                await redis.lpush("tx_queue", json.dumps(payload))
            else:
                # notify WebSocket and cache inside verify
                pass

        except Exception as e:
            print(f"[ERROR] {e}")
            await asyncio.sleep(10)
            await redis.lpush("tx_queue", json.dumps(payload))

if __name__ == "__main__":
    asyncio.run(worker_loop())
