import asyncio, json, time
from app.services.verifier import verify
import aioredis
import os

async def worker_loop():
    redis = aioredis.from_url(os.environ["REDIS_URL"])
    while True:
        _, item = await redis.brpop("tx_queue", timeout=5)
        if not item:
            await asyncio.sleep(1); continue
        payload = json.loads(item)
        try:
            result = await verify(payload)
            # اگر وضعیت pending، مجدداً در صف قرار بگیره بعد از زمان کوتاه
            if result['status'] == 'pending':
                await asyncio.sleep(5)
                await redis.lpush("tx_queue", json.dumps(payload))
            else:
                # notify WebSocket and cache inside verify for success/fail
                pass
        except Exception as e:
            # در صورت خطا، دوباره تلاش کن
            await asyncio.sleep(10)
            await redis.lpush("tx_queue", json.dumps(payload))

if __name__ == "__main__":
    asyncio.run(worker_loop())
  
