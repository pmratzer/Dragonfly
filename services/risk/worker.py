import os, json, asyncio, aio_pika
from dotenv import load_dotenv
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy import text

load_dotenv()

AMQP_URL = os.getenv("AMQP_URL", "amqp://user:pass@localhost:5672/")
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql+asyncpg://app:app@localhost:5432/exchange")

PRICE_PER_SHARE = 100.0  # keep aligned with Matching for now
engine = create_async_engine(DATABASE_URL, echo=False, future=True)

PRICES = {"AAPL": 100.0, "MSFT": 120.0, "GOOG": 90.0}
ALLOWED = set(PRICES.keys())

def basic_validate_shape(o: dict) -> tuple[bool,str]:
    if o.get("type") != "order.v1": return False, "bad_type"
    sym = o.get("symbol", "").upper()
    if sym not in ALLOWED: return False, "symbol_not_allowed"
    side = o.get("side", "").upper()
    if side not in {"BUY","SELL"}: return False, "bad_side"
    qty = o.get("qty")
    if not isinstance(qty,int) or not (1 <= qty <= 100): return False, "bad_qty"
    return True, "ok"

async def fetch_cash(uid: str) -> float:
    async with AsyncSession(engine) as ses:
        res = await ses.execute(text("SELECT cash_balance FROM users WHERE id = :uid"), {"uid": uid})
        row = res.first()
        return float(row[0]) if row else 0.0
    
async def fetch_pos(uid: str, symbol: str) -> int:
    async with AsyncSession(engine) as ses:
        res = await ses.execute(
            text("SELECT qty FROM positions WHERE user_id = :uid AND symbol = :sym"),
            {"uid": uid, "sym": symbol},
        )
        row = res.first()
        return int(row[0]) if row else 0

def basic_validate_shape(o: dict) -> tuple[bool,str]:
    if o.get("type") != "order.v1": return False, "bad_type"
    if o.get("symbol") not in {"AAPL","MSFT"}: return False, "symbol_not_allowed"
    if o.get("side") not in {"BUY","SELL"}: return False, "bad_side"
    qty = o.get("qty")
    if not isinstance(qty,int) or not (1 <= qty <= 100): return False, "bad_qty"
    return True, "ok"

async def main():
    conn = await aio_pika.connect_robust(AMQP_URL)
    ch = await conn.channel()
    await ch.set_qos(prefetch_count=50)

    orders_ex = await ch.declare_exchange("orders.direct", aio_pika.ExchangeType.DIRECT, durable=True)
    events_ex = await ch.declare_exchange("orders.events", aio_pika.ExchangeType.DIRECT, durable=True)

    q_new = await ch.declare_queue("orders.new", durable=True)
    await q_new.bind(orders_ex, routing_key="new")

    q_acc = await ch.declare_queue("orders.accepted", durable=True)
    await q_acc.bind(events_ex, routing_key="accepted")

    q_rej = await ch.declare_queue("orders.rejected", durable=True)
    await q_rej.bind(events_ex, routing_key="rejected")

    print("Risk listening on 'orders.new' ...")
    async with q_new.iterator() as it:
        async for msg in it:
            async with msg.process():
                order = json.loads(msg.body)
                ok, reason = basic_validate_shape(order)
                if not ok:
                    payload = {"type":"order_rejected.v1","order_id":order.get("order_id"),"reason":reason}
                    await events_ex.publish(aio_pika.Message(json.dumps(payload).encode(), delivery_mode=aio_pika.DeliveryMode.PERSISTENT), routing_key="rejected")
                    print("REJECTED:", payload); continue

                side = order["side"]; qty = int(order["qty"]); user_id = order["user_id"]
                if side == "BUY":
                    price = PRICES[order["symbol"].upper()]
                    needed = price * qty
                    cash = await fetch_cash(user_id)
                    if cash < needed:
                        payload = {"type":"order_rejected.v1","order_id":order["order_id"],"reason":"insufficient_funds","needed":needed,"cash":cash}
                        await events_ex.publish(aio_pika.Message(json.dumps(payload).encode(), delivery_mode=aio_pika.DeliveryMode.PERSISTENT), routing_key="rejected")
                        print("REJECTED:", payload); continue

                if side == "SELL":
                    held = await fetch_pos(user_id, order["symbol"])
                    print(f"[RISK] SELL check user={user_id} held={held} qty={qty}")  # <-- add this

                    if held < qty:
                        payload = {"type":"order_rejected.v1","order_id":order["order_id"],
                                   "reason":"insufficient_shares","have":held,"needed":qty}
                        await events_ex.publish(aio_pika.Message(json.dumps(payload).encode(),
                                                                 delivery_mode=aio_pika.DeliveryMode.PERSISTENT),
                                                routing_key="rejected")
                        print("REJECTED:", payload)
                        continue
                    
                payload = {"type":"order_accepted.v1","order_id":order["order_id"],"symbol":order["symbol"],"qty":qty,"side":side,"user_id":user_id}
                await events_ex.publish(aio_pika.Message(json.dumps(payload).encode(), delivery_mode=aio_pika.DeliveryMode.PERSISTENT), routing_key="accepted")
                print("ACCEPTED:", payload)

if __name__ == "__main__":
    asyncio.run(main())
