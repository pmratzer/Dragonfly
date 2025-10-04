import os, json
from fastapi import FastAPI, Query
from dotenv import load_dotenv
import aio_pika
import asyncio
import uuid
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy import text

load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql+asyncpg://app:app@localhost:5432/exchange")
AMQP_URL = os.getenv("AMQP_URL", "amqp://user:pass@localhost:5672/")
db_engine = None  # global

app = FastAPI()
connection = channel = None

@app.on_event("startup")
async def startup():
    global connection, channel, db_engine
    connection = await aio_pika.connect_robust(AMQP_URL)
    channel = await connection.channel()
    # declare exchange & queue
    ex = await channel.declare_exchange("orders.direct", aio_pika.ExchangeType.DIRECT, durable=True)
    q = await channel.declare_queue("orders.new", durable=True)
    await q.bind(ex, routing_key="new")

    db_engine = create_async_engine(DATABASE_URL, echo=False, future=True)

@app.on_event("shutdown")
async def shutdown():
    global db_engine
    await channel.close()
    await connection.close()
    if db_engine is not None:
        await db_engine.dispose()

@app.post("/orders")
async def post_order(
    user_id: str = Query("u1"),
    side: str = Query("BUY"),
    qty: int = Query(1),
    symbol: str = Query("AAPL"),
):
    msg = {
        "type": "order.v1",
        "order_id": str(uuid.uuid4()),  # unique each time
        "user_id": user_id,
        "symbol": symbol.upper(),
        "side": side.upper(),
        "qty": int(qty),
    }
    body = json.dumps(msg).encode()
    ex = await channel.get_exchange("orders.direct")
    await ex.publish(aio_pika.Message(body, delivery_mode=aio_pika.DeliveryMode.PERSISTENT),
                     routing_key="new")
    return {"published": msg}

from fastapi import HTTPException

@app.get("/balances")
async def get_balances(user_id: str = Query(...)):
    async with AsyncSession(db_engine) as ses:
        # cash
        res = await ses.execute(text("SELECT cash_balance FROM users WHERE id = :uid"), {"uid": user_id})
        row = res.first()
        if not row:
            return {"user_id": user_id, "cash": 0.0, "positions": []}
        cash = float(row[0])

        # positions
        res = await ses.execute(text("""
            SELECT symbol, qty
            FROM positions
            WHERE user_id = :uid
            ORDER BY symbol
        """), {"uid": user_id})
        positions = [{"symbol": r[0], "qty": int(r[1])} for r in res.all()]

    return {"user_id": user_id, "cash": cash, "positions": positions}

@app.get("/balances/all") #get all balances
async def get_all_balances():
    async with AsyncSession(db_engine) as ses:
        # fetch all users and their cash
        res = await ses.execute(text("SELECT id, cash_balance FROM users ORDER BY id"))
        users = res.all()

        # build full dict for each user
        result = []
        for uid, cash in users:
            res2 = await ses.execute(text("""
                SELECT symbol, qty FROM positions
                WHERE user_id = :uid ORDER BY symbol
            """), {"uid": uid})
            positions = [{"symbol": r[0], "qty": int(r[1])} for r in res2.all()]
            result.append({"user_id": uid, "cash": float(cash), "positions": positions})

    return {"users": result}