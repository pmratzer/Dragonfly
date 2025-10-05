import os, json, asyncio, aio_pika
from dotenv import load_dotenv
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy import text

load_dotenv()
AMQP_URL = os.getenv("AMQP_URL", "amqp://user:pass@localhost:5672/")
DATABASE_URL = os.getenv("DATABASE_URL")

engine = None  # global async engine

#redundancy to ensure tables exist if not already created using demo reset script
DDL = """ 
CREATE TABLE IF NOT EXISTS users (
  id TEXT PRIMARY KEY,
  cash_balance NUMERIC NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS positions (
  user_id TEXT NOT NULL,
  symbol  TEXT NOT NULL,
  qty     INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY (user_id, symbol)
);
CREATE TABLE IF NOT EXISTS trades (
  trade_id TEXT PRIMARY KEY,
  symbol   TEXT NOT NULL,
  qty      INTEGER NOT NULL,
  price    NUMERIC NOT NULL,
  buy_user TEXT NOT NULL,
  sell_user TEXT NOT NULL,
  ts TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE TABLE IF NOT EXISTS ledger_entries (
  id BIGSERIAL PRIMARY KEY,
  user_id TEXT NOT NULL,
  trade_id TEXT NOT NULL,
  symbol TEXT NOT NULL,
  delta_cash NUMERIC NOT NULL,
  delta_qty  INTEGER NOT NULL,
  ts TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (trade_id, user_id)
);
"""

UPSERT_USER = text("""
INSERT INTO users (id, cash_balance)
VALUES (:uid, :delta_cash)
ON CONFLICT (id) DO UPDATE
SET cash_balance = users.cash_balance + EXCLUDED.cash_balance
RETURNING cash_balance;
""")

UPSERT_POS = text("""
INSERT INTO positions (user_id, symbol, qty)
VALUES (:uid, :symbol, :delta_qty)
ON CONFLICT (user_id, symbol) DO UPDATE
SET qty = positions.qty + EXCLUDED.qty
RETURNING qty;
""")

INSERT_TRADE = text("""
INSERT INTO trades (trade_id, symbol, qty, price, buy_user, sell_user)
VALUES (:trade_id, :symbol, :qty, :price, :buy_user, :sell_user)
ON CONFLICT (trade_id) DO NOTHING;
""")

INSERT_LEDGER = text("""
INSERT INTO ledger_entries (user_id, trade_id, symbol, delta_cash, delta_qty)
VALUES (:uid, :trade_id, :symbol, :delta_cash, :delta_qty)
ON CONFLICT (trade_id, user_id) DO NOTHING;
""")

async def init_db():
    stmts = DDL.strip().split(";")
    async with engine.begin() as conn:
        for stmt in stmts:
            s = stmt.strip()
            if s:
                await conn.execute(text(s))

async def apply_settlement(trade: dict):
    """Idempotent apply: insert trade, upsert buyer/seller cash & positions, record ledger rows."""
    buy_user  = trade["buy_user"]
    sell_user = trade["sell_user"]
    symbol    = trade["symbol"]
    qty       = int(trade["qty"])
    price     = float(trade["price"])
    trade_id  = trade["trade_id"]

    cash_buyer = -(price * qty)
    cash_seller = +(price * qty)

    async with AsyncSession(engine) as ses:
        async with ses.begin():
            # 1) record trade (no-op on replays)
            await ses.execute(INSERT_TRADE, {
                "trade_id": trade_id, "symbol": symbol, "qty": qty,
                "price": price, "buy_user": buy_user, "sell_user": sell_user
            })

            # 2) buyer deltas (cash -, pos +)  — capture RETURNING values
            res = await ses.execute(UPSERT_USER, {"uid": buy_user, "delta_cash": cash_buyer})
            buyer_cash_after = float(res.scalar_one())
            res = await ses.execute(UPSERT_POS, {"uid": buy_user, "symbol": symbol, "delta_qty": +qty})
            buyer_pos_after = int(res.scalar_one())
            await ses.execute(INSERT_LEDGER, {
                "uid": buy_user, "trade_id": trade_id, "symbol": symbol,
                "delta_cash": cash_buyer, "delta_qty": +qty
            })

            # 3) seller deltas (cash +, pos -) — capture RETURNING values
            res = await ses.execute(UPSERT_USER, {"uid": sell_user, "delta_cash": cash_seller})
            seller_cash_after = float(res.scalar_one())
            res = await ses.execute(UPSERT_POS, {"uid": sell_user, "symbol": symbol, "delta_qty": -qty})
            seller_pos_after = int(res.scalar_one())
            await ses.execute(INSERT_LEDGER, {
                "uid": sell_user, "trade_id": trade_id, "symbol": symbol,
                "delta_cash": cash_seller, "delta_qty": -qty
            })

        # (commit happens here on success)

    # Log the post-apply state so you can verify effects quickly (for demo purposes)
    print("[SETTLEMENT] cash_after:", {"buyer": [buy_user, buyer_cash_after],
                                       "seller": [sell_user, seller_cash_after]})
    print("[SETTLEMENT] pos_after:", {"buyer": [buy_user, symbol, buyer_pos_after],
                                      "seller": [sell_user, symbol, seller_pos_after]})


async def main():
    global engine
    engine = create_async_engine(DATABASE_URL, echo=False, future=True)
    await init_db()

    conn = await aio_pika.connect_robust(AMQP_URL)
    ch = await conn.channel()
    await ch.set_qos(prefetch_count=50)

    trades_ex = await ch.declare_exchange("trades.fanout", aio_pika.ExchangeType.FANOUT, durable=True)
    q_settle = await ch.declare_queue("trades.to_settle", durable=True)
    await q_settle.bind(trades_ex, routing_key="")

    print("Settlement listening on 'trades.to_settle' ...")
    async with q_settle.iterator() as it:
        async for msg in it:
            async with msg.process():
                trade = json.loads(msg.body)
                await apply_settlement(trade)
                print("SETTLED:", trade["trade_id"], trade["symbol"], trade["qty"], trade["price"])

if __name__ == "__main__":
    asyncio.run(main())
