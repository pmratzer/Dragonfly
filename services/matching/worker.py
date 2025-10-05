# services/matching/worker.py
import os, json, asyncio, aio_pika
AMQP_URL = os.getenv("AMQP_URL", "amqp://user:pass@localhost:5672/")
PRICES = {
    "AAPL": 225.00, "MSFT": 415.00, "GOOG": 168.00, "AMZN": 185.00, "META": 510.00,
    "NVDA": 115.00, "TSLA": 205.00, "NFLX": 620.00, "AVGO": 1720.00, "AMD": 155.00,
}


async def main():
    conn = await aio_pika.connect_robust(AMQP_URL)
    ch = await conn.channel()
    await ch.set_qos(prefetch_count=50)

    events_ex = await ch.declare_exchange("orders.events", aio_pika.ExchangeType.DIRECT, durable=True)
    trades_ex = await ch.declare_exchange("trades.fanout", aio_pika.ExchangeType.FANOUT, durable=True)

    q_acc = await ch.declare_queue("orders.accepted", durable=True)
    await q_acc.bind(events_ex, routing_key="accepted")

    print("Matching listening on 'orders.accepted' ...")
    async with q_acc.iterator() as it:
        async for msg in it:
            async with msg.process():
                order = json.loads(msg.body)
                uid   = order["user_id"]
                side  = order["side"].upper()
                sym   = order["symbol"]
                qty   = int(order["qty"])

                if side == "BUY":
                    buy_user, sell_user = uid, "mm"
                else:  # SELL
                    buy_user, sell_user = "mm", uid

                sym = order["symbol"].upper()
                price = PRICES[sym]
                trade = {
                    "type": "trade_fill.v1",
                    "trade_id": f"t-{order['order_id']}",
                    "symbol": sym,
                    "qty": qty,
                    "price": price,
                    "buy_user": buy_user,
                    "sell_user": sell_user,
                }

                await trades_ex.publish(
                    aio_pika.Message(json.dumps(trade).encode(),
                                     delivery_mode=aio_pika.DeliveryMode.PERSISTENT),
                    routing_key=""
                )
                print("FILLED:", trade)

if __name__ == "__main__":
    asyncio.run(main())
