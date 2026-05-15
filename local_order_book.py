import asyncio
from binance import AsyncClient, BinanceSocketManager

buffer = asyncio.Queue(1000)

local_order_book = {}


async def get_order_book_snapshot(client_from_main):
    order_book = await client_from_main.get_order_book(symbol='BTCUSDT', limit=1000)
    print(f"oder book: {order_book}")
    local_order_book.update({

        'lastUpdateID': order_book.get('lastUpdateId'),
        'bids' : dict(
            order_book.get('bids')
        ),
        'asks' : dict(
            order_book.get('asks')
        )
         })


    snapshot_ID = order_book.get('lastUpdateId')
    return snapshot_ID


async def condition(client_from_main, depth_stream):

    async def func():
        async with depth_stream as stream:
            while True:
                res = await stream.recv()
                await buffer.put(res)

    task1 = asyncio.create_task(func())
    await asyncio.sleep(3)

    snapshot_ID = await get_order_book_snapshot(client_from_main)

    while True:
        package = await buffer.get()
        websocket_U = package.get('U')
        websocket_u = package.get('u')
        if websocket_U<=snapshot_ID<=websocket_u:
            print(f"local order book {local_order_book}")
            print(websocket_U,snapshot_ID,websocket_u)
            break
        else:
            continue


async def get_websocket_data(client_from_main):
    websocket_conn = BinanceSocketManager(client=client_from_main)
    depth_stream = websocket_conn.depth_socket(symbol='BTCUSDT')
    return depth_stream

async def main():
    client_conn = await AsyncClient.create()

    taks_0 = await get_websocket_data(client_conn)
    task_1 = condition(client_conn, taks_0)
    await task_1

    await client_conn.close_connection()

asyncio.run(main())



