import asyncio
from binance import AsyncClient, BinanceSocketManager

async def get_order_book_snapshot(client_from_main):
    order_book = await client_from_main.get_order_book(symbol='BTCUSDT', limit=1000)
    snapshot_ID = order_book.get('lastUpdateId')
    return snapshot_ID

websocket_data = []

async def get_websocket_data(client_from_main):
    websocket_conn = BinanceSocketManager(client=client_from_main)
    depth_streams = websocket_conn.depth_socket(symbol='BTCUSDT')

    async def func():
        async with depth_streams as stream:
            while True:
                res = await stream.recv()
                websocket_data.append(res)

    task1 = asyncio.create_task(func())
    await asyncio.sleep(3)
    websocket_U = websocket_data[0]['U']
    websocket_u = websocket_data[0]['u']

    while True:
        res1 = await get_order_book_snapshot(client_from_main)
        if websocket_U > res1:
            continue
        else:
            print(websocket_U,res1)
            break


async def main():
    client_conn = await AsyncClient.create()
    await get_websocket_data(client_conn)
    await client_conn.close_connection()

asyncio.run(main())



