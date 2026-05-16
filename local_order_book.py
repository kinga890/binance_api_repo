import asyncio
from binance import AsyncClient, BinanceSocketManager

buffer = asyncio.Queue(1000)
local_order_book = {
    'lastUpdateId': 0,
    'bids': {},
    'asks': {}
}


def apply_updates(package, websocket_u):
    for x in package.get('b'):
        if float(x[1]) > 0.0:  # quantity change or new price appeared
            local_order_book['bids'][x[0]] = x[1]
        else:  # bid disappeared float(x[1]) == 0.0
            local_order_book['bids'].pop(x[0], None)
    for y in package.get('a'):
        if float(y[1]) > 0.0:  # quantity change or new price appeared
            local_order_book['asks'][y[0]] = y[1]
        else:  # ask disappeared: float(y[1]) == 0.0
            local_order_book['asks'].pop(y[0], None)
    local_order_book['lastUpdateId'] = int(websocket_u)


async def get_order_book_snapshot(client_from_main):
    order_book = await client_from_main.get_order_book(symbol='BTCUSDT', limit=1000)
    local_order_book.update({
        'lastUpdateId': order_book.get('lastUpdateId'),
        'bids': dict(
            order_book.get('bids')
        ),
        'asks': dict(
            order_book.get('asks')
        )
    })
    snapshot_ID = int(order_book.get('lastUpdateId'))
    return snapshot_ID


async def get_websocket_data(client_from_main):
    websocket_conn = BinanceSocketManager(client=client_from_main)
    depth_stream = websocket_conn.depth_socket(symbol='BTCUSDT')
    return depth_stream


async def first_update(snapshot_ID):
    while True:
        package = await buffer.get()
        websocket_U = int(package.get('U'))
        websocket_u = int(package.get('u'))
        if websocket_U <= snapshot_ID <= websocket_u:
            apply_updates(package, websocket_u)
            break
        else:
            continue


async def condition(client_from_main, depth_stream):
    async def func():
        async with depth_stream as stream:
            while True:
                res = await stream.recv()
                await buffer.put(res)

    task1 = asyncio.create_task(func())
    await asyncio.sleep(3)
    while True:
        local_order_book.clear()
        snapshot_ID = await get_order_book_snapshot(client_from_main)
        await first_update(snapshot_ID)
        while True:
            package = await buffer.get()
            websocket_U = int(package.get('U'))
            websocket_u = int(package.get('u'))
            current_lastUpdateId = int(local_order_book.get('lastUpdateId'))
            if websocket_u < current_lastUpdateId:
                continue
            if websocket_U > (current_lastUpdateId + 1):
                break  # error and exception handling needed here
            if websocket_U == (current_lastUpdateId + 1):
                # subsequent updates
                apply_updates(package, websocket_u)


async def main():
    client_conn = await AsyncClient.create()
    taks_0 = await get_websocket_data(client_conn)
    task_1 = condition(client_conn, taks_0)
    await task_1
    await client_conn.close_connection()


if __name__ == "__main__":
    asyncio.run(main())
