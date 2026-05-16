import asyncio
from binance import AsyncClient, BinanceSocketManager, BinanceAPIException, BinanceRequestException
import logging
from logging.handlers import RotatingFileHandler

logging.basicConfig(level = logging.INFO,format = '%(asctime)s - %(levelname)s - %(message)s',
                    handlers=[ RotatingFileHandler('log.log', maxBytes=5*1024*1024, backupCount=5),
        logging.StreamHandler() ]
)
buffer = asyncio.Queue()

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

    while True:
        try:
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
        except (BinanceAPIException,BinanceRequestException) as error:
            logging.error(error)
            await asyncio.sleep(5)
        except Exception as e:
            logging.error(f'unknown error: {e}')
            await asyncio.sleep(5)


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
        while True:
            try:
                async with depth_stream as stream:
                    while True:
                        res = await asyncio.wait_for(stream.recv(), timeout= 10.0)
                        await buffer.put(res)
            except Exception as e:
                logging.error('websocket error')
                await asyncio.sleep(5)

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
                break
            if websocket_U == (current_lastUpdateId + 1):
                # subsequent updates
                apply_updates(package, websocket_u)


async def main():

    while True:
        try:
            client_conn = await AsyncClient.create()
            break
        except Exception as e:
            logging.error('client error')
            await asyncio.sleep(5)
    try:
        taks_0 = await get_websocket_data(client_conn)
        task_1 = condition(client_conn, taks_0)
        await task_1
    finally:
        await client_conn.close_connection()


if __name__ == "__main__":
    asyncio.run(main())

