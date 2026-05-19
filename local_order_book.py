import asyncio
import time
from binance import AsyncClient, BinanceSocketManager
import logging
from logging.handlers import RotatingFileHandler

logging.basicConfig(level = logging.INFO,format = '%(asctime)s - %(levelname)s - %(message)s',
                    handlers=[ RotatingFileHandler('logs/log.log', maxBytes=5 * 1024 * 1024, backupCount=5),
        logging.StreamHandler() ]
)
buffer = asyncio.Queue()

local_order_book = {
    'E' : 0,
    'data_obtained_time' : 0,
    'lastUpdateId': 0,
    'bids': {},
    'asks': {}
}

def apply_updates(package, websocket_u,event_time, data_obtained_time):
    for x in package.get('b'):
        bid_price = float(x[0])
        bid_quantity = float(x[1])
        if float(bid_quantity) > 0.0:  # quantity change or new price appeared
            local_order_book['bids'][bid_price] = bid_quantity
        else:  # bid disappeared float(x[1]) == 0.0
            local_order_book['bids'].pop(bid_price, None)
    for y in package.get('a'):
        ask_price = float(y[0])
        ask_quantity = float(y[1])
        if float(ask_quantity) > 0.0:  # quantity change or new price appeared
            local_order_book['asks'][ask_price] = ask_quantity
        else:  # ask disappeared: float(y[1]) == 0.0
            local_order_book['asks'].pop(ask_price, None)
    local_order_book['E'] = event_time
    local_order_book['data_obtained_time'] = data_obtained_time
    local_order_book['lastUpdateId'] = websocket_u


async def get_order_book_snapshot(client_connection):

    while True:
        try:
            order_book = await client_connection.get_order_book(symbol='BTCUSDT', limit=1000)
            local_order_book.update({
                # 'E': int(time.time() * 1000),
                # 'data_obtained_time': int(time.time() * 1000)
                # both are useless here since im overwriting those in websocket_snapshot_match()
                'lastUpdateId': int(order_book.get('lastUpdateId')),
                'bids': {float(price) : float(quantity) for price,quantity in order_book.get('bids')},
                'asks': {float(price) : float(quantity) for price,quantity in order_book.get('asks')}
                # bids and asks must be here since its a full picture of the market not just changes as in websocket packages
            })
            snapshot_ID = order_book.get('lastUpdateId')  #tu było wcześniej int ale nie potrzeba bo to już jest intem
            return snapshot_ID
        except Exception as e:
            logging.error(f'getting snapshot error: {e}')
            await asyncio.sleep(5)


async def create_websocket_tunnel(client_connection):
    websocket_tunnel = BinanceSocketManager(client=client_connection)
    websocket_stream = websocket_tunnel.depth_socket(symbol='BTCUSDT')
    return websocket_stream


async def websocket_snapshot_match(snapshot_ID):
    while True:
        package = await buffer.get()
        websocket_U = int(package.get('U'))
        websocket_u = int(package.get('u'))
        event_time = int(package.get('E'))
        data_obtained_time = package.get('data_obtained_time')
        if websocket_U <= snapshot_ID <= websocket_u:
            apply_updates(package, websocket_u,event_time,data_obtained_time)
            break
        else:
            continue

async def fetch_websocket_data(websocket_stream):
    while True:
        try:
            async with websocket_stream as stream:
                while True:
                    res = await asyncio.wait_for(stream.recv(), timeout= 10.0)
                    res['data_obtained_time'] = int(time.time() * 1000)
                    await buffer.put(res)
        except Exception as e:
            logging.error('websocket error')
            await asyncio.sleep(5)


async def maintain_order_book(client_connection):

    while True:
        if buffer.qsize() != 0:
            local_order_book.clear()    #data must go into csv before this !!!
            snapshot_ID = await get_order_book_snapshot(client_connection)
            await websocket_snapshot_match(snapshot_ID)
            while True:
                package = await buffer.get()
                websocket_U = int(package.get('U'))
                websocket_u = int(package.get('u'))
                event_time = int(package.get('E'))
                data_obtained_time = package.get('data_obtained_time')   # already int
                current_id = local_order_book.get('lastUpdateId')
                if websocket_u < current_id:
                    continue
                if websocket_U > (current_id + 1):
                    break        #!!!! i wanna se Nan in csv
                if websocket_U == (current_id + 1):
                    # subsequent updates
                    apply_updates(package, websocket_u,event_time,data_obtained_time)
        else:
            await asyncio.sleep(3)


async def main():

    client_connection = await AsyncClient.create()

    websocket_stream = await create_websocket_tunnel(client_connection)   #creating tunnel and returning websocket stream

    asyncio.create_task(fetch_websocket_data(websocket_stream))   #fetching data from a websocket stream and putting them into the buffer (this never stops)

    await maintain_order_book(client_connection)

    await client_connection.close_connection()


if __name__ == "__main__":
    asyncio.run(main())

