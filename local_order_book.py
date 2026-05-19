import asyncio
import time
from binance import AsyncClient, BinanceSocketManager
import logging
from logging.handlers import RotatingFileHandler
import aiofiles
import aiofiles.os
import heapq


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
                'lastUpdateId': int(order_book.get('lastUpdateId')),
                'bids': {float(price) : float(quantity) for price,quantity in order_book.get('bids')},
                'asks': {float(price) : float(quantity) for price,quantity in order_book.get('asks')}
                # those bids and asks are a full picture of the market, not just changes that have appeared
            })
            snapshot_ID = order_book.get('lastUpdateId')
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
            local_order_book.clear()
            snapshot_ID = await get_order_book_snapshot(client_connection)
            await websocket_snapshot_match(snapshot_ID)
            while True:
                package = await buffer.get()
                websocket_U = int(package.get('U'))
                websocket_u = int(package.get('u'))
                event_time = int(package.get('E'))
                data_obtained_time = package.get('data_obtained_time')
                current_id = local_order_book.get('lastUpdateId')
                if websocket_u < current_id:
                    continue
                if websocket_U > (current_id + 1):

                    nan_row = "Nan,Nan,Nan,Nan,Nan\n"
                    async with aiofiles.open('database.csv', mode='a') as file:
                        await file.write(nan_row)

                    break
                if websocket_U == (current_id + 1):
                    # subsequent updates
                    apply_updates(package, websocket_u,event_time,data_obtained_time)

                    await write_to_database(event_time)

        else:
            await asyncio.sleep(3)

async def write_to_database(event_time):

        #taking top 50 bids from a package
        normal_row = []
        top_50_bids = heapq.nlargest(50, local_order_book['bids'].items(), key= lambda x : x[0])
        top_50_asks = heapq.nsmallest(50, local_order_book['asks'].items(), key= lambda x : x[0])
        for (x,y), (z,w) in zip(top_50_bids,top_50_asks):
            bid_price = x
            bid_quantity = y
            ask_price = z
            ask_quantity = w

            normal_row.append(f"{event_time},{bid_price},{bid_quantity},{ask_price},{ask_quantity}\n")

        file_exists = await aiofiles.os.path.exists('database.csv')

        async with aiofiles.open('database.csv', mode='a') as file:

            if not file_exists:
                
                header = "time,bid_price,bid_quantity,ask_price,ask_quantity\n"
                await file.write(header)

            await file.writelines(normal_row)


class ContextManager:
    def __init__(self, client):
        self.client = client     #just an attribute

    async def __aenter__(self):
        client_connection = self.client
        return client_connection

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.client.close_connection()


async def main():
    async with ContextManager(await AsyncClient.create()) as client_connection:

        websocket_stream = await create_websocket_tunnel(client_connection)  # creating tunnel and returning websocket stream

        asyncio.create_task(fetch_websocket_data(websocket_stream))   # fetching data from a websocket stream and putting them into the buffer (this never stops)

        await maintain_order_book(client_connection)



if __name__ == "__main__":
    asyncio.run(main())




