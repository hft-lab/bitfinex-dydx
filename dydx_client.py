import aiohttp
import asyncio
import uuid
import http.client
import time
import json
import threading

URI_WS = 'wss://api.dydx.exchange/v3/ws'
URI_API = 'https://api.dydx.exchange'

class DydxClient:

    orderbook = {'asks': [], 'bids': []}
    ask_offset = 0
    bid_offset = 0
    _updates = 0

    def __init__(self, symbol, keys=None):
        self._loop = asyncio.new_event_loop()
        self._connected = asyncio.Event()
        self.symbol = symbol


    def run_updater(self):
        self.wst = threading.Thread(target=lambda: self._loop.run_until_complete(self._run_ws_loop()))
        self.wst.daemon = True
        self.wst.start()
            # except Exception as e:
            #     print(f"Error line 33: {e}")

    async def _run_ws_loop(self):
        async with aiohttp.ClientSession() as s:
            async with s.ws_connect(URI_WS) as ws:
                self._connected.set()
                try:
                    self._ws = ws
                    self._loop.create_task(self.subsribe_orderbook(self.symbol))
                    async for msg in ws:
                        self._process_msg(msg)
                finally:
                    self._connected.clear()

    async def subsribe_orderbook(self, symbol):
        msg = {
        'type': 'subscribe',
        'channel': 'v3_orderbook',
        'id': symbol,
        'includeOffsets': True
        }
        await self._connected.wait()
        await self._ws.send_json(msg)

    def first_update(self, ob: dict):
        orderbook = {'asks': [[float(ask['price']), float(ask['size'])] for ask in ob['asks']
                              if float(ask['size']) > 0],
                     'bids': [[float(bid['price']), float(bid['size'])] for bid in ob['bids']
                              if float(bid['size']) > 0]
                     }
        orderbook['asks'], orderbook['bids'] = sorted(orderbook['asks']), sorted(orderbook['bids'])[::-1]
        self.orderbook = orderbook

    def append_new_bid(self, bids):
        orderbook = self.orderbook
        for new_bid in bids:
            found = False
            for bid in orderbook['bids']:
                if float(new_bid[0]) == bid[0]:
                    found = True
                    if float(new_bid[1]) > 0:
                        bid[1] = float(new_bid[1])
                    else:
                        orderbook['bids'].remove(bid)
            if not found:
                new_bid = [float(new_bid[0]), float(new_bid[1])]
                orderbook['bids'].append(new_bid)
                orderbook['bids'] = sorted(orderbook['bids'])[::-1]
        self.orderbook = orderbook

    def append_new_ask(self, asks):
        orderbook = self.orderbook
        for new_ask in asks:
            found = False
            for ask in orderbook['asks']:
                if float(new_ask[0]) == ask[0]:
                    found = True
                    if float(new_ask[1]) > 0:
                        ask[1] = float(new_ask[1])
                    else:
                        orderbook['asks'].remove(ask)
            if not found:
                new_ask = [float(new_ask[0]), float(new_ask[1])]
                orderbook['asks'].append(new_ask)
                orderbook['asks'] = sorted(orderbook['asks'])
        self.orderbook = orderbook

    def channel_update(self, ob: dict):
        if len(ob['bids']):
            if self.bid_offset < int(ob['offset']):
                self.append_new_bid(ob['bids'])
        elif len(ob['asks']):
            if self.ask_offset < int(ob['offset']):
                self.append_new_ask(ob['asks'])


    def _process_msg(self, msg: aiohttp.WSMessage):
        self._updates += 1
        if msg.type == aiohttp.WSMsgType.TEXT:
            obj = json.loads(msg.data)
            if obj['type'] == 'connected':
                pass
            elif obj['type'] == 'subscribed':
                self.first_update(obj['contents'])
            elif obj['type'] == 'channel_data':
                self.channel_update(obj['contents'])
            else:
                print(f"Unknown data: {obj}")

    def get_orderbook(self):
        return self.orderbook


# client = DydxClient('BTC-USD')
# client.run_updater()
# while True:
#     time.sleep(5)
#     print(client.get_orderbook())