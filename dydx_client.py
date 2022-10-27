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
    _updates = 0
    offsets = {}


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
                    self._loop.create_task(self._subsribe_orderbook(self.symbol))
                    async for msg in ws:
                        self._process_msg(msg)
                finally:
                    self._connected.clear()


    async def _subsribe_orderbook(self, symbol):
        msg = {
        'type': 'subscribe',
        'channel': 'v3_orderbook',
        'id': symbol,
        'includeOffsets': True
        }
        await self._connected.wait()
        await self._ws.send_json(msg)


    def _first_update(self, ob: dict):
        for ask in ob['asks']:
            if float(ask['size']) > 0:
                self.orderbook['asks'].append([float(ask['price']), float(ask['size']), int(ask['offset'])])
            self.offsets[ask['price']] = int(ask['offset'])
        for bid in ob['bids']:
            if float(bid['size']) > 0:
                self.orderbook['bids'].append([float(bid['price']), float(bid['size']), int(bid['offset'])])
            self.offsets[bid['price']] = int(bid['offset'])
        self.orderbook['asks'] = sorted(self.orderbook['asks'])
        self.orderbook['bids'] = sorted(self.orderbook['bids'])[::-1]


    def _append_new_order(self, ob, side):
        offset = int(ob['offset'])
        for new_order in ob[side]:
            if self.offsets.get(new_order[0]):
                if self.offsets[new_order[0]] > offset:
                    continue
            self.offsets[new_order[0]] = offset
            new_order = [float(new_order[0]), float(new_order[1]), offset]
            index = 0
            for order in self.orderbook[side]:
                if new_order[0] == order[0]:
                    if new_order[1] != 0.0:
                        order[1] = new_order[1]
                        order[2] = offset
                        break
                    else:
                        self.orderbook[side].remove(order)
                        break
                if side == 'bids':
                    if new_order[0] > order[0]:
                        self.orderbook[side].insert(index, new_order)
                        break
                elif side == 'asks':
                    if new_order[0] < order[0]:
                        self.orderbook[side].insert(index, new_order)
                        break
                index += 1
            if index == 0:
                self._check_for_error()



    def _channel_update(self, ob: dict):
        if len(ob['bids']):
            self._append_new_order(ob, 'bids')
        if len(ob['asks']):
            self._append_new_order(ob, 'asks')


    def _check_for_error(self):
        orderbook = self.orderbook
        top_ask = orderbook['asks'][0]
        top_bid = orderbook['bids'][0]
        if top_ask[0] < top_bid[0]:
            if top_ask[2] <= top_bid[2]:
                self.orderbook['asks'].remove(top_ask)
            else:
                self.orderbook['bids'].remove(top_bid)



    def _process_msg(self, msg: aiohttp.WSMessage):
        self._updates += 1
        if msg.type == aiohttp.WSMsgType.TEXT:
            obj = json.loads(msg.data)
            if obj['type'] == 'connected':
                pass
            elif obj['type'] == 'subscribed':
                self._first_update(obj['contents'])
            elif obj['type'] == 'channel_data':
                self._channel_update(obj['contents'])


    def get_orderbook(self):
        return self.orderbook


# client = DydxClient('BTC-USD')
# client.run_updater()
# while True:
#     time.sleep(.1)
#     orderbook = client.get_orderbook()
#     print(f"Asks: {orderbook['asks'][:10]}")
#     print(f"Bids: {orderbook['bids'][:10]}")
#     print()