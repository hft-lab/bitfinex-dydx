import aiohttp
import asyncio
import uuid
import http.client
import time
import json
import threading
from web3 import Web3
from dydx3 import Client
from dydx3.constants import API_HOST_MAINNET
from dydx3.constants import WS_HOST_MAINNET
from dydx3.constants import API_HOST_MAINNET
from dydx3.constants import NETWORK_ID_MAINNET
from dydx3.helpers.request_helpers import generate_now_iso

URI_WS = 'wss://api.dydx.exchange/v3/ws'
URI_API = 'https://api.dydx.exchange'



class DydxClient:


    orderbook = {'asks': [], 'bids': []}
    positions = {}
    orders = {}
    
    _updates = 0
    offsets = {}



    def __init__(self, symbol, keys=None, leverage=2):
        self._loop = asyncio.new_event_loop()
        self._connected = asyncio.Event()
        self.symbol = symbol
        self.API_KEYS = {"secret" : keys['secret'],
                    "key" : keys['key'],
                    "passphrase" : keys['passphrase']}
        self.client = Client(
            network_id=NETWORK_ID_MAINNET,
            host=API_HOST_MAINNET,
            default_ethereum_address=keys['eth_address'],
            web3=Web3(Web3.WebsocketProvider(f'wss://mainnet.infura.io/ws/v3/{keys["infura_key"]}')),
            eth_private_key=keys['eth_private_key'],
            stark_private_key=keys['privateKey'],
            stark_public_key=keys['publicKey'],
            stark_public_key_y_coordinate=keys['publicKeyYCoordinate'],
            web3_provider=f'https://mainnet.infura.io/v3/{keys["infura_key"]}',
            api_key_credentials=self.API_KEYS
        )

        self.orders = {}
        self.positions = {}
        self.fills = []
        self.balance = {'free': 0, 'total': 0}

        self.keys = keys
        self.user = self.client.private.get_user().data
        self.balance = self.client.private.get_account().data
        self.markets = self.client.public.get_markets()
        self.leverage = leverage

        self.position_id = self.balance['account']['positionId']

        self.maker_fee = float(self.user['user']['makerFeeRate']) * 0.7
        self.taker_fee = float(self.user['user']['takerFeeRate']) * 0.7

        self.ticksize = float(self.markets.data['markets'][symbol]['tickSize'])
        self.stepsize = float(self.markets.data['markets'][symbol]['stepSize'])

    def cancel_order(self, orderID):
        self.client.private.cancel_order(order_id=orderID)

    def presize_amount(self, amount):
        if '.' in str(self.stepsize):
            round_amount_len = len(str(self.stepsize).split('.')[1])
        else:
            round_amount_len = 0
        amount = str(round(amount - (amount % self.stepsize), round_amount_len))
        return amount

    def presize_price(self, price):
        if '.' in str(self.ticksize):
            round_price_len = len(str(self.ticksize).split('.')[1])
        else:
            round_price_len = 0
        price = str(round(price - (price % self.ticksize), round_price_len))
        return price


    def create_order(self, amount, price, side, type, expire=100):
        expire_date = int(round(time.time()) + expire)
        amount = self.presize_amount(amount)
        price = self.presize_price(price)
        self.client.private.create_order(
            position_id=self.position_id,  # required for creating the order signature
            market=self.symbol,
            side=side,
            order_type=type,
            post_only=False,
            size=amount,
            price=price,
            limit_fee='0.0008',
            expiration_epoch_seconds=expire_date,
            time_in_force='GTT',
            )


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
                    self._loop.create_task(self._subscribe_account())
                    async for msg in ws:
                        self._process_msg(msg)
                finally:
                    self._connected.clear()

    async def _subscribe_account(self):
        now_iso_string = generate_now_iso()
        signature = self.client.private.sign(
            request_path='/ws/accounts',
            method='GET',
            iso_timestamp=now_iso_string,
            data={},
        )
        msg = {
            'type': 'subscribe',
            'channel': 'v3_accounts',
            'accountNumber': '0',
            'apiKey': self.keys['key'],
            'passphrase': self.keys['passphrase'],
            'timestamp': now_iso_string,
            'signature': signature,
        }
        await self._connected.wait()
        await self._ws.send_json(msg)

    async def _subsribe_orderbook(self, symbol):
        msg = {
        'type': 'subscribe',
        'channel': 'v3_orderbook',
        'id': symbol,
        'includeOffsets': True
        }
        await self._connected.wait()
        await self._ws.send_json(msg)


    def _first_orderbook_update(self, ob: dict):
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



    def _channel_orderbook_update(self, ob: dict):
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

    def _update_positions(self, positions):
        for position in positions:
            self.positions.update({position['market']: position})
            # position_example = [{'id': '312711e6-d172-5e5b-9dc8-362101e94756',
            # 'accountId': 'f47ae945-06ae-5c47-aaad-450c0ffc6164', 'market': 'SNX-USD',
            # 'side': 'LONG/SHORT',
            # 'status': 'OPEN', 'size': '13129.1', 'maxSize': '25107', 'entryPrice': '2.363965',
            # 'exitPrice': '2.398164', 'openTransactionId': '110960769',
            # 'closeTransactionId': None, 'lastTransactionId': '114164888', 'closedAt': None,
            # 'updatedAt': '2022-10-11T00:50:34.217Z', 'createdAt': '2022-10-11T00:50:34.217Z',
            # 'sumOpen': '219717.4', 'sumClose': '206588.3', 'netFunding': '706.266653',
            # 'realizedPnl': '7771.372704'}]

    def get_positions(self):
        return self.positions

    def _update_orders(self, orders):
        for order in orders:
            if self.orders.get(order['market']):
                if not order['status'] in ['CANCELED', 'FILLED']:
                    self.orders[order['market']].update({order['id']: order})
                else:
                    self.orders.pop(order['id'])
            else:
                self.orders.update({order['market']: {order['id']: order}})
            # order_example = [{'id': '28c21ee875838a5e349cf96d678d8c6151a250f979d6a025b3f79dcca703558',
            # 'clientId': '7049071120643888', 'market': 'SNX-USD',
            # 'accountId': 'f47ae945-06ae-5c47-aaad-450c0ffc6164', 'side': 'SELL', 'size': '483.3',
            # 'remainingSize': '0', 'limitFee': '0.0008', 'price': '2.47', 'triggerPrice': None,
            # 'trailingPercent': None, 'type': 'LIMIT', 'status': 'FILLED/OPEN/PENDING/CANCELED',
            # 'signature': '',
            # 'timeInForce': 'GTT', 'postOnly': False, 'cancelReason': None,
            # 'expiresAt': '2022-11-04T13:11:20.000Z', 'unfillableAt': '2022-11-03T13:18:00.185Z',
            # 'updatedAt': '2022-11-03T13:18:00.185Z', 'createdAt': '2022-11-03T13:18:00.148Z',
            # 'reduceOnly': False, 'country': 'JP', 'client': None, 'reduceOnlySize': None}]

    def get_orders(self):
        return self.orders

    def __update_fill(self, accumulated_fills, fill):
        old_size = accumulated_fills[fill['market']]['size']
        old_price = accumulated_fills[fill['market']]['price']
        old_fee = accumulated_fills[fill['market']]['fee']

        new_size = old_size + float(fill['size'])
        new_price = (old_price * old_size + float(fill['price']) * float(fill['size'])) / (new_size)
        new_fee = old_fee + float(fill['fee'])

        accumulated_fills[fill['market']]['size'] = new_size
        accumulated_fills[fill['market']]['price'] = new_price
        accumulated_fills[fill['market']]['fee'] = new_fee
        return accumulated_fills

    def _update_fills(self, fills):
        accumulated_fills = {}
        for fill in fills:
            if not accumulated_fills.get(fill['market']):
                for key in ['fee', 'price', 'size']:
                    fill[key] = float(fill[key])
                accumulated_fills.update({fill['market']: fill})
            else:
                accumulated_fills = self.__update_fill(accumulated_fills, fill)
        for market, fill in accumulated_fills:
            if self.fills.get(market):
                self.fills[market].insert(0, fill)
            else:
                self.fills.update({market: [fill]})
        # example = [{'market': 'SNX-USD', 'transactionId': '114163898', 'quoteAmount': '17.29',
        # 'price': '2.470000', 'size': '7', 'liquidity': 'TAKER',
        # 'accountId': 'f47ae945-06ae-5c47-aaad-450c0ffc6164', 'side': 'SELL',
        # 'orderId': '28c21ee875838a5e349cf96d678d8c6151a250f979d6a025b3f79dcca703558',
        # 'fee': '0.004599', 'type': 'LIMIT', 'id': 'b6252559-f7c2-5ad5-afb5-0e33144ccdfc',
        # 'nonce': None, 'forcePositionId': None, 'updatedAt': '2022-11-03T13:18:00.185Z',
        # 'createdAt': '2022-11-03T13:18:00.185Z', 'orderClientId': '7049071120643888'}]

    def get_fills(self):
        return self.fills

    def get_balance(self):
        return self.balance

    def _update_account(self, account):
        self.balance = {'free': float(account['freeCollateral']),
                        'total': float(account['equity'])}
        for market, position in account['openPositions'].items():
            self.positions[market] = position
    # example = {'starkKey': '03124cf5bb8e07d4a5d05cd2d6f79a13f4c370130296df9698210dbec21d927a',
    #    'positionId': '208054', 'equity': '71276.226361', 'freeCollateral': '63848.633515',
    #    'pendingDeposits': '0.000000', 'pendingWithdrawals': '0.000000', 'openPositions': {
    # 'SNX-USD': {'market': 'SNX-USD', 'status': 'OPEN', 'side': 'LONG', 'size': '13438.1',
    #             'maxSize': '25107', 'entryPrice': '2.363881', 'exitPrice': '2.397996',
    #             'unrealizedPnl': '1506.202651', 'realizedPnl': '7737.476749',
    #             'createdAt': '2022-10-11T00:50:34.217Z', 'closedAt': None,
    #             'sumOpen': '219543.1', 'sumClose': '206105.0', 'netFunding': '706.266653'},
    # 'ETH-USD': {'market': 'ETH-USD', 'status': 'OPEN', 'side': 'SHORT', 'size': '-10.688',
    #             'maxSize': '-25.281', 'entryPrice': '1603.655165',
    #             'exitPrice': '1462.275353', 'unrealizedPnl': '749.364703',
    #             'realizedPnl': '8105.787595', 'createdAt': '2022-08-16T22:56:10.625Z',
    #             'closedAt': None, 'sumOpen': '71.478', 'sumClose': '60.790',
    #             'netFunding': '-488.691199'}}, 'accountNumber': '0',
    #    'id': 'f47ae945-06ae-5c47-aaad-450c0ffc6164', 'quoteBalance': '87257.614961',
    #    'createdAt': '2022-08-16T18:52:16.881Z'}


    def get_available_balance(self, side):
        balance = self.balance
        positions = self.positions
        tot_pos_value = 0
        for market, position in positions.items():
            pos_size = abs(float(position['size']))
            pos_entry = float(position['entryPrice'])
            pos_unrealized = float(position['unrealizedPnl'])
            if market == self.symbol:
                position_value = pos_size * pos_entry + pos_unrealized
                continue
            tot_pos_value += pos_size * pos_entry + pos_unrealized
        position_value = position_value if position['side'] == 'LONG' else -position_value
        total_available_margin = balance['total'] * self.leverage
        available_margin = total_available_margin - tot_pos_value
        if side == 'Buy':
            return available_margin - position_value
        elif side == 'Sell':
            return available_margin + position_value

    def _process_msg(self, msg: aiohttp.WSMessage):
        if msg.type == aiohttp.WSMsgType.TEXT:
            obj = json.loads(msg.data)
            if obj.get('channel'):
                if obj['channel'] == 'v3_orderbook':
                    self._updates += 1
                    if obj['type'] == 'subscribed':
                        self._first_orderbook_update(obj['contents'])
                    elif obj['type'] == 'channel_data':
                        self._channel_orderbook_update(obj['contents'])
                elif obj['channel'] == 'v3_accounts':
                    if obj['contents'].get('positions'):
                        if len(obj['contents']['positions']):
                            self._update_positions(obj['contents']['positions'])
                    if obj['contents'].get('orders'):
                        if len(obj['contents']['orders']):
                            self._update_orders(obj['contents']['orders'])
                    if obj['contents'].get('fills'):
                        if len(obj['contents']['fills']):
                            self._update_fills(obj['contents']['fills'])
                    if obj['contents'].get('account'):
                        if len(obj['contents']['account']):
                            self._update_account(obj['contents']['account'])

                            # print('ACCOUNT!!!:')
                            # print(obj['contents']['account'])
                            # print()

    def get_orderbook(self):
        return self.orderbook




# import configparser
# import sys
#
# cp = configparser.ConfigParser()
# if len(sys.argv) != 2:
#     # print("Usage %s <config.ini>" % sys.argv[0])
#     sys.exit(1)
# cp.read(sys.argv[1], "utf-8")
# #
# dydx_keys = cp['DYDX']
# client = DydxClient('ETH-USD', dydx_keys)
# client.run_updater()
# # time_start = time.time()
# # client.create_order(amount=0.1, price=1200, side='BUY', expire=100, type='LIMIT'/'MARKET')
# time.sleep(3)
# print(client.get_available_balance('Buy'))
# print(client.get_available_balance('Sell'))

# # print(f"Order create function time: {time.time() - time_start}")
# # while True:
#     # time.sleep(1)
#     # orderbook = client.get_orderbook()
# #     print(f"Asks: {orderbook['asks'][:10]}")
# #     print(f"Bids: {orderbook['bids'][:10]}")
# #     print()
# print('Balance')
# print(client.get_balance())
# print()
# print('Positions')
# print(client.get_positions())
# print()
# print('Orders')
# print(client.get_orders())
# print()
# print('Fills')
# print(client.get_fills())
# print()