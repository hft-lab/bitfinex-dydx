import websocket
import threading
import traceback
import json
import logging
import math
from util.subscriptions import NO_SYMBOL_SUBS, DEFAULT_SUBS
from util.api_key import generate_nonce, generate_signature
import time
import requests
import hashlib
import configparser
import sys
import base64
from bravado.client import SwaggerClient
from bravado.requests_client import RequestsClient
from APIKeyAuthenticator import APIKeyAuthenticator
import urllib.parse



# Naive implementation of connecting to BitMEX websocket for streaming realtime data.
# The Marketmaker still interacts with this as if it were a REST Endpoint, but now it can get
# much more realtime data without polling the hell out of the API.
#
# The Websocket offers a bunch of data as raw properties right on the object.
# On connect, it synchronously asks for a push of all this data then returns.
# Right after, the MM can start using its data. It will be updated in realtime, so the MM can
# poll really often if it wants.
class BitMEXWebsocket:

    # Don't grow a table larger than this amount. Helps cap memory usage.
    MAX_TABLE_LEN = 200
    endpoint = 'wss://ws.bitmex.com/realtime'

    def __init__(self, symbol, api_key=None, api_secret=None, subscriptions=DEFAULT_SUBS, leverage=2):
        '''Connect to the websocket and initialize data stores.'''
        subscriptions = ['execution',
                         'instrument',
                         'margin',
                         'order',
                         'position',
                         'quote',
                         'trade',
                         'orderBook10']
        self.logger = logging.getLogger(__name__)
        self.logger.debug("Initializing WebSocket.")

        self.leverage = leverage
        self.symbol = symbol
        self.contract_price = 0

        self.taker_fee = 0.00075
        self.maker_fee = -0.0001

        if api_key is not None and api_secret is None:
            raise ValueError('api_secret is required if api_key is provided')
        if api_key is None and api_secret is not None:
            raise ValueError('api_key is required if api_secret is provided')

        self.api_key = api_key
        self.api_secret = api_secret

        self.data = {}
        self.keys = {}
        self.exited = False

        self.swagger_client = self.swagger_client_init()

        # We can subscribe right in the connection querystring, so let's build that.
        # Subscribe to all pertinent endpoints
        wsURL = self.__get_url(subscriptions)
        self.logger.info("Connecting to %s" % wsURL)
        self.__connect(wsURL, symbol)
        self.logger.info('Connected to WS.')

        # Connected. Wait for partials
        self.__wait_for_symbol(symbol)
        if api_key:
            self.__wait_for_account()
        self.logger.info('Got all market data. Starting.')

    def swagger_client_init(self, config=None):
        if config is None:
            # See full config options at http://bravado.readthedocs.io/en/latest/configuration.html
            config = {
                # Don't use models (Python classes) instead of dicts for #/definitions/{models}
                'use_models': False,
                # bravado has some issues with nullable fields
                'validate_responses': False,
                # Returns response in 2-tuple of (body, response); if False, will only return body
                'also_return_response': True,
            }
        host = 'https://www.bitmex.com'
        spec_uri = host + '/api/explorer/swagger.json'
        if self.api_key and self.api_secret:
            request_client = RequestsClient()
            request_client.authenticator = APIKeyAuthenticator(host, self.api_key, self.api_secret)
            return SwaggerClient.from_url(spec_uri, config=config, http_client=request_client)
        else:
            return SwaggerClient.from_url(spec_uri, config=config)

    def exit(self):
        '''Call this to exit - will close websocket.'''
        self.exited = True
        self.ws.close()

    def get_instrument(self):
        '''Get the raw instrument data for this symbol.'''
        # Turn the 'tickSize' into 'tickLog' for use in rounding
        instrument = self.data['instrument'][0]
        instrument['tickLog'] = int(math.fabs(math.log10(instrument['tickSize'])))
        return instrument

    def get_ticker(self):
        '''Return a ticker object. Generated from quote and trade.'''
        lastQuote = self.data['quote'][-1]
        lastTrade = self.data['trade'][-1]
        ticker = {
            "last": lastTrade['price'],
            "buy": lastQuote['bidPrice'],
            "sell": lastQuote['askPrice'],
            "mid": (float(lastQuote['bidPrice'] or 0) + float(lastQuote['askPrice'] or 0)) / 2
        }

        # The instrument has a tickSize. Use it to round values.
        instrument = self.data['instrument'][0]
        return {k: round(float(v or 0), instrument['tickLog']) for k, v in ticker.items()}

    def funds(self):
        '''Get your margin details.'''
        return self.data['margin'][0]

    def positions(self):
        '''Get your positions.'''
        return self.data['position']

    def market_depth(self):
        return self.data['orderBook10']

    def open_orders(self, clOrdIDPrefix):
        '''Get all your open orders.'''
        orders = self.data['order']
        # Filter to only open orders and those that we actually placed
        return [o for o in orders if str(o['clOrdID']).startswith(clOrdIDPrefix) and self.order_leaves_quantity(o)]

    def recent_trades(self):
        '''Get recent trades.'''
        return self.data['trade']

    #
    # End Public Methods
    def presize_price(self, price):
        ticksize = self.get_instrument()['tickSize']
        if '.' in str(ticksize):
            round_price_len = len(str(ticksize).split('.')[1])
        else:
            round_price_len = 0
        price = round(price - (price % ticksize), round_price_len)
        return price

    def create_order(self, amount, price, side, type):
        price = self.presize_price(price)
        amount = int(round(amount))
        if type == 'Limit':
            self.swagger_client.Order.Order_new(symbol=self.symbol,
                                                side=side,
                                                ordType=type,
                                                orderQty=amount,
                                                price=price).result()
        else:
            self.swagger_client.Order.Order_new(symbol=self.symbol,
                                                side=side,
                                                ordType=type,
                                                orderQty=amount).result()

    def cancel_order(self, orderID):
        self.swagger_client.Order.Order_cancel(orderID=orderID).result()

    def __connect(self, wsURL, symbol):
        '''Connect to the websocket in a thread.'''
        self.logger.debug("Starting thread")

        self.ws = websocket.WebSocketApp(wsURL,
                                         on_message=self.__on_message,
                                         on_close=self.__on_close,
                                         on_open=self.__on_open,
                                         on_error=self.__on_error,
                                         header=self.__get_auth())

        self.wst = threading.Thread(target=lambda: self.ws.run_forever())
        self.wst.daemon = True
        self.wst.start()
        self.logger.debug("Started thread")

        # Wait for connect before continuing
        conn_timeout = 5
        while (not self.ws.sock or not self.ws.sock.connected) and conn_timeout:
            time.sleep(1)
            conn_timeout -= 1
        if not conn_timeout:
            self.logger.error("Couldn't connect to WS! Exiting.")
            self.exit()
            raise websocket.WebSocketTimeoutException('Couldn\'t connect to WS! Exiting.')

    def __get_auth(self):
        '''Return auth headers. Will use API Keys if present in settings.'''
        if self.api_key:
            self.logger.info("Authenticating with API Key.")
            # To auth to the WS using an API key, we generate a signature of a nonce and
            # the WS API endpoint.
            expires = generate_nonce()
            header =  [
                "api-expires: " + str(expires),
                "api-signature: " + generate_signature(self.api_secret, 'GET', '/realtime', expires, ''),
                "api-key:" + self.api_key
            ]
            return header
        else:
            self.logger.info("Not authenticating.")
            return []

    def __get_url(self, subscriptions):
        '''
        Generate a connection URL. We can define subscriptions right in the querystring.
        Most subscription topics are scoped by the symbol we're listening to.
        '''

        # Some subscriptions need to have the symbol appended.
        subscriptions_full = map(lambda sub: (
            sub if sub in NO_SYMBOL_SUBS
            else (sub + ':' + self.symbol)
        ), subscriptions)

        urlParts = list(urllib.parse.urlparse(self.endpoint))
        urlParts[2] += "?subscribe={}".format(','.join(subscriptions_full))
        urlParts[2] += ',orderBook10:XBTUSD'
        return urllib.parse.urlunparse(urlParts)

    def __wait_for_account(self):
        '''On subscribe, this data will come down. Wait for it.'''
        # Wait for the keys to show up from the ws
        while not {'margin', 'position', 'order', 'orderBook10'} <= set(self.data):
            time.sleep(0.1)

    def __wait_for_symbol(self, symbol):
        '''On subscribe, this data will come down. Wait for it.'''
        while not {'instrument', 'trade', 'quote'} <= set(self.data):
            time.sleep(0.1)

    def __send_command(self, command, args=None):
        '''Send a raw command.'''
        if args is None:
            args = []
        self.ws.send(json.dumps({"op": command, "args": args}))

    def __on_message(self, message):
        '''Handler for parsing WS messages.'''
        message = json.loads(message)
        self.logger.debug(json.dumps(message))

        table = message.get("table")
        action = message.get("action")
        try:
            if 'subscribe' in message:
                self.logger.debug("Subscribed to %s." % message['subscribe'])
            elif action:
                if table not in self.data and table == 'orderBook10':
                    self.data[table] = {}
                elif table not in self.data:
                    self.data[table] = []

                # There are four possible actions from the WS:
                # 'partial' - full table image
                # 'insert'  - new row
                # 'update'  - update row
                # 'delete'  - delete row
                if action == 'partial':
                    self.logger.debug("%s: partial" % table)
                    self.keys[table] = message['keys']
                    if table == 'orderBook10':
                        symbol = message['filter']['symbol']
                        self.data[table].update({symbol: message['data']})
                    else:
                        self.data[table] = message['data']
                    # Keys are communicated on partials to let you know how to uniquely identify
                    # an item. We use it for updates.

                elif action == 'insert':
                    self.logger.debug('%s: inserting %s' % (table, message['data']))
                    self.data[table] += message['data']

                    # Limit the max length of the table to avoid excessive memory usage.
                    # Don't trim orders because we'll lose valuable state if we do.
                    if table not in ['order', 'orderBook10'] and len(self.data[table]) > BitMEXWebsocket.MAX_TABLE_LEN:
                        self.data[table] = self.data[table][BitMEXWebsocket.MAX_TABLE_LEN // 2:]

                elif action == 'update':
                    # print(message)
                    self.logger.debug('%s: updating %s' % (table, message['data']))

                    # Locate the item in the collection and update it.
                    for updateData in message['data']:
                        if table == 'orderBook10':
                            symbol = updateData['symbol']
                            self.data[table].update({symbol: updateData})
                        else:
                            item = self.find_by_keys(self.keys[table], self.data[table], updateData)
                            if not item:
                                return  # No item found to update. Could happen before push

                            item.update(updateData)
                            # Remove cancelled / filled orders
                            if table == 'order' and not self.order_leaves_quantity(item):
                                self.data[table].remove(item)
                elif action == 'delete':
                    self.logger.debug('%s: deleting %s' % (table, message['data']))
                    # Locate the item in the collection and remove it.
                    for deleteData in message['data']:
                        item = self.find_by_keys(self.keys[table], self.data[table], deleteData)
                        self.data[table].remove(item)
                else:
                    raise Exception("Unknown action: %s" % action)
        except:
            self.logger.error(traceback.format_exc())

    def __on_error(self, error):
        '''Called on fatal websocket errors. We exit on these.'''
        if not self.exited:
            self.logger.error("Error : %s" % error)
            raise websocket.WebSocketException(error)

    def __on_open(self):
        '''Called when the WS opens.'''
        self.logger.debug("Websocket Opened.")

    def __on_close(self):
        '''Called on websocket close.'''
        self.logger.info('Websocket Closed')

    def order_leaves_quantity(self, o):
        if o['leavesQty'] is None:
            return True
        return o['leavesQty'] > 0

    def find_by_keys(self, keys, table, matchData):
        for item in table:
            if all(item[k] == matchData[k] for k in keys):
                return item

    def get_available_balance(self, side):
        funds = self.funds()
        positions = self.positions()
        change = self.market_depth()['XBTUSD']['bids'][0][0]
        for position in positions:
            if position['symbol'] == self.symbol:
                if position['currentCost']:
                    position_value = -position['foreignNotional'] if position['currentQty'] < 0 else position['foreignNotional']
                    self.contract_price = abs(position_value / position['currentQty'])
                else:
                    position_value = 0
                # currency = position['currency']
        # if currency == 'XBt':
        available_balance = (funds['walletBalance'] / 10**8) * change * self.leverage
        if side == 'Buy':
            return available_balance - position_value
        else:
            return available_balance + position_value


# cp = configparser.ConfigParser()
# if len(sys.argv) != 2:
#     # print("Usage %s <config.ini>" % sys.argv[0])
#     sys.exit(1)
# cp.read(sys.argv[1], "utf-8")
#
# api_key = cp["BITMEX"]["api_key"]
# api_secret = cp["BITMEX"]["api_secret"]
#
# bitmex_client = BitMEXWebsocket(symbol='ETHUSD', api_key=api_key, api_secret=api_secret)
# # print(bitmex_client.market_depth())
# # bitmex_client.create_order(1, 1300, 'Sell', 'Market')
# # bitmex_client.create_order(1, 1300, 'Sell', 'Market')
# # print(bitmex_client.recent_trades())
# # #
# # open_orders = bitmex_client.open_orders('')
# # print(open_orders)
# # id = open_orders[0]['orderID']
#
# # bitmex_client.cancel_order(id)
# while True:
#     time.sleep(1)
#     print(bitmex_client.get_available_balance('Buy'))
#     print(bitmex_client.get_available_balance('Sell'))
#     print(bitmex_client.contract_price)
    # print('FUNDS')
    # print(bitmex_client.funds())
    # print('POSITIONS')
    # print(bitmex_client.positions())
    # print('ORDERBOOK')
    # print(bitmex_client.market_depth())
    # print('\n\n\n\n')

