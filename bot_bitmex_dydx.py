import bitmexClient
import dydx_client
import time
import telebot
import logging
import configparser
import sys
import re
from dataBaseBitmex import database

cp = configparser.ConfigParser()
if len(sys.argv) != 2:
    # print("Usage %s <config.ini>" % sys.argv[0])
    sys.exit(1)
cp.read(sys.argv[1], "utf-8")

FORMAT = '%(asctime)s %(levelname)s %(filename)s:%(lineno)d %(message)s (%(funcName)s)'
logging.basicConfig(format=FORMAT)
log = logging.getLogger("sample bot")
log.setLevel(logging.DEBUG)


class botDydxBitmex:

    chat_id = -627169029
    # chat_id = cp["TELEGRAM"]["chat_id"]
    telegram_bot = telebot.TeleBot('5217076830:AAGm9ecNut2j0oQIQRPc31RGiHK4-lTsw1s')

    def __init__(self, symbol_bitmex, symbol_dydx):
        self.symbol_bitmex = symbol_bitmex
        self.symbol_dydx = symbol_dydx
        self.contract_price = 0.0015
        self.contract_price_asset = 'BTC'
        self.last_message = None

        self.database = database(self.telegram_bot, self.chat_id)
        self.client_Bitmex = bitmexClient.BitMEXWebsocket(symbol_bitmex,
                                                          cp["BITMEX"]["api_key"],
                                                          cp["BITMEX"]["api_secret"])

        self.client_DYDX = dydx_client.DydxClient(symbol_dydx, cp['DYDX'])
        self.client_DYDX.run_updater()
        self.dydx_fee = self.client_DYDX.taker_fee
        # self.dydx_fee = 0.00014
        self.bitmex_fee = self.client_Bitmex.taker_fee
        self.bitmex_fee = 0.00025

        self.telegram_bot.send_message(self.chat_id, f'Bot Bitmex-Dydx started. Pairs: {symbol_bitmex}-{symbol_dydx}')

    def find_price_diffs(self):
        orderbook_bitmex = self.client_Bitmex.market_depth()[self.symbol_bitmex]
        orderbook_dydx = self.client_DYDX.get_orderbook()
        if orderbook_dydx['bids'][0][0] > orderbook_bitmex['asks'][0][0]:
            self.count_dydx_sell_profit()
        if orderbook_bitmex['bids'][0][0] > orderbook_dydx['asks'][0][0]:
            self.count_bitmex_sell_profit()

    def count_bitmex_sell_profit(self):
        orderbook_bitmex = self.client_Bitmex.market_depth()[self.symbol_bitmex]
        orderbook_dydx = self.client_DYDX.get_orderbook()
        buy_price = orderbook_dydx['asks'][0][0] + self.dydx_fee * orderbook_dydx['asks'][0][0]
        sell_price = orderbook_bitmex['bids'][0][0] - self.bitmex_fee * orderbook_bitmex['bids'][0][0]
        profit = (sell_price - buy_price) / buy_price * 100
        # print(f"SELL bitmex profit: {profit}")
        if profit > 0:
            if self.last_message == f"Ask: {orderbook_dydx['asks'][0]}, Bid: {orderbook_bitmex['bids'][0]}, Line 66":
                return
            # print(f"Ask: {orderbook_dydx['asks'][0]}, Bid: {orderbook_bitmex['bids'][0]}, Line 66")
            self.last_message = f"Ask: {orderbook_dydx['asks'][0]}, Bid: {orderbook_bitmex['bids'][0]}, Line 66"
            self.create_deal(orderbook_dydx['asks'][0], orderbook_bitmex['bids'][0], 'BITMEX')

    def count_dydx_sell_profit(self):
        orderbook_bitmex = self.client_Bitmex.market_depth()[self.symbol_bitmex]
        orderbook_dydx = self.client_DYDX.get_orderbook()
        buy_price = orderbook_bitmex['asks'][0][0] + self.bitmex_fee * orderbook_bitmex['asks'][0][0]
        sell_price = orderbook_dydx['bids'][0][0] - self.dydx_fee * orderbook_dydx['bids'][0][0]
        profit = (sell_price - buy_price) / buy_price * 100
        # print(f"BUY bitmex profit: {profit}")
        if profit > 0:
            if self.last_message == f"Ask: {orderbook_bitmex['asks'][0]}, Bid: {orderbook_dydx['bids'][0]}, Line 75":
                return
            # print(f"Ask: {orderbook_bitmex['asks'][0]}, Bid: {orderbook_dydx['bids'][0]}, Line 75")
            self.last_message = f"Ask: {orderbook_bitmex['asks'][0]}, Bid: {orderbook_dydx['bids'][0]}, Line 75"
            self.create_deal(orderbook_bitmex['asks'][0], orderbook_dydx['bids'][0], 'DYDX')

    def _amounts_define(self, amount, contract_price):
        lot_size = self.client_Bitmex.get_instrument()['lotSize']
        if self.symbol_bitmex == 'XBTUSD':
            amount = int(round(amount - (amount % lot_size)))
            return amount, amount
        else:
            amount_bitmex = int(round(amount / contract_price))
            amount_bitmex -= amount_bitmex % lot_size
            amount = amount_bitmex * contract_price
            return amount, amount_bitmex


    def create_deal(self, ask, bid, exchange):
        contract_price = self.client_Bitmex.contract_price
        side_bit = 'Sell' if exchange == 'BITMEX' else 'Buy'
        side_dydx = 'Buy' if exchange == 'BITMEX' else 'Sell'
        avail_bal_dydx = self.client_DYDX.get_available_balance(side_dydx)
        avail_bal_bitmex = self.client_Bitmex.get_available_balance(side_bit)
        if exchange == 'BITMEX':
            amount = min((bid[1] * contract_price), (ask[1] * ask[0]), avail_bal_dydx, avail_bal_bitmex)
            if amount > 100:
                amount, amount_bitmex = self._amounts_define(amount, contract_price)
                amount_dydx = amount / ask[0]
                self.client_Bitmex.create_order(amount_bitmex, bid[0], 'Sell', 'Market')
                self.client_DYDX.create_order(amount_dydx, ask[0] * 1.01, 'BUY', 'LIMIT')
            else:
                return
        else:
            amount = min((ask[1] * contract_price), (bid[1] * bid[0]), avail_bal_dydx, avail_bal_bitmex)
            if amount > 100:
                amount, amount_bitmex = self._amounts_define(amount, contract_price)
                amount_dydx = amount / bid[0]
                self.client_Bitmex.create_order(amount_bitmex, ask[0], 'Buy', 'Market')
                self.client_DYDX.create_order(amount_dydx, bid[0] / 1.01, 'SELL', 'LIMIT')
            else:
                return
        self.create_message(ask, bid, exchange, amount, amount_dydx)

    def define_real_bit_price(self, exchange):
        side = 'Sell' if exchange == "BITMEX" else 'Buy'
        last_trades = self.client_Bitmex.recent_trades()
        real_bitmex_price = None
        for trade in last_trades:
            if trade['side'] == side and trade['symbol'] == self.symbol_bitmex:
                real_bitmex_price = trade['price']
        return real_bitmex_price

    def define_real_dydx_price(self, exchange):
        side = 'BUY' if exchange == "BITMEX" else 'SELL'
        last_trade_dydx = self.client_DYDX.get_fills()
        real_dydx_price = None
        if last_trade_dydx.get(self.client_DYDX.symbol):
            last_trade_dydx = last_trade_dydx[self.client_DYDX.symbol][0]
            if last_trade_dydx['side'] == side:
                real_dydx_price = last_trade_dydx['price']
        return real_dydx_price

    def create_to_base(self, ask, bid, exchange, amount, amount_dydx, real_sell_price, real_buy_price):
        sell_price = real_sell_price if real_sell_price else bid[0]
        buy_price = real_buy_price if real_buy_price else ask[0]
        if exchange == 'BITMEX':
            profit = ((1 - self.bitmex_fee) * sell_price - (1 + self.dydx_fee) * buy_price) / buy_price
        else:
            profit = ((1 - self.dydx_fee) * sell_price - (1 + self.bitmex_fee) * buy_price) / buy_price
        profit_USD = profit * amount
        to_base = {'timestamp': int(round(time.time())),
                   'sell_exchange': exchange,
                   'sell_price': sell_price,
                   'buy_price': buy_price,
                   'amount_USD': amount,
                   'amount_coin': amount_dydx,
                   'profit_USD': profit_USD,
                   'profit_relative': profit,
                   'fee_dydx': self.dydx_fee,
                   'fee_bitmex': self.bitmex_fee}
        return to_base

    def create_message(self, ask, bid, exchange, amount, amount_dydx):
        time.sleep(0.5)
        real_price_bitmex = self.define_real_bit_price(exchange)
        real_price_dydx = self.define_real_dydx_price(exchange)
        real_sell_price = real_price_bitmex if exchange == 'BITMEX' else real_price_dydx
        real_buy_price = real_price_dydx if exchange == 'BITMEX' else real_price_bitmex
        to_base = self.create_to_base(ask, bid, exchange, amount, amount_dydx, real_sell_price, real_buy_price)
        self.database.base_update(to_base)
        message = f"Found profit deal"
        message += f"\nPair: {self.symbol_dydx}"
        message += f"\nSell exchange: {exchange}"
        message += f"\nProfit relative: {to_base['profit_relative']}%"
        message += f"\nProfit, USD: {to_base['profit_USD']}"
        message += f"\nBuy price: {ask[0]}"
        message += f"\nSell price: {bid[0]}"
        message += f"\nReal buy price: {real_buy_price}"
        message += f"\nReal sell price: {real_sell_price}"
        message += f"\nAmount, USD: {amount}"
        message += f"\nAmount, {self.symbol_dydx.split('-')[0]}: {amount_dydx}"
        message += f"\nFee DYDX: {self.dydx_fee * 100}%"
        message += f"\nFee Bitmex: {self.bitmex_fee * 100}%"
        try:
            self.telegram_bot.send_message(self.chat_id, '<pre>' + message + '</pre>', parse_mode = 'HTML')
        except:
            pass

    def run(self):
        self.telegram_bot.send_message(self.chat_id, f"Parsing started")
        while True:
            time.sleep(0.001)
            self.find_price_diffs()




bot = botDydxBitmex('XBTUSDT', 'BTC-USD')
# bot.client_DYDX.
# orderbook = bot.client_DYDX.get_orderbook()
# # # #
# bot.client_DYDX.create_order(amount=0.02, price=orderbook['asks'][0][0], side='BUY', type='LIMIT')
# doc = open('deals.db', 'rb')
# bot.telegram_bot.send_document(bot.chat_id, doc)
bot.run()

    # except Exception as e:
    #     pass
# secret = 'H6ZALDVGPMLDBKZY'
# public = '581E0097FF284E9C81E112AEEAE5B21F'
# import ccxt
# bot_TIMEX = ccxt.timex({'apiKey': public, 'secret': secret,
#     'enableRateLimit': True})
# print(bot_TIMEX.fetchBalance())
# print()
# print()
#
# print(bot_TIMEX.fetch_trading_fee('ETH/AUDT'))
# # print(bot_TIMEX.fetch_trading_fees('ETH/AUDT'))
#
# markets_TIMEX = bot_TIMEX.load_markets()
# print(markets_TIMEX)



# symbol = 'SOLUSDT'
# sol_bot = BitMEXWebsocket(symbol)
# contract_price = 0.0001
#
#
# print(xbt_bot.funds())

# print('SOL DATA')
# for key, item in sol_bot.get_instrument().items():
#     print(f"{key} = {str(item)}")
# print()
# print('XBT DATA')
# for key, item in xbt_bot.get_instrument().items():
#     print(f"{key} = {str(item)}")
# print()