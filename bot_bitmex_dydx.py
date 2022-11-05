import bitmexClient
import dydx_client
import time
import telebot
import logging
import configparser
import sys
import re

cp = configparser.ConfigParser()
if len(sys.argv) != 2:
    # print("Usage %s <config.ini>" % sys.argv[0])
    sys.exit(1)
cp.read(sys.argv[1], "utf-8")

# FORMAT = '%(asctime)s %(levelname)s %(filename)s:%(lineno)d %(message)s (%(funcName)s)'
# logging.basicConfig(format=FORMAT)
# log = logging.getLogger("sample bot")
# log.setLevel(logging.DEBUG)
#
# if len(sys.argv) != 2:
#     # print("Usage %s <config.ini>" % sys.argv[0])
#     sys.exit(1)
# cp.read(sys.argv[1], "utf-8")


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

        self.client_Bitmex = bitmexClient.BitMEXWebsocket(symbol_bitmex,
                                                          cp["BITMEX"]["api_key"],
                                                          cp["BITMEX"]["api_secret"])

        self.client_DYDX = dydx_client.DydxClient(symbol_dydx, cp['DYDX'])
        self.client_DYDX.run_updater()
        self.dydx_fee = self.client_DYDX.taker_fee
        self.bitmex_fee = self.client_Bitmex.taker_fee

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
        if profit > 0:
            if self.last_message == f"Ask: {orderbook_dydx['asks'][0]}, Bid: {orderbook_bitmex['bids'][0]}, Line 66":
                return
            print(f"Ask: {orderbook_dydx['asks'][0]}, Bid: {orderbook_bitmex['bids'][0]}, Line 66")
            self.last_message = f"Ask: {orderbook_dydx['asks'][0]}, Bid: {orderbook_bitmex['bids'][0]}, Line 66"
            self.create_deal(orderbook_dydx['asks'][0], orderbook_bitmex['bids'][0], profit, 'BITMEX')

    def count_dydx_sell_profit(self):
        orderbook_bitmex = self.client_Bitmex.market_depth()[self.symbol_bitmex]
        orderbook_dydx = self.client_DYDX.get_orderbook()
        buy_price = orderbook_bitmex['asks'][0][0] + self.bitmex_fee * orderbook_bitmex['asks'][0][0]
        sell_price = orderbook_dydx['bids'][0][0] - self.dydx_fee * orderbook_dydx['bids'][0][0]
        profit = (sell_price - buy_price) / buy_price * 100
        if profit > 0:
            if self.last_message == f"Ask: {orderbook_bitmex['asks'][0]}, Bid: {orderbook_dydx['bids'][0]}, Line 75":
                return
            print(f"Ask: {orderbook_bitmex['asks'][0]}, Bid: {orderbook_dydx['bids'][0]}, Line 75")
            self.last_message = f"Ask: {orderbook_bitmex['asks'][0]}, Bid: {orderbook_dydx['bids'][0]}, Line 75"
            self.create_deal(orderbook_bitmex['asks'][0], orderbook_dydx['bids'][0], profit, 'DYDX')

    def create_deal(self, ask, bid, profit, exchange):
        if exchange == 'BITMEX':
            avail_bal_dydx = self.client_DYDX.get_available_balance('Buy')
            avail_bal_bitmex = self.client_Bitmex.get_available_balance('Sell')
            contract_price = self.client_Bitmex.contract_price
            amount = min((bid[1] * contract_price), (ask[1] * ask[0]), avail_bal_dydx, avail_bal_bitmex)
            if amount > 100:
                amount = amount - (amount % contract_price)
                amount_bitmex = int(round(amount / contract_price))
                amount_dydx = amount / ask[0]
                self.client_Bitmex.create_order(amount_bitmex, bid[0], 'Sell', 'Market')
                self.client_DYDX.create_order(amount_dydx, ask[0] * 2, 'BUY', 'LIMIT')
            else:
                return
        else:
            avail_bal_dydx = self.client_DYDX.get_available_balance('Sell')
            avail_bal_bitmex = self.client_Bitmex.get_available_balance('Buy')
            contract_price = self.client_Bitmex.contract_price
            amount = min((ask[1] * contract_price), (bid[1] * bid[0]), avail_bal_dydx, avail_bal_bitmex)
            if amount > 100:
                amount = amount - (amount % contract_price)
                amount_bitmex = int(round(amount / contract_price))
                amount_dydx = amount / bid[0]
                self.client_Bitmex.create_order(amount_bitmex, ask[0], 'Buy', 'Market')
                self.client_DYDX.create_order(amount_dydx, bid[0] / 2, 'SELL', 'LIMIT')
            else:
                return
        self.create_message(ask, bid, profit, exchange, amount, amount_bitmex, amount_dydx)

    def create_message(self, ask, bid, profit, exchange, amount, amount_bitmex, amount_dydx):
        time.sleep(1)
        last_trade_bitmex = self.client_Bitmex.recent_trades()
        last_trade_dydx = self.client_DYDX.get_fills()
        # amount_dydx = self.client_DYDX.presize_amount(amount_dydx)
        real_sell_price = None
        real_buy_price = None
        if exchange == 'BITMEX':
            for trade in last_trade_bitmex:
                if trade['side'] == 'Sell':
                    real_sell_price = trade['price']
            if last_trade_dydx.get(self.client_DYDX.symbol):
                last_trade_dydx = last_trade_dydx[self.client_DYDX.symbol][0]
                if last_trade_dydx['side'] == 'BUY':
                    real_buy_price = last_trade_dydx['price']
                else:
                    print(f"Last trade dydx: {last_trade_dydx['size']}\nAmount dydx: {amount_dydx}")
        else:
            for trade in last_trade_bitmex:
                if trade['side'] == 'Buy':
                    real_buy_price = trade['price']
            if last_trade_dydx.get(self.client_DYDX.symbol):
                last_trade_dydx = last_trade_dydx[self.client_DYDX.symbol][0]
                if last_trade_dydx['side'] == 'SELL':
                    real_sell_price = last_trade_dydx['price']
                else:
                    print(f"Last trade dydx: {last_trade_dydx['size']}\nAmount dydx: {amount_dydx}")
        message = f"Found profit deal"
        message += f"\nPair: {self.symbol_dydx}"
        message += f"\nSell exchange: {exchange}"
        message += f"\nProfit: {profit}%"
        message += f"\nBuy price: {ask[0]}"
        message += f"\nSell price: {bid[0]}"
        message += f"\nReal buy price: {real_buy_price}"
        message += f"\nReal sell price: {real_sell_price}"
        message += f"\nAmount: {amount} USD"
        message += f"\nContract price: {self.contract_price} {self.contract_price_asset}"
        try:
            self.telegram_bot.send_message(self.chat_id, message)
        except:
            pass

    def run(self):
        self.telegram_bot.send_message(self.chat_id, f"Parsing started")
        while True:
            time.sleep(0.001)
            self.find_price_diffs()




bot = botDydxBitmex('ETHUSD', 'ETH-USD')
# while True:
    # try:
bot.run()
    # except Exception as e:
    #     pass





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