import bitmexClient
import dydx_client
import time
import telebot


# cp = configparser.ConfigParser()
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
        self.contract_price = 1
        self.contract_price_asset = 'USD'
        self.last_message = None

        self.dydx_fee = 0.000304
        self.bitmex_fee = -0.0001

        self.client_Bitmex = bitmexClient.BitMEXWebsocket(symbol_bitmex)

        self.client_DYDX = dydx_client.DydxClient(symbol_dydx)
        self.client_DYDX.run_updater()
        self.telegram_bot.send_message(self.chat_id, f'Bot Bitmex-Dydx started. Pairs: {symbol_bitmex}-{symbol_dydx}')

    def find_price_diffs(self):
        orderbook_bitmex = self.client_Bitmex.market_depth()[0]
        orderbook_dydx = self.client_DYDX.get_orderbook()
        if orderbook_dydx['bids'][0][0] > orderbook_bitmex['asks'][0][0]:
            self.count_dydx_sell_profit()
        if orderbook_bitmex['bids'][0][0] > orderbook_dydx['asks'][0][0]:
            self.count_bitmex_sell_profit()

    def count_bitmex_sell_profit(self):
        orderbook_bitmex = self.client_Bitmex.market_depth()[0]
        orderbook_dydx = self.client_DYDX.get_orderbook()
        buy_price = orderbook_dydx['asks'][0][0] + self.dydx_fee * orderbook_dydx['asks'][0][0]
        sell_price = orderbook_bitmex['bids'][0][0] - self.bitmex_fee * orderbook_bitmex['bids'][0][0]
        profit = (sell_price - buy_price) / buy_price * 100
        amount = {'dydx': orderbook_dydx['asks'][0][1], 'bitmex_contracts': orderbook_bitmex['bids'][0][1]}
        if profit > 0:
            self.create_message(profit, sell_price, buy_price, amount, 'BITMEX')

    def count_dydx_sell_profit(self):
        orderbook_bitmex = self.client_Bitmex.market_depth()[0]
        orderbook_dydx = self.client_DYDX.get_orderbook()
        buy_price = orderbook_bitmex['asks'][0][0] + self.bitmex_fee * orderbook_bitmex['asks'][0][0]
        sell_price = orderbook_dydx['bids'][0][0] - self.dydx_fee * orderbook_dydx['bids'][0][0]
        profit = (sell_price - buy_price) / buy_price * 100
        amount = {'dydx': orderbook_dydx['bids'][0][1], 'bitmex_contracts': orderbook_bitmex['asks'][0][1]}
        if profit > 0:
            self.create_message(orderbook_bitmex['asks'][0], orderbook_dydx['bids'][0], profit, sell_price, buy_price, amount, 'DYDX')

    def create_message(self, ask, bid, profit, sell_price, buy_price, amount, exchange):
        message = f"Found profit deal"
        message += f"\nPair: {self.symbol_dydx}"
        message += f"\nSell exchange: {exchange}"
        message += f"\nProfit: {profit}%"
        message += f"\nBuy price: {buy_price}"
        message += f"\nSell price: {sell_price}"
        message += f"\nAsk: {ask}"
        message += f"\nBid: {bid}"
        message += f"\nAmount: {amount}"
        message += f"\nContract price: {self.contract_price} {self.contract_price_asset}"
        if [ask, bid] == self.last_message:
            return
        self.last_message = [ask, bid]
        self.telegram_bot.send_message(self.chat_id, message)

    def run(self):
        self.telegram_bot.send_message(self.chat_id, f"Parsing started")
        while True:
            self.find_price_diffs()




bot = botDydxBitmex('XBTUSD', 'BTC-USD')
bot.run()


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