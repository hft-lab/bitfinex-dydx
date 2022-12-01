import datetime
import random
import string
import logging
import configparser
import sys
import time
import requests
import multicall
import bitmexClient
import dydx_client
import telebot
from dataBaseBitmex import database

cp = configparser.ConfigParser()
if len(sys.argv) != 2:
    print("Usage %s <config.ini>" % sys.argv[0])
    sys.exit(1)
cp.read(sys.argv[1], "utf-8")

FORMAT = '%(asctime)s %(levelname)s %(filename)s:%(lineno)d %(message)s (%(funcName)s)'
logging.basicConfig(format=FORMAT)
log = logging.getLogger("sample bot")
log.setLevel(logging.DEBUG)


class botDydxBitmex:

    def __init__(self, symbol_bitmex, symbol_dydx, side):
        self.side = side
        self.cp = cp
        self.max_size = float(cp['SETTINGS']['order_size'])
        self.symbol_bitmex = symbol_bitmex
        self.symbol_dydx = symbol_dydx
        self.last_message = None
        self.start_timestamp = int(round(time.time()))
        self.executions = []
        self.pos_power = 6 if 'USDT' in symbol_bitmex else 8
        self.currency = 'USDt' if 'USDT' in self.symbol_bitmex else 'XBt'
        self.rate = 0
        self.profit_taker = float(cp['SETTINGS']['target_profit'])
        self.profit_maker = 0.0001
        self.price_shift = 0.0008 if 'ETH' in symbol_dydx else 0
        self.ping_dydx = False
        self.ping_bitmex = False

        self.chat_id = cp["TELEGRAM"]["chat_id"]
        self.daily_chat_id = cp["TELEGRAM"]["daily_chat_id"]
        self.inv_chat_id = cp["TELEGRAM"]["inv_chat_id"]
        self.telegram_bot = telebot.TeleBot(cp["TELEGRAM"]["token"])
        self.pool = multicall.Pool()
        self.database = database(self.telegram_bot, self.chat_id, self.symbol_bitmex)
        self.init_clients()
        self.dydx_fee = self.client_DYDX.taker_fee
        self.bitmex_fee = self.client_Bitmex.taker_fee
        self.bitmex_maker_fee = self.client_Bitmex.maker_fee
        self.send_message(self.chat_id, f'Bot BITMEX-DYDX started.\nBITMEX: {symbol_bitmex}\nDYDX: {symbol_dydx}')

    def init_clients(self):
        self.client_Bitmex = bitmexClient.BitMEXWebsocket(symbol=self.symbol_bitmex,
                                                          api_key=self.cp["BITMEX"]["api_key"],
                                                          api_secret=self.cp["BITMEX"]["api_secret"])
        self.cancel_all_orders_bit()
        self.client_DYDX = dydx_client.DydxClient(self.symbol_dydx, self.cp['DYDX'])
        self.client_DYDX.run_updater()

    def find_price_diffs(self):
        if self.total_position() > 200:
            return
        orderbook_bitmex = self.client_Bitmex.market_depth()[self.symbol_bitmex]
        orderbook_dydx = self.client_DYDX.get_orderbook()
        timestamp_dif = abs(orderbook_dydx['timestamp'] - orderbook_bitmex['timestamp'])
        if timestamp_dif > 5:
            # self.send_message(self.chat_id, 'ORDERBOOK UPDATES ERROR. WAITING.')
            self.client_Bitmex.exit()
            self.client_DYDX.exit()
            self.init_clients()
            print(f"Clients rebooted")
            time.sleep(3)
            return
        if orderbook_dydx['bids'][0][0] > orderbook_bitmex['asks'][0][0] / (1 + self.price_shift):
            self.taker_dydx_sell_profit()
        if orderbook_bitmex['bids'][0][0] / (1 + self.price_shift) > orderbook_dydx['asks'][0][0]:
            self.taker_bitmex_sell_profit()

    def send_message(self, chat_id, message):
        try:
            self.pool.add(self.telegram_bot.send_message, chat_id, '<pre>' + message + '</pre>', parse_mode='HTML')
            self.pool.call_all()
        except:
            pass

    def find_makers(self):
        orderbook_dydx = self.client_DYDX.get_orderbook()

        av_bal_bitmex_sell = self.avail_balance_define('BITMEX')
        size_dydx = orderbook_dydx['asks'][0][1] * orderbook_dydx['asks'][0][0]
        max_sell_size = min(size_dydx, av_bal_bitmex_sell, self.max_size)
        if max_sell_size > 100:
            self.makers_bitmex_sell_profit(max_sell_size)

        av_bal_bitmex_buy = self.avail_balance_define('DYDX')
        size_dydx = orderbook_dydx['bids'][0][1] * orderbook_dydx['bids'][0][0]
        max_buy_size = min(size_dydx, av_bal_bitmex_buy, self.max_size)
        if max_buy_size > 100:
            self.makers_bitmex_buy_profit(max_buy_size)

    def makers_bitmex_sell_profit(self, order_size):
        orderbook_bitmex = self.client_Bitmex.market_depth()[self.symbol_bitmex]
        orderbook_dydx = self.client_DYDX.get_orderbook()
        ticksize = self.client_Bitmex.ticksize
        for bit_ask in orderbook_bitmex['asks']:
            sell_price = self.client_Bitmex.presize_price(bit_ask[0] / (1 + self.price_shift) - ticksize)
            sell_price = sell_price if sell_price != orderbook_bitmex['bids'][0][0] else bit_ask[0] / (1 + self.price_shift)
            profit_sell_price = (sell_price / self.rate) if 'USDT' in self.symbol_bitmex else sell_price
            profit = (profit_sell_price - orderbook_dydx['asks'][0][0]) / orderbook_dydx['asks'][0][0]
            if profit - (self.bitmex_maker_fee) > self.profit_maker:
                order_id = f"MAKER SELL {self.symbol_bitmex}/{self.id_generator(6)}"
                self.maker_order_change(sell_price, order_size, order_id)
                return

    def makers_bitmex_buy_profit(self, order_size):
        orderbook_bitmex = self.client_Bitmex.market_depth()[self.symbol_bitmex]
        orderbook_dydx = self.client_DYDX.get_orderbook()
        ticksize = self.client_Bitmex.ticksize
        for bit_bid in orderbook_bitmex['bids']:
            buy_price = self.client_Bitmex.presize_price(bit_bid[0] / (1 + self.price_shift) + ticksize)
            buy_price = buy_price if buy_price != orderbook_bitmex['asks'][0][0] else bit_bid[0]  / (1 + self.price_shift)
            profit_buy_price = (buy_price / self.rate) if 'USDT' in self.symbol_bitmex else buy_price
            profit = (orderbook_dydx['bids'][0][0] - profit_buy_price) / profit_buy_price
            if profit - (self.bitmex_maker_fee) > self.profit_maker:
                order_id = f"MAKER BUY {self.symbol_bitmex}/{self.id_generator(6)}"
                self.maker_order_change(buy_price, order_size, order_id)
                return

    def maker_order_change(self, price, order_size, order_id):
        for order in self.client_Bitmex.open_orders(''):
            if order_id.split('/')[0] in order['clOrdID']:
                if abs(price - order['price']) < self.client_Bitmex.ticksize * 1.1:
                    return
                else:
                    contract_price = self.client_Bitmex.contract_price
                    amount, amount_bitmex = self._amounts_define(order_size, contract_price)
                    amount_bitmex = amount_bitmex if amount_bitmex != order['orderQty'] else None
                    try:
                        self.pool.add(self.client_Bitmex.change_order, amount_bitmex, price, order['orderID'])
                        self.pool.call_all()
                        order['price'] = price
                        order['orderQty'] = amount_bitmex if amount_bitmex else order['orderQty']
                    except:
                        pass
                    return
        side = 'Sell' if 'SELL' in order_id else 'Buy'
        contract_price = self.client_Bitmex.contract_price
        amount, amount_bitmex = self._amounts_define(order_size, contract_price)
        self.client_Bitmex.create_order(amount_bitmex, price, side, 'Limit', order_id)
        return

    def taker_bitmex_sell_profit(self):
        orderbook_bitmex = self.client_Bitmex.market_depth()[self.symbol_bitmex]
        orderbook_dydx = self.client_DYDX.get_orderbook()
        if 'USDT' in self.symbol_bitmex:
            price_bitmex = (orderbook_bitmex['bids'][0][0] / (1 + self.price_shift)) / self.rate
        else:
            price_bitmex = orderbook_bitmex['bids'][0][0] / (1 + self.price_shift)
        buy_price = orderbook_dydx['asks'][0][0] + self.dydx_fee * orderbook_dydx['asks'][0][0]
        sell_price = price_bitmex - self.bitmex_fee * price_bitmex
        profit = (sell_price - buy_price) / buy_price
        if profit > self.profit_taker:
            self.execute_taker_deal(orderbook_dydx['asks'][0], orderbook_bitmex['bids'][0], 'BITMEX')

    def taker_dydx_sell_profit(self):
        orderbook_bitmex = self.client_Bitmex.market_depth()[self.symbol_bitmex]
        orderbook_dydx = self.client_DYDX.get_orderbook()
        if 'USDT' in self.symbol_bitmex:
            price_bitmex = (orderbook_bitmex['asks'][0][0] / (1 + self.price_shift)) / self.rate
        else:
            price_bitmex = (orderbook_bitmex['asks'][0][0] / (1 + self.price_shift))
        buy_price = price_bitmex + self.bitmex_fee * price_bitmex
        sell_price = orderbook_dydx['bids'][0][0] - self.dydx_fee * orderbook_dydx['bids'][0][0]
        profit = (sell_price - buy_price) / buy_price
        if profit > self.profit_taker:
            self.execute_taker_deal(orderbook_bitmex['asks'][0], orderbook_dydx['bids'][0], 'DYDX')

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

    def create_taker_orders(self, amount_bitmex, amount_dydx, bid, ask, exchange):
        if exchange == 'BITMEX':
            if self.side == 'BITMEX':
                self.pool.add(self.client_Bitmex.create_order, amount=amount_bitmex, price=bid[0], side='Sell', type='Market')
                self.pool.call_all_and_wait()
            elif self.side == "DYDX":
                self.pool.add(self.client_DYDX.create_order, amount=amount_dydx, price=ask[0] * 1.001, side='BUY', type='LIMIT')
                self.pool.call_all_and_wait()
            elif self.side == "BOTH":
                self.pool.add(self.client_Bitmex.create_order, amount=amount_bitmex, price=bid[0], side='Sell', type='Market')
                self.pool.add(self.client_DYDX.create_order, amount=amount_dydx, price=ask[0] * 1.001, side='BUY', type='LIMIT')
                self.pool.call_all_and_wait()
        else:
            if self.side == 'BITMEX':
                self.pool.add(self.client_Bitmex.create_order, amount=amount_bitmex, price=ask[0], side='Buy', type='Market')
                self.pool.call_all_and_wait()
            elif self.side == "DYDX":
                self.pool.add(self.client_DYDX.create_order, amount=amount_dydx, price=bid[0] / 1.001, side='SELL', type='LIMIT')
                self.pool.call_all_and_wait()
            elif self.side == "BOTH":
                self.pool.add(self.client_Bitmex.create_order, amount=amount_bitmex, price=ask[0], side='Buy', type='Market')
                self.pool.add(self.client_DYDX.create_order, amount=amount_dydx, price=bid[0] / 1.001, side='SELL', type='LIMIT')
                self.pool.call_all_and_wait()


    def execute_taker_deal(self, ask, bid, exchange):
        contract_price = self.client_Bitmex.contract_price
        avail_bal_bitmex = self.avail_balance_define(exchange)
        if exchange == 'BITMEX':
            amount = min((bid[1] * contract_price), (ask[1] * ask[0]), avail_bal_bitmex, self.max_size)
            if amount > 100:
                amount, amount_bitmex = self._amounts_define(amount, contract_price)
                amount_dydx = amount / ask[0]
                self.create_taker_orders(amount_bitmex, amount_dydx, bid, ask, exchange)
            else:
                return
        else:
            amount = min((ask[1] * contract_price), (bid[1] * bid[0]), avail_bal_bitmex, self.max_size)
            if amount > 100:
                amount, amount_bitmex = self._amounts_define(amount, contract_price)
                amount_dydx = amount / bid[0]
                self.create_taker_orders(amount_bitmex, amount_dydx, bid, ask, exchange)
            else:
                return
        to_base = self.order_to_base(exchange, ask, bid, amount, amount_dydx)
        # if self.side in ['DYDX', 'BOTH']:
        self.taker_message(ask, bid, exchange, amount, amount_dydx, to_base)

    def define_real_bit_price(self, exchange):
        side = 'Sell' if exchange == "BITMEX" else 'Buy'
        last_trades = self.client_Bitmex.recent_trades()
        real_bitmex_price = None
        for trade in last_trades:
            if trade['side'] == side and trade['symbol'] == self.symbol_bitmex:
                real_bitmex_price = trade['price']
        if real_bitmex_price:
            return real_bitmex_price / (1 + self.price_shift)
        else:
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

    def create_to_base(self, exchange, amount, amount_dydx, deal_type, real_sell_price, real_buy_price, taker_data=None):
        if taker_data:
            sell_price = real_sell_price if real_sell_price else taker_data['bid'][0]
            buy_price = real_buy_price if real_buy_price else taker_data['ask'][0]
            bitmex_fee = self.bitmex_fee
        else:
            sell_price = real_sell_price
            buy_price = real_buy_price
            bitmex_fee = self.bitmex_maker_fee
        if exchange == 'BITMEX':
            profit_sell_price = (sell_price / self.rate) if 'USDT' in self.symbol_bitmex else sell_price
            profit = ((1 - bitmex_fee) * profit_sell_price - (1 + self.dydx_fee) * buy_price) / buy_price
        else:
            profit_buy_price = (buy_price / self.rate) if 'USDT' in self.symbol_bitmex else buy_price
            profit = ((1 - self.dydx_fee) * sell_price - (1 + bitmex_fee) * profit_buy_price) / profit_buy_price
        long_side = 'DYDX' if self.client_DYDX.positions[self.symbol_dydx]['side'] == 'LONG' else 'BITMEX'
        to_base = {'timestamp': int(round(time.time())),
                   'sell_exchange': exchange,
                   'sell_price': sell_price,
                   'buy_price': buy_price,
                   'amount_USD': amount,
                   'amount_coin': amount_dydx,
                   'profit_USD': profit * amount,
                   'profit_relative': profit,
                   'fee_dydx': self.dydx_fee,
                   'fee_bitmex': bitmex_fee,
                   'pnl_dydx': self.client_DYDX.get_pnl(),
                   'pnl_bitmex': self.client_Bitmex.get_pnl()[0],
                   'long_side': long_side,
                   'deal_type': deal_type,
                   'USDC_rate': self.rate}
        return to_base

    def real_last_prices(self, exchange):
        real_price_bitmex = self.define_real_bit_price(exchange)
        real_price_dydx = self.define_real_dydx_price(exchange)
        real_sell_price = real_price_bitmex if exchange == 'BITMEX' else real_price_dydx
        real_buy_price = real_price_dydx if exchange == 'BITMEX' else real_price_bitmex
        return real_sell_price, real_buy_price

    def order_to_base(self, exchange, ask, bid, amount, amount_dydx):
        real_sell_price, real_buy_price = self.real_last_prices(exchange)
        taker_data = {'ask': ask, 'bid': bid}
        to_base = self.create_to_base(exchange, amount, amount_dydx, 'TAKER', real_sell_price, real_buy_price, taker_data)
        self.database.base_update(to_base)
        return to_base

    def taker_message(self, ask, bid, exchange, amount, amount_dydx, to_base):
        if self.side != 'BOTH':
            time.sleep(3)
        real_sell_price, real_buy_price = self.real_last_prices(exchange)
        message = f"TAKER EXECUTED:"
        message += f"Server side: {self.side}"
        message += f"\nPair: {self.symbol_dydx}"
        message += f"\nSell exchange: {exchange}"
        message += f"\nProfit relative: {round(to_base['profit_relative'] * 100, 4)}%"
        message += f"\nProfit, USD: {round(to_base['profit_USD'], 2)}"
        if exchange == 'BITMEX':
            message += f"\nBuy price: {ask[0]}"
            message += f"\nSell price: {bid[0] / (1 + self.price_shift)}"
        if exchange == 'DYDX':
            message += f"\nBuy price: {ask[0] / (1 + self.price_shift)}"
            message += f"\nSell price: {bid[0]}"
        message += f"\nReal buy price: {real_buy_price}"
        message += f"\nReal sell price: {real_sell_price}"
        message += f"\nAmount, USD: {round(amount)}"
        message += f"\nAmount, {self.symbol_dydx.split('-')[0]}: {round(amount_dydx, 4)}"
        if 'USDT' in self.symbol_bitmex:
            message += f"\nUSDC/USDT rate: {self.rate}"
        message += f"\nFee DYDX: {self.dydx_fee * 100}%"
        message += f"\nFee Bitmex: {self.bitmex_fee * 100}%"
        message += f"\nPrice shift: {self.price_shift * 100}%"
        time.sleep(1)
        self.send_message(self.chat_id, message)

    def day_deals_count(self, base_data):
        timestamp = int(round(time.time() - 86400))
        data = {'taker_deal_count': 0,
                'maker_deal_count': 0,
                'total_deal_count': 0,
                'volume_makers': 0,
                'volume_takers': 0,
                'theory_profit_makers': 0,
                'theory_profit_takers': 0
                }
        for deal in base_data[::-1]:
            if deal[1] < timestamp:
                break
            data['total_deal_count'] += 1
            if deal[14] == 'MAKER':
                data['maker_deal_count'] += 1
                data['volume_makers'] += deal[5]
                data['theory_profit_makers'] += deal[7]
            else:
                data['taker_deal_count'] += 1
                data['volume_takers'] += deal[5]
                data['theory_profit_takers'] += deal[7]
        return data

    def pnl_count(self, base_data, deal_count):
        last_pnl = 0
        total_pnl = 0
        final_pnl = 0
        last_long_side = None
        if deal_count:
            first_pnl = base_data[-deal_count][11] + base_data[-deal_count][12]
            for deal in base_data[-deal_count:]:
                if last_long_side != deal[13]:
                    last_long_side = deal[13]
                    total_pnl += last_pnl
                last_pnl = deal[11] + deal[12]
            final_pnl = total_pnl + last_pnl - first_pnl
        return final_pnl

    def daily_report(self):
        base_data = self.database.fetch_data_from_table(f'deals_{self.symbol_bitmex}')
        counted_deals = self.day_deals_count(base_data)
        pnl_diff = self.pnl_count(base_data, counted_deals['total_deal_count'])
        message = self.create_daily_message(counted_deals, pnl_diff)
        self.send_message(self.daily_chat_id, message)
        self.send_message(self.inv_chat_id, message)

    def create_daily_message(self, deals_data, pnl_diff):
        message = f'DAILY REPORT FOR {str(datetime.datetime.now()).split(" ")[0]}'
        message += f"\nServer side sender: {cp['SETTINGS']['side']}"
        message += f"\nEXCHANGES: BITMEX-DYDX"
        message += f"\nBITMEX: {self.symbol_bitmex} DYDX: {self.symbol_dydx}"
        message += f"\nDeals number: {deals_data['total_deal_count']}"
        message += f"\n  Takers: {deals_data['taker_deal_count']}"
        message += f"\n  Makers: {deals_data['maker_deal_count']}"
        message += f"\nVolume per side, USD: {deals_data['volume_makers'] + deals_data['volume_takers']}"
        message += f"\n  Takers, USD: {deals_data['volume_takers']}"
        message += f"\n  Makers, USD: {deals_data['volume_makers']}"
        message += f"\nTheory profit: {round(deals_data['theory_profit_takers'] + deals_data['theory_profit_makers'], 2)}"
        message += f"\n  Takers: {round(deals_data['theory_profit_takers'], 2)}"
        message += f"\n  Makers: {round(deals_data['theory_profit_makers'], 2)}"
        message += f"\nTheory profit per deal:"
        message += f"\n  Takers: {round(deals_data['theory_profit_takers'] / deals_data['taker_deal_count'], 3)}"
        if deals_data['maker_deal_count']:
            message += f"\n  Makers: {round(deals_data['theory_profit_makers'] / deals_data['maker_deal_count'], 3)}"
        message += f"\nPNL diff. profit: {pnl_diff}"
        message += f"\nTarget profit makers: {self.profit_maker}"
        message += f"\nTarget profit takers: {self.profit_taker}"
        return message

        # data = {'taker_deal_count': 0,
                # 'maker_deal_count': 0,
                # 'total_deal_count': 0,
                # 'volume_makers': 0,
                # 'volume_takers': 0,
                # 'theory_profit_makers': 0,
                # 'theory_profit_takers': 0
                # }

    def find_xbt_pos(self):
        xbt_pos = 0
        if not 'USDT' in self.symbol_bitmex:
            bal_bitmex = [x for x in self.client_Bitmex.funds() if x['currency'] == self.currency][0]
            xbt_pos = bal_bitmex['walletBalance'] / 10 ** self.pos_power
        return xbt_pos

    def total_position(self):
        orderbook = self.client_Bitmex.market_depth()[self.symbol_bitmex]
        change = (orderbook['asks'][0][0] + orderbook['bids'][0][0]) / 2
        pos_bitmex = [x for x in self.client_Bitmex.positions() if x['symbol'] == self.symbol_bitmex]
        pos_bitmex = 0 if not len(pos_bitmex) else pos_bitmex[0]['homeNotional']
        if self.client_DYDX.positions.get(self.symbol_dydx):
            pos_dydx = self.client_DYDX.positions[self.symbol_dydx]['size']
        else:
            pos_dydx = 0
        if self.symbol_bitmex == 'XBTUSD':
            xbt_pos = self.find_xbt_pos()
            tot_position = pos_bitmex + float(pos_dydx) + xbt_pos
        else:
            tot_position = pos_bitmex + float(pos_dydx)
        tot_position = abs(tot_position) * change
        return tot_position

    def pos_balancing(self, pos_bitmex, pos_dydx):
        if self.symbol_bitmex == 'XBTUSD':
            xbt_pos = self.find_xbt_pos()
            position_change = float(pos_dydx) + pos_bitmex + xbt_pos
        else:
            position_change = float(pos_dydx) + pos_bitmex
        orderbook = self.client_Bitmex.market_depth()[self.symbol_bitmex]
        change = (orderbook['asks'][0][0] + orderbook['bids'][0][0]) / 2
        amount_to_balancing = abs(position_change) * change
        if amount_to_balancing < 150:
            return
        side = 'Sell' if position_change > 0 else 'Buy'
        contract_price = self.client_Bitmex.contract_price
        amount, amount_bitmex = self._amounts_define(amount_to_balancing, contract_price)
        ticksize = self.client_Bitmex.ticksize
        if side == 'Buy':
            price = orderbook['asks'][0][0] - ticksize
        else:
            price = orderbook['bids'][0][0] + ticksize
        if amount_bitmex:
            open_orders = self.client_Bitmex.open_orders(clOrdIDPrefix='BALANCING')
            for order in open_orders:
                self.pool.add(self.client_Bitmex.cancel_order, order['orderID'])
            self.pool.add(self.client_Bitmex.create_order, amount_bitmex, price, side, 'Limit', f'BALANCING {self.id_generator(6)}')
            self.pool.call_all()

    def balance_message(self):
        bal_bitmex = self.client_Bitmex.get_real_balance()
        tot_bal_bitmex = (bal_bitmex / 10 ** self.pos_power)
        bal_dydx = self.client_DYDX.get_real_balance()

        pos_bitmex = [x for x in self.client_Bitmex.positions() if x['symbol'] == self.symbol_bitmex]
        pos_bitmex = 0 if not len(pos_bitmex) else pos_bitmex[0]['homeNotional']
        if self.client_DYDX.positions.get(self.symbol_dydx):
            pos_dydx = self.client_DYDX.positions[self.symbol_dydx]['size']
        else:
            pos_dydx = 0
        if self.side in ['DYDX', 'BOTH']:
            self.pos_balancing(pos_bitmex, pos_dydx)
        message = self.create_balance_message(bal_dydx, pos_dydx, tot_bal_bitmex, pos_bitmex)
        self.send_message(self.chat_id, message)

    def bitmex_pnl_w_mark_price(self):
        bitmex_pnl = self.client_Bitmex.get_pnl()
        position = bitmex_pnl[1]
        realized_pnl = bitmex_pnl[2]
        size = position['homeNotional']
        # if self.symbol_bitmex == 'XBTUSD':
            # size -= self.find_xbt_pos()
        entry_price = position['avgEntryPrice']
        index_price = self.client_DYDX.orderbook
        index_price = (index_price['asks'][0][0] + index_price['bids'][0][0]) / 2
        unrealized_pnl = size * (index_price - entry_price)
        bitmex_pnl = unrealized_pnl + realized_pnl
        return bitmex_pnl

    def avail_balance_define(self, exchange):
        if exchange == 'DYDX':
            avail_bal_dydx = self.client_DYDX.get_available_balance('Sell')
            avail_bal_bitmex = self.client_Bitmex.get_available_balance('Buy')
            return min(avail_bal_dydx, avail_bal_bitmex)
        else:
            avail_bal_dydx = self.client_DYDX.get_available_balance('Buy')
            avail_bal_bitmex = self.client_Bitmex.get_available_balance('Sell')
            return min(avail_bal_dydx, avail_bal_bitmex)

    def create_balance_message(self, bal_dydx, pos_dydx, tot_bal_bitmex, pos_bitmex):
        coin = self.symbol_dydx.split('-')[0]
        dydx_pnl = float(self.client_DYDX.get_pnl())
        bitmex_pnl = self.bitmex_pnl_w_mark_price()
        orderbook_btc = self.client_Bitmex.market_depth()['XBTUSD']
        index_price = self.client_Bitmex.market_depth()[self.symbol_bitmex]
        index_price = (index_price['asks'][0][0] + index_price['bids'][0][0]) / 2
        change = 1 if self.currency == 'USDt' else (orderbook_btc['asks'][0][0] + orderbook_btc['bids'][0][0]) / 2
        message = f'BALANCES AND POSITIONS'
        message += f"\nDYDX:"
        message += f"\n Tot.bal.: {round(bal_dydx)} USDC"
        message += f"\n Pos.: {pos_dydx} {coin}"
        message += f"\n PNL: {round(dydx_pnl)} USD"
        message += f"\nBITMEX:"
        if self.currency == 'USDt':
            message += f"\n Tot.bal.: {round(tot_bal_bitmex)} {self.currency}"
        else:
            message += f"\n Tot.bal.: {round(tot_bal_bitmex, 4)} {self.currency}"
        message += f"\n Pos.: {pos_bitmex} {coin}"
        message += f"\n PNL: {round(bitmex_pnl)} USD"
        message += f"\nTOTAL:"
        message += f"\n Balance: {round(bal_dydx + tot_bal_bitmex * change)} USD"
        if self.symbol_bitmex == 'XBTUSD':
            xbt_pos = self.find_xbt_pos()
            message += f"\n Position: {round(pos_bitmex +  float(pos_dydx) + xbt_pos, 4)} {coin}"
        else:
            message += f"\n Position: {round(pos_bitmex +  float(pos_dydx), 4)} {coin}"
        message += f"\n PNL diff.: {round(dydx_pnl + bitmex_pnl)} USD"
        message += f"\n Index price: {index_price}"
        if len(self.client_Bitmex.open_orders('')):
            message += f"\nOPEN ORDERS:"
            message += self.get_open_makers()
        message += f"\nAvail. Bit sell, USD: {round(self.avail_balance_define('BITMEX'))}"
        message += f"\nAvail. Bit buy, USD: {round(self.avail_balance_define('DYDX'))}"
        last_timestamp = self.database.fetch_data_from_table(f'deals_{self.symbol_bitmex}')
        last_timestamp = 0 if not len(last_timestamp) else last_timestamp[-1][1]
        mins_to_last_deal = round((time.time() - last_timestamp) / 60)
        message += f"\nLast deal was {mins_to_last_deal} minutes before"
        return message

    def create_ping_order(self):
        print("PING ORDERS")
        if self.side == 'DYDX':
            self.client_DYDX.create_order(0.1, 1000000, 'SELL', 'LIMIT', expire=180)
        if self.side == 'BITMEX':
            self.client_DYDX.create_order(0.1, 2000000, 'SELL', 'LIMIT', expire=180)


    def check_opposite(self):
        print(f"CHECK STARTED")
        self.ping_dydx = False
        self.ping_bitmex = False
        for order in self.client_DYDX.orders[self.symbol_dydx]:
            ord = self.client_DYDX.orders[self.symbol_dydx][order]
            if int(round(float(ord['price']))) == 1000000:
                self.ping_dydx = True
            elif int(round(float(ord['price']))) == 2000000:
                self.ping_bitmex = True
        if not self.ping_dydx:
            self.send_message(self.chat_id, 'DYDX SIDE OFF. QUIT')
            sys.exit(1)
        if not self.ping_bitmex:
            self.send_message(self.chat_id, 'BITMEX SIDE OFF. QUIT')
            sys.exit(1)


    def time_based_messages(self):
        time_from = (int(round(time.time())) - self.start_timestamp) % 180
        if self.side in ['BOTH', 'DYDX']:
            time_from_parser = (int(round(time.time())) - self.start_timestamp) % 10
            if time_from_parser == 0:
                price_bitmex = self.client_Bitmex.market_depth()[self.symbol_bitmex]
                price_bitmex = str((price_bitmex['asks'][0][0] + price_bitmex['bids'][0][0]) / 2)
                price_dydx = self.client_DYDX.get_orderbook()
                price_dydx = str((price_dydx['asks'][0][0] + price_dydx['bids'][0][0]) / 2)
                with open('prices.txt', 'a') as file:
                    file.write(f"{price_bitmex} {price_dydx}\n")
                self.start_timestamp -= 1
            if time_from == 0:
                if 'USDT' in self.symbol_bitmex:
                    self.fetch_usdc_rate()
                self.balance_message()
                self.start_timestamp -= 1
        if not self.side == 'BOTH' and time_from == 0:
            self.create_ping_order()
            if self.side == 'BITMEX':
                self.start_timestamp -= 1
        if ' 09:00:00' in str(datetime.datetime.now()):
            self.daily_report()
            time.sleep(1)

    def id_generator(self, size, chars=string.ascii_uppercase + string.digits):
        return ''.join(random.choice(chars) for _ in range(size))

    def check_executions(self):
        for exec in self.client_Bitmex.data['execution']:
            if exec['ordStatus'] in ['New', 'Canceled']:
                self.client_Bitmex.data['execution'].remove(exec)
                continue
            if 'BALANCING' in exec['clOrdID']:
                if not exec['homeNotional']:
                    continue
                self.execute_balancing(exec)
                self.client_Bitmex.data['execution'].remove(exec)
            if 'MAKER' in exec['clOrdID']:
                self.executed_maker_deal(exec)
                self.client_Bitmex.data['execution'].remove(exec)

    def executed_maker_deal(self, exec):
        side = 'SELL' if exec['side'] == 'Buy' else 'BUY'
        if not exec['homeNotional']:
            print(exec)
            return
        amount = abs(exec['homeNotional'])
        orderbook = self.client_DYDX.get_orderbook()
        price = orderbook['asks'][0][0] if side == 'BUY' else orderbook['bids'][0][0]
        if side == 'BUY':
            self.client_DYDX.create_order(amount, price * 1.001, side, 'LIMIT')
        else:
            self.client_DYDX.create_order(amount, price / 1.001, side, 'LIMIT')
        amount_usd = amount * orderbook['asks'][0][0]
        self.maker_message(exec, amount_usd, amount, price)

    def get_open_makers(self):
        orders = self.client_Bitmex.open_orders('')
        orderbook = self.client_Bitmex.market_depth()[self.symbol_bitmex]
        message = ''
        for order in orders:
            if 'MAKER' in order['clOrdID']:
                top_price = orderbook['bids'][0][0] if order['side'] == 'Buy' else orderbook['asks'][0][0]
                message += f"\n{order['side']}:px/toppx: {order['price']}/{top_price}"
        return message
    # open_orders_resp = [{'orderID': '20772f48-24a3-4cff-a470-670d51a1666e', 'clOrdID': 'BALANCING', 'clOrdLinkID': '', 'account': 2133275,
    #   'symbol': 'XBTUSDT', 'side': 'Buy', 'simpleOrderQty': None, 'orderQty': 1000, 'price': 15000, 'displayQty': None,
    #   'stopPx': None, 'pegOffsetValue': None, 'pegPriceType': '', 'currency': 'USDT', 'settlCurrency': 'USDt',
    #   'ordType': 'Limit', 'timeInForce': 'GoodTillCancel', 'execInst': '', 'contingencyType': '', 'exDestination': 'XBME',
    #   'ordStatus': 'New', 'triggered': '', 'workingIndicator': True, 'ordRejReason': '', 'simpleLeavesQty': None,
    #   'leavesQty': 1000, 'simpleCumQty': None, 'cumQty': 0, 'avgPx': None, 'multiLegReportingType': 'SingleSecurity',
    #   'text': 'Submitted via API.', 'transactTime': '2022-11-16T17:18:45.740Z', 'timestamp': '2022-11-16T17:18:45.740Z'}]

    def maker_message(self, exec, amount_usd, amount, price):
        exchange = 'DYDX' if exec['side'] == 'Buy' else 'BITMEX'
        if exchange == 'DYDX':
            sell_price = price
            buy_price = exec['price']
        else:
            sell_price = exec['price']
            buy_price = price
        to_base = self.create_to_base(exchange, amount_usd, amount, 'MAKER', sell_price, buy_price)
        self.database.base_update(to_base)
        message = f"MAKER EXECUTED:"
        message += f"\nPair: {self.symbol_dydx}"
        message += f"\nSell exchange: {exchange}"
        message += f"\nProfit relative: {round(to_base['profit_relative'] * 100, 4)}%"
        message += f"\nProfit, USD: {round(to_base['profit_USD'], 2)}"
        message += f"\nReal buy price: {buy_price}"
        message += f"\nReal sell price: {sell_price}"
        message += f"\nAmount, USD: {round(amount_usd)}"
        message += f"\nAmount, {self.symbol_dydx.split('-')[0]}: {round(amount, 4)}"
        if 'USDT' in self.symbol_bitmex:
            message += f"\nUSDC/USDT rate: {self.rate}"
        message += f"\nFee DYDX: {self.dydx_fee * 100}%"
        message += f"\nFee Bitmex: {self.bitmex_maker_fee * 100}%"
        self.send_message(self.chat_id, message)
     # exec = {'orderID': 'baf6fc1e-8f76-4090-a3f3-254314da86b4',
     # 'clOrdID': 'BALANCING BTC', 'clOrdLinkID': '', 'account': 2133275, 'symbol': 'XBTUSDT', 'side': 'Buy',
     # 'simpleOrderQty': None, 'orderQty': 1000, 'price': 17000, 'displayQty': None, 'stopPx': None,
     # 'pegOffsetValue': None,
     # 'pegPriceType': '', 'currency': 'USDT', 'settlCurrency': 'USDt', 'ordType': 'Limit',
     # 'timeInForce': 'GoodTillCancel',
     # 'execInst': '', 'contingencyType': '', 'exDestination': 'XBME', 'ordStatus': 'Filled', 'triggered': '',
     # 'workingIndicator': False, 'ordRejReason': '', 'simpleLeavesQty': None, 'leavesQty': 0, 'simpleCumQty': None,
     # 'cumQty': 1000, 'avgPx': 16519, 'multiLegReportingType': 'SingleSecurity', 'text': 'Submitted via API.',
     # 'transactTime': '2022-11-16T17:24:10.721Z', 'timestamp': '2022-11-16T17:24:10.721Z', 'lastQty': 1000,
     # 'lastPx': 16519,
     # 'lastLiquidityInd': 'RemovedLiquidity', 'tradePublishIndicator': 'PublishTrade',
     # 'trdMatchID': '15cd273d-ded8-e339-b3b1-9a9080b5d10f', 'execID': 'adcc6b75-2d57-a9d2-47c4-8db921d8aae1',
     # 'execType': 'Trade', 'execCost': 16519000, 'homeNotional': 0.001, 'foreignNotional': -16.519,
     # 'commission': 0.00022500045, 'lastMkt': 'XBME', 'execComm': 3716, 'underlyingLastPx': None}]

    def execute_balancing(self, exec):
        message = 'BALANCING ORDER EXECUTED'
        message += f"\n Symbol: {exec['symbol']}"
        message += f"\n Side: {exec['side']}"
        message += f"\n Size, {self.symbol_dydx.split('-')[0]}: {abs(exec['homeNotional'])}"
        message += f"\n Size, USD: {exec['foreignNotional']}"
        message += f"\n Price: {exec['avgPx']}"
        message += f"\n Fee: {exec['execComm'] / 10 ** self.pos_power} {exec['currency']}"
        self.send_message(self.chat_id, message)

    def run(self):
        self.send_message(self.chat_id, f"Parsing started. Takers+Makers")
        if 'USDT' in self.symbol_bitmex:
            self.fetch_usdc_rate()
        time.sleep(1)
        self.balance_message()
        while True:
            time.sleep(0.001)
            self.time_based_messages()
            if self.side in ['BITMEX', 'BOTH']:
                self.check_executions()
            if not self.side == 'BOTH' and (round(time.time()) - self.start_timestamp) > 400:
                self.check_opposite()
            # self.find_makers()
            self.find_price_diffs()
            # print(self.client_Bitmex.open_orders(''))
            # print(self.client_Bitmex.market_depth()[self.symbol_bitmex]['asks'][0][0])
            # print(self.client_Bitmex.market_depth()[self.symbol_bitmex]['bids'][0][0])

    def fetch_usdc_rate(self):
        resp = requests.get('https://api.kraken.com/0/public/Depth?pair=USDCUSDT').json()
        orderbook = resp['result']['USDCUSDT']
        self.rate = (float(orderbook['asks'][0][0]) + float(orderbook['bids'][0][0])) / 2

    def cancel_all_orders_bit(self):
        orders = self.client_Bitmex.open_orders('')
        for order in orders:
            self.client_Bitmex.cancel_order(order['orderID'])

bot = botDydxBitmex(cp['BITMEX']['symbol'], cp['DYDX']['symbol'], side=cp['SETTINGS']['side'])
doc = open('deals.db', 'rb')
try:
    bot.telegram_bot.send_document(bot.chat_id, doc)
except:
    pass
bot.run()