import sqlite3
import datetime

class database:

    def __init__(self, telegram_bot, chat_id):
        self.chat_id = chat_id
        self.telegram_bot = telegram_bot

        self.connect = sqlite3.connect('deals.db')
        self.sql_create_orders_table()

    def sql_create_orders_table(self):
        cursor = self.connect.cursor()
        cursor.execute("""CREATE TABLE IF NOT EXISTS deals (
        order_num INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp REAL,
        sell_exchange TEXT,
        sell_price REAL,
        buy_price REAL,
        amount_USD REAL,
        amount_coin REAL,
        profit_USD REAL,
        profit_relative REAL,
        fee_dydx REAL,
        fee_bitmex REAL,
        pnl_dydx REAL,
        pnl_bitmex REAL,
        long_side TEXT,
        deal_type TEXT,
        USDC_rate REAL
        );""")
        self.connect.commit()
        cursor.close()

    def base_update(self, to_base):
        cursor = self.connect.cursor()
        sql = f"""INSERT INTO deals (
        timestamp,
        sell_exchange,
        sell_price,
        buy_price,
        amount_USD,
        amount_coin,
        profit_USD,
        profit_relative,
        fee_dydx,
        fee_bitmex,
        pnl_dydx,
        pnl_bitmex,
        long_side,
        deal_type,
        USDC_rate)
        VALUES ({to_base["timestamp"]},
        '{to_base["sell_exchange"]}', 
        {to_base["sell_price"]}, 
        {to_base["buy_price"]}, 
        {to_base["amount_USD"]},
        {to_base["amount_coin"]},
        {to_base["profit_USD"]},
        {to_base["profit_relative"]},
        {to_base["fee_dydx"]},
        {to_base["fee_bitmex"]},
        {to_base["pnl_dydx"]},
        {to_base["pnl_bitmex"]},
        "{to_base['long_side']}",
        "{to_base['deal_type']}",
        {to_base['USDC_rate']}
        )"""
        # try:
        cursor.execute(sql)
        # except Exception as e:
        # self.telegram_bot.send_message(chat_id, f"DB error {e}\nData {sql}")
        self.connect.commit()
        cursor.close()


    def fetch_data_from_table(self, table):
        if not table == 'deals':
            raise Exception('Have only tables: deals')
        cursor = self.connect.cursor()
        data = cursor.execute(f"SELECT * FROM {table};").fetchall()
        cursor.close()
        return data


    def close_connection(self):
        self.connect.close()