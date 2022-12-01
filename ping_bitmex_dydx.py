# from web3 import Web3
from dydx3 import Client
from dydx3.constants import API_HOST_MAINNET
import time
from bravado.client import SwaggerClient
from bravado.requests_client import RequestsClient
import urllib.parse

config = {
        'use_models': False,
        'validate_responses': False,
        'also_return_response': True,
    }
host = 'https://www.bitmex.com'
spec_uri = host + '/api/explorer/swagger.json'
swagger_client = SwaggerClient.from_url(spec_uri, config=config)

client = Client(host=API_HOST_MAINNET)
average_dydx = []
average_bitmex = []
while True:
    try:
        time_start = time.time()
        orderbook = client.public.get_orderbook(market='BTC-USD').data
        # print(f"DYDX time:   {time.time() - time_start} sec")
        average_dydx.append(time.time() - time_start)
        time_start = time.time()
        orderbook = swagger_client.OrderBook.OrderBook_getL2(symbol='XBTUSD').result()
        # print(f"Bitmex time: {time.time() - time_start} sec")
        average_bitmex.append(time.time() - time_start)
        # print()
    except:
        pass
    print(f"Av. results:\nBitm. {sum(average_bitmex) / len(average_bitmex)}")
    print(f"DYDX. {sum(average_dydx) / len(average_dydx)}")
    print()
