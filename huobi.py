# -*— coding:utf-8 -*-

"""
Huobi Future Demo.

Author: QiaoXiaofeng
Date:   2020/1/10
Email:  andyjoe318@gmail.com
"""

import time
import sys
from strategy import MyStrategy

if len(sys.argv) > 1:
    config_file = sys.argv[1]
else:
    config_file = None


def initialize():
    # global client

    client = MyStrategy()


def main():
    if len(sys.argv) > 1:
        config_file = sys.argv[1]
    else:
        config_file = None

    from alpha.quant import quant
    quant.initialize(config_file)
    initialize()
    quant.start()


if __name__ == '__main__':
    client = MyStrategy()
    while True:
        print(1)
        time.sleep(1)
        print(f"Ask price: {client.ask1_price}")
        print(f"Ask size: {client.ask1_volume}")
        print(f"Bid price: {client.bid1_price}")
        print(f"Bid size: {client.bid1_volume}")
