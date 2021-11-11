# from streams import ThreadedWebsocketManager, FuturesType
from binance_service import binance_client

from binance import ThreadedWebsocketManager
from binance.enums import FuturesType

from binance_service import get_filtered_future_list



import traceback

from keys import API_KEY, API_SECRET

import time

import app
from model_service import sync_futures_prices

import model

cache = dict()

engine = model.get_engine()

def task_current_futures_price():
    engine.dispose()

    # time.sleep(5)

    twm = ThreadedWebsocketManager(api_key=API_KEY, api_secret=API_SECRET)
    twm.start()

    def handle_socket_message(msg):
        data = msg['data']

        symbol = data['s']
        # print(symbol)

        cache[symbol] = data

    #Tomar futuros desde la db

    streams = [f"{future['symbol'].lower()}@bookTicker" for future in get_filtered_future_list()]

    twm.start_futures_multiplex_socket(callback=handle_socket_message, streams=streams, futures_type=FuturesType.COIN_M)

    while app.running:
        try:
            to_save = []

            while len(cache):
                item_key, item_value = cache.popitem()
                to_save.append(item_value)

            if len(to_save):
                sync_futures_prices(engine, to_save)
        except Exception as ex:
            print(ex)
            traceback.print_stack()

if __name__ == '__main__':
    task_current_futures_price()