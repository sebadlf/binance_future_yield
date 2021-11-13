from binance.streams import ThreadedWebsocketManager

import traceback
import time
import app
from model_service import sync_spot_prices, get_current_spot_symbols

from keys import API_KEY, API_SECRET

import model

cache = dict()

engine = model.get_engine()

def task_spot_price():
    engine.dispose()

    twm = ThreadedWebsocketManager(api_key=API_KEY, api_secret=API_SECRET)
    twm.start()

    def handle_socket_message(msg):
        if not msg.get('data'):
            print(msg)

        data = msg['data']

        symbol = data['s']

        cache[symbol] = data

    streams = [f"{symbol.lower()}@bookTicker" for symbol in get_current_spot_symbols(engine)]

    twm.start_multiplex_socket(callback=handle_socket_message, streams=streams)

    while app.running:
        try:
            to_save = []

            while len(cache):
                item_key, item_value = cache.popitem()
                to_save.append(item_value)

            if len(to_save):
                sync_spot_prices(engine, to_save)
        except Exception as ex:
            print(ex)
            traceback.print_stack()

if __name__ == '__main__':
    task_current_spot_price()