from binance.client import Client
from operator import itemgetter
from keys import API_KEY, API_SECRET

import traceback

# import model_service

binance_client = Client(API_KEY, API_SECRET)

def get_filtered_future_list():
    futures = binance_client.futures_coin_exchange_info()

    futures = futures['symbols']

    filtered_futures = filter(lambda x: x['symbol'].find("_PERP") == -1, futures)

    sorted_futures = sorted(filtered_futures, key=itemgetter("symbol"))

    return sorted_futures
