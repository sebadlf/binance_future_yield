from binance_service import binance_client
import time

import app
from model_service import sync_spot

import model

engine = model.get_engine()

spot_symbols_with_futures = [
    'ADAUSDT',
    'BCHUSDT',
    'BNBUSDT',
    'BTCUSDT',
    'DOTUSDT',
    'ETHUSDT',
    'LINKUSDT',
    'LTCUSDT',
    'XRPUSDT'
]

def update_spot_current(engine_local):
    spot_list = binance_client.get_exchange_info()
    spot_list = spot_list['symbols']

    filtered_spot_list = [spot for spot in spot_list if spot['symbol'] in spot_symbols_with_futures]

    sync_spot(engine_local, filtered_spot_list)

def task_spot_current():
    engine.dispose()

    while app.running:
        try:
            update_spot_current(engine)
        except Exception as ex:
            print(ex)

        time.sleep(60)

if __name__ == '__main__':
    task_current_spot()
