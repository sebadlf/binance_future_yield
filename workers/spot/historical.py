from binance_service import binance_client
import time
import app
import model_service
import traceback

import model

from workers.spot.current import spot_symbols_with_futures

engine = model.get_engine()

def update_spot_historical(engine_local):

    for symbol in spot_symbols_with_futures:
        should_continue = True

        while should_continue:
            last_date = model_service.get_spot_historical_price_last_date(engine_local, symbol)

            model_service.delete_spot_last_date(engine_local, symbol, last_date)

            start_time = int(last_date.timestamp() * 1000)
            end_time = start_time + 82800000

            data = binance_client.get_historical_klines(
                symbol=symbol,
                interval="1m",
                limit=1500,
                start_str=start_time,
                end_str=end_time
            )

            model_service.save_historical_data_spot(engine_local, symbol, data)

            should_continue = len(data) > 1

            print(symbol, last_date, len(data))

def task_spot_historical():

    engine.dispose()

    while app.running:
        data = None

        try:
            data = update_spot_historical(engine)
        except:
            traceback.print_exc()
            pass

        if len(data) <= 1:
            time.sleep(30)
            print("Terminada la lista de spot, esperando 30 segundos")

if __name__ == '__main__':

    task_spot_historical()