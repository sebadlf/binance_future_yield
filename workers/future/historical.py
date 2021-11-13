from binance_service import binance_client
import time
import app
import model_service
import traceback

import model

engine = model.get_engine()

def update_future_historical(engine_local):
    future_symbols = model_service.get_current_future_symbols(engine_local)

    for symbol in future_symbols:
        should_continue = True

        while should_continue:
                last_date = model_service.get_future_historical_price_last_date(engine_local, symbol)

                model_service.delete_future_last_date(engine_local, symbol, last_date)

                start_time = int(last_date.timestamp() * 1000)
                end_time = start_time + 82800000

                data = binance_client.futures_coin_klines(
                    symbol=symbol,
                    interval="1m",
                    limit=1500,
                    startTime=start_time,
                    endTime=end_time
                )

                model_service.save_historical_data_futures(engine, symbol, data)

                should_continue = len(data) > 1

                print(symbol, last_date, len(data))

def task_future_historical():

    engine.dispose()

    while app.running:
        data = None

        try:
            data = update_future_historical(engine)
        except:
            traceback.print_exc()
            pass

        if not data or len(data) <= 1:
            time.sleep(30)
            print("Terminada la lista de futuros, esperando 30 segundos")

if __name__ == '__main__':

    task_future_historical()