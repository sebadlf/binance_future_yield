from binance_service import get_filtered_future_list
import time
import app
import model
from model_service import sync_futures

engine = model.get_engine()

def task_current_futures():
    engine.dispose()

    while app.running:
        try:
            futures = get_filtered_future_list()
            sync_futures(engine, futures)
        except Exception as ex:
            print(ex)

        time.sleep(60)

if __name__ == '__main__':
    task_current_futures()