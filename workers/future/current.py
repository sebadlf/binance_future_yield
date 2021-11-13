from binance_service import get_filtered_future_list
import time
import app
import model
from model_service import sync_futures

engine = model.get_engine()

def update_future_current(engine_local):
    futures = get_filtered_future_list()
    sync_futures(engine_local, futures)

def task_future_current():
    engine.dispose()

    while app.running:
        try:
            update_future_current(engine)
        except Exception as ex:
            print(ex)

        time.sleep(60)

if __name__ == '__main__':
    task_future_current()