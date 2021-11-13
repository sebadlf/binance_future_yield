from threading import Thread
from multiprocessing import Process

from workers.spot.current import update_spot_current, task_spot_current
from workers.spot.historical import update_spot_historical, task_spot_historical
from workers.spot.prices import task_spot_price

from workers.future.current import update_future_current, task_future_current
from workers.future.historical import update_future_historical, task_future_historical
from workers.future.prices import task_future_price

import model
import model_view

if __name__ == '__main__':
    engine = model.get_engine()

    model.create_tables()

    model_view.create_views()

    print("Initialization start")

    # Inicialización de datos
    update_future_current(engine)
    update_future_historical(engine)

    update_spot_current(engine)
    update_spot_historical(engine)

    print("Initialization end")

    thread_current_spot = Thread(name="thread_current_spot", target=task_spot_current)
    thread_current_spot.start()

    thread_current_futures = Thread(name="thread_current_futures", target=task_future_current)
    thread_current_futures.start()

    thread_spot_historical = Thread(name="thread_spot_historical", target=task_spot_historical)
    thread_spot_historical.start()

    thread_future_historical = Thread(name="thread_future_historical", target=task_future_historical)
    thread_future_historical.start()

    process_spot_price = Process(name="process_spot_price", target=task_spot_price)
    process_spot_price.start()

    process_future_price = Process(name="process_future_price", target=task_future_price)
    process_future_price.start()







