from threading import Thread
from multiprocessing import Process

from workers.spot.current import update_spot_current, task_spot_current
from workers.spot.historical import update_spot_historical, task_spot_historical
from workers.spot.prices import task_spot_price

from workers.future.current import update_future_current, task_future_current
from workers.future.historical import update_future_historical, task_future_historical
from workers.future.prices import task_future_price

from workers.indicators.calc_avg import task_avg_ratio

import model
import model_view
import model_service

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

    tickers = model_service.get_current_future_symbols(engine)

    process_avg_ratio_monthly = Process(name="process_avg_ratio_monthly", target=task_avg_ratio, args=(tickers, 'monthly_avg_year_ratio', 43200))
    process_avg_ratio_monthly.start()

    process_avg_ratio_weekly = Process(name="process_avg_ratio_weekly", target=task_avg_ratio, args=(tickers, 'weekly_avg_year_ratio', 10080))
    process_avg_ratio_weekly.start()

    process_avg_ratio_daily = Process(name="process_avg_ratio_daily", target=task_avg_ratio, args=(tickers, 'daily_avg_year_ratio', 1440))
    process_avg_ratio_daily.start()

    process_avg_ratio_six_hours = Process(name="process_avg_ratio_six_hours", target=task_avg_ratio, args=(tickers, 'six_hours_avg_year_ratio', 360))
    process_avg_ratio_six_hours.start()

    process_avg_ratio_hourly = Process(name="process_avg_ratio_hourly", target=task_avg_ratio, args=(tickers, 'hourly_avg_year_ratio', 60))
    process_avg_ratio_hourly.start()

    process_avg_ratio_ten_minutes = Process(name="process_avg_ratio_ten_minutes", target=task_avg_ratio, args=(tickers, 'ten_minutes_avg_year_ratio', 10))
    process_avg_ratio_ten_minutes.start()







