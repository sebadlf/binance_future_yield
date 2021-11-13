from sqlalchemy.orm import Session, aliased
from datetime import datetime, timezone
from sqlalchemy import func, delete

import model

def get_current_spot_symbols(engine):
    with Session(engine) as session, session.begin():
        spots = session.query(model.Spot).\
            all()

        symbols = [spot.symbol for spot in spots]

        return symbols

def get_spot_historical_price_last_date(engine, symbol):
    last_date = None

    with Session(engine) as session, session.begin():
        result = session.query(func.max(model.SpotHistorical.open_time)).\
            where(model.SpotHistorical.symbol == symbol).\
            one()

        last_date = result[0]

    if not last_date:
        with Session(engine) as session, session.begin():
            result = session.query(func.min(model.Future.onboard_date)). \
                all()

            last_date = result[0][0] if result[0] else None

    return last_date

def delete_spot_last_date(engine, symbol, last_date):
    with Session(engine) as session, session.begin():
        session.execute(
            delete(model.SpotHistorical).\
            where(
                model.SpotHistorical.symbol == symbol,
                model.SpotHistorical.open_time == last_date
            )
        )

def get_current_future_symbols(engine):
    with Session(engine) as session, session.begin():
        futures = session.query(model.Future).\
            where(
                model.Future.onboard_date < datetime.utcnow(),
                model.Future.delivery_date > datetime.utcnow()
            ).\
            all()

        symbols = [future.symbol for future in futures]

        return symbols

def delete_future_last_date(engine, symbol, last_date):
    with Session(engine) as session, session.begin():
        session.execute(
            delete(model.FutureHistorical).\
            where(
                model.FutureHistorical.symbol == symbol,
                model.FutureHistorical.open_time == last_date
            )
        )



def get_future_historical_price_last_date(engine, symbol):
    last_date = None

    with Session(engine) as session, session.begin():
        result = session.query(func.max(model.FutureHistorical.open_time)).\
            where(model.FutureHistorical.symbol == symbol).\
            one()

        last_date = result[0]

    if not last_date:
        with Session(engine) as session, session.begin():
            result = session.query(model.Future.onboard_date). \
                where(model.Future.symbol == symbol). \
                all()

            last_date = result[0][0] if result[0] else None

    return last_date

def sync_spot(engine, spot_list):
    with Session(engine) as session, session.begin():
        for spot in spot_list:
            symbol = spot['symbol']

            price_filter = list(filter(lambda coin: coin['filterType'] == "PRICE_FILTER", spot['filters']))[0]
            lot_filter = list(filter(lambda coin: coin['filterType'] == "LOT_SIZE", spot['filters']))[0]

            spot_db = session.query(model.Spot).get(symbol)

            if not spot_db:
                spot_db = model.Spot(symbol=symbol)
                session.add(spot_db)

            spot_db.base_asset = spot['baseAsset']
            spot_db.quote_asset = spot['quoteAsset']

            spot_db.min_price = price_filter['minPrice']
            spot_db.max_price = price_filter['maxPrice']
            spot_db.tick_size = lot_filter['stepSize']

def sync_spot_prices(engine, spot_prices):
    with Session(engine) as session, session.begin():
        for spot_price in spot_prices:
            symbol = spot_price['s']

            spot_price_db = session.query(model.SpotPrice).get(symbol)

            if not spot_price_db:
                spot_price_db = model.SpotPrice(symbol=symbol)
                session.add(spot_price_db)

            spot_price_db.ask_price = spot_price['a']
            spot_price_db.ask_qty = spot_price['A']
            spot_price_db.bid_price = spot_price['b']
            spot_price_db.bid_qty = spot_price['B']

def save_historical_data_spot(engine, symbol, historical_data):
    with Session(engine) as session, session.begin():
        for hour_data in historical_data:
            market_data = model.SpotHistorical(symbol=symbol)
            session.add(market_data)

            market_data.open_time = datetime.fromtimestamp(hour_data[0] / 1000)
            market_data.open = hour_data[1]
            market_data.high = hour_data[2]
            market_data.low = hour_data[3]
            market_data.close = hour_data[4]
            market_data.volume = hour_data[5]
            market_data.close_time = datetime.fromtimestamp(hour_data[6] / 1000)
            market_data.quote_asset_volume = hour_data[7]
            market_data.trades = hour_data[8]
            market_data.taker_buy_base = hour_data[9]
            market_data.taker_buy_quote = hour_data[10]
            market_data.ignore = hour_data[11]

def sync_futures(engine, futures):
    with Session(engine) as session, session.begin():
        for future in futures:
            symbol = future['symbol']

            future_db = session.query(model.Future).get(symbol)

            if not future_db:
                future_db = model.Future(symbol=symbol)
                session.add(future_db)

            future_db.pair = future['pair']
            future_db.contract_type = future['contractType']

            future_db.delivery_timestamp = future['deliveryDate']
            future_db.delivery_date = datetime.fromtimestamp(future['deliveryDate'] / 1000)

            future_db.onboard_timestamp = future['onboardDate']
            future_db.onboard_date = datetime.fromtimestamp(future['onboardDate'] / 1000)

            future_db.contract_status = future['contractStatus']
            future_db.contract_size = future['contractSize']

            future_db.base_asset = future['baseAsset']
            future_db.quote_asset = future['quoteAsset']

def sync_futures_prices(engine, futures_prices):
    with Session(engine) as session, session.begin():
        for future_price in futures_prices:
            symbol = future_price['s']

            future_price_db = session.query(model.FuturePrice).get(symbol)

            if not future_price_db:
                future_price_db = model.FuturePrice(symbol=symbol)
                session.add(future_price_db)

            future_price_db.pair = future_price['ps']
            # future_price_db.mark_price = future_price['markPrice']
            # future_price_db.index_price = future_price['indexPrice']
            # future_price_db.estimated_settle_price = future_price['estimatedSettlePrice']
            # future_price_db.last_funding_rate = future_price['lastFundingRate'] if len(
            #     future_price['lastFundingRate']) else None
            # future_price_db.interest_rate = future_price['lastFundingRate'] if len(
            #     future_price['lastFundingRate']) else None
            #
            # future_price_db.next_funding_timestamp = future_price['nextFundingTime']
            # future_price_db.next_funding_time = datetime.fromtimestamp(future_price['nextFundingTime'] / 1000)
            #
            # future_price_db.timestamp = future_price['time']
            # future_price_db.time = datetime.fromtimestamp(future_price['time'] / 1000)

            future_price_db.ask_price = future_price['a']
            future_price_db.ask_qty = future_price['A']
            future_price_db.bid_price = future_price['b']
            future_price_db.bid_qty = future_price['B']

def save_historical_data_futures(engine, symbol, historical_data):
    with Session(engine) as session, session.begin():
        for hour_data in historical_data:
            market_data = model.FutureHistorical(symbol=symbol)
            session.add(market_data)

            market_data.open_time = datetime.fromtimestamp(hour_data[0] / 1000)
            market_data.open = hour_data[1]
            market_data.high = hour_data[2]
            market_data.low = hour_data[3]
            market_data.close = hour_data[4]
            market_data.volume = hour_data[5]
            market_data.close_time = datetime.fromtimestamp(hour_data[6] / 1000)
            market_data.quote_asset_volume = hour_data[7]
            market_data.trades = hour_data[8]
            market_data.taker_buy_base = hour_data[9]
            market_data.taker_buy_quote = hour_data[10]
            market_data.ignore = hour_data[11]

if __name__ == '__main__':
    print(get_future_historical_price_last_date(model.get_engine(), 'ADAUSD_211231'))