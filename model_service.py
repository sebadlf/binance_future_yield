from sqlalchemy.orm import Session, aliased
from datetime import datetime, timezone

import model

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