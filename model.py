from sqlalchemy import Column, Integer, BigInteger, String, Float, Boolean, ForeignKey, create_engine, Index, func
from sqlalchemy.dialects.mysql import DATETIME
from sqlalchemy.orm import relationship, sessionmaker, declarative_base, remote, foreign

import operator

from datetime import datetime

import keys

# declarative base class
Base = declarative_base()


class Future(Base):
    __tablename__ = 'future'

    symbol = Column(String(20), primary_key=True)
    pair = Column(String(10))
    contract_type = Column(String(15))

    delivery_timestamp = Column(BigInteger)
    delivery_date = Column(DATETIME)

    onboard_timestamp = Column(BigInteger)
    onboard_date = Column(DATETIME)

    contract_status = Column(String(20))
    contract_size = Column(Integer)

    base_asset = Column(String(10))
    quote_asset = Column(String(10))

    # operations = relationship("Operation", back_populates="future_relation")
    #
    future_price = relationship("FuturePrice", uselist=False, back_populates="future")
    # balance = relationship("FutureBalance", uselist=False, back_populates="future")

    inserted = Column(DATETIME(fsp=6), default=datetime.utcnow)
    updated = Column(DATETIME(fsp=6), default=datetime.utcnow, onupdate=datetime.utcnow)

    __table_args__ = (
        Index('base_asset', base_asset),
    )

class FuturePrice(Base):
    __tablename__ = 'future_price'

    symbol = Column(String(20), ForeignKey('future.symbol'), primary_key=True)

    future = relationship("Future", back_populates="future_price")

    pair = Column(String(10))
    # spot_price = relationship("SpotPrice", back_populates="futures")

    # spot_price = relationship(
    #     "SpotPrice",
    #     primaryjoin=func.concat(pair, 'T') == foreign(SpotPrice.symbol),
    #     uselist=False,
    #     viewonly=True,
    # )

    ask_price = Column(Float)
    ask_qty = Column(Float)
    bid_price = Column(Float)
    bid_qty = Column(Float)

    # mark_price = Column(Float)
    # index_price = Column(Float)
    # estimated_settle_price = Column(Float)
    # last_funding_rate = Column(Float)
    # interest_rate = Column(Float)
    #
    # next_funding_timestamp = Column(BigInteger)
    # next_funding_time = Column(DATETIME)

    # timestamp = Column(BigInteger)
    # time = Column(DATETIME)

    inserted = Column(DATETIME(fsp=6), default=datetime.utcnow)
    updated = Column(DATETIME(fsp=6), default=datetime.utcnow, onupdate=datetime.utcnow)

class FutureHistorical(Base):
    __tablename__ = 'future_historical'

    # id = Column(Integer, primary_key=True)
    symbol = Column(String(20), ForeignKey('future.symbol'), primary_key=True)
    open_time = Column(DATETIME, primary_key=True)
    open = Column(Float)
    high = Column(Float)
    low = Column(Float)
    close = Column(Float)
    volume = Column(Float)
    close_time = Column(DATETIME)
    quote_asset_volume = Column(Float)
    trades = Column(Integer)
    taker_buy_base = Column(Float)
    taker_buy_quote = Column(Float)
    ignore = Column(Float)

    inserted = Column(DATETIME(fsp=6), default=datetime.utcnow)
    updated = Column(DATETIME(fsp=6), default=datetime.utcnow, onupdate=datetime.utcnow)

    __table_args__ = (
        Index('symbol', symbol, open_time),
    )

engine = create_engine(keys.DB_CONNECTION)

view_tables = ['current_operations_to_open', 'current_operations_to_close']

real_tables = [table_value for (table_key, table_value) in Base.metadata.tables.items() if table_key not in view_tables]

def create_tables():
    Base.metadata.create_all(engine, tables=real_tables)

def get_engine():
    return create_engine(keys.DB_CONNECTION)

if __name__ == '__main__':
    create_tables()