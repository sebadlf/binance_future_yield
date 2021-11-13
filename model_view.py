from sqlalchemy.sql import text
from model import engine

def create_view(name, query, **kwargs):
    stm_delete = f"""
    DROP VIEW IF EXISTS {name}
    """

    with engine.connect() as connection:
        connection.execute(stm_delete)

    stm_create = text(f"""
    CREATE VIEW {name} AS 
    {query}
    """)

    with engine.connect() as connection:
        connection.execute(stm_create, **kwargs)

historical_ratios = '''
    SELECT 	fh.open_time,
            f.symbol as future_symbol,
            fh.close as future_price,
            s.symbol as spot_symbol,
            sh.close as spot_price,
            ((fh.close / sh.close - 1) * 100) as direct_ratio,
            TIMESTAMPDIFF(HOUR, fh.open_time, f.delivery_date) + 1 as hours,
            ((fh.close / sh.close - 1) * 100) / (TIMESTAMPDIFF(HOUR, fh.open_time, f.delivery_date) + 1) as hour_ratio,
            TIMESTAMPDIFF(DAY, fh.open_time, f.delivery_date) days,
            ((fh.close / sh.close - 1) * 100) / (TIMESTAMPDIFF(hour, fh.open_time, f.delivery_date) + 1)  * 24 * 365 as year_ratio,
            f.contract_size,
            f.contract_size / fh.close as buy_per_contract,
            s.tick_size,
            s.base_asset
    FROM 	spot s
    JOIN 	spot_historical sh
    ON		s.symbol = sh.symbol
    JOIN 	future f
    ON		s.symbol = concat(f.pair, 'T')
    JOIN 	future_historical fh
    ON		f.symbol = fh.symbol
            AND sh.open_time = fh.open_time
    '''

def create_views():
    create_view("historical_ratios", historical_ratios)

if __name__ == '__main__':
    create_views()
