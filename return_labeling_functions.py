import duckdb 

con = duckdb.connect("stocks_data.db")



def fill_return_label_stock_long(label_name, minutes_ahead, order_dir="ASC"):
    """
    label_name: column to update, e.g. 'opt_ret_10m'
    minutes_ahead: int, e.g. 10, 30, 60
    order_dir: 'ASC' = earliest future bar, 'DESC' = latest future bar
    """

    if order_dir not in ("ASC", "DESC"):
        raise ValueError("order_dir must be 'ASC' or 'DESC'")

    con.execute(f"""
        UPDATE stock_bars_enriched_5m base
        SET {label_name} = (
            SELECT (f.close - base.close) / base.close
            FROM stock_bars_enriched_5m f
            WHERE f.symbol = base.symbol
              AND f.timestamp >= base.timestamp + INTERVAL '{minutes_ahead} minutes'
            ORDER BY f.timestamp {order_dir}
            LIMIT 1
        )
        WHERE {label_name} IS NULL;
    """)




def fill_return_label_stock_shortdb(label_name, minutes_ahead, order_dir="ASC"):
    """
    label_name: column to update
    minutes_ahead: int, e.g. 10, 30, 60
    order_dir: 'ASC' = earliest future bar, 'DESC' = latest future bar
    """

    if order_dir not in ("ASC", "DESC"):
        raise ValueError("order_dir must be 'ASC' or 'DESC'")

    con.execute(f"""
        UPDATE stock_bars_enriched_5m_3d base
        SET {label_name} = (
            SELECT (f.close - base.close) / base.close
            FROM stock_bars_enriched_5m_3d f
            WHERE f.symbol = base.symbol
              AND f.timestamp >= base.timestamp + INTERVAL '{minutes_ahead} minutes'
            ORDER BY f.timestamp {order_dir}
            LIMIT 1
        )
        WHERE {label_name} IS NULL;
    """)







def fill_return_label_stock_execution_long(label_name, minutes_ahead, order_dir="ASC"):
    """
    label_name: column to update
    minutes_ahead: int, e.g. 10, 30, 60
    order_dir: 'ASC' = earliest future bar, 'DESC' = latest future bar
    """

    if order_dir not in ("ASC", "DESC"):
        raise ValueError("order_dir must be 'ASC' or 'DESC'")

    con.execute(f"""
        UPDATE stock_execution_signals_5m base
        SET {label_name} = (
            SELECT (f.close - base.close) / base.close
            FROM stock_execution_signals_5m f
            WHERE f.symbol = base.symbol
              AND f.timestamp >= base.timestamp + INTERVAL '{minutes_ahead} minutes'
            ORDER BY f.timestamp {order_dir}
            LIMIT 1
        )
        WHERE {label_name} IS NULL;
    """)








def fill_return_label_stock_execution_short(label_name, minutes_ahead, order_dir="ASC"):
    """
    label_name: column to update
    minutes_ahead: int, e.g. 10, 30, 60
    order_dir: 'ASC' = earliest future bar, 'DESC' = latest future bar
    """

    if order_dir not in ("ASC", "DESC"):
        raise ValueError("order_dir must be 'ASC' or 'DESC'")

    con.execute(f"""
        UPDATE stock_execution_signals_5m_3d base
        SET {label_name} = (
            SELECT (f.close - base.close) / base.close
            FROM stock_execution_signals_5m_3d f
            WHERE f.symbol = base.symbol
              AND f.timestamp >= base.timestamp + INTERVAL '{minutes_ahead} minutes'
            ORDER BY f.timestamp {order_dir}
            LIMIT 1
        )
        WHERE {label_name} IS NULL;
    """)
