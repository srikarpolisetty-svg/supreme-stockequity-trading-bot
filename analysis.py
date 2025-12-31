from analysisfunctions import load_all_symbols
import duckdb
from analysisfunctions import get_stock_metrics
from analysisfunctions import update_stock_signal
from message import send_text


import duckdb

def run_stock_pressure_signal(symbol: str):
    con = duckdb.connect("stocks_data.db")

    groups = load_all_symbols(con, [symbol])
    if groups is None:
     return "no data"

    # ===== STOCK METRICS =====
    stock = get_stock_metrics(groups, symbol)

    if stock is None:
     return "no data"


    stock_short = stock["short"]
    z_price_2_3day   = stock_short["z_price"]
    z_volume_2_3day  = stock_short["z_volume"]
    z_vol_2_3day     = stock_short["z_volatility"]
    price            = stock_short["close"]
    symbol           = stock_short["symbol"]
    snapshot_id_3d   = stock_short["snapshot_id"]

    stock_long = stock["long"]
    z_price_5w      = stock_long["z_price"]
    z_volume_5w     = stock_long["z_volume"]
    z_vol_5w        = stock_long["z_volatility"]
    snapshot_id_5w  = stock_long["snapshot_id"]

    # ===== SIGNAL LOGIC =====
    stock_signal = (
        z_price_5w  > 1.5 and
        z_volume_5w > 1.5 and
        z_vol_5w    > 1.5 and
        z_price_2_3day  > 1.5 and
        z_volume_2_3day > 1.5 and
        z_vol_2_3day    > 1.5
    )

    if stock_signal:
        send_text(
            f"🚀 STRONG STOCK PRESSURE SIGNAL\n\n"
            f"Symbol: {symbol}\n"
            f"Price (close): {price}\n\n"
            f"Price / Volume / Volatility Z-scores > 1.5\n"
            f"in BOTH 2–3 day and 5-week windows.\n"
            f"Significant market pressure detected."
        )
        print("ALERT SENT (STOCK PRESSURE)")

        update_stock_signal(
            con,
            short_snapshot_id=snapshot_id_3d,
            long_snapshot_id=snapshot_id_5w,
            symbol=symbol,
            signal_column="trade_signal"
        )
    else:
        print("No stock signal. Z-scores not all > 1.5.")

    return stock_signal
