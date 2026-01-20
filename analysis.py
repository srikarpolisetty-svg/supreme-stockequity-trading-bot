import duckdb
import sys
from datetime import datetime
from zoneinfo import ZoneInfo
import exchange_calendars as ecals

from analysisfunctions import load_all_symbols
from analysisfunctions import get_stock_metrics
from analysisfunctions import update_stock_signal
from message import send_text


def run_stock_pressure_signal(
    symbol: str,

    # short-term (3d)
    thr_price_3d: float = 0.8,
    thr_volume_3d: float = 1.0,
    thr_vol_3d: float = 0.6,

    # structural (35d)
    thr_price_35d: float = 1.2,
    thr_volume_35d: float = 1.3,
    thr_vol_35d: float = 1.0,
):

    # -------------------------
    # Market-hours gate (same as options)
    # -------------------------
    NY_TZ = ZoneInfo("America/New_York")


    now = datetime.now(NY_TZ)


    print(f"Run time: {now.strftime('%Y-%m-%d %H:%M')}")

    # -------------------------
    # Load data
    # -------------------------
    con = duckdb.connect("stocks_data.db")

    groups = load_all_symbols(con, [symbol])
    if groups is None:
        print("No data loaded")
        con.close()
        return

    stock = get_stock_metrics(groups, symbol)
    if stock is None:
        print(f"[SKIP] {symbol}: no metrics")
        con.close()
        return

    # -------------------------
    # Helpers (same style as options)
    # -------------------------
    def gt(x, thr):
        try:
            if x is None:
                return False
            if x != x:  # NaN
                return False
            return x > thr
        except Exception:
            return False

    # -------------------------
    # Unpack metrics
    # -------------------------
    short = stock["short"]
    long  = stock["long"]

    z_price_3d   = short["z_price"]
    z_volume_3d  = short["z_volume"]
    z_vol_3d     = short["z_volatility"]
    snapshot_3d  = short["snapshot_id"]

    z_price_35d  = long["z_price"]
    z_volume_35d = long["z_volume"]
    z_vol_35d    = long["z_volatility"]
    snapshot_35d = long["snapshot_id"]

    price  = stock["close"]
    symbol = stock["symbol"]

    # -------------------------
    # Signal logic (custom thresholds)
    # -------------------------
    stock_signal = (
        gt(z_price_35d,  thr_price_35d)  and
        gt(z_volume_35d, thr_volume_35d) and
        gt(z_vol_35d,    thr_vol_35d)    and
        gt(z_price_3d,   thr_price_3d)   and
        gt(z_volume_3d,  thr_volume_3d)  and
        gt(z_vol_3d,     thr_vol_3d)
    )

    # -------------------------
    # Act
    # -------------------------
    if stock_signal:
        send_text(
            f"🚀 STRONG STOCK PRESSURE SIGNAL\n\n"
            f"Symbol: {symbol}\n"
            f"Price (close): {price}\n\n"
            f"3D thresholds:  price>{thr_price_3d}, volume>{thr_volume_3d}, vol>{thr_vol_3d}\n"
            f"35D thresholds: price>{thr_price_35d}, volume>{thr_volume_35d}, vol>{thr_vol_35d}\n"
        )

        print("ALERT SENT (STOCK PRESSURE)")

        update_stock_signal(
            con,
            short_snapshot_id=snapshot_3d,
            long_snapshot_id=snapshot_35d,
            symbol=symbol,
            signal_column="trade_signal",
        )
    else:
        print("No stock signal. Z-scores not all > threshold.")

    con.close()
    return stock_signal
