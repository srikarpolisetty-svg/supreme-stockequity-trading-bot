import duckdb
import sys
from datetime import datetime
from zoneinfo import ZoneInfo
import exchange_calendars as ecals

from analysisfunctions import load_all_symbols
from analysisfunctions import get_stock_metrics
from analysisfunctions import update_stock_signal
from message import send_text


def run_stock_pressure_signal(symbol: str):
    # -------------------------
    # Market-hours gate (same as options)
    # -------------------------
    NY_TZ = ZoneInfo("America/New_York")
    XNYS = ecals.get_calendar("XNYS")

    now = datetime.now(NY_TZ)
    if not XNYS.is_open_on_minute(now, ignore_breaks=True):
        print(f"Market closed — skipping stock signal. now={now}")
        sys.exit(0)

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
    def gt(x, thr=1.0):
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
    # Signal logic (identical structure to options)
    # -------------------------
    stock_signal = (
        gt(z_price_35d)  and
        gt(z_volume_35d) and
        gt(z_vol_35d)    and
        gt(z_price_3d)   and
        gt(z_volume_3d)  and
        gt(z_vol_3d)
    )

    # -------------------------
    # Act
    # -------------------------
    if stock_signal:
        send_text(
            f"🚀 STRONG STOCK PRESSURE SIGNAL\n\n"
            f"Symbol: {symbol}\n"
            f"Price (close): {price}\n\n"
            f"Price / Volume / Volatility Z-scores > 1.0\n"
            f"in BOTH 3-day and 35-day windows.\n"
            f"Significant market pressure detected."
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
