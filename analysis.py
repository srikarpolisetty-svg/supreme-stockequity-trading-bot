import duckdb
from datetime import datetime
from zoneinfo import ZoneInfo

from analysisfunctions import load_all_symbols, get_stock_metrics, update_stock_signal
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
    NY_TZ = ZoneInfo("America/New_York")
    now = datetime.now(NY_TZ)
    print(f"Run time: {now.strftime('%Y-%m-%d %H:%M')}")

    con = duckdb.connect("stocks_data.db")
    try:
        groups = load_all_symbols(con, [symbol])
        if groups is None:
            print("No data loaded")
            return None

        stock = get_stock_metrics(groups, symbol)
        if stock is None:
            print(f"[SKIP] {symbol}: no metrics")
            return None

        def gt(x, thr):
            try:
                if x is None:
                    return False
                if x != x:  # NaN
                    return False
                return x > thr
            except Exception:
                return False

        short = stock["short"]
        long  = stock["long"]

        z_price_3d   = short.get("z_price")
        z_volume_3d  = short.get("z_volume")
        z_vol_3d     = short.get("z_volatility")

        z_price_35d  = long.get("z_price")
        z_volume_35d = long.get("z_volume")
        z_vol_35d    = long.get("z_volatility")

        snapshot_id = stock.get("snapshot_id")
        price  = stock.get("close")
        symbol = stock.get("symbol")

        stock_signal = (
            gt(z_price_35d,  thr_price_35d)  and
            gt(z_volume_35d, thr_volume_35d) and
            gt(z_vol_35d,    thr_vol_35d)    and
            gt(z_price_3d,   thr_price_3d)   and
            gt(z_volume_3d,  thr_volume_3d)  and
            gt(z_vol_3d,     thr_vol_3d)
        )

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
                con=con,
                symbol=symbol,
                snapshot_id=snapshot_id,
                signal_column="trade_signal",
            )
        else:
            print("No stock signal. Z-scores not all > threshold.")

        return stock_signal

    finally:
        con.close()
