import sys
from zoneinfo import ZoneInfo
import exchange_calendars as ecals
from datetime import datetime, timezone, timedelta
from config import SECRET_KEY, ACCOUNT_ID
from execution_functions import get_access_token
from execution_functions import get_portfolio
from execution_functions import to_float
from execution_functions import get_daily_unrealized_pnl_equity
from execution_functions import place_close_order
import duckdb
from execution_functions import get_all_symbols
from execution_functions import load_all_symbols_execution
from execution_functions import place_equity_order
from execution_functions import preflight_single_leg_equity
from execution_functions import place_stop_close_order
from execution_functions import trail_exit_signals

NY_TZ = ZoneInfo("America/New_York")
XNYS = ecals.get_calendar("XNYS")  # NYSE


def run_equity_bot_for_symbol(symbol: str):
    now1 = datetime.now(NY_TZ)

    # True only if the exchange is actually open right now (includes holidays/early closes)
    if not XNYS.is_open_on_minute(now1, ignore_breaks=True):
        print(f"Market closed (holiday/after-hours) — skipping insert. now={now1}")
        sys.exit(0)

    token_response = get_access_token(SECRET_KEY)
    data = get_portfolio(ACCOUNT_ID, token_response)

    cash_bp = to_float(data["buyingPower"]["cashOnlyBuyingPower"])

    positions = [
        p for p in data.get("positions", [])
        if p.get("instrument", {}).get("type") == "EQUITY"
    ]

    if not positions:
        print("No equity positions to close")

    orders = [
        o for o in data.get("orders", [])
        if o.get("instrument", {}).get("type") == "EQUITY"
    ]

    if not orders:
        print("No equity orders found")

    PER_TRADE_RISK_PCT = 0.005
    PER_DAY_RISK_PCT = 0.01

    max_risk_per_trade = cash_bp * PER_TRADE_RISK_PCT
    max_risk_per_day = cash_bp * PER_DAY_RISK_PCT

    execute_trades = False

    ACTIVE_STATUSES = {
        "NEW",
        "PARTIALLY_FILLED",
    }

    # puts in a list
    active_buy_open_market = [
        o for o in orders
        if o.get("side") == "BUY"
        and o.get("openCloseIndicator") == "OPEN"
        and o.get("type") == "MARKET"
        and o.get("status") in ACTIVE_STATUSES
    ]

    num_buy_open_market = len(active_buy_open_market)

    if num_buy_open_market >= 3:
        execute_trades = False
    else:
        execute_trades = True

    import pytz
    est = pytz.timezone("America/New_York")
    now = datetime.now(est)

    for order in active_buy_open_market:
        created_at_str = order.get("createdAt")
        if not created_at_str:
            continue

        created_at = datetime.fromisoformat(
            created_at_str.replace("Z", "+00:00")
        )

        # 1 hour = 60 * 60 seconds
        if (now - created_at).total_seconds() <= 60 * 60:
            execute_trades = False
            continue

    daily_unrealized = get_daily_unrealized_pnl_equity(data)

    if daily_unrealized < 0 and abs(daily_unrealized) >= max_risk_per_day:
        for pos in positions:
            # skip non-equity positions
            if pos.get("instrument", {}).get("type") != "EQUITY":
                continue

            place_close_order(
                ACCOUNT_ID,
                token_response,
                order_id=pos["orderId"],
                symbol=pos["instrument"]["symbol"],
                side="SELL",          # closing a long
                order_type="MARKET",
                quantity=pos["quantity"]
            )

    con = duckdb.connect("stocks_data.db")

    df = con.execute(
        """
        SELECT *
        FROM options_snapshots_enriched
        WHERE symbol = ?
        ORDER BY timestamp DESC
        LIMIT 1
        """,
        [symbol]
    ).df()

    signal_cols = [
        "trade_signal"
    ]

    for row in df.itertuples(index=False):
        fired_signals = [col for col in signal_cols if getattr(row, col)]

        if not fired_signals:
            continue

        # Preflight risk check
        flight = preflight_single_leg_equity(
            account_id=ACCOUNT_ID,
            access_token=token_response,
            symbol=row.symbol,
            quantity=1
        )
        if flight.get("estimatedCost", float("inf")) <= max_risk_per_trade:
            if execute_trades:
                place_equity_order(
                    account_id=ACCOUNT_ID,
                    access_token=token_response,
                    symbol=row.symbol,
                    side="BUY",
                    quantity=2,
                    execute=False
                )

    positions = data.get("positions", [])

    if positions:
        for pos in positions:
            # ✅ only equity
            if (pos.get("instrument", {}) or {}).get("type") != "EQUITY":
                continue

            equity_return = to_float(
                (pos.get("costBasis") or {}).get("gainPercentage")
            )

            if equity_return is None:
                continue

            if equity_return >= 0.5:
                entry_price = to_float(
                    (pos.get("costBasis") or {}).get("unitCost")
                )

                if entry_price is None:
                    continue

                stop_price = entry_price

                place_stop_close_order(
                    ACCOUNT_ID,
                    token_response,
                    symbol=pos["instrument"]["symbol"],
                    side="SELL",
                    quantity=pos["quantity"],
                    stop_price=stop_price
                )

    trail_exit_signals(data=data, token_response=token_response)


# Example call:
# run_equity_bot_for_symbol("AAPL")
