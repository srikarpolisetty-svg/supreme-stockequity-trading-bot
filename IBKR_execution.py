from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import duckdb
import exchange_calendars as ecals
from ib_insync import IB, Contract, MarketOrder, StopOrder, Order


# =========================
# Config
# =========================
HOST = "127.0.0.1"
PORT = 4002
DB_PATH = "/home/ubuntu/supreme-stockequity-trading-bot/stocks_data.db"


EXECUTE_TRADES_DEFAULT = True
ALLOW_EXITS_WHEN_KILLED = True


@dataclass
class RiskConfig:
    per_trade_risk_pct: float = 0.005
    per_day_risk_pct: float = 0.01
    max_open_orders: int = 3
    min_order_age_seconds: int = 60 * 60
    trail_pct: float = 0.10      # 10% trail
    trail_tif: str = "GTC"
    entry_qty: int = 2


class IBKREquityExecutionEngine:
    """
    IBKR state-driven equity execution engine.
    Mirrors the options engine architecture.
    """

    def __init__(
        self,
        client_id: int,
        risk: RiskConfig = RiskConfig(),
        execute_trades_default: bool = EXECUTE_TRADES_DEFAULT,
        allow_exits_when_killed: bool = ALLOW_EXITS_WHEN_KILLED,
    ):
        self.client_id = client_id
        self.risk = risk
        self.execute_trades_default = execute_trades_default
        self.allow_exits_when_killed = allow_exits_when_killed

        self.ib = IB()
        self.NY_TZ = ZoneInfo("America/New_York")
        self.XNYS = ecals.get_calendar("XNYS")

        self.order_submit_time: dict[int, datetime] = {}

    # -------------------------
    # Connection
    # -------------------------
    def connect(self):
        if not self.ib.isConnected():
            self.ib.connect(HOST, PORT, clientId=self.client_id)

    def disconnect(self):
        if self.ib.isConnected():
            self.ib.disconnect()

    # -------------------------
    # Contract helpers
    # -------------------------
    def stock_contract_from_conid(self, conid: int) -> Contract:
        return Contract(conId=int(conid), secType="STK", exchange="SMART", currency="USD")

    # -------------------------
    # Account / Positions
    # -------------------------
    def get_positions(self):
        return [p for p in self.ib.positions() if p.contract.secType == "STK"]

    def _already_long_conid(self, conid: int) -> bool:
        for p in self.get_positions():
            if int(p.contract.conId) == int(conid) and p.position > 0:
                return True
        return False

    # -------------------------
    # Orders
    # -------------------------
    def _track_trade(self, trade):
        try:
            self.order_submit_time[trade.order.orderId] = datetime.now(timezone.utc)
        except Exception:
            pass

    def place_market_buy(self, contract: Contract, qty: int, allow: bool):
        if not allow:
            return None
        qty = int(qty)
        t = self.ib.placeOrder(contract, MarketOrder("BUY", qty))
        self._track_trade(t)
        self.ib.sleep(0.2)
        return t

    def place_market_sell(self, contract: Contract, qty: int, allow: bool):
        if not allow:
            return None
        qty = int(qty)
        t = self.ib.placeOrder(contract, MarketOrder("SELL", qty))
        self._track_trade(t)
        self.ib.sleep(0.2)
        return t

    def place_trailing_stop(self, contract: Contract, qty: int, allow: bool):
        if not allow:
            return None
        qty = int(qty)
        o = Order(
            action="SELL",
            orderType="TRAIL",
            totalQuantity=qty,
            trailingPercent=float(self.risk.trail_pct * 100.0),
            tif=self.risk.trail_tif,
        )
        t = self.ib.placeOrder(contract, o)
        self._track_trade(t)
        self.ib.sleep(0.2)
        return t


    # -------------------------
    # DB
    # -------------------------
    def load_latest_signal(self, symbol: str):
        con = duckdb.connect(DB_PATH)
        df = con.execute(
            """
            SELECT *
            FROM stock_execution_signals_5m
            WHERE symbol = ?
              AND trade_signal = TRUE
            ORDER BY timestamp DESC
            LIMIT 1
            """,
            [symbol],
        ).df()
        con.close()
        return df

    # -------------------------
    # Main loop
    # -------------------------
    def run(self, symbol: str):
        now = datetime.now(self.NY_TZ)
        if not self.XNYS.is_open_on_minute(now, ignore_breaks=True):
            return

        self.connect()

        allow_orders = bool(self.execute_trades_default)
        allow_exits = bool(allow_orders or self.allow_exits_when_killed)

        # Buying power
        acct = {r.tag: float(r.value) for r in self.ib.accountSummary() if r.value}
        buying_power = acct.get("BuyingPower", acct.get("AvailableFunds", 0.0))

        max_trade_risk = buying_power * self.risk.per_trade_risk_pct
        max_day_risk = buying_power * self.risk.per_day_risk_pct

        # -------------------------
        # Entry gates
        # -------------------------
        open_trades = [
            t for t in self.ib.trades()
            if t.orderStatus.status in {"Submitted", "PreSubmitted"}
        ]

        if len(open_trades) >= self.risk.max_open_orders:
            allow_orders = False

        now_utc = datetime.now(timezone.utc)
        for t in open_trades:
            ts = self.order_submit_time.get(t.order.orderId)
            if ts and (now_utc - ts).total_seconds() < self.risk.min_order_age_seconds:
                allow_orders = False

        # -------------------------
        # Load signal
        # -------------------------
        df = self.load_latest_signal(symbol)
        if df.empty:
            return

        row = df.iloc[0]
        conid = int(row["con_id"])

        if self._already_long_conid(conid):
            return

        contract = self.stock_contract_from_conid(conid)

        # -------------------------
        # Entry
        # -------------------------
        if allow_orders:
            tr = self.place_market_buy(contract, self.risk.entry_qty, allow_orders)
            if tr is not None:
                self.place_trailing_stop(contract, self.risk.entry_qty, allow_exits)

        # -------------------------
        # Position management
        # -------------------------
        for p in self.get_positions():
            qty = int(p.position)
            if qty <= 0:
                continue

            # Safety: ensure trailing exists
            has_trail = any(
                t.order.orderType == "TRAIL"
                and int(t.contract.conId) == int(p.contract.conId)
                for t in self.ib.trades()
            )

            if not has_trail:
                self.place_trailing_stop(p.contract, qty, allow_exits)


def main_execution(client_id: int, symbols):
    eng = IBKREquityExecutionEngine(client_id)
    try:
        for i, sym in enumerate(symbols, start=1):
            print(f"[EQUITY EXEC] ({i}/{len(symbols)}) {sym}")
            eng.run(sym)
            time.sleep(0.15)
    finally:
        eng.disconnect()
