from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import duckdb
import exchange_calendars as ecals
from ib_insync import IB, Contract, MarketOrder, Order, StopOrder,LimitOrder


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
    trail_pct: float = 0.02      # 10% trail (server-side)
    trail_tif: str = "GTC"
    entry_qty: int = 2
    preflight_stop_pct: float = 0.04   # 4% "stop" used ONLY for risk math


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

        # cache for single PnL subscription
        self._pnl_sub = None


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


    def place_market_sell(self, contract: Contract, qty: int, allow: bool):
        if not allow:
            return None
        qty = int(qty)
        t = self.ib.placeOrder(contract, MarketOrder("SELL", qty))
        self._track_trade(t)
        self.ib.sleep(0.2)
        return t

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
    # Pricing (for preflight)
    # -------------------------

    # -------------------------
    # DB
    # -------------------------
# =========================
# 1) Guard DuckDB load + con_id parsing
# =========================

    # -------------------------
    # DB (SAFE)
    # -------------------------
    def load_latest_signal(self, symbol: str):
        """
        Safe DB read. Returns a df or empty df on failure.
        """
        try:
            con = duckdb.connect(DB_PATH, read_only=True)
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
        except Exception as e:
            try:
                print(
                    f"[EQUITY EXEC][DB] load_latest_signal failed for {symbol}: {e}",
                    flush=True,
                )
            except Exception:
                pass
            # return empty dataframe
            return duckdb.connect(":memory:").execute(
                "SELECT 1 WHERE 0=1"
            ).df()

    def _safe_int(self, v):
        """
        Safely coerce conId-like values to int or return None.
        Handles None / NaN / strings / floats.
        """
        try:
            if v is None:
                return None

            import math
            if isinstance(v, float) and math.isnan(v):
                return None

            if isinstance(v, str):
                v = v.strip()
                if not v:
                    return None
                if "." in v:
                    v = float(v)

            return int(v)
        except Exception:
            return None

    def get_daily_pnl(self) -> float | None:
        try:
            acct = self.ib.managedAccounts()
            if not acct:
                return None
            account = acct[0]

            # subscribe ONCE
            if self._pnl_sub is None:
                self._pnl_sub = self.ib.reqPnL(account, "")
                self.ib.sleep(0.25)   # let first value arrive
            else:
                self.ib.sleep(0.05)   # yield to event loop

            daily = getattr(self._pnl_sub, "dailyPnL", None)
            if daily is None:
                return None
            return float(daily)
        except Exception:
            return None


    def close_all_stock_positions(self, allow: bool):
        """
        Force-liquidate all STK positions with market sells.
        """
        if not allow:
            return

        for p in self.get_positions():
            try:
                qty = int(p.position)
                if qty > 0:
                    self.place_market_sell(p.contract, qty, allow=True)
            except Exception:
                continue

    def enforce_daily_loss_killswitch(self, max_day_risk: float, allow_exits: bool) -> bool:
        """
        If today's PnL <= -max_day_risk:
          - closes all stock positions (if exits allowed)
          - returns False (disallow new entries)
        Otherwise returns True.
        """
        daily_pnl = self.get_daily_pnl()
        if daily_pnl is None:
            return True  # can't measure -> don't kill

        if daily_pnl <= -float(max_day_risk):
            print(
                f"[EQUITY EXEC][KILL] daily_pnl={daily_pnl:.2f} <= -max_day_risk={max_day_risk:.2f} -> liquidate",
                flush=True
            )
            self.close_all_stock_positions(allow=allow_exits)
            return False

        return True
    # -------------------------
    # Order classification
    # -------------------------
    def _is_entry_order_trade(self, t) -> bool:
        """
        True only for ENTRY orders.
        Excludes protective TRAIL orders.
        """
        try:
            o = t.order
            if getattr(o, "orderType", None) == "TRAIL":
                return False
            if getattr(o, "action", None) != "BUY":
                return False
            return True
        except Exception:
            return False


    # -------------------------
    # Session helper
    # -------------------------
    def _is_rth_now(self) -> bool:
        now = datetime.now(self.NY_TZ)
        return bool(self.XNYS.is_open_on_minute(now, ignore_breaks=True))

    # -------------------------
    # Mark price (for mgmt + outside-RTH limits)
    # -------------------------
    def get_mark_price_snapshot(self, contract: Contract, wait_s: float = 0.6) -> float | None:
        """
        Snapshot mark: mid if bid/ask exist, else last, else close.
        Works pre/after too (when data exists).
        """
        t = self.ib.reqMktData(contract, "", snapshot=True, regulatorySnapshot=False)
        self.ib.sleep(wait_s)

        bid = t.bid if (t.bid and t.bid > 0) else None
        ask = t.ask if (t.ask and t.ask > 0) else None
        last = t.last if (t.last and t.last > 0) else None
        close = t.close if (t.close and t.close > 0) else None

        if bid is not None and ask is not None:
            return float((bid + ask) / 2.0)
        if last is not None:
            return float(last)
        if close is not None:
            return float(close)
        return None

    def _entry_from_position(self, p) -> float | None:
        """
        For stocks, avgCost is usually per-share.
        """
        try:
            ac = float(getattr(p, "avgCost", None) or 0.0)
            return ac if ac > 0 else None
        except Exception:
            return None

    # -------------------------
    # Open-order helpers (per conId)
    # -------------------------
    def _open_trades_for_conid(self, conid: int):
        working = {"Submitted", "PreSubmitted", "ApiPending"}
        out = []
        for t in self.ib.trades():
            try:
                if (
                    t.contract is not None
                    and int(t.contract.conId) == int(conid)
                    and getattr(getattr(t, "orderStatus", None), "status", None) in working
                ):
                    out.append(t)
            except Exception:
                continue
        return out

    def _has_working_breakeven_stop(self, conid: int) -> bool:
        """
        Any working SELL stop counts as 'breakeven stop already placed'.
        """
        for t in self._open_trades_for_conid(conid):
            try:
                o = t.order
                if o and o.action == "SELL" and o.orderType in {"STP", "STOP"}:
                    return True
            except Exception:
                pass
        return False

    def _has_working_scaleout_sell(self, conid: int) -> bool:
        """
        Prevent multiple 1-share take-profits stacking.
        We treat any working SELL MKT/LMT as already scaling out.
        """
        for t in self._open_trades_for_conid(conid):
            try:
                o = t.order
                if o and o.action == "SELL" and o.orderType in {"MKT", "LMT"}:
                    return True
            except Exception:
                pass
        return False

    # -------------------------
    # Order placement (pre/after supported)
    # -------------------------
    def place_buy_entry(self, contract: Contract, qty: int, allow: bool):
        """
        RTH: Market BUY
        Outside RTH: Limit BUY (outsideRth=True) at mark
        """
        if not allow:
            return None

        qty = int(qty)
        is_rth = self._is_rth_now()

        if is_rth:
            o = MarketOrder("BUY", qty)
        else:
            px = self.get_mark_price_snapshot(contract)
            if px is None or px <= 0:
                print("[EQUITY EXEC][ENTRY] skip: no price for outside-RTH limit", flush=True)
                return None
            # small cushion so you actually get filled when spreads are wide
            limit_px = round(px * 1.002, 2)  # +0.2%
            o = LimitOrder("BUY", qty, limit_px)
            o.outsideRth = True

        t = self.ib.placeOrder(contract, o)
        self._track_trade(t)
        self.ib.sleep(0.2)
        return t

    def place_sell_scaleout_1(self, contract: Contract, allow: bool):
        """
        RTH: Market SELL 1
        Outside RTH: Limit SELL 1 (outsideRth=True) at mark
        """
        if not allow:
            return None

        is_rth = self._is_rth_now()

        if is_rth:
            o = MarketOrder("SELL", 1)
        else:
            px = self.get_mark_price_snapshot(contract)
            if px is None or px <= 0:
                print("[EQUITY EXEC][SCALEOUT] skip: no price for outside-RTH limit", flush=True)
                return None
            limit_px = round(px * 0.998, 2)  # -0.2% to help fill
            o = LimitOrder("SELL", 1, limit_px)
            o.outsideRth = True

        t = self.ib.placeOrder(contract, o)
        self._track_trade(t)
        self.ib.sleep(0.2)
        return t

    def place_breakeven_stop(self, contract: Contract, qty: int, stop_price: float, allow: bool):
        """
        Place STOP SELL at entry price (breakeven).
        """
        if not allow:
            return None

        qty = int(qty)
        stop_price = float(stop_price)

        o = StopOrder("SELL", qty, stop_price)
        # stop orders generally work fine with default; outsideRth doesn't hurt
        o.outsideRth = True

        t = self.ib.placeOrder(contract, o)
        self._track_trade(t)
        self.ib.sleep(0.2)
        return t

    # -------------------------
    # Main loop
    # -------------------------
    def run(self, symbol: str):
        # NOTE: we DO NOT return for closed market anymore.
        # We allow pre/after orders (stocks support it), using limit orders outside RTH.

        self.connect()

        allow_orders = bool(self.execute_trades_default)
        allow_exits = bool(allow_orders or self.allow_exits_when_killed)

        # Buying power (SAFE)
        acct = {}
        for r in self.ib.accountSummary():
            v = getattr(r, "value", None)
            if not v:
                continue
            try:
                acct[r.tag] = float(str(v).replace(",", ""))
            except Exception:
                continue

        buying_power = acct.get("BuyingPower", acct.get("AvailableFunds", 0.0))
        max_trade_risk = float(buying_power) * self.risk.per_trade_risk_pct
        max_day_risk = float(buying_power) * self.risk.per_day_risk_pct

        allow_orders = allow_orders and self.enforce_daily_loss_killswitch(
            max_day_risk=max_day_risk,
            allow_exits=allow_exits
        )

        # -------------------------
        # Entry gates (ENTRY orders only)
        # -------------------------
        open_trades = [
            t for t in self.ib.trades()
            if getattr(getattr(t, "orderStatus", None), "status", None) in {"Submitted", "PreSubmitted", "ApiPending"}
        ]

        open_entry_trades = [t for t in open_trades if self._is_entry_order_trade(t)]

        if len(open_entry_trades) >= self.risk.max_open_orders:
            allow_orders = False

        now_utc = datetime.now(timezone.utc)
        for t in open_entry_trades:
            ts = self.order_submit_time.get(t.order.orderId)
            if ts and (now_utc - ts).total_seconds() < self.risk.min_order_age_seconds:
                allow_orders = False
                break

        # -------------------------
        # Load signal
        # -------------------------
        df = self.load_latest_signal(symbol)
        if df.empty:
            # still do management even if no new signal
            pass
        else:
            row = df.iloc[0]
            conid = self._safe_int(row["con_id"])
            if conid is None:
                print(f"[EQUITY EXEC][SIGNAL] skip {symbol}: bad con_id", flush=True)
                return

            if not self._already_long_conid(conid) and allow_orders:
                contract = self.stock_contract_from_conid(conid)

                # PREFLIGHT risk/cost check (still uses price)
                qty = int(self.risk.entry_qty)
                entry_price = self.get_mark_price_snapshot(contract)
                if entry_price is None or entry_price <= 0:
                    print(f"[EQUITY EXEC][PREFLIGHT] skip {symbol}: no price", flush=True)
                    return

                stop_price_for_math = float(entry_price) * (1.0 - float(self.risk.preflight_stop_pct))
                risk_per_share = float(entry_price) - float(stop_price_for_math)
                dollar_risk = float(risk_per_share) * float(qty)

                if dollar_risk > float(max_trade_risk):
                    print(
                        f"[EQUITY EXEC][PREFLIGHT] skip {symbol}: "
                        f"dollar_risk={dollar_risk:.2f} > max_trade_risk={max_trade_risk:.2f}",
                        flush=True
                    )
                    return

                # ENTRY (pre/after allowed)
                tr = self.place_buy_entry(contract, qty=qty, allow=allow_orders)
                if tr is not None:
                    # keep your trailing stop safety net if you want
                    self.place_trailing_stop(contract, qty, allow_exits)

        # -------------------------
        # Position management (applies to ALL open stock positions)
        # Rules:
        #  +0.5% -> stop at entry
        #  +1.0% -> sell 1 of 2
        # -------------------------
        for p in self.get_positions():
            try:
                qty = int(p.position)
                if qty <= 0:
                    continue

                conid = int(p.contract.conId)
                entry = self._entry_from_position(p)
                if entry is None or entry <= 0:
                    continue

                mark = self.get_mark_price_snapshot(p.contract)
                if mark is None or mark <= 0:
                    continue

                ret_pct = (float(mark) - float(entry)) / float(entry) * 100.0

                # +0.5% -> breakeven stop at entry (only once)
                if ret_pct >= 0.5 and not self._has_working_breakeven_stop(conid):
                    self.place_breakeven_stop(
                        contract=p.contract,
                        qty=qty,
                        stop_price=float(entry),
                        allow=allow_exits,
                    )

                # +1.0% -> sell 1 (only if you have >=2, only once)
                if ret_pct >= 1.0 and qty >= 2 and not self._has_working_scaleout_sell(conid):
                    self.place_sell_scaleout_1(
                        contract=p.contract,
                        allow=allow_exits,
                    )

                # Safety: ensure trailing exists (optional)
                has_trail = any(
                    getattr(t.order, "orderType", None) == "TRAIL"
                    and int(t.contract.conId) == int(conid)
                    for t in self.ib.trades()
                    if getattr(t, "order", None) is not None and getattr(t, "contract", None) is not None
                )
                if not has_trail:
                    self.place_trailing_stop(p.contract, qty, allow_exits)

            except Exception:
                continue



def main_execution(client_id: int, symbols):
    eng = IBKREquityExecutionEngine(client_id)
    try:
        for i, sym in enumerate(symbols, start=1):
            print(f"[EQUITY EXEC] ({i}/{len(symbols)}) {sym}")
            eng.run(sym)
            time.sleep(0.15)
    finally:
        eng.disconnect()
