from __future__ import annotations

import time
from dataclasses import dataclass
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import duckdb
import exchange_calendars as ecals
from ib_insync import IB, Contract, MarketOrder, Order, StopOrder, LimitOrder


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
    trail_pct: float = 0.02      # 2% trail (server-side)
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

        # ✅ NEW: suppress position-management spam (log PM only once per main_execution cycle)
        self._pm_logged_this_cycle: bool = False

    # =========================
    # PRINT VISIBILITY HELPERS (no refactor)
    # =========================
    def log(self, event: str, **fields):
        try:
            ts = datetime.now(self.NY_TZ).strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        extras = ""
        if fields:
            parts = []
            for k, v in fields.items():
                try:
                    parts.append(f"{k}={v}")
                except Exception:
                    parts.append(f"{k}=?")
            extras = " " + " ".join(parts)

        print(f"[EQUITY EXEC][{event}] {ts}{extras}", flush=True)

    def _print_positions_snapshot(self, where: str):
        try:
            pos = list(self.ib.positions())
        except Exception as e:
            self.log("POS_SNAPSHOT_ERR", where=where, err=str(e))
            return

        stk = []
        for p in pos:
            try:
                if p.contract and p.contract.secType == "STK" and float(p.position) != 0:
                    stk.append(p)
            except Exception:
                continue

        self.log("POS_SNAPSHOT", where=where, stk_positions=len(stk))

        for p in stk:
            try:
                self.log(
                    "POS",
                    where=where,
                    symbol=getattr(p.contract, "symbol", None),
                    conid=getattr(p.contract, "conId", None),
                    qty=float(p.position),
                    avgCost=getattr(p, "avgCost", None),
                )
            except Exception as e:
                self.log("POS_ERR", where=where, err=str(e))

    def _print_open_orders_snapshot(self, where: str):
        working_status = {"Submitted", "PreSubmitted", "ApiPending"}
        try:
            trades = list(self.ib.trades())
        except Exception as e:
            self.log("ORD_SNAPSHOT_ERR", where=where, err=str(e))
            return

        working = []
        for t in trades:
            try:
                st = getattr(getattr(t, "orderStatus", None), "status", None)
                if st in working_status:
                    working.append(t)
            except Exception:
                continue

        self.log("ORD_SNAPSHOT", where=where, working=len(working))

        for t in working:
            try:
                o = getattr(t, "order", None)
                c = getattr(t, "contract", None)
                self.log(
                    "ORD",
                    where=where,
                    orderId=getattr(o, "orderId", None),
                    status=getattr(getattr(t, "orderStatus", None), "status", None),
                    action=getattr(o, "action", None),
                    orderType=getattr(o, "orderType", None),
                    qty=getattr(o, "totalQuantity", None),
                    conid=getattr(c, "conId", None),
                    symbol=getattr(c, "symbol", None),
                )
            except Exception as e:
                self.log("ORD_ERR", where=where, err=str(e))

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
            self.log("ORDER_SELL_MKT_SKIP_ALLOW_FALSE", conid=getattr(contract, "conId", None), qty=qty)
            return None
        qty = int(qty)
        self.log("ORDER_SELL_MKT_PLACING", conid=getattr(contract, "conId", None), qty=qty)
        t = self.ib.placeOrder(contract, MarketOrder("SELL", qty))
        self._track_trade(t)
        self.ib.sleep(0.2)
        try:
            self.log("ORDER_SELL_MKT_PLACED", conid=getattr(contract, "conId", None), qty=qty, orderId=t.order.orderId)
        except Exception:
            self.log("ORDER_SELL_MKT_PLACED", conid=getattr(contract, "conId", None), qty=qty)
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
            self.log("ORDER_TRAIL_SKIP_ALLOW_FALSE", conid=getattr(contract, "conId", None), qty=qty)
            return None
        qty = int(qty)
        self.log(
            "ORDER_TRAIL_PLACING",
            conid=getattr(contract, "conId", None),
            qty=qty,
            trail_pct=float(self.risk.trail_pct * 100.0),
            tif=self.risk.trail_tif,
        )
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
        try:
            self.log("ORDER_TRAIL_PLACED", conid=getattr(contract, "conId", None), qty=qty, orderId=t.order.orderId)
        except Exception:
            self.log("ORDER_TRAIL_PLACED", conid=getattr(contract, "conId", None), qty=qty)
        return t

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
            self.log("LIQUIDATE_SKIP_EXITS_DISABLED")
            return

        self.log("LIQUIDATE_START")
        self._print_positions_snapshot(where="liquidate_start")

        sold = 0
        for p in self.get_positions():
            try:
                qty = int(p.position)
                if qty > 0:
                    conid = getattr(p.contract, "conId", None)
                    self.log("LIQUIDATE_SELL_MKT_PLACING", conid=conid, qty=qty)
                    self.place_market_sell(p.contract, qty, allow=True)
                    sold += 1
            except Exception as e:
                self.log("LIQUIDATE_ERR", err=str(e))
                continue

        self.log("LIQUIDATE_END", positions_sold=sold)
        self._print_positions_snapshot(where="liquidate_end")
        self._print_open_orders_snapshot(where="liquidate_end")

    def enforce_daily_loss_killswitch(self, max_day_risk: float, allow_exits: bool) -> bool:
        """
        If today's PnL <= -max_day_risk:
          - closes all stock positions (if exits allowed)
          - returns False (disallow new entries)
        Otherwise returns True.
        """
        daily_pnl = self.get_daily_pnl()
        self.log("DAILY_PNL", daily_pnl=daily_pnl, max_day_risk=round(float(max_day_risk), 2))

        if daily_pnl is None:
            return True  # can't measure -> don't kill

        if daily_pnl <= -float(max_day_risk):
            self.log(
                "DAILY_LOSS_BREACH",
                daily_pnl=round(float(daily_pnl), 2),
                max_day_risk=round(float(max_day_risk), 2),
                action="LIQUIDATE",
            )
            self.close_all_stock_positions(allow=allow_exits)
            self.log("DAILY_LOSS_KILL_DONE")
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

    def _has_working_trailing_sell(self, conid: int) -> bool:
        """
        FIX #2: Only count WORKING trailing orders, not filled/cancelled history.
        """
        working = {"Submitted", "PreSubmitted", "ApiPending"}
        for t in self.ib.trades():
            try:
                if (
                    getattr(getattr(t, "orderStatus", None), "status", None) in working
                    and getattr(getattr(t, "order", None), "orderType", None) == "TRAIL"
                    and getattr(getattr(t, "order", None), "action", None) == "SELL"
                    and int(getattr(getattr(t, "contract", None), "conId", -1)) == int(conid)
                ):
                    return True
            except Exception:
                continue
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
            self.log("ORDER_BUY_SKIP_ALLOW_FALSE", conid=getattr(contract, "conId", None), qty=qty)
            return None

        qty = int(qty)
        is_rth = self._is_rth_now()

        if is_rth:
            self.log("ORDER_BUY_MKT_PLACING", conid=getattr(contract, "conId", None), qty=qty, rth=True)
            o = MarketOrder("BUY", qty)
        else:
            px = self.get_mark_price_snapshot(contract)
            if px is None or px <= 0:
                self.log("ORDER_BUY_LMT_SKIP_NO_PRICE", conid=getattr(contract, "conId", None), rth=False)
                print("[EQUITY EXEC][ENTRY] skip: no price for outside-RTH limit", flush=True)
                return None
            limit_px = round(px * 1.002, 2)  # +0.2%
            self.log(
                "ORDER_BUY_LMT_PLACING",
                conid=getattr(contract, "conId", None),
                qty=qty,
                rth=False,
                mark=round(float(px), 4),
                limit_px=limit_px,
            )
            o = LimitOrder("BUY", qty, limit_px)
            o.outsideRth = True

        t = self.ib.placeOrder(contract, o)
        self._track_trade(t)
        self.ib.sleep(0.2)
        try:
            self.log("ORDER_BUY_PLACED", conid=getattr(contract, "conId", None), qty=qty, orderId=t.order.orderId)
        except Exception:
            self.log("ORDER_BUY_PLACED", conid=getattr(contract, "conId", None), qty=qty)
        return t

    def place_sell_scaleout_1(self, contract: Contract, allow: bool):
        """
        RTH: Market SELL 1
        Outside RTH: Limit SELL 1 (outsideRth=True) at mark
        """
        if not allow:
            self.log("ORDER_TP1_SKIP_ALLOW_FALSE", conid=getattr(contract, "conId", None))
            return None

        is_rth = self._is_rth_now()

        if is_rth:
            self.log("ORDER_TP1_MKT_PLACING", conid=getattr(contract, "conId", None), rth=True)
            o = MarketOrder("SELL", 1)
        else:
            px = self.get_mark_price_snapshot(contract)
            if px is None or px <= 0:
                self.log("ORDER_TP1_LMT_SKIP_NO_PRICE", conid=getattr(contract, "conId", None), rth=False)
                print("[EQUITY EXEC][SCALEOUT] skip: no price for outside-RTH limit", flush=True)
                return None
            limit_px = round(px * 0.998, 2)  # -0.2% to help fill
            self.log("ORDER_TP1_LMT_PLACING", conid=getattr(contract, "conId", None), rth=False, mark=round(float(px), 4), limit_px=limit_px)
            o = LimitOrder("SELL", 1, limit_px)
            o.outsideRth = True

        t = self.ib.placeOrder(contract, o)
        self._track_trade(t)
        self.ib.sleep(0.2)
        try:
            self.log("ORDER_TP1_PLACED", conid=getattr(contract, "conId", None), orderId=t.order.orderId)
        except Exception:
            self.log("ORDER_TP1_PLACED", conid=getattr(contract, "conId", None))
        return t

    def place_breakeven_stop(self, contract: Contract, qty: int, stop_price: float, allow: bool):
        """
        Place STOP SELL at entry price (breakeven).
        """
        if not allow:
            self.log("ORDER_BE_STOP_SKIP_ALLOW_FALSE", conid=getattr(contract, "conId", None), qty=qty)
            return None

        qty = int(qty)
        stop_price = float(stop_price)

        self.log("ORDER_BE_STOP_PLACING", conid=getattr(contract, "conId", None), qty=qty, stop_price=round(stop_price, 4))

        o = StopOrder("SELL", qty, stop_price)
        o.outsideRth = True

        t = self.ib.placeOrder(contract, o)
        self._track_trade(t)
        self.ib.sleep(0.2)
        try:
            self.log("ORDER_BE_STOP_PLACED", conid=getattr(contract, "conId", None), qty=qty, orderId=t.order.orderId)
        except Exception:
            self.log("ORDER_BE_STOP_PLACED", conid=getattr(contract, "conId", None), qty=qty)
        return t

    # -------------------------
    # Main loop
    # -------------------------
    def run(self, symbol: str):
        # NOTE: we DO NOT return for closed market anymore.
        # We allow pre/after orders (stocks support it), using limit orders outside RTH.

        self.log("RUN_START", symbol=symbol, client_id=self.client_id, port=PORT)

        self.connect()
        self.log("CONNECTED", symbol=symbol, isConnected=self.ib.isConnected())

        allow_orders = bool(self.execute_trades_default)
        allow_exits = bool(allow_orders or self.allow_exits_when_killed)
        allow_entries = bool(allow_orders)

        self.log(
            "GATE_ALLOW_FLAGS",
            symbol=symbol,
            allow_orders=allow_orders,
            allow_entries=allow_entries,
            allow_exits=allow_exits,
            rth_now=self._is_rth_now(),
        )

        # ✅ only do full snapshots once per cycle (not per symbol)
        if not self._pm_logged_this_cycle:
            self._print_positions_snapshot(where="run_start")
            self._print_open_orders_snapshot(where="run_start")

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

        self.log(
            "BUDGETS",
            symbol=symbol,
            buying_power=buying_power,
            per_trade_risk_pct=self.risk.per_trade_risk_pct,
            per_day_risk_pct=self.risk.per_day_risk_pct,
            max_trade_risk=round(float(max_trade_risk), 2),
            max_day_risk=round(float(max_day_risk), 2),
        )

        # FIX #1: Daily loss kill switch disables NEW ENTRIES only (exits stay allowed via allow_exits)
        self.log("DAILY_LOSS_CHECK", symbol=symbol, max_day_risk=round(float(max_day_risk), 2), allow_exits=allow_exits)
        allow_entries_before = bool(allow_entries)
        allow_entries = bool(allow_entries) and self.enforce_daily_loss_killswitch(
            max_day_risk=max_day_risk,
            allow_exits=allow_exits
        )
        self.log("DAILY_LOSS_RESULT", symbol=symbol, allow_entries_before=allow_entries_before, allow_entries_after=allow_entries)

        # -------------------------
        # Entry gates (ENTRY orders only)
        # -------------------------
        open_trades = [
            t for t in self.ib.trades()
            if getattr(getattr(t, "orderStatus", None), "status", None) in {"Submitted", "PreSubmitted", "ApiPending"}
        ]

        open_entry_trades = [t for t in open_trades if self._is_entry_order_trade(t)]
        self.log(
            "GATE_OPEN_ENTRY_TRADES",
            symbol=symbol,
            open_entry_trades=len(open_entry_trades),
            max_open_orders=self.risk.max_open_orders,
        )

        if len(open_entry_trades) >= self.risk.max_open_orders:
            allow_entries = False
            self.log("GATE_MAX_OPEN_ORDERS_HIT", symbol=symbol, open_entry_trades=len(open_entry_trades), max_open_orders=self.risk.max_open_orders)

        now_utc = datetime.now(timezone.utc)
        min_age_triggered = False
        for t in open_entry_trades:
            try:
                oid = t.order.orderId
            except Exception:
                continue
            ts = self.order_submit_time.get(oid)
            if ts and (now_utc - ts).total_seconds() < self.risk.min_order_age_seconds:
                allow_entries = False
                min_age_triggered = True
                age_s = int((now_utc - ts).total_seconds())
                self.log("GATE_MIN_ORDER_AGE_HIT", symbol=symbol, orderId=oid, age_s=age_s, min_age_s=self.risk.min_order_age_seconds)
                break

        self.log("GATE_MIN_ORDER_AGE", symbol=symbol, triggered=min_age_triggered, min_age_s=self.risk.min_order_age_seconds)

        # -------------------------
        # Load signal
        # -------------------------
        df = self.load_latest_signal(symbol)
        if df.empty:
            self.log("SIGNAL_NONE", symbol=symbol)
            # still do management even if no new signal
        else:
            # log signal row basic info
            try:
                row0 = df.iloc[0]
                ts0 = row0.get("timestamp", None) if hasattr(row0, "get") else None
                con0 = row0.get("con_id", None) if hasattr(row0, "get") else None
            except Exception:
                ts0 = None
                con0 = None

            self.log("SIGNAL_FOUND", symbol=symbol, row_ts=ts0, con_id=con0, allow_entries=allow_entries)

            row = df.iloc[0]
            conid = self._safe_int(row["con_id"])
            if conid is None:
                self.log("SIGNAL_SKIP_BAD_CONID", symbol=symbol, raw_con_id=row.get("con_id", None))
                print(f"[EQUITY EXEC][SIGNAL] skip {symbol}: bad con_id", flush=True)
                return

            if self._already_long_conid(conid):
                self.log("ENTRY_SKIP_ALREADY_LONG", symbol=symbol, conid=conid)
            elif not allow_entries:
                self.log("ENTRY_SKIP_ALLOW_ENTRIES_FALSE", symbol=symbol, conid=conid)
            else:
                contract = self.stock_contract_from_conid(conid)

                # PREFLIGHT risk/cost check (still uses price)
                qty = int(self.risk.entry_qty)
                entry_price = self.get_mark_price_snapshot(contract)
                if entry_price is None or entry_price <= 0:
                    self.log("PREFLIGHT_SKIP_NO_PRICE", symbol=symbol, conid=conid)
                    print(f"[EQUITY EXEC][PREFLIGHT] skip {symbol}: no price", flush=True)
                    return

                stop_price_for_math = float(entry_price) * (1.0 - float(self.risk.preflight_stop_pct))
                risk_per_share = float(entry_price) - float(stop_price_for_math)
                dollar_risk = float(risk_per_share) * float(qty)

                self.log(
                    "PREFLIGHT",
                    symbol=symbol,
                    conid=conid,
                    qty=qty,
                    entry_price=round(float(entry_price), 4),
                    preflight_stop_pct=self.risk.preflight_stop_pct,
                    dollar_risk=round(float(dollar_risk), 2),
                    max_trade_risk=round(float(max_trade_risk), 2),
                )

                if dollar_risk > float(max_trade_risk):
                    self.log("PREFLIGHT_SKIP_RISK_TOO_HIGH", symbol=symbol, conid=conid, dollar_risk=round(float(dollar_risk), 2), max_trade_risk=round(float(max_trade_risk), 2))
                    print(
                        f"[EQUITY EXEC][PREFLIGHT] skip {symbol}: "
                        f"dollar_risk={dollar_risk:.2f} > max_trade_risk={max_trade_risk:.2f}",
                        flush=True
                    )
                    return

                # ENTRY (pre/after allowed)
                tr = self.place_buy_entry(contract, qty=qty, allow=allow_entries)
                if tr is not None:
                    try:
                        oid = tr.order.orderId
                    except Exception:
                        oid = None
                    self.log("ORDER_BUY_DONE", symbol=symbol, conid=conid, qty=qty, orderId=oid)

                    # ✅ only snapshot after entry once per cycle (not per symbol)
                    if not self._pm_logged_this_cycle:
                        self._print_positions_snapshot(where="after_buy")
                        self._print_open_orders_snapshot(where="after_buy")

                    # trailing stop safety net (place only if no WORKING trail exists)
                    if not self._has_working_trailing_sell(conid):
                        self.place_trailing_stop(contract, qty, allow_exits)
                    else:
                        self.log("ORDER_TRAIL_SKIP_EXISTS", symbol=symbol, conid=conid)

                    # ✅ only snapshot after trail once per cycle (not per symbol)
                    if not self._pm_logged_this_cycle:
                        self._print_positions_snapshot(where="after_trail")
                        self._print_open_orders_snapshot(where="after_trail")

        # -------------------------
        # Position management (applies to ALL open stock positions)
        # Rules:
        #  +0.5% -> stop at entry
        #  +1.0% -> sell 1 of 2
        # -------------------------
        do_log_pm = not self._pm_logged_this_cycle

        for p in self.get_positions():
            try:
                qty = int(p.position)
                if qty <= 0:
                    continue

                conid = int(p.contract.conId)
                pos_sym = getattr(p.contract, "symbol", None)

                entry = self._entry_from_position(p)
                if entry is None or entry <= 0:
                    if do_log_pm:
                        self.log("PM_SKIP_NO_ENTRY", symbol=pos_sym, conid=conid, qty=qty)
                    continue

                mark = self.get_mark_price_snapshot(p.contract)
                if mark is None or mark <= 0:
                    if do_log_pm:
                        self.log("PM_SKIP_NO_MARK", symbol=pos_sym, conid=conid, qty=qty)
                    continue

                ret_pct = (float(mark) - float(entry)) / float(entry) * 100.0

                if do_log_pm:
                    self.log(
                        "PM_STATE",
                        symbol=pos_sym,
                        conid=conid,
                        qty=qty,
                        entry=round(float(entry), 4),
                        mark=round(float(mark), 4),
                        ret_pct=round(float(ret_pct), 3),
                    )

                # +0.5% -> breakeven stop at entry (only once)
                if ret_pct >= 0.5 and not self._has_working_breakeven_stop(conid):
                    if do_log_pm:
                        self.log("PM_BE_TRIGGER", symbol=pos_sym, conid=conid, qty=qty, stop_price=round(float(entry), 4), ret_pct=round(float(ret_pct), 3))
                    self.place_breakeven_stop(
                        contract=p.contract,
                        qty=qty,
                        stop_price=float(entry),
                        allow=allow_exits,
                    )
                    if do_log_pm:
                        self._print_open_orders_snapshot(where="after_be_stop")

                # +1.0% -> sell 1 (only if you have >=2, only once)
                if ret_pct >= 1.0 and qty >= 2 and not self._has_working_scaleout_sell(conid):
                    if do_log_pm:
                        self.log("PM_TP1_TRIGGER", symbol=pos_sym, conid=conid, ret_pct=round(float(ret_pct), 3))
                    self.place_sell_scaleout_1(
                        contract=p.contract,
                        allow=allow_exits,
                    )
                    if do_log_pm:
                        self._print_positions_snapshot(where="after_tp1")
                        self._print_open_orders_snapshot(where="after_tp1")

                # Safety: ensure trailing exists (WORKING only)
                if not self._has_working_trailing_sell(conid):
                    if do_log_pm:
                        self.log("PM_TRAIL_ENSURE_TRIGGER", symbol=pos_sym, conid=conid, qty=qty)
                    self.place_trailing_stop(p.contract, qty, allow_exits)
                    if do_log_pm:
                        self._print_open_orders_snapshot(where="after_trail_ensure")
                else:
                    if do_log_pm:
                        self.log("PM_TRAIL_EXISTS", symbol=pos_sym, conid=conid)

            except Exception as e:
                if do_log_pm:
                    self.log("PM_ERR", symbol=symbol, err=str(e))
                continue

        # ✅ flip once after first symbol's management pass
        if not self._pm_logged_this_cycle:
            self._pm_logged_this_cycle = True

        self.log("RUN_END", symbol=symbol)


def main_execution(client_id: int, symbols):
    eng = IBKREquityExecutionEngine(client_id)
    try:
        # ✅ reset once per execution cycle so PM logs happen only once per run
        eng._pm_logged_this_cycle = False

        for i, sym in enumerate(symbols, start=1):
            print(f"[EQUITY EXEC] ({i}/{len(symbols)}) {sym}")
            eng.run(sym)
            time.sleep(0.15)
    finally:
        eng.disconnect()
