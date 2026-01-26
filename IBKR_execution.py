from __future__ import annotations

import os
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

        # ✅ suppress PM logs: allow PM logs only once per main_execution cycle
        self._pm_logged_this_cycle: bool = False

        # =========================
        # Logging controls
        # =========================
        # LOG_LEVEL=INFO (default) or DEBUG
        self.log_level: str = str(os.getenv("LOG_LEVEL", "INFO")).strip().upper()
        if self.log_level not in {"INFO", "DEBUG"}:
            self.log_level = "INFO"

        # per-run counters
        self._stats: dict[str, int] = {}
        self._run_symbol: str | None = None

    # =========================
    # Logging helpers
    # =========================
    def _ts(self) -> str:
        try:
            return datetime.now(self.NY_TZ).strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            return datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    def _fmt_fields(self, fields: dict) -> str:
        if not fields:
            return ""
        parts = []
        for k, v in fields.items():
            try:
                parts.append(f"{k}={v}")
            except Exception:
                parts.append(f"{k}=?")
        return " " + " ".join(parts)

    def log_info(self, event: str, **fields):
        print(f"[EQUITY EXEC][{event}] {self._ts()}{self._fmt_fields(fields)}")

    def log_debug(self, event: str, **fields):
        if self.log_level != "DEBUG":
            return
        print(f"[EQUITY EXEC][{event}] {self._ts()}{self._fmt_fields(fields)}")

    def log_error(self, event: str, **fields):
        self._inc("errs")
        print(f"[EQUITY EXEC][{event}] {self._ts()}{self._fmt_fields(fields)}")

    # ✅ NEW: one-line confirmation for the specific trade you just placed (no dumping everything)
    def _log_trade_one_liner(self, label: str, trade, symbol: str | None = None):
        try:
            if trade is None:
                return
            o = getattr(trade, "order", None)
            c = getattr(trade, "contract", None)
            st = getattr(getattr(trade, "orderStatus", None), "status", None)
            self.log_info(
                label,
                symbol=symbol,
                orderId=getattr(o, "orderId", None),
                status=st,
                action=getattr(o, "action", None),
                orderType=getattr(o, "orderType", None),
                qty=getattr(o, "totalQuantity", None),
                conid=getattr(c, "conId", None),
            )
        except Exception:
            pass

    # =========================
    # Per-run stats helpers
    # =========================
    def _reset_stats(self, symbol: str):
        self._run_symbol = symbol
        self._stats = {
            "signal": 0,
            "entry": 0,
            "be": 0,
            "tp1": 0,
            "trail_ensure": 0,
            "skips_no_price": 0,
            "errs": 0,
        }

    def _inc(self, k: str, n: int = 1):
        try:
            self._stats[k] = int(self._stats.get(k, 0)) + int(n)
        except Exception:
            pass

    def _print_run_summary(self):
        sym = self._run_symbol or "?"
        s = self._stats or {}
        print(
            f"[EQUITY EXEC][SUMMARY] SYM={sym} "
            f"signal={s.get('signal', 0)} "
            f"entry={s.get('entry', 0)} "
            f"be={s.get('be', 0)} "
            f"tp1={s.get('tp1', 0)} "
            f"trail_ensure={s.get('trail_ensure', 0)} "
            f"skips_no_price={s.get('skips_no_price', 0)} "
            f"errs={s.get('errs', 0)}"
        )

    # =========================
    # Snapshots (DEBUG by default)
    # =========================
    def _print_positions_snapshot(self, where: str):
        try:
            pos = list(self.ib.positions())
        except Exception as e:
            self.log_error("POS_SNAPSHOT_ERR", where=where, err=str(e))
            return

        stk = []
        for p in pos:
            try:
                if p.contract and p.contract.secType == "STK" and float(p.position) != 0:
                    stk.append(p)
            except Exception:
                continue

        self.log_debug("POS_SNAPSHOT", where=where, stk_positions=len(stk))

        for p in stk:
            try:
                self.log_debug(
                    "POS",
                    where=where,
                    symbol=getattr(p.contract, "symbol", None),
                    conid=getattr(p.contract, "conId", None),
                    qty=float(p.position),
                    avgCost=getattr(p, "avgCost", None),
                )
            except Exception as e:
                self.log_error("POS_ERR", where=where, err=str(e))

    def _print_open_orders_snapshot(self, where: str):
        working_status = {"Submitted", "PreSubmitted", "ApiPending"}
        try:
            trades = list(self.ib.trades())
        except Exception as e:
            self.log_error("ORD_SNAPSHOT_ERR", where=where, err=str(e))
            return

        working = []
        for t in trades:
            try:
                st = getattr(getattr(t, "orderStatus", None), "status", None)
                if st in working_status:
                    working.append(t)
            except Exception:
                continue

        self.log_debug("ORD_SNAPSHOT", where=where, working=len(working))

        for t in working:
            try:
                o = getattr(t, "order", None)
                c = getattr(t, "contract", None)
                self.log_debug(
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
                self.log_error("ORD_ERR", where=where, err=str(e))

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

    def get_stock_con_id(self, symbol: str) -> int | None:
        """
        Resolve conId ONLY when a signal is TRUE.
        Uses ib_insync qualifyContracts (no extra IBAPI threads).
        """
        try:
            self.connect()
            c = Contract(symbol=symbol.upper().strip(), secType="STK", exchange="SMART", currency="USD")
            qualified = self.ib.qualifyContracts(c)
            if not qualified:
                return None
            conid = int(getattr(qualified[0], "conId", 0) or 0)
            return conid if conid > 0 else None
        except Exception as e:
            self.log_error("CONID_RESOLVE_FAIL", symbol=symbol, err=str(e))
            return None

    def place_market_sell(self, contract: Contract, qty: int, allow: bool):
        if not allow:
            self.log_info("ORDER_SELL_MKT_SKIP_ALLOW_FALSE", conid=getattr(contract, "conId", None), qty=qty)
            return None
        qty = int(qty)
        self.log_info("ORDER_SELL_MKT_PLACING", conid=getattr(contract, "conId", None), qty=qty)
        t = self.ib.placeOrder(contract, MarketOrder("SELL", qty))
        self._track_trade(t)
        self.ib.sleep(0.2)
        self._log_trade_one_liner("ORDER_SELL_MKT_STATUS", t)
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
            self.log_info("ORDER_TRAIL_SKIP_ALLOW_FALSE", conid=getattr(contract, "conId", None), qty=qty)
            return None
        qty = int(qty)
        self.log_info(
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
        self._log_trade_one_liner("ORDER_TRAIL_STATUS", t)
        return t

    # -------------------------
    # DB (SAFE)
    # -------------------------
    def load_latest_signal(self, symbol: str) -> int | None:
        """
        Load the latest row for symbol.
        If trade_signal is TRUE, resolve + return conId.
        Else return None.
        """
        try:
            # ✅ NEW: context manager so close is guaranteed
            with duckdb.connect(DB_PATH, read_only=True) as con:
                df = con.execute(
                    """
                    SELECT trade_signal
                    FROM stock_execution_signals_5m
                    WHERE symbol = ?
                    ORDER BY timestamp DESC
                    LIMIT 1
                    """,
                    [symbol],
                ).df()

            if df.empty:
                return None

            trade_signal = df.iloc[0]["trade_signal"]

            # ✅ NEW: treat numpy.bool_ / 1/0 cleanly; ignore None/NaN safely
            try:
                import pandas as pd
                if pd.isna(trade_signal):
                    return None
                trade_signal_bool = bool(trade_signal)
            except Exception:
                trade_signal_bool = (trade_signal is True)

            if trade_signal_bool:
                return self.get_stock_con_id(symbol)

            return None

        except Exception as e:
            self.log_error("DB_LOAD_LATEST_SIGNAL_FAIL", symbol=symbol, err=str(e))
            return None

    def get_daily_pnl(self) -> float | None:
        try:
            acct = self.ib.managedAccounts()
            if not acct:
                return None
            account = acct[0]

            if self._pnl_sub is None:
                self._pnl_sub = self.ib.reqPnL(account, "")
                self.ib.sleep(0.25)
            else:
                self.ib.sleep(0.05)

            daily = getattr(self._pnl_sub, "dailyPnL", None)
            if daily is None:
                return None
            return float(daily)
        except Exception:
            return None

    def close_all_stock_positions(self, allow: bool):
        if not allow:
            self.log_info("LIQUIDATE_SKIP_EXITS_DISABLED")
            return

        self.log_info("LIQUIDATE_START")
        self._print_positions_snapshot(where="liquidate_start")

        sold = 0
        for p in self.get_positions():
            try:
                qty = int(p.position)
                if qty > 0:
                    conid = getattr(p.contract, "conId", None)
                    self.log_info("LIQUIDATE_SELL_MKT_PLACING", conid=conid, qty=qty)
                    self.place_market_sell(p.contract, qty, allow=True)
                    sold += 1
            except Exception as e:
                self.log_error("LIQUIDATE_ERR", err=str(e))
                continue

        self.log_info("LIQUIDATE_END", positions_sold=sold)
        self._print_positions_snapshot(where="liquidate_end")
        self._print_open_orders_snapshot(where="liquidate_end")

    def enforce_daily_loss_killswitch(self, max_day_risk: float, allow_exits: bool) -> bool:
        daily_pnl = self.get_daily_pnl()
        self.log_info("DAILY_PNL", daily_pnl=daily_pnl, max_day_risk=round(float(max_day_risk), 2))

        if daily_pnl is None:
            return True

        if daily_pnl <= -float(max_day_risk):
            self.log_info(
                "DAILY_LOSS_BREACH",
                daily_pnl=round(float(daily_pnl), 2),
                max_day_risk=round(float(max_day_risk), 2),
                action="LIQUIDATE",
            )
            self.close_all_stock_positions(allow=allow_exits)
            self.log_info("DAILY_LOSS_KILL_DONE")
            return False

        return True

    # -------------------------
    # Order classification
    # -------------------------
    def _is_entry_order_trade(self, t) -> bool:
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
        for t in self._open_trades_for_conid(conid):
            try:
                o = t.order
                if o and o.action == "SELL" and o.orderType in {"STP", "STOP"}:
                    return True
            except Exception:
                pass
        return False

    def _has_working_scaleout_sell(self, conid: int) -> bool:
        for t in self._open_trades_for_conid(conid):
            try:
                o = t.order
                if o and o.action == "SELL" and o.orderType in {"MKT", "LMT"}:
                    return True
            except Exception:
                pass
        return False

    def _has_working_trailing_sell(self, conid: int) -> bool:
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
        if not allow:
            self.log_info("ORDER_BUY_SKIP_ALLOW_FALSE", conid=getattr(contract, "conId", None), qty=qty)
            return None

        qty = int(qty)
        is_rth = self._is_rth_now()

        if is_rth:
            self.log_info("ORDER_BUY_MKT_PLACING", conid=getattr(contract, "conId", None), qty=qty, rth=True)
            o = MarketOrder("BUY", qty)
        else:
            px = self.get_mark_price_snapshot(contract)
            if px is None or px <= 0:
                self._inc("skips_no_price")
                self.log_info("ORDER_BUY_LMT_SKIP_NO_PRICE", conid=getattr(contract, "conId", None), rth=False)
                return None
            limit_px = round(px * 1.002, 2)  # +0.2%
            self.log_info(
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
        self._log_trade_one_liner("ORDER_BUY_STATUS", t)
        return t

    def place_sell_scaleout_1(self, contract: Contract, allow: bool):
        if not allow:
            self.log_info("ORDER_TP1_SKIP_ALLOW_FALSE", conid=getattr(contract, "conId", None))
            return None

        is_rth = self._is_rth_now()

        if is_rth:
            self.log_info("ORDER_TP1_MKT_PLACING", conid=getattr(contract, "conId", None), rth=True)
            o = MarketOrder("SELL", 1)
        else:
            px = self.get_mark_price_snapshot(contract)
            if px is None or px <= 0:
                self._inc("skips_no_price")
                self.log_info("ORDER_TP1_LMT_SKIP_NO_PRICE", conid=getattr(contract, "conId", None), rth=False)
                return None
            limit_px = round(px * 0.998, 2)  # -0.2% to help fill
            self.log_info(
                "ORDER_TP1_LMT_PLACING",
                conid=getattr(contract, "conId", None),
                rth=False,
                mark=round(float(px), 4),
                limit_px=limit_px,
            )
            o = LimitOrder("SELL", 1, limit_px)
            o.outsideRth = True

        t = self.ib.placeOrder(contract, o)
        self._track_trade(t)
        self.ib.sleep(0.2)
        self._log_trade_one_liner("ORDER_TP1_STATUS", t)
        return t

    def place_breakeven_stop(self, contract: Contract, qty: int, stop_price: float, allow: bool):
        if not allow:
            self.log_info("ORDER_BE_STOP_SKIP_ALLOW_FALSE", conid=getattr(contract, "conId", None), qty=qty)
            return None

        qty = int(qty)
        stop_price = float(stop_price)

        self.log_info(
            "ORDER_BE_STOP_PLACING",
            conid=getattr(contract, "conId", None),
            qty=qty,
            stop_price=round(stop_price, 4),
        )

        o = StopOrder("SELL", qty, stop_price)
        o.outsideRth = True

        t = self.ib.placeOrder(contract, o)
        self._track_trade(t)
        self.ib.sleep(0.2)
        self._log_trade_one_liner("ORDER_BE_STOP_STATUS", t)
        return t

    # -------------------------
    # Main loop
    # -------------------------
    def run(self, symbol: str):
        self._reset_stats(symbol)

        try:
            self.log_info("RUN_START", symbol=symbol, client_id=self.client_id, port=PORT, log_level=self.log_level)

            self.connect()
            self.log_info("CONNECTED", symbol=symbol, isConnected=self.ib.isConnected())

            allow_orders = bool(self.execute_trades_default)
            allow_exits = bool(allow_orders or self.allow_exits_when_killed)
            allow_entries = bool(allow_orders)

            # ✅ NEW: simple test-mode override (minimum BS)
            #   ALLOW_ENTRIES=0  -> never enter, but still reads signals + manages exits
            env_allow_entries = str(os.getenv("ALLOW_ENTRIES", "")).strip()
            if env_allow_entries in {"0", "false", "False", "no", "NO"}:
                allow_entries = False

            self.log_debug(
                "GATE_ALLOW_FLAGS",
                symbol=symbol,
                allow_orders=allow_orders,
                allow_entries=allow_entries,
                allow_exits=allow_exits,
                rth_now=self._is_rth_now(),
            )

            if not self._pm_logged_this_cycle and self.log_level == "DEBUG":
                self._print_positions_snapshot(where="run_start")
                self._print_open_orders_snapshot(where="run_start")

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

            self.log_info(
                "BUDGETS",
                symbol=symbol,
                buying_power=buying_power,
                per_trade_risk_pct=self.risk.per_trade_risk_pct,
                per_day_risk_pct=self.risk.per_day_risk_pct,
                max_trade_risk=round(float(max_trade_risk), 2),
                max_day_risk=round(float(max_day_risk), 2),
            )

            self.log_info("DAILY_LOSS_CHECK", symbol=symbol, max_day_risk=round(float(max_day_risk), 2), allow_exits=allow_exits)
            allow_entries_before = bool(allow_entries)
            allow_entries = bool(allow_entries) and self.enforce_daily_loss_killswitch(
                max_day_risk=max_day_risk,
                allow_exits=allow_exits
            )
            self.log_info("DAILY_LOSS_RESULT", symbol=symbol, allow_entries_before=allow_entries_before, allow_entries_after=allow_entries)

            open_trades = [
                t for t in self.ib.trades()
                if getattr(getattr(t, "orderStatus", None), "status", None) in {"Submitted", "PreSubmitted", "ApiPending"}
            ]
            open_entry_trades = [t for t in open_trades if self._is_entry_order_trade(t)]

            self.log_debug(
                "GATE_OPEN_ENTRY_TRADES",
                symbol=symbol,
                open_entry_trades=len(open_entry_trades),
                max_open_orders=self.risk.max_open_orders,
            )

            if len(open_entry_trades) >= self.risk.max_open_orders:
                allow_entries = False
                self.log_info("GATE_MAX_OPEN_ORDERS_HIT", symbol=symbol, open_entry_trades=len(open_entry_trades), max_open_orders=self.risk.max_open_orders)

            now_utc = datetime.now(timezone.utc)
            for t in open_entry_trades:
                try:
                    oid = t.order.orderId
                except Exception:
                    continue
                ts = self.order_submit_time.get(oid)
                if ts and (now_utc - ts).total_seconds() < self.risk.min_order_age_seconds:
                    allow_entries = False
                    age_s = int((now_utc - ts).total_seconds())
                    self.log_info("GATE_MIN_ORDER_AGE_HIT", symbol=symbol, orderId=oid, age_s=age_s, min_age_s=self.risk.min_order_age_seconds)
                    break

            # -------------------------
            # Load signal -> conId (only resolves conId if latest signal is TRUE)
            # -------------------------
            conid = self.load_latest_signal(symbol)
            if conid is None:
                self.log_info("NO_SIGNAL", symbol=symbol)
                return

            self._inc("signal")

            # ✅ NEW: tiny invariants (hard stops) before any entry attempt
            if not isinstance(conid, int) or conid <= 0:
                self.log_error("INV_BAD_CONID", symbol=symbol, conid=conid)
                return

            qty = int(self.risk.entry_qty)
            if qty <= 0:
                self.log_error("INV_BAD_QTY", symbol=symbol, qty=qty)
                return

            if self._already_long_conid(conid):
                self.log_info("ENTRY_SKIP_ALREADY_LONG", symbol=symbol, conid=conid)
            elif not allow_entries:
                self.log_info("ENTRY_SKIP_ALLOW_ENTRIES_FALSE", symbol=symbol, conid=conid)
            else:
                contract = self.stock_contract_from_conid(conid)

                entry_price = self.get_mark_price_snapshot(contract)
                if entry_price is None or entry_price <= 0:
                    self._inc("skips_no_price")
                    self.log_info("PREFLIGHT_SKIP_NO_PRICE", symbol=symbol, conid=conid)
                    return

                stop_price_for_math = float(entry_price) * (1.0 - float(self.risk.preflight_stop_pct))
                risk_per_share = float(entry_price) - float(stop_price_for_math)
                dollar_risk = float(risk_per_share) * float(qty)

                self.log_info(
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
                    self.log_info(
                        "PREFLIGHT_SKIP_RISK_TOO_HIGH",
                        symbol=symbol,
                        conid=conid,
                        dollar_risk=round(float(dollar_risk), 2),
                        max_trade_risk=round(float(max_trade_risk), 2),
                    )
                    return

                tr = self.place_buy_entry(contract, qty=qty, allow=allow_entries)
                if tr is not None:
                    self._inc("entry")
                    self.log_info("ORDER_BUY_DONE", symbol=symbol, conid=conid, qty=qty)

                    if not self._pm_logged_this_cycle and self.log_level == "DEBUG":
                        self._print_positions_snapshot(where="after_buy")
                        self._print_open_orders_snapshot(where="after_buy")

                    if not self._has_working_trailing_sell(conid):
                        self.place_trailing_stop(contract, qty, allow_exits)
                    else:
                        self.log_info("ORDER_TRAIL_SKIP_EXISTS", symbol=symbol, conid=conid)

                    if not self._pm_logged_this_cycle and self.log_level == "DEBUG":
                        self._print_positions_snapshot(where="after_trail")
                        self._print_open_orders_snapshot(where="after_trail")

            # -------------------------
            # Position management (applies to ALL open stock positions)
            # -------------------------
            do_log_pm = (not self._pm_logged_this_cycle) and (self.log_level == "DEBUG")

            for p in self.get_positions():
                try:
                    qty_pos = int(p.position)
                    if qty_pos <= 0:
                        continue

                    conid_pos = int(p.contract.conId)
                    pos_sym = getattr(p.contract, "symbol", None)

                    entry = self._entry_from_position(p)
                    if entry is None or entry <= 0:
                        if do_log_pm:
                            self.log_debug("PM_SKIP_NO_ENTRY", symbol=pos_sym, conid=conid_pos, qty=qty_pos)
                        continue

                    mark = self.get_mark_price_snapshot(p.contract)
                    if mark is None or mark <= 0:
                        if do_log_pm:
                            self.log_debug("PM_SKIP_NO_MARK", symbol=pos_sym, conid=conid_pos, qty=qty_pos)
                        continue

                    ret_pct = (float(mark) - float(entry)) / float(entry) * 100.0

                    if do_log_pm:
                        self.log_debug(
                            "PM_STATE",
                            symbol=pos_sym,
                            conid=conid_pos,
                            qty=qty_pos,
                            entry=round(float(entry), 4),
                            mark=round(float(mark), 4),
                            ret_pct=round(float(ret_pct), 3),
                        )

                    if ret_pct >= 0.5 and not self._has_working_breakeven_stop(conid_pos):
                        self._inc("be")
                        self.log_info("PM_BE_TRIGGER", symbol=pos_sym, conid=conid_pos, qty=qty_pos, stop_price=round(float(entry), 4), ret_pct=round(float(ret_pct), 3))
                        self.place_breakeven_stop(
                            contract=p.contract,
                            qty=qty_pos,
                            stop_price=float(entry),
                            allow=allow_exits,
                        )
                        if do_log_pm:
                            self._print_open_orders_snapshot(where="after_be_stop")

                    if ret_pct >= 1.0 and qty_pos >= 2 and not self._has_working_scaleout_sell(conid_pos):
                        self._inc("tp1")
                        self.log_info("PM_TP1_TRIGGER", symbol=pos_sym, conid=conid_pos, ret_pct=round(float(ret_pct), 3))
                        self.place_sell_scaleout_1(
                            contract=p.contract,
                            allow=allow_exits,
                        )
                        if do_log_pm:
                            self._print_positions_snapshot(where="after_tp1")
                            self._print_open_orders_snapshot(where="after_tp1")

                    if not self._has_working_trailing_sell(conid_pos):
                        self._inc("trail_ensure")
                        self.log_info("PM_TRAIL_ENSURE_TRIGGER", symbol=pos_sym, conid=conid_pos, qty=qty_pos)
                        self.place_trailing_stop(p.contract, qty_pos, allow_exits)
                        if do_log_pm:
                            self._print_open_orders_snapshot(where="after_trail_ensure")
                    else:
                        if do_log_pm:
                            self.log_debug("PM_TRAIL_EXISTS", symbol=pos_sym, conid=conid_pos)

                except Exception as e:
                    self.log_error("PM_ERR", symbol=symbol, err=str(e))
                    continue

            self._pm_logged_this_cycle = True

        finally:
            self._print_run_summary()


def main_execution(client_id: int, symbols):
    eng = IBKREquityExecutionEngine(client_id)
    try:
        eng._pm_logged_this_cycle = False

        for i, sym in enumerate(symbols, start=1):
            print(f"[EQUITY EXEC] ({i}/{len(symbols)}) {sym}")
            eng.run(sym)
            time.sleep(0.15)
    finally:
        eng.disconnect()
