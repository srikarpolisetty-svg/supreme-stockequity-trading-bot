# ibkr_stock_5m_ingest.py
from __future__ import annotations

import os
import threading
import time
from dataclasses import dataclass
from datetime import datetime
from zoneinfo import ZoneInfo

import duckdb
import pandas as pd
from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract

from dbfunctions import compute_z_scores_for_stock  # keep your existing function


HOST, PORT = "127.0.0.1", 4002
NY_TZ = ZoneInfo("America/New_York")

# ✅ absolute DB path (match your options style)
DB_PATH = "/home/ubuntu/supreme-stockequity-trading-bot/stocks_data.db"


# -------------------------
# Contracts
# -------------------------
def make_stock(symbol: str) -> Contract:
    c = Contract()
    c.symbol = str(symbol).upper().strip()
    c.secType = "STK"
    c.exchange = "SMART"
    c.currency = "USD"
    return c


# -------------------------
# Config
# -------------------------
@dataclass
class StockIngestConfig:
    duration_str: str = "5 D"        # match yfinance 5d
    bar_size: str = "5 mins"         # match yfinance 5m
    what_to_show: str = "TRADES"
    use_rth: int = 1                 # regular trading hours
    timeout_s: float = 8.0           # bounded wait per symbol


class StockApp(EWrapper, EClient):
    """
    IBKR state-centered stock 5m bar ingest:
      - one connection per shard
      - per symbol: qualify stock -> reqHistoricalData -> take latest bar
      - write parquet: raw/enriched/signals
      - includes stock conId (IBKR contract id) in ALL outputs
    """

    def __init__(self, cfg: StockIngestConfig = StockIngestConfig()):
        EClient.__init__(self, self)

        self.cfg = cfg
        self.connected_event = threading.Event()

        self._next_req_id = 1

        # per-symbol state
        self.symbol = ""
        self._req_errors: dict[int, tuple[int, str]] = {}
        self._pending_contract_details: dict[int, threading.Event] = {}
        self._pending_hist_end: dict[int, threading.Event] = {}

        self._stock_contract: Contract | None = None
        self._hist_rows: list[dict] = []

    # -------------------------
    # Connection lifecycle
    # -------------------------
    def nextValidId(self, orderId: int):
        # delayed-ok (matches your options app behavior)
        self.reqMarketDataType(3)
        self.connected_event.set()

    def error(self, reqId, errorCode, errorString, advancedOrderRejectJson=""):
        print(f"[STOCK] ERROR {reqId} {errorCode} {errorString}")
        self._req_errors[reqId] = (int(errorCode), str(errorString))

        ev = self._pending_contract_details.get(reqId)
        if ev:
            ev.set()

        ev = self._pending_hist_end.get(reqId)
        if ev:
            ev.set()

    # -------------------------
    # ReqId
    # -------------------------
    def _new_req_id(self) -> int:
        rid = self._next_req_id
        self._next_req_id += 1
        return rid

    # -------------------------
    # Reset per symbol
    # -------------------------
    def reset_for_symbol(self, symbol: str):
        self.symbol = str(symbol).upper().strip()
        self._stock_contract = None
        self._hist_rows = []

        self._req_errors = {}
        self._pending_contract_details = {}
        self._pending_hist_end = {}

    # -------------------------
    # Contract qualification (captures conId)
    # -------------------------
    def start_symbol(self, symbol: str):
        self.reset_for_symbol(symbol)
        rid = self._new_req_id()
        ev = threading.Event()
        self._pending_contract_details[rid] = ev
        self.reqContractDetails(rid, make_stock(symbol))

    def contractDetails(self, reqId, cd):
        con = cd.contract
        if con.secType == "STK" and con.symbol == self.symbol:
            self._stock_contract = con  # includes con.conId

        ev = self._pending_contract_details.get(reqId)
        if ev:
            ev.set()

    # -------------------------
    # Historical bars
    # -------------------------
    def request_hist_5m(self) -> int:
        if self._stock_contract is None:
            raise RuntimeError("Stock contract not qualified")

        rid = self._new_req_id()
        self._pending_hist_end[rid] = threading.Event()

        self.reqHistoricalData(
            reqId=rid,
            contract=self._stock_contract,
            endDateTime="",
            durationStr=self.cfg.duration_str,
            barSizeSetting=self.cfg.bar_size,
            whatToShow=self.cfg.what_to_show,
            useRTH=self.cfg.use_rth,
            formatDate=1,
            keepUpToDate=False,
            chartOptions=[],
        )
        return rid

    def historicalData(self, reqId, bar):
        self._hist_rows.append(
            {
                "date": str(bar.date),
                "open": float(bar.open),
                "high": float(bar.high),
                "low": float(bar.low),
                "close": float(bar.close),
                "volume": int(bar.volume) if bar.volume is not None else None,
            }
        )

    def historicalDataEnd(self, reqId: int, start: str, end: str):
        ev = self._pending_hist_end.get(reqId)
        if ev:
            ev.set()

    # -------------------------
    # One-symbol sequence
    # -------------------------
    def run_sequence(self, run_id: str, shard_id: int):
        if not self._pending_contract_details:
            print(f"[STOCK] skip {self.symbol}: no contractDetails request")
            return None

        reqId, ev = next(iter(self._pending_contract_details.items()))
        ev.wait(timeout=2.0)
        self._pending_contract_details.pop(reqId, None)

        if reqId in self._req_errors or self._stock_contract is None:
            print(f"[STOCK] skip {self.symbol}: contractDetails failed")
            return None

        stock_conid = int(getattr(self._stock_contract, "conId", 0) or 0)
        if stock_conid <= 0:
            print(f"[STOCK] skip {self.symbol}: missing stock conId")
            return None

        hist_req = self.request_hist_5m()
        self._pending_hist_end[hist_req].wait(timeout=self.cfg.timeout_s)
        self._pending_hist_end.pop(hist_req, None)

        if hist_req in self._req_errors:
            print(f"[STOCK] skip {self.symbol}: historicalData failed")
            return None

        if not self._hist_rows:
            print(f"[STOCK] skip {self.symbol}: no hist rows")
            return None

        latest = self._hist_rows[-1]

        open_ = float(latest["open"])
        high = float(latest["high"])
        low = float(latest["low"])
        close = float(latest["close"])
        volume = int(latest["volume"]) if latest["volume"] is not None else None

        range_pct = (high - low) / close if close else None

        ts = datetime.now(NY_TZ).strftime("%Y-%m-%d %H:%M:%S")
        snapshot_id = f"{self.symbol}_{ts}"

        # -------------------------
        # RAW -> parquet
        # -------------------------
        cols_raw = [
            "con_id",
            "snapshot_id",
            "timestamp",
            "symbol",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "range_pct",
        ]

        df_raw = pd.DataFrame([[
            stock_conid,
            snapshot_id,
            ts,
            self.symbol,
            open_,
            high,
            low,
            close,
            volume,
            range_pct,
        ]], columns=cols_raw)

        out_dir = f"runs/{run_id}/stock_bars_raw_5m"
        os.makedirs(out_dir, exist_ok=True)
        df_raw.to_parquet(f"{out_dir}/shard_{shard_id}_{self.symbol}.parquet", index=False)

        # -------------------------
        # ENRICHED -> parquet
        # -------------------------
        (
            close_z_3d,
            volume_z_3d,
            range_z_3d,
            close_z_35d,
            volume_z_35d,
            range_z_35d,
        ) = compute_z_scores_for_stock(
            symbol=self.symbol,
            current_close=close,
            current_volume=volume,
            current_range_pct=range_pct,
        )

        cols_enriched = [
            *cols_raw,
            "close_z_3d",
            "volume_z_3d",
            "range_z_3d",
            "close_z_35d",
            "volume_z_35d",
            "range_z_35d",
            "opt_ret_10m",
            "opt_ret_1h",
            "opt_ret_eod",
            "opt_ret_next_open",
            "opt_ret_1d",
            "opt_ret_2d",
            "opt_ret_3d",
        ]

        df_enriched = pd.DataFrame([[
            stock_conid,
            snapshot_id,
            ts,
            self.symbol,
            open_,
            high,
            low,
            close,
            volume,
            range_pct,
            close_z_3d,
            volume_z_3d,
            range_z_3d,
            close_z_35d,
            volume_z_35d,
            range_z_35d,
            None, None, None, None, None, None, None,
        ]], columns=cols_enriched)

        out_dir = f"runs/{run_id}/stock_bars_enriched_5m"
        os.makedirs(out_dir, exist_ok=True)
        df_enriched.to_parquet(f"{out_dir}/shard_{shard_id}_{self.symbol}.parquet", index=False)

        # -------------------------
        # SIGNALS -> parquet
        # -------------------------
        cols_signals = [*cols_enriched, "trade_signal"]

        df_signals = pd.DataFrame([[
            stock_conid,
            snapshot_id,
            ts,
            self.symbol,
            open_,
            high,
            low,
            close,
            volume,
            range_pct,
            close_z_3d,
            volume_z_3d,
            range_z_3d,
            close_z_35d,
            volume_z_35d,
            range_z_35d,
            None, None, None, None, None, None, None,
            None,
        ]], columns=cols_signals)

        out_dir = f"runs/{run_id}/stock_execution_signals_5m"
        os.makedirs(out_dir, exist_ok=True)
        df_signals.to_parquet(f"{out_dir}/shard_{shard_id}_{self.symbol}.parquet", index=False)

        print(f"[STOCK] {snapshot_id} con_id={stock_conid}")
        return snapshot_id


# -------------------------
# Connection helper
# -------------------------
def open_connection(client_id: int, cfg: StockIngestConfig = StockIngestConfig()) -> StockApp:
    app = StockApp(cfg=cfg)
    app.connect(HOST, PORT, clientId=client_id)

    threading.Thread(target=app.run, daemon=True).start()

    if not app.connected_event.wait(timeout=10):
        raise RuntimeError("Failed to connect to IBKR")

    return app


# -------------------------
# Shard runner
# -------------------------
def main_parquet(
    client_id: int,
    shard: int,
    run_id: str,
    symbols,
    cfg: StockIngestConfig = StockIngestConfig(),
):
    app = open_connection(client_id, cfg=cfg)
    try:
        for symbol in symbols:
            try:
                app.start_symbol(symbol)
                res = app.run_sequence(run_id=run_id, shard_id=shard)
                if res is None:
                    continue
                time.sleep(0.10)
            except Exception as e:
                print(f"[STOCK][SHARD {shard}] skip {symbol}: {e}", flush=True)
                continue
    finally:
        app.disconnect()


# -------------------------
# Master ingest
# -------------------------
def master_ingest_5m(run_id: str, db_path: str = DB_PATH):
    raw_dir = f"runs/{run_id}/stock_bars_raw_5m"
    enriched_dir = f"runs/{run_id}/stock_bars_enriched_5m"
    signals_dir = f"runs/{run_id}/stock_execution_signals_5m"

    raw_glob = f"{raw_dir}/shard_*.parquet"
    enriched_glob = f"{enriched_dir}/shard_*.parquet"
    signals_glob = f"{signals_dir}/shard_*.parquet"

    con = duckdb.connect(db_path)
    try:
        con.execute("BEGIN;")

        con.execute(
            """
            INSERT INTO stock_execution_signals_5m
            SELECT * FROM read_parquet(?)
            """,
            [signals_glob],
        )

        con.execute(
            """
            INSERT INTO stock_bars_enriched_5m
            SELECT * FROM read_parquet(?)
            """,
            [enriched_glob],
        )

        con.execute(
            """
            INSERT INTO stock_bars_raw_5m
            SELECT * FROM read_parquet(?)
            """,
            [raw_glob],
        )

        con.execute("COMMIT;")
        print(f"[STOCK][INGEST] committed run_id={run_id}")

    except Exception:
        con.execute("ROLLBACK;")
        raise
    finally:
        con.close()
