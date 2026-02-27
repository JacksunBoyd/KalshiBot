"""
Kalshi Order Book Watcher — GUI entry point
Run with:  python app.py
"""

import asyncio
import csv
import datetime
import json
import os
import queue
import collections
import threading
import tkinter as tk
from tkinter import ttk, simpledialog
import math
import urllib.parse
import urllib.request

import websockets
from dotenv import load_dotenv
from order_book import OrderBook
from auth import KalshiAuth

load_dotenv()

WS_URL      = "wss://api.elections.kalshi.com/trade-api/ws/v2"
SESSIONS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "sessions")
os.makedirs(SESSIONS_DIR, exist_ok=True)
REST_BASE = "https://api.elections.kalshi.com/trade-api/v2"

auth = KalshiAuth(
    api_key=os.environ["KALSHI_API_KEY_ID"],
    key_path=os.environ["KALSHI_API_PATH"],
)


# ── helpers ──────────────────────────────────────────────────────────────────

def rest_get(path, params=None):
    full_path = f"/trade-api/v2{path}"
    url = REST_BASE + path
    if params:
        url += "?" + urllib.parse.urlencode(params)
    headers = auth.rest_headers("GET", full_path)
    req = urllib.request.Request(url, headers=headers)
    with urllib.request.urlopen(req) as resp:
        return json.loads(resp.read())


# ── order-book tab ────────────────────────────────────────────────────────────

class BookFrame(ttk.Frame):
    """One tab: live order-book display for a single ticker."""

    def __init__(self, parent_notebook, ticker: str, loop: asyncio.AbstractEventLoop):
        super().__init__(parent_notebook)
        self.ticker   = ticker
        self.loop     = loop
        self.side     = "yes"
        self.book     = OrderBook(ticker)
        self._q       = queue.Queue()
        self._running = True
        self._future  = None

        self._build_ui()
        self._start_ws()
        self._poll()

    # ── UI ───────────────────────────────────────────────────────────────────

    def _build_ui(self):
        # Header row
        hdr = ttk.Frame(self)
        hdr.pack(fill="x", padx=12, pady=8)

        ttk.Label(hdr, text=self.ticker,
                  font=("Consolas", 14, "bold")).pack(side="left")

        self.side_var = tk.StringVar(value="YES")
        ttk.Radiobutton(hdr, text="NO",  variable=self.side_var, value="NO",
                        command=self._toggle).pack(side="right", padx=4)
        ttk.Radiobutton(hdr, text="YES", variable=self.side_var, value="YES",
                        command=self._toggle).pack(side="right", padx=4)

        # Status / summary
        self.status_var  = tk.StringVar(value="Connecting…")
        self.summary_var = tk.StringVar(value="")

        ttk.Label(self, textvariable=self.status_var,
                  foreground="gray", font=("Consolas", 10)).pack(anchor="w", padx=12)
        ttk.Label(self, textvariable=self.summary_var,
                  foreground="#2a9d8f",
                  font=("Consolas", 12, "bold")).pack(anchor="w", padx=12, pady=2)

        ttk.Separator(self, orient="horizontal").pack(fill="x", padx=12, pady=4)

        # Book text area (dark)
        self.txt = tk.Text(
            self,
            font=("Consolas", 11),
            state="disabled",
            bg="#1e1e2e",
            fg="#cdd6f4",
            relief="flat",
            borderwidth=0,
            height=20,
            width=52,
            cursor="arrow",
        )
        self.txt.pack(fill="both", expand=True, padx=12, pady=(0, 12))
        self.txt.tag_configure("ask", foreground="#f38ba8")  # red-ish
        self.txt.tag_configure("bid", foreground="#a6e3a1")  # green-ish
        self.txt.tag_configure("hdr", foreground="#89b4fa",
                               font=("Consolas", 11, "bold"))
        self.txt.tag_configure("sep", foreground="#45475a")

    # ── side toggle ──────────────────────────────────────────────────────────

    def _toggle(self):
        self.side = self.side_var.get().lower()
        self._redraw()

    # ── websocket (runs in asyncio thread) ───────────────────────────────────

    def _start_ws(self):
        self._future = asyncio.run_coroutine_threadsafe(self._ws_loop(), self.loop)

    async def _ws_loop(self):
        q = self._q
        try:
            hdrs = auth.ws_headers()
            async with websockets.connect(WS_URL, additional_headers=hdrs) as ws:
                await ws.send(json.dumps({
                    "id": 1,
                    "cmd": "subscribe",
                    "params": {
                        "channels": ["orderbook_delta"],
                        "market_tickers": [self.ticker],
                    },
                }))
                q.put(("status", "Live"))
                while True:
                    raw  = await ws.recv()
                    data = json.loads(raw)
                    t    = data.get("type")
                    if t in ("orderbook_snapshot", "orderbook_delta"):
                        q.put(("data", data))
                    elif t == "error":
                        q.put(("status", f"Error: {data.get('msg')}"))
                        return
        except asyncio.CancelledError:
            return
        except Exception as exc:
            q.put(("status", f"Disconnected — {exc}"))

    # ── queue polling (tkinter thread, every 80 ms) ───────────────────────────

    def _poll(self):
        if not self._running:
            return
        try:
            while True:
                kind, payload = self._q.get_nowait()
                if kind == "status":
                    self.status_var.set(payload)
                elif kind == "data":
                    msg_type = payload.get("type")
                    if msg_type == "orderbook_snapshot":
                        self.book.apply_snapshot(payload["msg"])
                    else:
                        self.book.apply_delta(payload["msg"])
                    self._redraw()
        except queue.Empty:
            pass
        self.after(80, self._poll)

    # ── rendering ────────────────────────────────────────────────────────────

    def _redraw(self):
        side  = self.side
        bk    = self.book
        bid   = bk.best_bid(side)
        ask   = bk.best_ask(side)
        spread = bk.spread(side)
        label = side.upper()

        if bid is not None:
            self.summary_var.set(
                f"Best Bid: {bid}¢     Best Ask: {ask}¢     Spread: {spread}¢"
            )
        else:
            self.summary_var.set("Waiting for snapshot…")

        other = bk.no  if side == "yes" else bk.yes
        own   = bk.yes if side == "yes" else bk.no
        asks  = sorted(other.items(), reverse=True)[:8]
        bids  = sorted(own.items(),   reverse=True)[:8]

        t = self.txt
        t.config(state="normal")
        t.delete("1.0", "end")

        t.insert("end", f"  ── {label} ASKS ──────────────────────────\n", "hdr")
        for price, qty in reversed(asks):
            t.insert("end", f"    {100 - price:3d}¢   {qty:>12,} contracts\n", "ask")

        t.insert("end", f"  {'─' * 42}\n", "sep")

        t.insert("end", f"  ── {label} BIDS ──────────────────────────\n", "hdr")
        for price, qty in bids:
            t.insert("end", f"    {price:3d}¢   {qty:>12,} contracts\n", "bid")

        t.config(state="disabled")

    # ── lifecycle ────────────────────────────────────────────────────────────

    def stop(self):
        self._running = False
        if self._future and not self._future.done():
            self._future.cancel()


# ── BTC 15-minute dedicated tab ───────────────────────────────────────────────

class BTC15Frame(ttk.Frame):
    """
    Auto-rolling watcher for BTC 15-minute Kalshi contracts.

    Ticker format:  KXBTC15M-{DD}{MON}{YY}{HH}{MM}-{MM}
    Example:        KXBTC15M-21FEB261845-45
      DD  = day (zero-padded)
      MON = month abbreviation (uppercase)
      YY  = 2-digit year
      HH  = expiry hour (24h, zero-padded)
      MM  = expiry minute  (also repeated as the final segment)

    At 18:30 the active contract expires at 18:45 → ticker ends in 1845-45.
    When the clock reaches the expiry the tab automatically rolls to the next
    15-minute contract and logs a ROLL marker.
    """

    # ── shared across all BTC15Frame instances ────────────────────────────────
    _shared_btc_price: "float | None" = None
    _shared_strike:    "float | None" = None
    _btc_loop_started: bool = False
    _pct_samples:      "collections.deque | None" = None   # lazy-init

    def __init__(self, parent_notebook, loop: asyncio.AbstractEventLoop,
                 side: str = "yes", on_contract_end=None, on_event=None):
        super().__init__(parent_notebook)
        self.loop     = loop
        self._q       = queue.Queue()
        self._running = True
        self._future: asyncio.Future | None = None
        self._on_contract_end = on_contract_end
        self._on_event        = on_event

        # live state
        self.ticker  = ""
        self.expiry: datetime.datetime | None = None
        self.book: OrderBook | None = None
        self.side    = side

        # session tracking
        self._contract_start: datetime.datetime | None = None
        self._session_start_idx: int = 0
        self._pending_autosave: tuple | None = None  # (old_ticker, old_expiry, old_cs) set in _roll
        self._skip_first_save: bool = False          # True if app opened mid-contract

        # log
        self._log: list[dict] = []

        # cycle tracking — two-state machine: watching ≤40, then waiting for ≥45
        self._in_trigger: bool = False
        self._trigger_time: datetime.datetime | None = None
        self._cycle_count: int = 0
        self._cycle_log: list = []
        self._stop_loss_count: int = 0
        self._stopped_out: bool = False   # stop loss hit — no more tracking this contract

        self._last_price: int | None = None    # last Kalshi traded price (cents) from ticker channel
        self._entry_ask:  int | None = None    # ask at last T40 trigger (for P&L calc)

        # ensure the shared 60-second sample deque exists
        if BTC15Frame._pct_samples is None:
            BTC15Frame._pct_samples = collections.deque()

        self._build_ui()
        self._roll()      # connect to current contract
        self._poll()      # start queue consumer
        self._tick()      # start 1-second countdown / auto-roll
        self._log_tick()  # start independent 500ms logging timer

        # only one scraper thread for all BTC15Frame instances
        if not BTC15Frame._btc_loop_started:
            BTC15Frame._btc_loop_started = True
            threading.Thread(target=self._btc_price_loop, daemon=True).start()

    # ── ticker math ──────────────────────────────────────────────────────────

    @staticmethod
    def _compute(now: datetime.datetime):
        """Return (ticker_str, expiry_datetime) for the active 15-min contract."""
        nm = ((now.minute // 15) + 1) * 15
        if nm >= 60:
            exp = (now.replace(minute=0, second=0, microsecond=0)
                   + datetime.timedelta(hours=1))
        else:
            exp = now.replace(minute=nm, second=0, microsecond=0)

        dd  = exp.strftime("%d")
        mon = exp.strftime("%b").upper()
        yy  = exp.strftime("%y")
        hh  = exp.strftime("%H")
        mm  = exp.strftime("%M")
        ticker = f"KXBTC15M-{yy}{mon}{dd}{hh}{mm}-{mm}"
        return ticker, exp

    # ── UI ───────────────────────────────────────────────────────────────────

    def _build_ui(self):
        # ── header row ──
        hdr = ttk.Frame(self)
        hdr.pack(fill="x", padx=12, pady=8)

        self.ticker_var    = tk.StringVar(value="Computing…")
        self.countdown_var = tk.StringVar(value="")
        self.status_var    = tk.StringVar(value="Connecting…")
        self.summary_var   = tk.StringVar(value="")

        ttk.Label(hdr, textvariable=self.ticker_var,
                  font=("Consolas", 13, "bold")).pack(side="left")
        ttk.Label(hdr, textvariable=self.countdown_var,
                  foreground="#fab387",
                  font=("Consolas", 12)).pack(side="right", padx=8)

        ttk.Label(hdr, text=self.side.upper(),
                  font=("Consolas", 12, "bold"),
                  foreground="#89b4fa").pack(side="right", padx=8)

        # rules bar + no-entry-after control
        rules_bar = ttk.Frame(self)
        rules_bar.pack(fill="x", padx=12, pady=(0, 2))
        ttk.Label(rules_bar, text="Rules:  Entry ≤40¢  |  TP ≥45¢  |  SL ≤10¢",
                  font=("Consolas", 9), foreground="#a6adc8").pack(side="left")
        ttk.Label(rules_bar, text="     No entry after:",
                  font=("Consolas", 9), foreground="#a6adc8").pack(side="left")
        self._no_entry_after_var = tk.IntVar(value=0)
        ttk.Spinbox(rules_bar, from_=0, to=14, width=3,
                    textvariable=self._no_entry_after_var,
                    font=("Consolas", 9)).pack(side="left", padx=(2, 2))
        ttk.Label(rules_bar, text="min  (0=off)  |  Max cycles:",
                  font=("Consolas", 9), foreground="#a6adc8").pack(side="left")
        self._max_cycles_var = tk.IntVar(value=0)
        ttk.Spinbox(rules_bar, from_=0, to=9, width=2,
                    textvariable=self._max_cycles_var,
                    font=("Consolas", 9)).pack(side="left", padx=(2, 2))
        ttk.Label(rules_bar, text="(0=∞)  |  SL delay:",
                  font=("Consolas", 9), foreground="#a6adc8").pack(side="left")
        self._sl_delay_var = tk.IntVar(value=0)
        ttk.Spinbox(rules_bar, from_=0, to=120, width=3,
                    textvariable=self._sl_delay_var,
                    font=("Consolas", 9)).pack(side="left", padx=(2, 2))
        ttk.Label(rules_bar, text="s (0=off)",
                  font=("Consolas", 9), foreground="#a6adc8").pack(side="left")

        ttk.Label(self, textvariable=self.status_var,
                  foreground="gray",
                  font=("Consolas", 10)).pack(anchor="w", padx=12)
        ttk.Label(self, textvariable=self.summary_var,
                  foreground="#2a9d8f",
                  font=("Consolas", 12, "bold")).pack(anchor="w", padx=12, pady=2)

        ttk.Separator(self, orient="horizontal").pack(fill="x", padx=12, pady=4)

        # ── vertical pane: order book (top) + log (bottom) ──
        pane = ttk.PanedWindow(self, orient="vertical")
        pane.pack(fill="both", expand=True, padx=12, pady=(0, 4))

        # order book text widget
        bf = ttk.Frame(pane)
        pane.add(bf, weight=2)

        self.txt = tk.Text(
            bf,
            font=("Consolas", 11),
            state="disabled",
            bg="#1e1e2e",
            fg="#cdd6f4",
            relief="flat",
            borderwidth=0,
            height=10,
            cursor="arrow",
        )
        self.txt.tag_configure("ask", foreground="#f38ba8")
        self.txt.tag_configure("bid", foreground="#a6e3a1")
        self.txt.tag_configure("hdr", foreground="#89b4fa",
                               font=("Consolas", 11, "bold"))
        self.txt.tag_configure("sep", foreground="#45475a")

        # Tracker panel — pack BEFORE txt so it reserves space on the right
        tracker_frame = ttk.Frame(bf, width=168)
        tracker_frame.pack(side="right", fill="y", padx=(6, 0))
        tracker_frame.pack_propagate(False)

        ttk.Label(tracker_frame, text="BTC vs Strike",
                  font=("Consolas", 9, "bold"),
                  foreground="#1a56c4").pack(pady=(8, 4))

        self._trk_strike_var = tk.StringVar(value="Strike:  fetching…")
        self._trk_btc_var    = tk.StringVar(value="BTC:     —")
        self._trk_delta_var  = tk.StringVar(value="Delta:   —")
        self._trk_chg_var    = tk.StringVar(value="—")

        ttk.Label(tracker_frame, textvariable=self._trk_strike_var,
                  font=("Consolas", 9), foreground="#333333").pack(anchor="w", padx=6)
        ttk.Label(tracker_frame, textvariable=self._trk_btc_var,
                  font=("Consolas", 9), foreground="#333333").pack(anchor="w", padx=6)
        ttk.Label(tracker_frame, textvariable=self._trk_delta_var,
                  font=("Consolas", 9), foreground="#333333").pack(anchor="w", padx=6)

        ttk.Separator(tracker_frame, orient="horizontal").pack(fill="x", padx=4, pady=5)

        self._trk_chg_lbl = ttk.Label(tracker_frame, textvariable=self._trk_chg_var,
                                       font=("Consolas", 14, "bold"), anchor="center",
                                       foreground="#888888")
        self._trk_chg_lbl.pack(fill="x", padx=4)

        ttk.Separator(tracker_frame, orient="horizontal").pack(fill="x", padx=4, pady=(6, 3))

        self._trk_vol5_var  = tk.StringVar(value="RVol 5m:  —")
        self._trk_vol15_var = tk.StringVar(value="RVol 15m: —")
        ttk.Label(tracker_frame, textvariable=self._trk_vol5_var,
                  font=("Consolas", 9), foreground="#555555").pack(anchor="w", padx=6)
        ttk.Label(tracker_frame, textvariable=self._trk_vol15_var,
                  font=("Consolas", 9), foreground="#555555").pack(anchor="w", padx=6)

        # Pack txt after tracker so the tracker's width is reserved first
        self.txt.pack(side="left", fill="both", expand=True)

        # strategy log treeview
        lf = ttk.Frame(pane)
        pane.add(lf, weight=1)

        ttk.Label(lf, text="  ── Strategy Log ──",
                  font=("Consolas", 10, "bold"),
                  foreground="#89b4fa").pack(anchor="w", pady=(4, 2))

        cols = ("time", "session", "ticker", "bid", "ask", "spread", "mid",
                "bid_qty", "ask_qty", "last", "btc", "strike", "delta", "chg_pct", "evt")
        self.tree = ttk.Treeview(lf, columns=cols, show="headings",
                                 height=7, selectmode="none")

        col_defs = [
            ("time",    "Time",    72,  "center"),
            ("session", "Sn Time", 52,  "center"),
            ("ticker",  "Ticker",  180, "w"),
            ("bid",     "Bid",     45,  "center"),
            ("ask",     "Ask",     45,  "center"),
            ("spread",  "Sprd",    45,  "center"),
            ("mid",     "Mid",     52,  "center"),
            ("bid_qty", "Bid Qty", 72,  "center"),
            ("ask_qty", "Ask Qty", 72,  "center"),
            ("last",    "Last",    45,  "center"),
            ("btc",     "BTC $",   90,  "center"),
            ("strike",  "Strike",  88,  "center"),
            ("delta",   "Δ Strike", 88, "center"),
            ("chg_pct", "Chg%",    72,  "center"),
            ("evt",     "Event",   65,  "center"),
        ]
        for col, label, w, anchor in col_defs:
            self.tree.heading(col, text=label)
            self.tree.column(col, width=w, anchor=anchor,
                             stretch=(col == "ticker"))

        sb = ttk.Scrollbar(lf, orient="vertical", command=self.tree.yview)
        self.tree.config(yscrollcommand=sb.set)
        self.tree.pack(side="left", fill="both", expand=True)
        sb.pack(side="left", fill="y")

        self.tree.tag_configure("roll",       foreground="#fab387")
        self.tree.tag_configure("event_t",    background="#3d1500", foreground="#ff9944")
        self.tree.tag_configure("event_tgt",  background="#003d1a", foreground="#4ade80")
        self.tree.tag_configure("stop_loss",  background="#3d0000", foreground="#ff4444")
        self.tree.tag_configure("milestone",  background="#4a0a8e", foreground="#ffffff")

        # bottom button row
        btns = ttk.Frame(self)
        btns.pack(fill="x", padx=12, pady=(0, 8))
        ttk.Button(btns, text="Export CSV",
                   command=self._export).pack(side="right")
        ttk.Button(btns, text="Clear Log",
                   command=self._clear_log).pack(side="right", padx=4)

    # ── roll to active contract ───────────────────────────────────────────────

    def _roll(self):
        """Cancel any existing WS connection and connect to the current contract."""
        if self._future and not self._future.done():
            self._future.cancel()

        _is_first_roll = (self.ticker == "")  # True only on the very first call from __init__

        # Snapshot info for auto-save BEFORE any state reset
        if self.ticker:
            self._pending_autosave = (self.ticker, self.expiry, self._contract_start)

        # Build cycle summary before resetting signal state
        open_trig_str = (self._trigger_time.strftime("%H:%M:%S")
                         if self._in_trigger and self._trigger_time else None)
        cycle_data = (self._cycle_count, open_trig_str, self._stop_loss_count)

        # Fire history callback before resetting (skip the very first empty roll)
        if self._on_contract_end and self.ticker:
            self._on_contract_end(
                self.ticker, self.side,
                self._cycle_count, self._stop_loss_count,
                bool(self._in_trigger),
            )

        # Reset cycle tracking
        self._in_trigger      = False
        self._trigger_time    = None
        self._cycle_count     = 0
        self._cycle_log       = []
        self._stop_loss_count = 0
        self._stopped_out     = False

        # Capture end ask BEFORE discarding the old book
        if self.book is not None:
            last_ask = self.book.best_ask(self.side)
            if last_ask is not None:
                self._prev_end_ask = last_ask

        ticker, expiry = self._compute(datetime.datetime.now())
        self.ticker         = ticker
        self.expiry         = expiry
        self._contract_start = expiry - datetime.timedelta(minutes=15)
        self.book        = OrderBook(ticker)

        # If the app opened mid-contract, flag the first save to be skipped
        if _is_first_roll:
            elapsed = (datetime.datetime.now() - self._contract_start).total_seconds()
            if elapsed > 30:
                self._skip_first_save = True

        BTC15Frame._shared_strike = None
        if BTC15Frame._pct_samples is not None:
            BTC15Frame._pct_samples.clear()
        self._last_price = None
        self._entry_ask  = None

        self.ticker_var.set(ticker)
        self.status_var.set("Connecting…")
        self.summary_var.set("")
        self._clear_book()

        # Fetch strike price (price to beat) from market rules_primary
        threading.Thread(target=self._fetch_strike, args=(ticker,), daemon=True).start()

        # queue a roll marker (processed by _poll in tk thread)
        self._q.put(("roll", (ticker, cycle_data)))
        self._future = asyncio.run_coroutine_threadsafe(self._ws_loop(), self.loop)

    # ── Strike + BTC price ───────────────────────────────────────────────────

    def _fetch_strike(self, ticker: str):
        """Background thread: parse strike from rules_primary ('is at least X'),
        with fallback to yes_sub_title ('Price to beat: $X'). Retries up to 6x."""
        import re, time as _time
        _rules_pat    = re.compile(r'is at least\s*\$?([\d,]+\.?\d*)', re.IGNORECASE)
        _subtitle_pat = re.compile(r'Price to beat:\s*\$?([\d,]+\.?\d*)', re.IGNORECASE)
        for attempt in range(6):
            try:
                data = rest_get(f"/markets/{ticker}")
                mkt  = data.get("market", {})

                # Primary: rules_primary is the authoritative contract definition
                m = _rules_pat.search(mkt.get("rules_primary", ""))
                # Fallback: yes_sub_title display field
                if not m:
                    m = _subtitle_pat.search(mkt.get("yes_sub_title", ""))

                if m:
                    val = float(m.group(1).replace(",", ""))
                    BTC15Frame._shared_strike = val
                    self._q.put(("strike", val))
                    return
                # neither field populated yet — retry
            except Exception as e:
                if attempt == 5:
                    self._q.put(("status", f"Strike error: {e}"))
                    self._q.put(("strike_failed", None))
                    return
            _time.sleep(7)
        self._q.put(("strike_failed", None))

    def _btc_price_loop(self):
        """Daemon thread: polls Coinbase BTC-USD spot price every 500 ms.
        Uses a fixed-interval timer so HTTP latency does not add to the sleep gap."""
        import time
        INTERVAL = 0.5  # seconds between fetches
        while True:
            t0 = time.monotonic()
            try:
                req = urllib.request.Request(
                    "https://api.coinbase.com/v2/prices/BTC-USD/spot",
                    headers={"User-Agent": "Mozilla/5.0", "Accept": "application/json"},
                )
                with urllib.request.urlopen(req, timeout=4) as resp:
                    data = json.loads(resp.read())
                price = float(data["data"]["amount"])
                BTC15Frame._shared_btc_price = price
                self._q.put(("btc_price", price))
            except Exception:
                pass
            elapsed = time.monotonic() - t0
            sleep_for = max(0.0, INTERVAL - elapsed)
            time.sleep(sleep_for)

    # ── WebSocket (asyncio thread) ────────────────────────────────────────────

    async def _ws_loop(self):
        while True:
            try:
                async with websockets.connect(
                    WS_URL, additional_headers=auth.ws_headers()
                ) as ws:
                    await ws.send(json.dumps({
                        "id": 1,
                        "cmd": "subscribe",
                        "params": {
                            "channels": ["orderbook_delta", "ticker"],
                            "market_tickers": [self.ticker],
                        },
                    }))
                    self._q.put(("status", "Live"))
                    while True:
                        raw  = await ws.recv()
                        data = json.loads(raw)
                        t    = data.get("type")
                        if t in ("orderbook_snapshot", "orderbook_delta"):
                            self._q.put(("data", data))
                        elif t == "ticker":
                            self._q.put(("ticker", data.get("msg", {})))
                        elif t == "error":
                            self._q.put(("status", f"WS error: {data.get('msg')}"))
                            return   # explicit server error — don't retry
            except asyncio.CancelledError:
                return
            except Exception as exc:
                if not self._running:
                    return
                self._q.put(("status", f"Reconnecting… ({type(exc).__name__})"))
            try:
                await asyncio.sleep(5)
            except asyncio.CancelledError:
                return

    # ── queue poll (tkinter thread, 80 ms) ───────────────────────────────────

    def _poll(self):
        if not self._running:
            return
        try:
            while True:
                kind, payload = self._q.get_nowait()
                if kind == "status":
                    self.status_var.set(payload)
                elif kind == "data":
                    mt = payload.get("type")
                    if mt == "orderbook_snapshot":
                        self.book.apply_snapshot(payload["msg"])
                    else:
                        self.book.apply_delta(payload["msg"])
                    self._check_events()
                    self._redraw()
                elif kind == "ticker":
                    lp = payload.get("price") or payload.get("last_price")
                    if lp is not None:
                        self._last_price = int(lp)
                elif kind == "strike":
                    BTC15Frame._shared_strike = payload
                    self._update_tracker()
                elif kind == "strike_failed":
                    self._trk_strike_var.set("Strike:  unavailable")
                elif kind == "btc_price":
                    BTC15Frame._shared_btc_price = payload
                    self._add_pct_sample()
                    self._update_tracker()
                elif kind == "roll":
                    ticker, cycle_data = payload
                    self._append_roll_marker(ticker, cycle_data)
        except queue.Empty:
            pass
        self.after(80, self._poll)

    # ── 1-second tick: countdown + auto-roll ─────────────────────────────────

    def _tick(self):
        if not self._running:
            return
        now = datetime.datetime.now()
        if self.expiry:
            secs = (self.expiry - now).total_seconds()
            if secs <= 0:
                self._roll()
            else:
                m, s = divmod(int(secs), 60)
                elapsed = max(0, 900 - secs)  # 15 min window = 900 s
                em, es = divmod(int(elapsed), 60)
                self.countdown_var.set(f"Rem {m:02d}:{s:02d}  |  Sn {em:02d}:{es:02d}")
        self.after(1_000, self._tick)

    def _log_tick(self):
        """Independent 500ms timer — logs regardless of which tab is visible."""
        if not self._running:
            return
        self._add_pct_sample()   # keep samples fresh even if queue thread is on old instance
        if self.book is not None:
            self._maybe_log()
            self._update_tracker()
        self.after(500, self._log_tick)

    def _depth_at_40(self) -> tuple:
        """Return (bid_qty, ask_qty) at the 40¢ level for this frame's side."""
        bk = self.book
        if bk is None:
            return 0, 0
        if self.side == "yes":
            return bk.yes.get(40, 0), bk.no.get(60, 0)
        else:
            return bk.no.get(40, 0), bk.yes.get(60, 0)

    def _btc_momentum(self, seconds: int) -> str:
        """BTC price change vs the oldest sample inside the last N seconds."""
        now_ts = datetime.datetime.now().timestamp()
        cutoff = now_ts - seconds
        past_btc = None
        for ts, b in BTC15Frame._pct_samples:
            if ts >= cutoff:
                past_btc = b
                break
        current = BTC15Frame._shared_btc_price
        if past_btc is None or current is None:
            return "—"
        return f"{current - past_btc:+,.2f}"

    def _realized_vol(self, seconds: int) -> str:
        """Stddev of 1-second log returns over the last N seconds, expressed as %."""
        now_ts = datetime.datetime.now().timestamp()
        prices = [b for ts, b in BTC15Frame._pct_samples if ts >= now_ts - seconds]
        if len(prices) < 3:
            return "—"
        log_returns = [math.log(prices[i + 1] / prices[i])
                       for i in range(len(prices) - 1) if prices[i] > 0]
        if len(log_returns) < 2:
            return "—"
        n    = len(log_returns)
        mean = sum(log_returns) / n
        var  = sum((r - mean) ** 2 for r in log_returns) / (n - 1)
        return f"{math.sqrt(var) * 100:.4f}%"

    def _chg_pct_str(self) -> str:
        """Compute BTC-vs-strike % using the 60s moving-average BTC price (4 dp)."""
        strike = BTC15Frame._shared_strike
        if strike is None or strike == 0:
            return "—"
        _now_ts   = datetime.datetime.now().timestamp()
        _recent60 = [b for ts, b in BTC15Frame._pct_samples if ts >= _now_ts - 60.0]
        if _recent60:
            avg_btc = sum(_recent60) / len(_recent60)
        else:
            avg_btc = BTC15Frame._shared_btc_price
        if avg_btc is None:
            return "—"
        pct = (avg_btc - strike) / strike * 100
        return f"{pct:+.4f}%"

    def _btc_60s_avg(self) -> "float | None":
        """Return the 60-second moving average of BTC samples (Kalshi settlement basis)."""
        now_ts    = datetime.datetime.now().timestamp()
        recent60  = [b for ts, b in BTC15Frame._pct_samples if ts >= now_ts - 60.0]
        if recent60:
            return sum(recent60) / len(recent60)
        return BTC15Frame._shared_btc_price  # fallback to raw if deque is empty

    def _btc_delta_str(self) -> str:
        """Return directional dollar delta between 60s-avg BTC price and the strike.
        Format: ▲+123.45 or ▼-45.67, or '—' if either value is unavailable."""
        btc    = self._btc_60s_avg()
        strike = BTC15Frame._shared_strike
        if btc is None or strike is None:
            return "—"
        diff = btc - strike
        arrow = "▲" if diff >= 0 else "▼"
        return f"{arrow}{diff:+,.2f}"

    def _add_pct_sample(self):
        """Append a raw BTC price to the shared 60-second rolling window."""
        btc = BTC15Frame._shared_btc_price
        if btc is None:
            return
        now_ts = datetime.datetime.now().timestamp()
        BTC15Frame._pct_samples.append((now_ts, btc))
        cutoff = now_ts - 900.0   # keep 15 minutes of history
        while BTC15Frame._pct_samples and BTC15Frame._pct_samples[0][0] < cutoff:
            BTC15Frame._pct_samples.popleft()

    # ── rendering ────────────────────────────────────────────────────────────

    def _clear_book(self):
        self.txt.config(state="normal")
        self.txt.delete("1.0", "end")
        self.txt.config(state="disabled")
        self._trk_strike_var.set("Strike:  fetching…")
        self._trk_btc_var.set("BTC:     —")
        self._trk_delta_var.set("Delta:   —")
        self._trk_chg_var.set("—")
        self._trk_chg_lbl.config(foreground="#888888")

    def _update_tracker(self):
        # Realized vol doesn't depend on strike/btc — update unconditionally
        self._trk_vol5_var.set(f"RVol 5m:  {self._realized_vol(300)}")
        self._trk_vol15_var.set(f"RVol 15m: {self._realized_vol(900)}")

        strike = BTC15Frame._shared_strike
        btc    = BTC15Frame._shared_btc_price

        self._trk_strike_var.set(
            f"Strike:  ${strike:>10,.2f}" if strike is not None else "Strike:  fetching…"
        )
        self._trk_btc_var.set(
            f"BTC:     ${btc:>10,.2f}" if btc is not None else "BTC:     —"
        )

        if btc is None or strike is None or strike == 0:
            self._trk_delta_var.set("Delta:   —")
            self._trk_chg_var.set("—")
            self._trk_chg_lbl.config(foreground="#888888")
            return

        diff = btc - strike
        sign = "▲" if diff > 0 else ("▼" if diff < 0 else "─")
        self._trk_delta_var.set(f"Delta: {sign}{abs(diff):,.2f}")

        # % change using 60-second moving average of BTC price vs strike
        _now_ts   = datetime.datetime.now().timestamp()
        _recent60 = [b for ts, b in BTC15Frame._pct_samples if ts >= _now_ts - 60.0]
        if _recent60:
            avg_btc  = sum(_recent60) / len(_recent60)
            disp_pct = (avg_btc - strike) / strike * 100
        else:
            disp_pct = (diff / strike) * 100

        d_sign  = "▲" if disp_pct > 0 else ("▼" if disp_pct < 0 else "─")
        d_color = "#15803d" if disp_pct > 0 else ("#dc2626" if disp_pct < 0 else "#666666")
        self._trk_chg_var.set(f"{d_sign} {disp_pct:+.4f}%")
        self._trk_chg_lbl.config(foreground=d_color)

    def _redraw(self):
        side   = self.side
        bk     = self.book
        bid    = bk.best_bid(side)
        ask    = bk.best_ask(side)
        spread = bk.spread(side)
        label  = side.upper()

        if bid is not None:
            self.summary_var.set(
                f"Best Bid: {bid}¢     Best Ask: {ask}¢     Spread: {spread}¢"
            )
        else:
            self.summary_var.set("Waiting for snapshot…")

        other = bk.no  if side == "yes" else bk.yes
        own   = bk.yes if side == "yes" else bk.no
        asks  = sorted(other.items(), reverse=True)[:8]
        bids  = sorted(own.items(),   reverse=True)[:8]

        t = self.txt
        t.config(state="normal")
        t.delete("1.0", "end")

        t.insert("end", f"  ── {label} ASKS ──────────────────────────\n", "hdr")
        for price, qty in reversed(asks):
            t.insert("end", f"    {100 - price:3d}¢   {qty:>12,} contracts\n", "ask")

        t.insert("end", f"  {'─' * 42}\n", "sep")

        t.insert("end", f"  ── {label} BIDS ──────────────────────────\n", "hdr")
        for price, qty in bids:
            t.insert("end", f"    {price:3d}¢   {qty:>12,} contracts\n", "bid")

        t.config(state="disabled")
        self._update_tracker()

    # ── logging ──────────────────────────────────────────────────────────────

    def _maybe_log(self):
        """Write a log snapshot — called every 500ms by _log_tick."""
        bk   = self.book
        side = self.side
        bid  = bk.best_bid(side)
        ask  = bk.best_ask(side)
        spr  = bk.spread(side)
        mid  = round((bid + ask) / 2, 1) if bid is not None and ask is not None else None

        # quantity at best bid / best ask levels
        own   = bk.yes if side == "yes" else bk.no
        other = bk.no  if side == "yes" else bk.yes
        bq    = own.get(bid, 0) if bid is not None else 0
        ob    = max(other.keys(), default=None)
        aq    = other.get(ob, 0) if ob is not None else 0

        chg_str   = self._chg_pct_str()
        delta_str = self._btc_delta_str()
        btc       = self._btc_60s_avg()

        now = datetime.datetime.now()
        row = {
            "time":    now.strftime("%H:%M:%S"),
            "session": self._session_elapsed_str(now),
            "ticker":  self.ticker,
            "bid":     str(bid) if bid is not None else "—",
            "ask":     str(ask) if ask is not None else "—",
            "spread":  str(spr) if spr is not None else "—",
            "mid":     str(mid) if mid is not None else "—",
            "bid_qty": str(bq),
            "ask_qty": str(aq),
            "last":    str(self._last_price) if self._last_price is not None else "—",
            "btc":     f"{btc:.2f}" if btc is not None else "—",
            "strike":  f"{BTC15Frame._shared_strike:,.2f}" if BTC15Frame._shared_strike is not None else "—",
            "delta":   delta_str,
            "chg_pct": chg_str,
            "evt":     "",
        }
        self._log.append(row)
        self.tree.insert("", "end", values=tuple(row.values()))
        self._scroll_if_at_bottom()

    def _check_events(self):
        """Two-state cycle tracker:
          Entry:  ask ≤ 40  (lift the offer — executable buy)
          Target: bid ≥ 45  (hit the bid — passive limit sell is fillable)
          Stop:   ask ≤ 5  (market has dropped hard — exit at market)
        New triggers also blocked in the last 30 s (settlement noise)."""
        if self._stopped_out:
            return
        ask = self.book.best_ask(self.side)
        bid = self.book.best_bid(self.side)
        if ask is None:
            return
        now = datetime.datetime.now()
        secs_remaining = (self.expiry - now).total_seconds() if self.expiry else 999

        if not self._in_trigger:
            # Don't open a new cycle in the final 30 seconds — settlement noise
            if secs_remaining < 30:
                return
            # No-entry-after cutoff (user-configurable, 0 = off)
            try:
                no_entry_mins = self._no_entry_after_var.get()
            except tk.TclError:
                no_entry_mins = 0
            if no_entry_mins > 0 and self._contract_start is not None:
                elapsed_mins = (now - self._contract_start).total_seconds() / 60
                if elapsed_mins >= no_entry_mins:
                    return
            if ask <= 40:
                self._in_trigger   = True
                self._trigger_time = now
                self._entry_ask    = ask
                self._add_event_row("T40", now, ask)
        else:
            # Trigger active: stop loss at ≤5 ends the contract, target at ≥45
            if ask <= 10:
                # Honour stop-loss delay — SL not armed until N seconds after entry
                try:
                    sl_delay = self._sl_delay_var.get()
                except tk.TclError:
                    sl_delay = 0
                elapsed = (now - self._trigger_time).total_seconds() if self._trigger_time else 999
                if sl_delay > 0 and elapsed < sl_delay:
                    pass  # stop not yet armed — keep monitoring
                else:
                    self._stop_loss_count += 1
                    self._in_trigger   = False
                    self._trigger_time = None
                    self._stopped_out  = True
                    self._add_event_row("STOP LOSS", now, ask)
                    return
            elif bid is not None and bid >= 45:
                # Only fill if someone is actually bidding ≥ 45 (passive limit sell executable)
                secs = (now - self._trigger_time).total_seconds()
                if secs < 2.0:
                    pass  # sub-2s fill — data artifact, keep monitoring
                else:
                    self._cycle_count += 1
                    self._cycle_log.append((self._trigger_time, now))
                    self._in_trigger   = False
                    self._trigger_time = None
                    self._add_event_row(f"TARGET #{self._cycle_count}", now, bid, secs)
                if self._cycle_count == 4:
                    self._add_milestone_row(now)
                # Stop trading if max cycles reached
                try:
                    max_c = self._max_cycles_var.get()
                except tk.TclError:
                    max_c = 0
                if max_c > 0 and self._cycle_count >= max_c:
                    self._stopped_out = True

    def _session_elapsed_str(self, t: datetime.datetime) -> str:
        """Return elapsed time since contract start as 'M:SS', or '' if unknown."""
        if self._contract_start is None:
            return ""
        elapsed = max(0, (t - self._contract_start).total_seconds())
        m, s = divmod(int(elapsed), 60)
        return f"{m}:{s:02d}"

    def _add_event_row(self, label: str, time: datetime.datetime,
                       ask: float, duration: float | None = None):
        """Insert a highlighted trigger or target event row into the log."""
        time_str = time.strftime("%H:%M:%S")
        if duration is None:
            dur_str = ""
        elif duration < 1:
            dur_str = "<1s"
        else:
            dur_str = f"{int(duration)}s"
        if label.startswith("TARGET"):
            tag = "event_tgt"
        elif label == "STOP LOSS":
            tag = "stop_loss"
        else:
            tag = "event_t"
        chg_str   = self._chg_pct_str()
        delta_str = self._btc_delta_str()
        btc       = self._btc_60s_avg()
        row = {
            "time":    time_str,
            "session": self._session_elapsed_str(time),
            "ticker":  self.ticker,
            "bid":     "—",
            "ask":     str(ask),
            "spread":  "—",
            "mid":     "—",
            "bid_qty": dur_str,
            "ask_qty": "—",
            "last":    "—",
            "btc":     f"{btc:.2f}" if btc is not None else "—",
            "strike":  f"{BTC15Frame._shared_strike:,.2f}" if BTC15Frame._shared_strike is not None else "—",
            "delta":   delta_str,
            "chg_pct": chg_str,
            "evt":     label,
        }
        self._log.append(row)
        self.tree.insert("", "end", values=tuple(row.values()), tags=(tag,))
        self._scroll_if_at_bottom()
        if self._on_event:
            bk = self.book
            if bk is not None:
                own   = bk.yes if self.side == "yes" else bk.no
                other = bk.no  if self.side == "yes" else bk.yes
                bp    = bk.best_bid(self.side)
                bq    = own.get(bp, 0) if bp is not None else 0
                ob    = max(other.keys(), default=None)
                aq    = other.get(ob, 0) if ob is not None else 0
                depth_str = f"B:{bq:,} A:{aq:,}"
            else:
                depth_str = "—"
            pnl_val = None
            if label.startswith("TARGET") or label == "STOP LOSS":
                pnl_val = 10.0 * (ask - 40) / 40   # always 40¢ entry per strategy rule
            analytics = {
                "depth":      depth_str,
                "pnl":        f"{pnl_val:+.2f}" if pnl_val is not None else "—",
                "pnl_value":  pnl_val,
                "btc_mom_1m": self._btc_momentum(60),
                "btc_mom_5m": self._btc_momentum(300),
                "rvol_5m":    self._realized_vol(300),
                "rvol_15m":   self._realized_vol(900),
            }
            self._on_event(self.side, self.ticker, time_str,
                           self._session_elapsed_str(time),
                           str(ask), chg_str, label, dur_str, analytics)

    def _add_milestone_row(self, now: datetime.datetime):
        """Bright row inserted when 4 ≤40→≥45 cycles complete in one contract."""
        row = {
            "time":    now.strftime("%H:%M:%S"),
            "session": self._session_elapsed_str(now),
            "ticker":  f"★  4 CYCLES  ≤40 → ≥45  completed in {self.ticker}  ★",
            "bid": "", "ask": "", "spread": "",
            "mid": "", "bid_qty": "", "ask_qty": "",
            "last": "", "btc": "", "strike": "", "delta": "", "chg_pct": "",
            "evt": "MILESTONE",
        }
        self._log.append(row)
        self.tree.insert("", "end", values=tuple(row.values()), tags=("milestone",))
        self._scroll_if_at_bottom()
        if self._on_event:
            bk = self.book
            if bk is not None:
                own   = bk.yes if self.side == "yes" else bk.no
                other = bk.no  if self.side == "yes" else bk.yes
                bp    = bk.best_bid(self.side)
                bq    = own.get(bp, 0) if bp is not None else 0
                ob    = max(other.keys(), default=None)
                aq    = other.get(ob, 0) if ob is not None else 0
                depth_str = f"B:{bq:,} A:{aq:,}"
            else:
                depth_str = "—"
            analytics = {
                "depth":      depth_str,
                "pnl":        "—",
                "pnl_value":  None,
                "btc_mom_1m": self._btc_momentum(60),
                "btc_mom_5m": self._btc_momentum(300),
                "rvol_5m":    self._realized_vol(300),
                "rvol_15m":   self._realized_vol(900),
            }
            self._on_event(self.side, self.ticker, now.strftime("%H:%M:%S"),
                           self._session_elapsed_str(now),
                           "", "", "MILESTONE", "", analytics)

    def _scroll_if_at_bottom(self):
        """Scroll to the latest row only when the user is already at the bottom."""
        kids = self.tree.get_children()
        if not kids:
            return
        _, bottom = self.tree.yview()
        if bottom >= 0.99:
            self.tree.see(kids[-1])

    def _append_roll_marker(self, ticker: str, cycle_data: tuple):
        """Insert a CONTRACT END summary row then a ROLL divider row, then auto-save."""
        cycle_count, open_trig_str, stop_loss_count = cycle_data
        now      = datetime.datetime.now()
        now_str  = now.strftime("%H:%M:%S")
        sn_str   = self._session_elapsed_str(now)
        stop_sfx = f"  |  {stop_loss_count} stop loss" + ("es" if stop_loss_count > 1 else "") if stop_loss_count else ""

        # ── CONTRACT END summary ──
        if cycle_count >= 4:
            summary = f"★ {cycle_count} CYCLES completed  ≤40→≥45{stop_sfx}  ★"
            tag     = "milestone"
            evt_lbl = f"CYCLES:{cycle_count}"
        elif cycle_count > 0:
            summary = f"{cycle_count} cycle(s)  ≤40→≥45  completed{stop_sfx}"
            if open_trig_str:
                summary += f"  +  open trigger @ {open_trig_str}"
            tag     = "event_tgt"
            evt_lbl = f"CYCLES:{cycle_count}"
        elif open_trig_str:
            summary = f"≤40 hit @ {open_trig_str}  →  ≥45 never reached{stop_sfx}"
            tag     = "event_t"
            evt_lbl = "CYCLE ✗"
        else:
            summary = f"ask never reached ≤40{stop_sfx}"
            tag     = "roll"
            evt_lbl = "NO TRIG"

        end_row = {
            "time":    now_str,
            "session": sn_str,
            "ticker":  summary,
            "bid": "—", "ask": "—", "spread": "—",
            "mid": "—", "bid_qty": "—", "ask_qty": "—",
            "last": "—", "btc": "—", "strike": "—", "delta": "—", "chg_pct": "—",
            "evt":     evt_lbl,
        }
        self._log.append(end_row)
        self.tree.insert("", "end", values=tuple(end_row.values()), tags=(tag,))

        # ── ROLL divider ──
        roll_row = {
            "time":    now_str,
            "session": "",
            "ticker":  f"▶ ROLL → {ticker}",
            "bid": "", "ask": "", "spread": "",
            "mid": "", "bid_qty": "", "ask_qty": "",
            "last": "", "btc": "", "strike": "", "delta": "", "chg_pct": "", "evt": "",
        }
        self._log.append(roll_row)
        self.tree.insert("", "end", values=tuple(roll_row.values()), tags=("roll",))
        self._scroll_if_at_bottom()

        # ── auto-save completed session ──
        if self._pending_autosave:
            old_ticker, old_expiry, old_cs = self._pending_autosave
            rows = self._log[self._session_start_idx:]
            if self._skip_first_save:
                self._skip_first_save = False  # reset — all future contracts save normally
            else:
                self._auto_save_session(old_ticker, old_expiry, old_cs, list(rows))
            self._session_start_idx = len(self._log)
            self._pending_autosave  = None

    def _auto_save_session(self, old_ticker: str, old_expiry, old_cs, rows: list):
        """Write a session CSV to the sessions/ folder automatically."""
        if not rows or not old_ticker:
            return
        try:
            if old_expiry and old_cs:
                day      = old_expiry.strftime("%A")
                date_str = f"{old_expiry.month}/{old_expiry.day}/{old_expiry.year}"
                sh = old_cs.hour % 12 or 12
                eh = old_expiry.hour % 12 or 12
                sm = f"{old_cs.minute:02d}"
                em = f"{old_expiry.minute:02d}"
                ampm_s = "AM" if old_cs.hour < 12 else "PM"
                ampm_e = "AM" if old_expiry.hour < 12 else "PM"
                if ampm_s == ampm_e:
                    label = f"{day} {date_str} {sh}:{sm}-{eh}:{em} {ampm_e} BTC 15 Minute"
                else:
                    label = f"{day} {date_str} {sh}:{sm} {ampm_s}-{eh}:{em} {ampm_e} BTC 15 Minute"
                fname = (f"{old_cs.strftime('%Y-%m-%d_%H%M')}"
                         f"_{self.side.upper()}_btc15.csv")
            else:
                label = old_ticker
                fname = f"{old_ticker}_{self.side.upper()}_btc15.csv"

            no_entry_mins = self._no_entry_after_var.get()
            rules = "Entry ≤40¢  |  Take Profit ≥45¢  |  Stop Loss ≤10¢"
            if no_entry_mins > 0:
                rules += f"  |  No entry after {no_entry_mins} min"

            fields = ["time", "session", "ticker", "bid", "ask", "spread",
                      "mid", "bid_qty", "ask_qty", "last", "btc", "strike", "delta", "chg_pct", "evt"]
            path = os.path.join(SESSIONS_DIR, fname)
            with open(path, "w", newline="", encoding="utf-8-sig") as f:
                f.write(f"# Session: {label}\n")
                f.write(f"# Side: {self.side.upper()}\n")
                f.write(f"# Rules: {rules}\n")
                f.write(f"# Ticker: {old_ticker}\n")
                w = csv.DictWriter(f, fieldnames=fields)
                w.writeheader()
                for row in rows:
                    w.writerow({k: row.get(k, "") for k in fields})
        except Exception:
            pass  # never crash the UI on a save error

    def _clear_log(self):
        self._log.clear()
        self.tree.delete(*self.tree.get_children())

    def _export(self):
        import tkinter.filedialog as fd
        if not self._log:
            return
        path = fd.asksaveasfilename(
            defaultextension=".csv",
            filetypes=[("CSV files", "*.csv"), ("All files", "*.*")],
            initialfile=(
                f"btc15_log_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            ),
        )
        if not path:
            return
        fields = ["time", "session", "ticker", "bid", "ask", "spread",
                  "mid", "bid_qty", "ask_qty", "last", "btc", "strike", "delta", "chg_pct", "evt"]
        with open(path, "w", newline="", encoding="utf-8-sig") as f:
            w = csv.DictWriter(f, fieldnames=fields)
            w.writeheader()
            for row in self._log:
                w.writerow({k: row.get(k, "") for k in fields})

    # ── lifecycle ─────────────────────────────────────────────────────────────

    def stop(self):
        self._running = False
        if self._future and not self._future.done():
            self._future.cancel()


# ── search dialog ─────────────────────────────────────────────────────────────

class SearchDialog(tk.Toplevel):
    """Two-step dialog: events list → markets list → returns ticker."""

    def __init__(self, parent):
        super().__init__(parent)
        self.title("Search Markets")
        self.resizable(True, False)
        self.result  = None
        self._events  = []
        self._markets = []
        self._mode    = "events"
        self._build()
        self.grab_set()
        self.focus_force()
        self.bind("<Escape>", lambda _: self.destroy())

    def _build(self):
        # Search bar
        bar = ttk.Frame(self)
        bar.pack(fill="x", padx=10, pady=10)

        ttk.Label(bar, text="Search:").pack(side="left")
        self._qvar = tk.StringVar()
        entry = ttk.Entry(bar, textvariable=self._qvar, width=40)
        entry.pack(side="left", padx=6)
        entry.focus()
        entry.bind("<Return>", lambda _: self._search())
        ttk.Button(bar, text="Go", command=self._search).pack(side="left")

        # Listbox + scrollbar
        lf = ttk.Frame(self)
        lf.pack(fill="both", expand=True, padx=10)

        self._lb = tk.Listbox(lf, font=("Consolas", 10), width=76, height=16,
                               selectmode="single", activestyle="dotbox")
        sb = ttk.Scrollbar(lf, orient="vertical", command=self._lb.yview)
        self._lb.config(yscrollcommand=sb.set)
        self._lb.pack(side="left", fill="both", expand=True)
        sb.pack(side="left", fill="y")
        self._lb.bind("<Double-1>", lambda _: self._select())

        # Status
        self._status = tk.StringVar(value="Enter a query and press Go.")
        ttk.Label(self, textvariable=self._status,
                  foreground="gray").pack(anchor="w", padx=10, pady=4)

        # Buttons
        btns = ttk.Frame(self)
        btns.pack(pady=(0, 10))
        ttk.Button(btns, text="Select", command=self._select).pack(side="left", padx=6)
        ttk.Button(btns, text="Cancel", command=self.destroy).pack(side="left", padx=6)

    # ── search (REST in bg thread) ────────────────────────────────────────────

    def _search(self):
        q = self._qvar.get().strip()
        if not q:
            return
        self._status.set("Searching…")
        self.update()
        threading.Thread(target=self._bg_search, args=(q,), daemon=True).start()

    def _bg_search(self, q):
        try:
            data   = rest_get("/events", {"limit": 200})
            events = data.get("events", [])
            ql     = q.lower()
            self._events = [
                e for e in events
                if ql in e.get("title", "").lower()
                or ql in e.get("event_ticker", "").lower()
            ][:25]
            self.after(0, self._show_events)
        except Exception as exc:
            self.after(0, lambda: self._status.set(f"Error: {exc}"))

    def _show_events(self):
        self._mode = "events"
        self._lb.delete(0, "end")
        for ev in self._events:
            ticker = ev.get("event_ticker", "")
            title  = ev.get("title", "")
            self._lb.insert("end", f"{ticker:<5}  {title[:45]}")
        self._status.set(
            f"{len(self._events)} event(s) found.  Double-click or Select to see markets."
        )

    # ── select ────────────────────────────────────────────────────────────────

    def _select(self):
        sel = self._lb.curselection()
        if not sel:
            return
        idx = sel[0]

        if self._mode == "events":
            ev     = self._events[idx]
            etick  = ev["event_ticker"]
            self._status.set(f"Loading markets for {etick}…")
            self.update()
            threading.Thread(target=self._bg_markets, args=(etick,), daemon=True).start()
        else:
            self.result = self._markets[idx]["ticker"]
            self.destroy()

    def _bg_markets(self, event_ticker):
        try:
            data           = rest_get("/markets", {"event_ticker": event_ticker, "limit": 100})
            self._markets  = data.get("markets", [])
            self.after(0, self._show_markets)
        except Exception as exc:
            self.after(0, lambda: self._status.set(f"Error: {exc}"))

    def _show_markets(self):
        if not self._markets:
            self._status.set("No markets found for this event.")
            return
        if len(self._markets) == 1:
            self.result = self._markets[0]["ticker"]
            self.destroy()
            return
        self._mode = "markets"
        self._lb.delete(0, "end")
        for m in self._markets:
            tk_  = m.get("ticker", "")
            sub  = m.get("subtitle", m.get("title", ""))
            self._lb.insert("end", f"{tk_:<52}  {sub[:28]}")
        self._status.set(
            f"{len(self._markets)} market(s).  Double-click or Select to open a book."
        )


# ── BTC15 contract history tab ────────────────────────────────────────────────

class HistoryFrame(ttk.Frame):
    """Tab that accumulates one summary row per completed BTC15 contract."""

    def __init__(self, parent_notebook):
        super().__init__(parent_notebook)
        self._records: list[dict] = []
        self._build_ui()

    def _build_ui(self):
        hdr = ttk.Frame(self)
        hdr.pack(fill="x", padx=12, pady=8)
        ttk.Label(hdr, text="BTC 15M — Contract History",
                  font=("Consolas", 13, "bold")).pack(side="left")
        ttk.Button(hdr, text="Export CSV",
                   command=self._export).pack(side="right")

        ttk.Separator(self, orient="horizontal").pack(fill="x", padx=12, pady=4)

        tf = ttk.Frame(self)
        tf.pack(fill="both", expand=True, padx=12, pady=(0, 12))

        cols = ("end_time", "ticker", "side", "cycles", "stops", "open_trig", "result")
        self.tree = ttk.Treeview(tf, columns=cols, show="headings", selectmode="none")

        col_defs = [
            ("end_time",  "End Time",  72,  "center"),
            ("ticker",    "Ticker",    200, "w"),
            ("side",      "Side",      42,  "center"),
            ("cycles",    "Cycles",    52,  "center"),
            ("stops",     "Stops",     52,  "center"),
            ("open_trig", "Open Trig", 68,  "center"),
            ("result",    "Result",    200, "w"),
        ]
        for col, label, w, anchor in col_defs:
            self.tree.heading(col, text=label)
            self.tree.column(col, width=w, anchor=anchor,
                             stretch=(col in ("ticker", "result")))

        sb = ttk.Scrollbar(tf, orient="vertical", command=self.tree.yview)
        self.tree.config(yscrollcommand=sb.set)
        self.tree.pack(side="left", fill="both", expand=True)
        sb.pack(side="left", fill="y")

        self.tree.tag_configure("milestone", background="#4a0a8e", foreground="#ffffff")
        self.tree.tag_configure("good",      foreground="#4ade80")
        self.tree.tag_configure("stop",      foreground="#ff4444")
        self.tree.tag_configure("neutral",   foreground="#fab387")

    def add_record(self, ticker: str, side: str, cycle_count: int,
                   stop_count: int, open_trig: bool):
        """Called by BTC15Frame on each contract roll."""
        now_str = datetime.datetime.now().strftime("%H:%M:%S")

        if cycle_count >= 4:
            result = f"★ {cycle_count} CYCLES ≤40→≥45"
            tag    = "milestone"
        elif cycle_count > 0:
            result = f"{cycle_count} cycle(s) ≤40→≥45"
            tag    = "good"
        elif open_trig:
            result = "trigger hit — target not reached"
            tag    = "stop"
        else:
            result = "no trigger"
            tag    = "neutral"

        if stop_count:
            result += f"  |  {stop_count} stop loss" + ("es" if stop_count > 1 else "")

        row = {
            "end_time":  now_str,
            "ticker":    ticker,
            "side":      side.upper(),
            "cycles":    str(cycle_count),
            "stops":     str(stop_count),
            "open_trig": "yes" if open_trig else "—",
            "result":    result,
        }
        self._records.append(row)
        self.tree.insert("", "end", values=tuple(row.values()), tags=(tag,))
        kids = self.tree.get_children()
        if kids:
            self.tree.see(kids[-1])

    def _export(self):
        import tkinter.filedialog as fd
        if not self._records:
            return
        path = fd.asksaveasfilename(
            defaultextension=".csv",
            filetypes=[("CSV files", "*.csv"), ("All files", "*.*")],
            initialfile=f"btc15_history_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
        )
        if not path:
            return
        fields = ["end_time", "ticker", "side", "cycles", "stops", "open_trig", "result"]
        with open(path, "w", newline="", encoding="utf-8-sig") as f:
            w = csv.DictWriter(f, fieldnames=fields)
            w.writeheader()
            for row in self._records:
                w.writerow({k: row.get(k, "") for k in fields})


# ── BTC trade history tab ─────────────────────────────────────────────────────

class TradeHistoryFrame(ttk.Frame):
    """Tab that records every signal event (trigger / target / stop) from both sides."""

    def __init__(self, parent_notebook):
        super().__init__(parent_notebook)
        self._stats = {
            "cycles_yes": 0,    "cycles_no": 0,
            "stops_yes": 0,     "stops_no": 0,
            "triggers_yes": 0,  "triggers_no": 0,
            "contracts_yes": 0, "contracts_no": 0,
            "milestones_yes": 0,"milestones_no": 0,
        }
        self._cumulative_pnl: float = 0.0
        self._records: list[dict] = []
        self._build_ui()

    def _build_ui(self):
        hdr = ttk.Frame(self)
        hdr.pack(fill="x", padx=12, pady=8)
        ttk.Label(hdr, text="BTC Trade History",
                  font=("Consolas", 13, "bold")).pack(side="left")
        ttk.Button(hdr, text="Export CSV",
                   command=self._export).pack(side="right")
        ttk.Button(hdr, text="Clear Trades",
                   command=self._clear).pack(side="right", padx=4)

        ttk.Separator(self, orient="horizontal").pack(fill="x", padx=12, pady=(0, 4))

        # Stats rows
        sf = ttk.Frame(self)
        sf.pack(fill="x", padx=12, pady=(0, 4))
        self._stats_yes_var   = tk.StringVar(value="YES — Cycles: 0  Stops: 0  Triggers: 0  Contracts: 0  Milestones: 0")
        self._stats_no_var    = tk.StringVar(value="NO  — Cycles: 0  Stops: 0  Triggers: 0  Contracts: 0  Milestones: 0")
        self._stats_total_var = tk.StringVar(value="Total — Cycles: 0  |  Stops: 0  |  Triggers: 0  |  Contracts: 0  |  Milestones: 0")
        ttk.Label(sf, textvariable=self._stats_yes_var,
                  font=("Consolas", 9), foreground="#4ade80").pack(anchor="w")
        ttk.Label(sf, textvariable=self._stats_no_var,
                  font=("Consolas", 9), foreground="#89b4fa").pack(anchor="w")
        ttk.Label(sf, textvariable=self._stats_total_var,
                  font=("Consolas", 9, "bold"), foreground="#fab387").pack(anchor="w")

        ttk.Separator(self, orient="horizontal").pack(fill="x", padx=12, pady=(4, 0))

        # Treeview
        tf = ttk.Frame(self)
        tf.pack(fill="both", expand=True, padx=12, pady=(0, 12))

        cols = ("time", "side", "ticker", "session", "ask", "chg_pct", "event", "duration",
                "depth", "pnl", "btc_mom_1m", "btc_mom_5m", "rvol_5m", "rvol_15m")
        self.tree = ttk.Treeview(tf, columns=cols, show="headings", selectmode="none")

        col_defs = [
            ("time",       "Time",      72,  "center"),
            ("side",       "Side",      40,  "center"),
            ("ticker",     "Ticker",    180, "w"),
            ("session",    "Sn Time",   52,  "center"),
            ("ask",        "Ask",       45,  "center"),
            ("chg_pct",    "Chg%",      72,  "center"),
            ("event",      "Event",     110, "center"),
            ("duration",   "Duration",  52,  "center"),
            ("depth",      "Depth",     120, "center"),
            ("pnl",        "P&L($)",    65,  "center"),
            ("btc_mom_1m", "BTC 1m",   80,  "center"),
            ("btc_mom_5m", "BTC 5m",   80,  "center"),
            ("rvol_5m",    "RVol 5m",  72,  "center"),
            ("rvol_15m",   "RVol 15m", 78,  "center"),
        ]
        for col, label, w, anchor in col_defs:
            self.tree.heading(col, text=label)
            self.tree.column(col, width=w, anchor=anchor,
                             stretch=(col == "ticker"))

        sb = ttk.Scrollbar(tf, orient="vertical", command=self.tree.yview)
        self.tree.config(yscrollcommand=sb.set)
        self.tree.pack(side="left", fill="both", expand=True)
        sb.pack(side="left", fill="y")

        self.tree.tag_configure("event_t",   background="#3d1500", foreground="#ff9944")
        self.tree.tag_configure("event_tgt", background="#003d1a", foreground="#4ade80")
        self.tree.tag_configure("stop_loss", background="#3d0000", foreground="#ff4444")
        self.tree.tag_configure("milestone", background="#4a0a8e", foreground="#ffffff")

    def _update_stats(self):
        s = self._stats
        cy, cn = s["cycles_yes"],    s["cycles_no"]
        sy, sn = s["stops_yes"],     s["stops_no"]
        ty, tn = s["triggers_yes"],  s["triggers_no"]
        ky, kn = s["contracts_yes"], s["contracts_no"]
        my, mn = s["milestones_yes"],s["milestones_no"]
        self._stats_yes_var.set(
            f"YES — Cycles: {cy}  Stops: {sy}  Triggers: {ty}  Contracts: {ky}  Milestones: {my}")
        self._stats_no_var.set(
            f"NO  — Cycles: {cn}  Stops: {sn}  Triggers: {tn}  Contracts: {kn}  Milestones: {mn}")
        pnl_sign = "+" if self._cumulative_pnl >= 0 else ""
        self._stats_total_var.set(
            f"Total — Cycles: {cy+cn}  |  Stops: {sy+sn}  |  Triggers: {ty+tn}"
            f"  |  Contracts: {ky+kn}  |  Milestones: {my+mn}"
            f"  |  Net P&L: ${pnl_sign}{self._cumulative_pnl:.2f}")

    def add_event(self, side: str, ticker: str, time_str: str, session_str: str,
                  ask_str: str, chg_pct: str, event_label: str, dur_str: str = "",
                  analytics: "dict | None" = None):
        """Called by BTC15Frame for every trigger / target / stop / milestone row."""
        if event_label.startswith("TARGET"):
            self._stats[f"cycles_{side}"] += 1
            tag = "event_tgt"
        elif event_label == "STOP LOSS":
            self._stats[f"stops_{side}"] += 1
            tag = "stop_loss"
        elif event_label == "T40":
            self._stats[f"triggers_{side}"] += 1
            tag = "event_t"
        elif event_label == "MILESTONE":
            self._stats[f"milestones_{side}"] += 1
            tag = "milestone"
        else:
            tag = "event_t"

        a = analytics or {}
        pnl_val = a.get("pnl_value")
        if pnl_val is not None:
            self._cumulative_pnl += pnl_val
        self._update_stats()

        row = {
            "time":       time_str,
            "side":       side.upper(),
            "ticker":     ticker,
            "session":    session_str,
            "ask":        ask_str,
            "chg_pct":    chg_pct,
            "event":      event_label,
            "duration":   dur_str,
            "depth":      a.get("depth", "—"),
            "pnl":        a.get("pnl", "—"),
            "btc_mom_1m": a.get("btc_mom_1m", "—"),
            "btc_mom_5m": a.get("btc_mom_5m", "—"),
            "rvol_5m":    a.get("rvol_5m", "—"),
            "rvol_15m":   a.get("rvol_15m", "—"),
        }
        self._records.append(row)
        self.tree.insert("", "end", values=tuple(row.values()), tags=(tag,))
        kids = self.tree.get_children()
        if kids:
            self.tree.see(kids[-1])

    def add_contract_end(self, ticker: str, side: str, cycle_count: int,
                         stop_count: int, open_trig: bool):
        """Increments contract counter — called alongside HistoryFrame.add_record."""
        self._stats[f"contracts_{side}"] += 1
        self._update_stats()

    def _clear(self):
        self._records.clear()
        self.tree.delete(*self.tree.get_children())
        for key in self._stats:
            self._stats[key] = 0
        self._cumulative_pnl = 0.0
        self._update_stats()

    def _export(self):
        import tkinter.filedialog as fd
        if not self._records:
            return
        path = fd.asksaveasfilename(
            defaultextension=".csv",
            filetypes=[("CSV files", "*.csv"), ("All files", "*.*")],
            initialfile=f"btc_trades_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
        )
        if not path:
            return
        fields = ["time", "side", "ticker", "session", "ask", "chg_pct", "event", "duration",
                  "depth", "pnl", "btc_mom_1m", "btc_mom_5m", "rvol_5m", "rvol_15m"]
        with open(path, "w", newline="", encoding="utf-8-sig") as f:
            w = csv.DictWriter(f, fieldnames=fields)
            w.writeheader()
            for row in self._records:
                w.writerow({k: row.get(k, "") for k in fields})


# ── main app window ───────────────────────────────────────────────────────────

class App(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("Kalshi Order Book Watcher")
        self.geometry("860x640")
        self.minsize(600, 420)

        # Start a dedicated asyncio event loop in a background daemon thread
        self._loop = asyncio.new_event_loop()
        threading.Thread(target=self._loop.run_forever, daemon=True).start()

        self._frames: dict[str, BookFrame] = {}
        self._placeholder = None
        self._btc15_yes:          BTC15Frame         | None = None
        self._btc15_no:           BTC15Frame         | None = None
        self._history_frame:      HistoryFrame       | None = None
        self._trade_history_frame: TradeHistoryFrame | None = None

        self._build_ui()
        self.protocol("WM_DELETE_WINDOW", self._quit)

    # ── UI ───────────────────────────────────────────────────────────────────

    def _build_ui(self):
        style = ttk.Style(self)
        try:
            style.theme_use("clam")
        except tk.TclError:
            pass

        # Toolbar
        tb = ttk.Frame(self)
        tb.pack(fill="x", padx=8, pady=6)

        ttk.Button(tb, text="BTC 15M",
                   command=self._open_btc15).pack(side="left", padx=4)
        ttk.Button(tb, text="Search Markets",
                   command=self._open_search).pack(side="left", padx=4)
        ttk.Button(tb, text="Watch Ticker",
                   command=self._open_ticker).pack(side="left", padx=4)
        ttk.Button(tb, text="Close Tab",
                   command=self._close_tab).pack(side="right", padx=4)

        ttk.Separator(self, orient="horizontal").pack(fill="x", padx=6)

        # Notebook (tabs)
        self.nb = ttk.Notebook(self)
        self.nb.pack(fill="both", expand=True, padx=6, pady=6)

        self._show_placeholder()

    def _show_placeholder(self):
        self._placeholder = ttk.Frame(self.nb)
        ttk.Label(
            self._placeholder,
            text='Click  "BTC 15M"  to open the auto-rolling BTC watcher,\n'
                 'or use  "Search Markets" / "Watch Ticker"  for any contract.',
            foreground="gray",
            font=("Segoe UI", 11),
            justify="center",
        ).place(relx=0.5, rely=0.5, anchor="center")
        self.nb.add(self._placeholder, text="  Welcome  ")

    def _remove_placeholder(self):
        if self._placeholder and self._placeholder.winfo_exists():
            try:
                self.nb.forget(self._placeholder)
            except tk.TclError:
                pass
            self._placeholder = None

    # ── open tabs ─────────────────────────────────────────────────────────────

    def _open_btc15(self):
        """Open (or focus) the BTC 15M YES, NO, History, and Trade History tabs."""
        # Passive tabs must exist first so they can receive callbacks
        if not (self._history_frame and self._history_frame.winfo_exists()):
            self._remove_placeholder()
            self._history_frame = HistoryFrame(self.nb)
            self.nb.add(self._history_frame, text="  BTC15 History  ")

        if not (self._trade_history_frame and self._trade_history_frame.winfo_exists()):
            self._remove_placeholder()
            self._trade_history_frame = TradeHistoryFrame(self.nb)
            self.nb.add(self._trade_history_frame, text="  BTC Trade History  ")

        def _combined_end(ticker, side, cycles, stops, open_trig):
            self._history_frame.add_record(ticker, side, cycles, stops, open_trig)
            self._trade_history_frame.add_contract_end(ticker, side, cycles, stops, open_trig)

        if self._btc15_yes and self._btc15_yes.winfo_exists():
            self.nb.select(self._btc15_yes)
        else:
            self._remove_placeholder()
            self._btc15_yes = BTC15Frame(
                self.nb, self._loop, side="yes",
                on_contract_end=_combined_end,
                on_event=self._trade_history_frame.add_event,
            )
            self.nb.add(self._btc15_yes, text="  BTC 15M YES  ")
            self.nb.select(self._btc15_yes)

        if not (self._btc15_no and self._btc15_no.winfo_exists()):
            self._btc15_no = BTC15Frame(
                self.nb, self._loop, side="no",
                on_contract_end=_combined_end,
                on_event=self._trade_history_frame.add_event,
            )
            self.nb.add(self._btc15_no, text="  BTC 15M NO  ")

    def _add_book(self, ticker: str):
        ticker = ticker.strip().upper()
        if not ticker:
            return
        if ticker in self._frames:
            self.nb.select(self._frames[ticker])
            return
        self._remove_placeholder()
        frame = BookFrame(self.nb, ticker, self._loop)
        self._frames[ticker] = frame
        self.nb.add(frame, text=f"  {ticker}  ")
        self.nb.select(frame)

    def _open_search(self):
        dlg = SearchDialog(self)
        self.wait_window(dlg)
        if dlg.result:
            self._add_book(dlg.result)

    def _open_ticker(self):
        ticker = simpledialog.askstring(
            "Watch Ticker",
            "Enter market ticker\n(e.g. KXPGATOUR-THGI26-RMCI):",
            parent=self,
        )
        if ticker:
            self._add_book(ticker)

    # ── close active tab ─────────────────────────────────────────────────────

    def _close_tab(self):
        sel = self.nb.select()
        if not sel:
            return
        widget = self.nb.nametowidget(sel)
        if isinstance(widget, BookFrame):
            widget.stop()
            self._frames.pop(widget.ticker, None)
        elif isinstance(widget, BTC15Frame):
            widget.stop()
            if widget is self._btc15_yes:
                self._btc15_yes = None
            elif widget is self._btc15_no:
                self._btc15_no = None
        elif isinstance(widget, HistoryFrame):
            self._history_frame = None
        elif isinstance(widget, TradeHistoryFrame):
            self._trade_history_frame = None
        self.nb.forget(sel)
        widget.destroy()
        if not self.nb.tabs():
            self._show_placeholder()

    # ── shutdown ─────────────────────────────────────────────────────────────

    def _quit(self):
        for frame in list(self._frames.values()):
            frame.stop()
        if self._btc15_yes is not None:
            self._btc15_yes.stop()
        if self._btc15_no is not None:
            self._btc15_no.stop()
        self._loop.call_soon_threadsafe(self._loop.stop)
        self.destroy()


if __name__ == "__main__":
    App().mainloop()
