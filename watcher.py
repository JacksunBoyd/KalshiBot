import asyncio
import json
import os
import sys
import urllib.parse
import urllib.request

import websockets
from dotenv import load_dotenv
from order_book import OrderBook
from auth import KalshiAuth

load_dotenv()

WS_URL = "wss://api.elections.kalshi.com/trade-api/ws/v2"
REST_BASE = "https://api.elections.kalshi.com/trade-api/v2"

auth = KalshiAuth(
    api_key=os.environ["KALSHI_API_KEY_ID"],
    key_path=os.environ["KALSHI_API_PATH"],
)


def _rest_get(path, params=None):
    full_path = f"/trade-api/v2{path}"
    url = REST_BASE + path
    if params:
        url += "?" + urllib.parse.urlencode(params)
    headers = auth.rest_headers("GET", full_path)
    req = urllib.request.Request(url, headers=headers)
    with urllib.request.urlopen(req) as resp:
        return json.loads(resp.read())


def search_and_pick_ticker(query):
    q = query.lower()
    print(f"\nSearching events for '{query}'...")

    try:
        data = _rest_get("/events", {"limit": 200})
        events = data.get("events", [])
    except Exception as e:
        print(f"Error fetching events: {e}")
        return None

    matches = [
        e for e in events
        if q in e.get("title", "").lower() or q in e.get("event_ticker", "").lower()
    ]

    if not matches:
        print("No matching events found.")
        return None

    print(f"\n{len(matches)} event(s) found:\n")
    shown = matches[:20]
    for i, e in enumerate(shown):
        title = e.get("title", "")
        ticker = e.get("event_ticker", "")
        print(f"  [{i + 1:2}] {title[:60]}  ({ticker})")

    print()
    choice = input("Pick an event (number): ").strip()
    try:
        idx = int(choice) - 1
        if not (0 <= idx < len(shown)):
            print("Invalid choice.")
            return None
    except ValueError:
        print("Invalid choice.")
        return None

    event = shown[idx]
    event_ticker = event["event_ticker"]

    try:
        data = _rest_get("/markets", {"event_ticker": event_ticker, "limit": 100})
        markets = data.get("markets", [])
    except Exception as e:
        print(f"Error fetching markets: {e}")
        return None

    if not markets:
        print("No markets found for this event.")
        return None

    if len(markets) == 1:
        ticker = markets[0]["ticker"]
        print(f"Auto-selecting only market: {ticker}")
        return ticker

    print(f"\n{len(markets)} market(s) in '{event.get('title', event_ticker)}':\n")
    for i, m in enumerate(markets):
        ticker = m.get("ticker", "")
        subtitle = m.get("subtitle", m.get("title", ""))
        print(f"  [{i + 1:2}] {ticker:<45} {subtitle[:40]}")

    print()
    choice = input("Pick a market (number): ").strip()
    try:
        idx = int(choice) - 1
        if 0 <= idx < len(markets):
            return markets[idx]["ticker"]
    except ValueError:
        pass

    print("Invalid choice.")
    return None


async def watch_orderbook(ticker, side="yes", debug=False):
    headers = auth.ws_headers()
    book = OrderBook(ticker)

    print(f"Connecting to {WS_URL}...")
    async with websockets.connect(WS_URL, additional_headers=headers) as ws:
        await ws.send(json.dumps({
            "id": 1,
            "cmd": "subscribe",
            "params": {
                "channels": ["orderbook_delta"],
                "market_tickers": [ticker],
            },
        }))
        print(f"Subscribed to orderbook_delta for {ticker}")

        while True:
            raw = await ws.recv()
            data = json.loads(raw)
            msg_type = data.get("type")

            if debug:
                print(json.dumps(data, indent=2))

            if msg_type == "orderbook_snapshot":
                book.apply_snapshot(data["msg"])
                if not debug:
                    book.display(side)

            elif msg_type == "orderbook_delta":
                book.apply_delta(data["msg"])
                if not debug:
                    book.display(side)

            elif msg_type == "error":
                print(f"Error: {data.get('msg')}")
                break


if __name__ == "__main__":
    args = sys.argv[1:]
    positional = [a for a in args if not a.startswith("--")]
    side = "no" if "--no" in args else "yes"
    debug = "--debug" in args

    if positional:
        ticker = positional[0]
    else:
        query = input("Search markets: ").strip()
        if not query:
            print("No query provided.")
            sys.exit(1)
        ticker = search_and_pick_ticker(query)
        if ticker is None:
            sys.exit(1)
        print()

    asyncio.run(watch_orderbook(ticker, side=side, debug=debug))