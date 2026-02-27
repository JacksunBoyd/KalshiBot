class OrderBook:
    def __init__(self, market_ticker):
        self.market_ticker = market_ticker
        self.yes = {}
        self.no = {}

    def apply_snapshot(self, msg):
        self.yes = {price: qty for price, qty in msg.get("yes", [])}
        self.no = {price: qty for price, qty in msg.get("no", [])}

    def apply_delta(self, msg):
        side = self.yes if msg["side"] == "yes" else self.no
        price = msg["price"]
        delta = msg["delta"]
        new_qty = side.get(price, 0) + delta
        if new_qty <= 0:
            side.pop(price, None)
        else:
            side[price] = new_qty

    def best_bid(self, side="yes"):
        book = self.yes if side == "yes" else self.no
        return max(book.keys()) if book else None

    def best_ask(self, side="yes"):
        other = self.no if side == "yes" else self.yes
        return (100 - max(other.keys())) if other else None

    def spread(self, side="yes"):
        bid = self.best_bid(side)
        ask = self.best_ask(side)
        if bid is not None and ask is not None:
            return ask - bid
        return None

    def display(self, side="yes"):
        bid = self.best_bid(side)
        ask = self.best_ask(side)
        spread = self.spread(side)
        label = side.upper()

        print(f"\033[2J\033[H", end="")
        print(f"  {self.market_ticker}  [{label}]")
        print(f"  Best Bid: {bid}c   Best Ask: {ask}c   Spread: {spread}c")
        print()

        # Asks — derived from the opposite side's bids
        other = self.no if side == "yes" else self.yes
        other_sorted = sorted(other.items(), reverse=True)[:8]
        print(f"  -- {label} Asks --")
        for price, qty in reversed(other_sorted):
            ask_price = 100 - price
            print(f"    {ask_price:3d}c  {qty:>8,} contracts")

        print(f"  {'-' * 32}")

        # Bids -- direct from this side
        own = self.yes if side == "yes" else self.no
        own_sorted = sorted(own.items(), reverse=True)
        print(f"  -- {label} Bids --")
        for price, qty in own_sorted[:8]:
            print(f"    {price:3d}¢  {qty:>8,} contracts")

        print()