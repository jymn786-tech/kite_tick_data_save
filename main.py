import os
import time
from datetime import datetime, timezone
from kiteconnect import KiteConnect, KiteTicker
import pandas as pd

# === API credentials ===
api_key = "1e1r0zxaaoypoy3z"
access_token = "VTsLoEHvbupAtw6c5A9V5pZLf2WGbnwd"

if not access_token:
    raise RuntimeError("❌ Missing KITE_ACCESS_TOKEN environment variable!")

# === Load symbols from CSV ===
symbols_df = pd.read_csv("Symbols.csv")
SYMBOLS = (
    symbols_df.iloc[:, 0]
    .dropna()
    .astype(str)
    .str.strip()
    .unique()
    .tolist()
)

EXCHANGE = "NFO"
MODE = "full"          # full | ltp | quote
CSV_FILE = "ticks.csv" # save ticks here
SAVE_EVERY = 5         # flush interval (seconds)

# Initialize KiteConnect for resolving tokens
kite = KiteConnect(api_key=api_key)
kite.set_access_token(access_token)


def resolve_tokens(symbols, exchange="NFO"):
    """Resolve tradingsymbols -> instrument_token."""
    instruments = kite.instruments(exchange)
    wanted = set(s.strip().upper() for s in symbols if s.strip())
    mapping = {}
    for inst in instruments:
        tsym = inst.get("tradingsymbol", "").upper()
        if tsym in wanted:
            mapping[inst["instrument_token"]] = tsym
    missing = wanted - set(mapping.values())
    if missing:
        print(f"⚠️ Could not resolve: {', '.join(sorted(missing))}")
    else:
        print("✅ All symbols resolved.")
    return mapping


def run_ws():
    token_to_sym = resolve_tokens(SYMBOLS, EXCHANGE)
    tokens = list(token_to_sym.keys())
    if not tokens:
        print("❌ No tokens resolved. Exiting.")
        return

    ticker = KiteTicker(api_key, access_token)

    ticks_disk = []
    last_save = time.time()

    def on_ticks(ws, ticks):
        nonlocal last_save, ticks_disk
        for t in ticks:
            token = t["instrument_token"]
            symbol = token_to_sym.get(token, str(token))
            ltp = t.get("last_price")
            if ltp is None:
                continue

            ts = t.get("exchange_timestamp") or t.get("last_trade_time") or datetime.now(timezone.utc)
            if hasattr(ts, "tzinfo") and ts.tzinfo is not None:
                ts = ts.astimezone(tz=None).replace(tzinfo=None)

            ltq = t.get("last_traded_quantity", 0)
            atp = t.get("average_price") or 0.0
            vol = t.get("volume") or 0
            oi = t.get("oi") or 0

            bid = t.get("depth", {}).get("buy", [{}])[0].get("price")
            ask = t.get("depth", {}).get("sell", [{}])[0].get("price")

            # Print each tick
            print(f"[TICK] {ts} {symbol} LTP:{ltp} LTQ:{ltq} ATP:{atp} Vol:{vol} OI:{oi} Bid:{bid} Ask:{ask}")

            # Save to buffer
            ticks_disk.append({
                "ts": ts, "symbol": symbol, "token": token,
                "ltp": float(ltp), "ltq": ltq,
                "atp": atp, "volume": vol, "oi": oi,
                "bid": bid, "ask": ask
            })

        # Flush to CSV
        if (time.time() - last_save) >= SAVE_EVERY and ticks_disk:
            df = pd.DataFrame(ticks_disk)
            df.to_csv(CSV_FILE, mode="a", header=not pd.io.common.file_exists(CSV_FILE), index=False)
            ticks_disk.clear()
            last_save = time.time()

    def on_connect(ws, response):
        print("✅ Connected. Subscribing...")
        ws.subscribe(tokens)
        md = ws.MODE_FULL if MODE == "full" else ws.MODE_LTP if MODE == "ltp" else ws.MODE_QUOTE
        ws.set_mode(md, tokens)

    ticker.on_ticks = on_ticks
    ticker.on_connect = on_connect

    print(f"🚀 Starting WebSocket. Subscribing to: {SYMBOLS}")
    ticker.connect(threaded=True)

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("🛑 Stopping...")
        if ticks_disk:
            df = pd.DataFrame(ticks_disk)
            df.to_csv(CSV_FILE, mode="a", header=not pd.io.common.file_exists(CSV_FILE), index=False)
        print("✅ Shutdown complete.")


if __name__ == "__main__":
    run_ws()
