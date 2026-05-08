#!/usr/bin/env python3
"""
IBKR TWS / IB Gateway starter for realtime market data and guarded order entry.

This script connects to a locally running Trader Workstation or IB Gateway TCP
socket. It streams quotes for one contract by default, and can optionally place
a single limit order when explicit trading flags are supplied.
"""

from __future__ import annotations

import argparse
import csv
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from ib_insync import IB, LimitOrder, Stock


MARKET_DATA_TYPES = {
    "live": 1,
    "frozen": 2,
    "delayed": 3,
    "delayed-frozen": 4,
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Connect to local IBKR TWS/Gateway, stream data, and optionally submit a guarded limit order."
    )
    parser.add_argument("--host", default="127.0.0.1", help="TWS/Gateway host. Keep local unless you know why.")
    parser.add_argument("--port", type=int, default=7496, help="Live TWS default: 7496. Paper TWS default: 7497.")
    parser.add_argument("--client-id", type=int, default=11, help="Unique client ID for this script.")
    parser.add_argument("--readonly", action="store_true", help="Connect in read-only mode for data-only runs.")
    parser.add_argument("--timeout", type=float, default=10.0, help="Connection timeout in seconds.")
    parser.add_argument("--connection-test", action="store_true", help="Connect, print account/session info, then exit.")

    parser.add_argument("--symbol", default="AAPL", help="Symbol to stream or trade.")
    parser.add_argument("--currency", default="USD", help="Contract currency.")
    parser.add_argument("--exchange", default="SMART", help="Exchange/routing for market data, e.g. SMART or OVERNIGHT.")
    parser.add_argument("--primary-exchange", default="NASDAQ", help="Primary listing exchange, e.g. NASDAQ, NYSE.")
    parser.add_argument(
        "--market-data-type",
        choices=MARKET_DATA_TYPES,
        default="live",
        help="Use delayed if your account lacks realtime market data permissions.",
    )
    parser.add_argument("--generic-ticks", default="", help="Optional IBKR generic tick list, comma-separated.")
    parser.add_argument("--csv", type=Path, help="Optional path to append streamed tick rows as CSV.")
    parser.add_argument("--duration", type=float, default=0.0, help="Seconds to stream. 0 means run until Ctrl-C.")

    parser.add_argument("--place-limit", choices=["BUY", "SELL"], help="Submit one limit order instead of data-only mode.")
    parser.add_argument("--quantity", type=float, default=1, help="Order quantity for --place-limit.")
    parser.add_argument("--limit-price", type=float, help="Limit price for --place-limit.")
    parser.add_argument(
        "--route",
        choices=["smart", "outside-rth", "overnight", "overnight-day"],
        default="smart",
        help="Order routing style. Use overnight for IBKR Overnight directed orders.",
    )
    parser.add_argument("--account", help="Optional IBKR account ID to assign on the order.")
    parser.add_argument("--allow-trading", action="store_true", help="Required before any order can be submitted.")
    parser.add_argument(
        "--i-understand-live-trading",
        action="store_true",
        help="Second required acknowledgement before any order can be submitted.",
    )
    return parser.parse_args()


def build_stock(symbol: str, exchange: str, currency: str, primary_exchange: str) -> Stock:
    contract = Stock(symbol=symbol.upper(), exchange=exchange.upper(), currency=currency.upper())
    if primary_exchange:
        contract.primaryExchange = primary_exchange.upper()
    return contract


def csv_writer(path: Optional[Path]):
    if not path:
        return None, None

    path.parent.mkdir(parents=True, exist_ok=True)
    exists = path.exists() and path.stat().st_size > 0
    handle = path.open("a", newline="")
    writer = csv.writer(handle)
    if not exists:
        writer.writerow(["ts_utc", "symbol", "exchange", "bid", "ask", "last", "bid_size", "ask_size", "last_size"])
        handle.flush()
    return handle, writer


def print_ticker(contract: Stock, ticker, writer=None, csv_handle=None) -> None:
    ts = datetime.now(timezone.utc).isoformat()

    line = (
        f"{ts} {contract.symbol}@{contract.exchange} "
        f"bid={ticker.bid} ask={ticker.ask} last={ticker.last} "
        f"bidSize={ticker.bidSize} askSize={ticker.askSize} lastSize={ticker.lastSize}"
    )
    print(line, flush=True)

    if writer:
        writer.writerow(
            [
                ts,
                contract.symbol,
                contract.exchange,
                ticker.bid,
                ticker.ask,
                ticker.last,
                ticker.bidSize,
                ticker.askSize,
                ticker.lastSize,
            ]
        )
        csv_handle.flush()


def ensure_order_flags(args: argparse.Namespace) -> None:
    if not args.place_limit:
        return
    if args.limit_price is None:
        raise SystemExit("--limit-price is required with --place-limit.")
    if args.readonly:
        raise SystemExit("Cannot place orders with --readonly.")
    if not args.allow_trading or not args.i_understand_live_trading:
        raise SystemExit(
            "Order blocked. Add both --allow-trading and --i-understand-live-trading after testing in paper."
        )


def place_limit_order(ib: IB, args: argparse.Namespace) -> None:
    exchange = "SMART"
    outside_rth = False

    if args.route == "overnight":
        exchange = "OVERNIGHT"
    elif args.route == "outside-rth":
        outside_rth = True
    elif args.route == "overnight-day":
        exchange = "SMART"
        outside_rth = True

    contract = build_stock(args.symbol, exchange, args.currency, args.primary_exchange)
    ib.qualifyContracts(contract)

    order = LimitOrder(args.place_limit, args.quantity, args.limit_price)
    order.outsideRth = outside_rth
    if args.account:
        order.account = args.account

    # Newer TWS API builds expose includeOvernight for SMART + OVERNIGHT style orders.
    if args.route == "overnight-day":
        try:
            setattr(order, "includeOvernight", True)
        except Exception:
            print("Warning: this API wrapper may not support includeOvernight; verify in TWS before live use.")

    print(
        f"Submitting {order.action} {order.totalQuantity} {contract.symbol} "
        f"limit={order.lmtPrice} exchange={contract.exchange} outsideRth={order.outsideRth}"
    )
    trade = ib.placeOrder(contract, order)
    ib.sleep(2)
    print(f"Order status: {trade.orderStatus.status}, orderId={trade.order.orderId}")


def stream_market_data(ib: IB, args: argparse.Namespace) -> None:
    contract = build_stock(args.symbol, args.exchange, args.currency, args.primary_exchange)
    ib.qualifyContracts(contract)
    ib.reqMarketDataType(MARKET_DATA_TYPES[args.market_data_type])
    ticker = ib.reqMktData(contract, genericTickList=args.generic_ticks, snapshot=False, regulatorySnapshot=False)

    csv_handle, writer = csv_writer(args.csv)

    def on_pending_tickers(tickers):
        if ticker in tickers:
            print_ticker(contract, ticker, writer=writer, csv_handle=csv_handle)

    ib.pendingTickersEvent += on_pending_tickers
    print(
        f"Streaming {contract.symbol}@{contract.exchange} via {args.host}:{args.port} "
        f"marketDataType={args.market_data_type}. Press Ctrl-C to stop."
    )

    try:
        if args.duration and args.duration > 0:
            ib.sleep(args.duration)
        else:
            while True:
                ib.sleep(1)
    except KeyboardInterrupt:
        print("\nStopping stream.")
    finally:
        ib.cancelMktData(contract)
        if csv_handle:
            csv_handle.close()


def main() -> int:
    args = parse_args()
    ensure_order_flags(args)

    ib = IB()
    try:
        ib.connect(args.host, args.port, clientId=args.client_id, timeout=args.timeout, readonly=args.readonly)
    except Exception as exc:
        print(f"Could not connect to IBKR at {args.host}:{args.port}: {exc}", file=sys.stderr)
        print("Check that TWS/Gateway is logged in, API sockets are enabled, and the port matches.", file=sys.stderr)
        return 2

    print(f"Connected. Server version={ib.client.serverVersion()} account(s)={ib.managedAccounts()}")
    try:
        if args.connection_test:
            return 0
        if args.place_limit:
            place_limit_order(ib, args)
        else:
            stream_market_data(ib, args)
    finally:
        ib.disconnect()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
