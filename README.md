# IBKR TWS Streaming Starter

This repo contains a small Python starter for connecting to Interactive Brokers Trader Workstation (TWS) or IB Gateway over the local TCP socket, streaming realtime market data, and optionally testing guarded limit-order submission. A Go alternative is also available at `cmd/ibkr-tws-stream-go`.

## What IBKR Must Be Running

Install and log in to **Trader Workstation** or **IB Gateway** before running the script. In TWS, open:

`Edit -> Global Configuration -> API -> Settings`

Enable:

- `Enable ActiveX and Socket Clients`
- Keep `Allow connections from localhost only` enabled when running this script on the same machine.
- Your live TWS is expected to listen on `7496`. Paper TWS usually listens on `7497`.
- For order placement, disable `Read-Only API` only after paper testing.

IBKR documents the API as a socket connection where TWS/Gateway is the local server and your script is the client. If you see connection error `502`, the usual causes are that TWS/Gateway is not running, the API socket is disabled, or the port does not match.

## Install

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Go Alternative

The Go implementation mirrors the Python CLI and uses the native TWS socket API through `github.com/hadrianl/ibapi`.

Run tests:

```bash
go test ./...
```

Build the Go command:

```bash
go build ./cmd/ibkr-tws-stream-go
```

Test the live TWS connection:

```bash
go run ./cmd/ibkr-tws-stream-go --readonly --connection-test
```

Stream realtime data:

```bash
go run ./cmd/ibkr-tws-stream-go --readonly --symbol AAPL --primary-exchange NASDAQ
```

Stream delayed data:

```bash
go run ./cmd/ibkr-tws-stream-go --readonly --symbol AAPL --market-data-type delayed
```

Save ticks to CSV:

```bash
go run ./cmd/ibkr-tws-stream-go --readonly --symbol AAPL --csv data/aapl_ticks.csv
```

Guarded outside-regular-hours limit order:

```bash
go run ./cmd/ibkr-tws-stream-go \
  --symbol AAPL \
  --primary-exchange NASDAQ \
  --place-limit BUY \
  --quantity 1 \
  --limit-price 100.00 \
  --route outside-rth \
  --allow-trading \
  --i-understand-live-trading
```

Note: the Python `ib-insync` client exposes a per-connection read-only option. The Go API wrapper used here does not expose the same option, so the Go command treats `--readonly` as a program-level order block. Keep TWS/Gateway `Read-Only API` enabled when you want server-side read-only enforcement.

## Test The Live TWS Connection

This confirms that Python can reach TWS without requesting market data or placing orders:

```bash
python ibkr_tws_stream.py --readonly --connection-test
```

## Stream Realtime Data

Live TWS, realtime data:

```bash
python ibkr_tws_stream.py --readonly --symbol AAPL --primary-exchange NASDAQ
```

If your account does not have realtime market-data permissions, request delayed data:

```bash
python ibkr_tws_stream.py --readonly --symbol AAPL --market-data-type delayed
```

Save ticks to CSV for later analysis:

```bash
python ibkr_tws_stream.py --readonly --symbol AAPL --csv data/aapl_ticks.csv
```

Stream the IBKR Overnight venue for an eligible US stock or ETF:

```bash
python ibkr_tws_stream.py --readonly --symbol AAPL --exchange OVERNIGHT --primary-exchange NASDAQ
```

IBKR notes that Overnight market data should use the same `OVERNIGHT` routing as Overnight orders because it may differ from regular `SMART` market data.

Stream a direct venue such as IEX or BATS:

```bash
python ibkr_tws_stream.py --readonly --symbol AAPL --venue iex
python ibkr_tws_stream.py --readonly --symbol AAPL --venue bats
```

You can also use the raw IBKR exchange code directly:

```bash
python ibkr_tws_stream.py --readonly --symbol AAPL --exchange IEX
python ibkr_tws_stream.py --readonly --symbol AAPL --exchange BATS
```

For your live TWS session, direct AAPL venue contracts qualify when `primaryExchange` is omitted. If you add `--primary-exchange NASDAQ` to direct `IEX` or `BATS`, TWS may reject the contract as an invalid destination.

Stream a Korean stock on KRX, for example Samsung Electronics:

```bash
python ibkr_tws_stream.py --readonly --symbol 005930 --venue krx --currency KRW
```

Your live TWS resolved Samsung Electronics as `005930` on `KRX`, local symbol `005930.KS`. `KSE` did not resolve through this API session.

## Guarded Order Examples

The script will not place an order unless you pass both acknowledgement flags:

- `--allow-trading`
- `--i-understand-live-trading`

Live example for an outside-regular-hours limit order. Use a tiny quantity and a deliberately conservative limit while testing:

```bash
python ibkr_tws_stream.py \
  --symbol AAPL \
  --primary-exchange NASDAQ \
  --place-limit BUY \
  --quantity 1 \
  --limit-price 100.00 \
  --route outside-rth \
  --allow-trading \
  --i-understand-live-trading
```

Live example for an IBKR Overnight directed order:

```bash
python ibkr_tws_stream.py \
  --symbol AAPL \
  --primary-exchange NASDAQ \
  --place-limit BUY \
  --quantity 1 \
  --limit-price 100.00 \
  --route overnight \
  --allow-trading \
  --i-understand-live-trading
```

Before any live order, confirm the TWS order preview manually and confirm account/product permissions.

## Notes For Overnight Trading

- IBKR lists US Stocks and ETFs overnight availability as 8:00 PM ET to 3:50 AM ET.
- API Overnight orders for US stocks use a contract exchange of `OVERNIGHT` and the instrument's `primaryExchange`, such as `NASDAQ` for AAPL.
- Overnight trading requires IBKR permissions and product eligibility.
- Limit orders are safer to test than market orders outside regular hours due to lower liquidity and wider spreads.

## Useful Official References

- TWS API setup: https://www.interactivebrokers.com/campus/ibkr-api-page/twsapi-doc/
- TWS socket connection: https://interactivebrokers.github.io/tws-api/connection.html
- TWS initial API setup and default ports: https://interactivebrokers.github.io/tws-api/initial_setup.html
- API Overnight Trading: https://www.interactivebrokers.com/campus/?p=201561
- IBKR Overnight Trading hours: https://www.interactivebrokers.com/en/trading/us-overnight-trading.php

This code is for technical integration/testing only and is not trading advice.
