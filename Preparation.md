# IBKR TWS Connection, Deployment, and Streaming Preparation

This report documents how to connect this project to Interactive Brokers Trader Workstation (TWS), deploy the local Python environment, and stream market data through the local TCP API.

## Current Verified Status

The local machine was tested against live TWS:

```bash
python3 ibkr_tws_stream.py --readonly --connection-test
```

Result:

```text
Connected. Server version=176 account(s)=['U25271001']
```

Port checks showed:

- `127.0.0.1:7496` is open and connected to live TWS.
- `127.0.0.1:7497` is closed, which means paper TWS is not currently running.

Conclusion: the TCP connection to live TWS is working.

## Architecture

IBKR TWS or IB Gateway runs locally and acts as the socket server. This Python project acts as the API client.

```text
Python script -> localhost TCP socket -> TWS / IB Gateway -> IBKR account/session
```

For this setup:

- Host: `127.0.0.1`
- Live TWS port: `7496`
- Paper TWS port: `7497`
- Script: `ibkr_tws_stream.py`
- Python library: `ib-insync`

## TWS Configuration

Open TWS and go to:

```text
Edit -> Global Configuration -> API -> Settings
```

Required settings:

- Enable `Enable ActiveX and Socket Clients`.
- Verify `Socket Port` is `7496` for live TWS.
- Keep `Allow connections from localhost only` enabled when the script runs on the same computer.
- Keep `Read-Only API` enabled for streaming-only usage.
- Disable `Read-Only API` only when intentionally placing API orders.

Security recommendation:

- Do not expose the TWS API socket to the public internet.
- Use `127.0.0.1` for local deployment.
- If remote access is ever needed, use a private VPN or SSH tunnel and restrict trusted IPs in TWS.

## Local Deployment

From the project folder:

```bash
cd "/Users/frederickmarvel/NOBI Labs/OvernightData"
```

Create and activate a Python virtual environment:

```bash
python3 -m venv .venv
source .venv/bin/activate
```

Install dependencies:

```bash
pip install -r requirements.txt
```

Verify that the dependency loads:

```bash
python3 -c "import ib_insync; print('ib_insync ok')"
```

## Connection Test

Use the connection test before streaming or trading:

```bash
python ibkr_tws_stream.py --readonly --connection-test
```

Expected successful output looks like:

```text
Connected. Server version=176 account(s)=['U25271001']
```

Optional raw port test:

```bash
nc -vz 127.0.0.1 7496
```

Expected:

```text
Connection to 127.0.0.1 port 7496 [tcp/*] succeeded!
```

## Stream Live Market Data

Basic live stream:

```bash
python ibkr_tws_stream.py --readonly --symbol AAPL --primary-exchange NASDAQ
```

Stream for a fixed duration:

```bash
python ibkr_tws_stream.py --readonly --symbol AAPL --primary-exchange NASDAQ --duration 60
```

Save streamed ticks to CSV:

```bash
python ibkr_tws_stream.py --readonly --symbol AAPL --primary-exchange NASDAQ --csv data/aapl_ticks.csv
```

The CSV will include:

- UTC timestamp
- Symbol
- Exchange
- Bid
- Ask
- Last
- Bid size
- Ask size
- Last size

## Stream Direct Venues: BATS and IEX

The script supports direct venue selection using `--venue`.

IEX:

```bash
python ibkr_tws_stream.py --readonly --symbol AAPL --venue iex
```

BATS:

```bash
python ibkr_tws_stream.py --readonly --symbol AAPL --venue bats
```

The same requests can be written with raw IBKR exchange codes:

```bash
python ibkr_tws_stream.py --readonly --symbol AAPL --exchange IEX
python ibkr_tws_stream.py --readonly --symbol AAPL --exchange BATS
```

Important:

- Direct venue market data depends on the exchange feed and account permissions.
- `SMART` gives IBKR's smart-routed view, while `IEX` and `BATS` ask for that venue directly.
- Your live TWS contract-detail test accepted direct `IEX` and `BATS` for AAPL only when `primaryExchange` was omitted.
- If IBKR returns a market-data subscription error, the TCP connection still works; the missing piece is the market-data entitlement for that requested venue/feed.

Test result on live TWS:

- `--venue iex` connected and qualified AAPL as `exchange='IEX'`, then returned IBKR error `10089`.
- `--venue bats` connected and qualified AAPL as `exchange='BATS'`, then returned IBKR error `10089`.
- This confirms the code path and direct venue contract are working; the remaining blocker is market-data permission/subscription for the requested top-of-book feed.

## Delayed Market Data

If realtime entitlement is unavailable, try delayed market data:

```bash
python ibkr_tws_stream.py --readonly --symbol AAPL --primary-exchange NASDAQ --market-data-type delayed
```

During testing, AAPL returned IBKR error `10089`:

```text
Requested market data requires additional subscription for API
```

That means the TWS socket is working, but the account/session needs the relevant market-data subscription for the requested feed.

## Stream Korean Stocks On KRX

Your live TWS session resolved Samsung Electronics with:

- Symbol: `005930`
- Exchange: `KRX`
- Currency: `KRW`
- Local symbol: `005930.KS`
- conId: `17382528`

Use:

```bash
python ibkr_tws_stream.py --readonly --symbol 005930 --venue krx --currency KRW
```

Or with the raw exchange code:

```bash
python ibkr_tws_stream.py --readonly --symbol 005930 --exchange KRX --currency KRW
```

Test result on live TWS:

- `005930` on `KRX` qualified successfully.
- A 20-second live stream produced quote callbacks.
- Values were unavailable at test time because the market/data session was not active: bid/ask returned `-1.0` with size `0.0`.
- `KSE` and `SMART` did not resolve Korean equities in this TWS session.

## Overnight Streaming

For IBKR Overnight trading data, use the `OVERNIGHT` exchange on eligible US stocks or ETFs:

```bash
python ibkr_tws_stream.py \
  --readonly \
  --symbol AAPL \
  --exchange OVERNIGHT \
  --primary-exchange NASDAQ
```

IBKR states that Overnight market data should use the same `OVERNIGHT` routing as Overnight orders because it can differ from regular `SMART` market data.

For US stocks and ETFs, IBKR lists overnight availability as 8:00 PM ET to 3:50 AM ET.

## Live Order Deployment Precautions

The script can place a guarded limit order, but order placement is blocked unless both acknowledgement flags are supplied:

```text
--allow-trading
--i-understand-live-trading
```

Outside regular trading hours example:

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

IBKR Overnight directed order example:

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

Before enabling live orders:

- Confirm the order manually in TWS first.
- Use very small quantities while testing.
- Use limit orders, especially outside regular trading hours.
- Confirm product eligibility and overnight trading permissions.
- Confirm that `Read-Only API` is disabled only when you intentionally want orders.

## Operational Checklist

Before each session:

- TWS is running and logged in.
- API socket is enabled in TWS.
- TWS socket port is `7496`.
- The script can pass `--connection-test`.
- Market-data subscription is active for the symbols you request.
- The symbol, primary exchange, and routing are correct.
- Orders are tested with conservative limits before any meaningful size is used.

## Troubleshooting

Connection refused:

- TWS is not running.
- TWS API socket is not enabled.
- The socket port does not match.
- You are using paper port `7497` while only live TWS is running.

Error `502`:

- IBKR commonly uses this when the socket cannot be opened.
- Check TWS, API settings, port, firewall, and trusted IPs.

Error `10089`:

- The API connection works.
- The requested market data feed requires an additional subscription or permission.
- Open TWS `Market Data Connections` for the subscription link/details.

No ticks printed:

- The market may be closed for the selected routing.
- The symbol may not be eligible for the selected exchange.
- Market-data permissions may be missing.
- Try `--market-data-type delayed`.
- Confirm the contract manually in TWS.

Duplicate client ID:

- Another API client is already connected with the same client ID.
- Run with a different ID:

```bash
python ibkr_tws_stream.py --readonly --client-id 12 --connection-test
```

## Deployment Options

Recommended local deployment:

- Run TWS and this script on the same Mac.
- Use `127.0.0.1:7496`.
- Keep localhost-only enabled in TWS.

Background streaming example:

```bash
source .venv/bin/activate
python ibkr_tws_stream.py \
  --readonly \
  --symbol AAPL \
  --primary-exchange NASDAQ \
  --csv data/aapl_ticks.csv
```

For long-running production-style deployment, prefer IB Gateway over full TWS because it is lighter, but keep the same safety model: local socket, restricted IPs, connection test first, and careful order controls.

## Files In This Project

- `ibkr_tws_stream.py`: connection, streaming, CSV, and guarded order script.
- `requirements.txt`: Python dependencies.
- `README.md`: quick start guide.
- `memory.md`: implementation memory and tested state.
- `Preparation.md`: this deployment and streaming runbook.
