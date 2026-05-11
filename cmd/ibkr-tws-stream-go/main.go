package main

import (
	"context"
	"encoding/csv"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	ibapi "github.com/hadrianl/ibapi"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var marketDataTypes = map[string]int64{
	"live":           1,
	"frozen":         2,
	"delayed":        3,
	"delayed-frozen": 4,
}

var venueExchanges = map[string]string{
	"smart":     "SMART",
	"iex":       "IEX",
	"bats":      "BATS",
	"byx":       "BYX",
	"edgx":      "EDGX",
	"edgea":     "EDGEA",
	"bex":       "BEX",
	"krx":       "KRX",
	"overnight": "OVERNIGHT",
}

type config struct {
	host                  string
	port                  int
	clientID              int64
	readonly              bool
	timeout               time.Duration
	connectionTest        bool
	symbol                string
	currency              string
	exchange              string
	venue                 string
	primaryExchange       string
	marketDataType        string
	genericTicks          string
	csvPath               string
	duration              time.Duration
	placeLimit            string
	quantity              float64
	limitPrice            float64
	limitPriceSet         bool
	route                 string
	account               string
	allowTrading          bool
	understandLiveTrading bool
}

type tickerState struct {
	bid      float64
	ask      float64
	last     float64
	bidSize  int64
	askSize  int64
	lastSize int64
}

type appWrapper struct {
	ibapi.Wrapper

	mu          sync.Mutex
	accounts    []string
	nextOrderID int64
	ticker      tickerState
	contract    *ibapi.Contract
	writer      *csv.Writer
	csvFile     *os.File
}

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(exitCode(err))
	}
}

func run(args []string) error {
	quietIBAPILogs()

	cfg, err := parseArgs(args)
	if err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return nil
		}
		return err
	}
	if err := cfg.validate(); err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), cfg.timeout)
	defer cancel()

	wrapper := &appWrapper{}
	client := ibapi.NewIbClient(wrapper)
	client.SetContext(ctx)

	if err := client.Connect(cfg.host, cfg.port, cfg.clientID); err != nil {
		return connectError(cfg, err)
	}
	if err := client.HandShake(); err != nil {
		return connectError(cfg, err)
	}

	fmt.Printf("Connected. Server version=%d account(s)=%v\n", client.ServerVersion(), wrapper.accountsSnapshot())
	if cfg.readonly {
		fmt.Fprintln(os.Stderr, "Note: --readonly is enforced by refusing orders in this program; enable Read-Only API in TWS/Gateway for server-side enforcement.")
	}
	if cfg.connectionTest {
		return client.Disconnect()
	}

	if cfg.placeLimit != "" {
		return placeLimitOrder(client, wrapper, cfg)
	}
	return streamMarketData(client, wrapper, cfg)
}

func parseArgs(args []string) (config, error) {
	var cfg config
	fs := flag.NewFlagSet("ibkr-tws-stream-go", flag.ContinueOnError)
	fs.SetOutput(io.Discard)

	timeout := fs.Float64("timeout", 10, "Connection timeout in seconds.")
	duration := fs.Float64("duration", 0, "Seconds to stream. 0 means run until Ctrl-C.")
	fs.StringVar(&cfg.host, "host", "127.0.0.1", "TWS/Gateway host. Keep local unless you know why.")
	fs.IntVar(&cfg.port, "port", 7496, "Live TWS default: 7496. Paper TWS default: 7497.")
	fs.Int64Var(&cfg.clientID, "client-id", 11, "Unique client ID for this program.")
	fs.BoolVar(&cfg.readonly, "readonly", false, "Refuse order placement from this program.")
	fs.BoolVar(&cfg.connectionTest, "connection-test", false, "Connect, print account/session info, then exit.")
	fs.StringVar(&cfg.symbol, "symbol", "AAPL", "Symbol to stream or trade.")
	fs.StringVar(&cfg.currency, "currency", "USD", "Contract currency.")
	fs.StringVar(&cfg.exchange, "exchange", "SMART", "Exchange/routing for market data, e.g. SMART or OVERNIGHT.")
	fs.StringVar(&cfg.venue, "venue", "", "Convenience alias for --exchange.")
	fs.StringVar(&cfg.primaryExchange, "primary-exchange", "", "Primary listing exchange, e.g. NASDAQ, NYSE.")
	fs.StringVar(&cfg.marketDataType, "market-data-type", "live", "Use delayed if your account lacks realtime market data permissions.")
	fs.StringVar(&cfg.genericTicks, "generic-ticks", "", "Optional IBKR generic tick list, comma-separated.")
	fs.StringVar(&cfg.csvPath, "csv", "", "Optional path to append streamed tick rows as CSV.")
	fs.StringVar(&cfg.placeLimit, "place-limit", "", "Submit one limit order instead of data-only mode. BUY or SELL.")
	fs.Float64Var(&cfg.quantity, "quantity", 1, "Order quantity for --place-limit.")
	fs.Float64Var(&cfg.limitPrice, "limit-price", math.NaN(), "Limit price for --place-limit.")
	fs.StringVar(&cfg.route, "route", "smart", "Order routing style: smart, outside-rth, overnight, overnight-day.")
	fs.StringVar(&cfg.account, "account", "", "Optional IBKR account ID to assign on the order.")
	fs.BoolVar(&cfg.allowTrading, "allow-trading", false, "Required before any order can be submitted.")
	fs.BoolVar(&cfg.understandLiveTrading, "i-understand-live-trading", false, "Second required acknowledgement before any order can be submitted.")

	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			fs.SetOutput(os.Stdout)
			fs.Usage()
			return cfg, err
		}
		return cfg, err
	}
	cfg.timeout = secondsDuration(*timeout)
	cfg.duration = secondsDuration(*duration)
	cfg.limitPriceSet = !math.IsNaN(cfg.limitPrice)
	cfg.normalize()
	return cfg, nil
}

func (cfg *config) normalize() {
	cfg.venue = strings.ToLower(cfg.venue)
	cfg.marketDataType = strings.ToLower(cfg.marketDataType)
	cfg.route = strings.ToLower(cfg.route)
	cfg.placeLimit = strings.ToUpper(cfg.placeLimit)
}

func (cfg config) validate() error {
	if cfg.venue != "" {
		if _, ok := venueExchanges[cfg.venue]; !ok {
			return fmt.Errorf("--venue must be one of: %s", sortedKeys(venueExchanges))
		}
	}
	if _, ok := marketDataTypes[cfg.marketDataType]; !ok {
		return fmt.Errorf("--market-data-type must be one of: %s", sortedKeys(marketDataTypes))
	}
	if cfg.route != "smart" && cfg.route != "outside-rth" && cfg.route != "overnight" && cfg.route != "overnight-day" {
		return errors.New("--route must be one of: smart, outside-rth, overnight, overnight-day")
	}
	if cfg.placeLimit == "" {
		return nil
	}
	if cfg.placeLimit != "BUY" && cfg.placeLimit != "SELL" {
		return errors.New("--place-limit must be BUY or SELL")
	}
	if !cfg.limitPriceSet {
		return errors.New("--limit-price is required with --place-limit")
	}
	if cfg.readonly {
		return errors.New("cannot place orders with --readonly")
	}
	if !cfg.allowTrading || !cfg.understandLiveTrading {
		return errors.New("order blocked. Add both --allow-trading and --i-understand-live-trading after testing in paper")
	}
	return nil
}

func streamMarketData(client *ibapi.IbClient, wrapper *appWrapper, cfg config) error {
	contract := buildStock(cfg.symbol, selectedExchange(cfg), cfg.currency, cfg.primaryExchange)
	csvFile, csvWriter, err := openCSV(cfg.csvPath)
	if err != nil {
		_ = client.Disconnect()
		return err
	}
	if csvFile != nil {
		defer csvFile.Close()
	}

	wrapper.setStream(&contract, csvFile, csvWriter)
	if err := client.Run(); err != nil {
		_ = client.Disconnect()
		return err
	}

	reqID := client.GetReqID()
	client.ReqMarketDataType(marketDataTypes[strings.ToLower(cfg.marketDataType)])
	client.ReqMktData(reqID, &contract, cfg.genericTicks, false, false, nil)
	fmt.Printf("Streaming %s@%s via %s:%d marketDataType=%s. Press Ctrl-C to stop.\n", contract.Symbol, contract.Exchange, cfg.host, cfg.port, cfg.marketDataType)

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(stop)

	if cfg.duration > 0 {
		select {
		case <-time.After(cfg.duration):
		case <-stop:
			fmt.Println("\nStopping stream.")
		}
	} else {
		<-stop
		fmt.Println("\nStopping stream.")
	}

	client.CancelMktData(reqID)
	time.Sleep(200 * time.Millisecond)
	return client.Disconnect()
}

func placeLimitOrder(client *ibapi.IbClient, wrapper *appWrapper, cfg config) error {
	exchange := "SMART"
	outsideRTH := false
	switch cfg.route {
	case "overnight":
		exchange = "OVERNIGHT"
	case "outside-rth":
		outsideRTH = true
	case "overnight-day":
		outsideRTH = true
	}

	contract := buildStock(cfg.symbol, exchange, cfg.currency, cfg.primaryExchange)
	order := ibapi.NewLimitOrder(strings.ToUpper(cfg.placeLimit), cfg.limitPrice, cfg.quantity)
	order.OutsideRTH = outsideRTH
	if cfg.account != "" {
		order.Account = cfg.account
	}

	if err := client.Run(); err != nil {
		_ = client.Disconnect()
		return err
	}

	orderID := wrapper.GetNextOrderID()
	fmt.Printf("Submitting %s %.6g %s limit=%.6g exchange=%s outsideRth=%v\n", order.Action, order.TotalQuantity, contract.Symbol, order.LimitPrice, contract.Exchange, order.OutsideRTH)
	if cfg.route == "overnight-day" {
		fmt.Fprintln(os.Stderr, "Warning: this Go IB API wrapper does not expose includeOvernight; verify the routed order in TWS before live use.")
	}
	client.PlaceOrder(orderID, &contract, order)
	time.Sleep(2 * time.Second)
	return client.Disconnect()
}

func buildStock(symbol, exchange, currency, primaryExchange string) ibapi.Contract {
	return ibapi.Contract{
		Symbol:          strings.ToUpper(symbol),
		SecurityType:    "STK",
		Exchange:        strings.ToUpper(exchange),
		Currency:        strings.ToUpper(currency),
		PrimaryExchange: strings.ToUpper(primaryExchange),
	}
}

func selectedExchange(cfg config) string {
	if cfg.venue != "" {
		return venueExchanges[strings.ToLower(cfg.venue)]
	}
	return cfg.exchange
}

func openCSV(path string) (*os.File, *csv.Writer, error) {
	if path == "" {
		return nil, nil, nil
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil && filepath.Dir(path) != "." {
		return nil, nil, err
	}
	existed := false
	if info, err := os.Stat(path); err == nil && info.Size() > 0 {
		existed = true
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, nil, err
	}
	writer := csv.NewWriter(file)
	if !existed {
		if err := writer.Write([]string{"ts_utc", "symbol", "exchange", "bid", "ask", "last", "bid_size", "ask_size", "last_size"}); err != nil {
			file.Close()
			return nil, nil, err
		}
		writer.Flush()
	}
	return file, writer, writer.Error()
}

func (w *appWrapper) setStream(contract *ibapi.Contract, file *os.File, writer *csv.Writer) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.contract = contract
	w.csvFile = file
	w.writer = writer
	w.ticker = tickerState{
		bid:  math.NaN(),
		ask:  math.NaN(),
		last: math.NaN(),
	}
}

func (w *appWrapper) ManagedAccounts(accountsList []string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.accounts = append([]string(nil), accountsList...)
}

func (w *appWrapper) NextValidID(reqID int64) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.nextOrderID = reqID
}

func (w *appWrapper) GetNextOrderID() int64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	id := w.nextOrderID
	w.nextOrderID++
	return id
}

func (w *appWrapper) TickPrice(reqID int64, tickType int64, price float64, attrib ibapi.TickAttrib) {
	w.mu.Lock()
	defer w.mu.Unlock()

	switch tickType {
	case ibapi.BID, ibapi.DELAYED_BID:
		w.ticker.bid = price
	case ibapi.ASK, ibapi.DELAYED_ASK:
		w.ticker.ask = price
	case ibapi.LAST, ibapi.DELAYED_LAST:
		w.ticker.last = price
	default:
		return
	}
	w.printTickerLocked()
}

func (w *appWrapper) TickSize(reqID int64, tickType int64, size int64) {
	w.mu.Lock()
	defer w.mu.Unlock()

	switch tickType {
	case ibapi.BID_SIZE, ibapi.DELAYED_BID_SIZE:
		w.ticker.bidSize = size
	case ibapi.ASK_SIZE, ibapi.DELAYED_ASK_SIZE:
		w.ticker.askSize = size
	case ibapi.LAST_SIZE, ibapi.DELAYED_LAST_SIZE:
		w.ticker.lastSize = size
	default:
		return
	}
	w.printTickerLocked()
}

func (w *appWrapper) OrderStatus(orderID int64, status string, filled float64, remaining float64, avgFillPrice float64, permID int64, parentID int64, lastFillPrice float64, clientID int64, whyHeld string, mktCapPrice float64) {
	fmt.Printf("Order status: %s, orderId=%d\n", status, orderID)
}

func (w *appWrapper) Error(reqID int64, errCode int64, errString string) {
	switch errCode {
	case 2104, 2106, 2158, 300:
		fmt.Fprintf(os.Stderr, "IBKR notice reqID=%d code=%d: %s\n", reqID, errCode, errString)
	default:
		fmt.Fprintf(os.Stderr, "IBKR error reqID=%d code=%d: %s\n", reqID, errCode, errString)
	}
}

func (w *appWrapper) accountsSnapshot() []string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([]string(nil), w.accounts...)
}

func (w *appWrapper) printTickerLocked() {
	if w.contract == nil {
		return
	}
	ts := time.Now().UTC().Format(time.RFC3339Nano)
	fmt.Printf("%s %s@%s bid=%s ask=%s last=%s bidSize=%d askSize=%d lastSize=%d\n",
		ts,
		w.contract.Symbol,
		w.contract.Exchange,
		formatFloat(w.ticker.bid),
		formatFloat(w.ticker.ask),
		formatFloat(w.ticker.last),
		w.ticker.bidSize,
		w.ticker.askSize,
		w.ticker.lastSize,
	)

	if w.writer == nil {
		return
	}
	_ = w.writer.Write([]string{
		ts,
		w.contract.Symbol,
		w.contract.Exchange,
		formatFloat(w.ticker.bid),
		formatFloat(w.ticker.ask),
		formatFloat(w.ticker.last),
		fmt.Sprint(w.ticker.bidSize),
		fmt.Sprint(w.ticker.askSize),
		fmt.Sprint(w.ticker.lastSize),
	})
	w.writer.Flush()
}

func formatFloat(v float64) string {
	if math.IsNaN(v) {
		return "nan"
	}
	return fmt.Sprintf("%.10g", v)
}

func secondsDuration(v float64) time.Duration {
	return time.Duration(v * float64(time.Second))
}

func quietIBAPILogs() {
	cfg := zap.NewProductionConfig()
	cfg.Level = zap.NewAtomicLevelAt(zapcore.FatalLevel)
	_ = ibapi.SetAPILogger(cfg)
}

func connectError(cfg config, err error) error {
	return fmt.Errorf("could not connect to IBKR at %s:%d: %w\nCheck that TWS/Gateway is logged in, API sockets are enabled, and the port matches", cfg.host, cfg.port, err)
}

func exitCode(err error) int {
	if err == nil || errors.Is(err, flag.ErrHelp) {
		return 0
	}
	return 2
}

func sortedKeys[V any](m map[string]V) string {
	keys := make([]string, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	// Tiny static maps; insertion sort keeps this dependency-free.
	for i := 1; i < len(keys); i++ {
		for j := i; j > 0 && keys[j] < keys[j-1]; j-- {
			keys[j], keys[j-1] = keys[j-1], keys[j]
		}
	}
	return strings.Join(keys, ", ")
}
