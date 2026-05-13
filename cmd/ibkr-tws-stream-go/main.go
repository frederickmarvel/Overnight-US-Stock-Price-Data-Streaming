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
	symbols               string
	currency              string
	exchange              string
	venue                 string
	primaryExchange       string
	marketDataType        string
	genericTicks          string
	csvPath               string
	duration              time.Duration
	subscribeInterval     time.Duration
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

type streamSubscription struct {
	contract       ibapi.Contract
	ticker         tickerState
	requestedAt    time.Time
	firstTickAt    time.Time
	priceCallbacks int64
	sizeCallbacks  int64
	updates        int64
}

type ibErrorEvent struct {
	reqID   int64
	code    int64
	message string
	at      time.Time
}

type appWrapper struct {
	ibapi.Wrapper

	mu          sync.Mutex
	accounts    []string
	nextOrderID int64
	streams     map[int64]*streamSubscription
	errors      []ibErrorEvent
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
	subscribeIntervalMS := fs.Int("subscribe-interval-ms", 25, "Milliseconds to wait between market-data subscriptions.")
	fs.StringVar(&cfg.host, "host", "127.0.0.1", "TWS/Gateway host. Keep local unless you know why.")
	fs.IntVar(&cfg.port, "port", 7496, "Live TWS default: 7496. Paper TWS default: 7497.")
	fs.Int64Var(&cfg.clientID, "client-id", 11, "Unique client ID for this program.")
	fs.BoolVar(&cfg.readonly, "readonly", false, "Refuse order placement from this program.")
	fs.BoolVar(&cfg.connectionTest, "connection-test", false, "Connect, print account/session info, then exit.")
	fs.StringVar(&cfg.symbol, "symbol", "AAPL", "Symbol to stream or trade.")
	fs.StringVar(&cfg.symbols, "symbols", "", "Comma-separated symbols to stream in one API session. For orders, use --symbol.")
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
	cfg.subscribeInterval = time.Duration(*subscribeIntervalMS) * time.Millisecond
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
	if cfg.subscribeInterval < 0 {
		return errors.New("--subscribe-interval-ms must be >= 0")
	}
	if cfg.symbols != "" {
		if _, err := parseSymbols(cfg.symbols); err != nil {
			return err
		}
	}
	if cfg.placeLimit == "" {
		return nil
	}
	if cfg.symbols != "" {
		return errors.New("--symbols is only supported for streaming; use --symbol for orders")
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
	symbols, err := streamSymbols(cfg)
	if err != nil {
		_ = client.Disconnect()
		return err
	}
	contracts := make([]ibapi.Contract, 0, len(symbols))
	for _, symbol := range symbols {
		contracts = append(contracts, buildStock(symbol, selectedExchange(cfg), cfg.currency, cfg.primaryExchange))
	}

	csvFile, csvWriter, err := openCSV(cfg.csvPath)
	if err != nil {
		_ = client.Disconnect()
		return err
	}
	if csvFile != nil {
		defer csvFile.Close()
	}

	wrapper.setStreamOutput(csvFile, csvWriter)
	if err := client.Run(); err != nil {
		_ = client.Disconnect()
		return err
	}

	client.ReqMarketDataType(marketDataTypes[strings.ToLower(cfg.marketDataType)])
	reqIDs := make([]int64, 0, len(contracts))
	for i := range contracts {
		reqID := client.GetReqID()
		reqIDs = append(reqIDs, reqID)
		wrapper.addStream(reqID, contracts[i], time.Now())
		client.ReqMktData(reqID, &contracts[i], cfg.genericTicks, false, false, nil)
		if cfg.subscribeInterval > 0 && i < len(contracts)-1 {
			time.Sleep(cfg.subscribeInterval)
		}
	}
	fmt.Printf("Streaming %s via %s:%d marketDataType=%s. Press Ctrl-C to stop.\n", streamLabel(contracts), cfg.host, cfg.port, cfg.marketDataType)

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

	for _, reqID := range reqIDs {
		client.CancelMktData(reqID)
	}
	time.Sleep(200 * time.Millisecond)
	wrapper.printStreamSummary()
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

func streamSymbols(cfg config) ([]string, error) {
	if cfg.symbols != "" {
		return parseSymbols(cfg.symbols)
	}
	return parseSymbols(cfg.symbol)
}

func parseSymbols(raw string) ([]string, error) {
	var symbols []string
	seen := make(map[string]bool)
	for _, item := range strings.Split(raw, ",") {
		symbol := strings.TrimSpace(item)
		if symbol == "" {
			continue
		}
		if seen[symbol] {
			continue
		}
		seen[symbol] = true
		symbols = append(symbols, symbol)
	}
	if len(symbols) == 0 {
		return nil, errors.New("at least one symbol is required")
	}
	return symbols, nil
}

func streamLabel(contracts []ibapi.Contract) string {
	labels := make([]string, 0, len(contracts))
	for _, contract := range contracts {
		labels = append(labels, fmt.Sprintf("%s@%s", contract.Symbol, contract.Exchange))
	}
	return strings.Join(labels, ", ")
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

func (w *appWrapper) setStreamOutput(file *os.File, writer *csv.Writer) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.csvFile = file
	w.writer = writer
	w.streams = make(map[int64]*streamSubscription)
}

func (w *appWrapper) addStream(reqID int64, contract ibapi.Contract, requestedAt time.Time) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.streams == nil {
		w.streams = make(map[int64]*streamSubscription)
	}
	w.streams[reqID] = &streamSubscription{
		contract:    contract,
		requestedAt: requestedAt,
		ticker: tickerState{
			bid:  math.NaN(),
			ask:  math.NaN(),
			last: math.NaN(),
		},
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

	stream, ok := w.streams[reqID]
	if !ok {
		return
	}
	switch tickType {
	case ibapi.BID, ibapi.DELAYED_BID:
		stream.ticker.bid = price
	case ibapi.ASK, ibapi.DELAYED_ASK:
		stream.ticker.ask = price
	case ibapi.LAST, ibapi.DELAYED_LAST:
		stream.ticker.last = price
	default:
		return
	}
	stream.priceCallbacks++
	stream.updates++
	if stream.firstTickAt.IsZero() {
		stream.firstTickAt = time.Now()
	}
	w.printTickerLocked(stream)
}

func (w *appWrapper) TickSize(reqID int64, tickType int64, size int64) {
	w.mu.Lock()
	defer w.mu.Unlock()

	stream, ok := w.streams[reqID]
	if !ok {
		return
	}
	switch tickType {
	case ibapi.BID_SIZE, ibapi.DELAYED_BID_SIZE:
		stream.ticker.bidSize = size
	case ibapi.ASK_SIZE, ibapi.DELAYED_ASK_SIZE:
		stream.ticker.askSize = size
	case ibapi.LAST_SIZE, ibapi.DELAYED_LAST_SIZE:
		stream.ticker.lastSize = size
	default:
		return
	}
	stream.sizeCallbacks++
	stream.updates++
	if stream.firstTickAt.IsZero() {
		stream.firstTickAt = time.Now()
	}
	w.printTickerLocked(stream)
}

func (w *appWrapper) OrderStatus(orderID int64, status string, filled float64, remaining float64, avgFillPrice float64, permID int64, parentID int64, lastFillPrice float64, clientID int64, whyHeld string, mktCapPrice float64) {
	fmt.Printf("Order status: %s, orderId=%d\n", status, orderID)
}

func (w *appWrapper) Error(reqID int64, errCode int64, errString string) {
	switch errCode {
	case 2104, 2106, 2158, 300:
		fmt.Fprintf(os.Stderr, "IBKR notice reqID=%d code=%d: %s\n", reqID, errCode, errString)
	default:
		w.recordError(reqID, errCode, errString)
		fmt.Fprintf(os.Stderr, "IBKR error reqID=%d code=%d: %s\n", reqID, errCode, errString)
	}
}

func (w *appWrapper) recordError(reqID int64, code int64, message string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.errors = append(w.errors, ibErrorEvent{
		reqID:   reqID,
		code:    code,
		message: message,
		at:      time.Now(),
	})
}

func (w *appWrapper) accountsSnapshot() []string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return append([]string(nil), w.accounts...)
}

func (w *appWrapper) printStreamSummary() {
	w.mu.Lock()
	defer w.mu.Unlock()

	fmt.Println("Stream summary:")
	if len(w.streams) == 0 {
		fmt.Println("  no active streams were registered")
	}
	for reqID, stream := range w.streams {
		latency := "no tick"
		if !stream.firstTickAt.IsZero() {
			latency = stream.firstTickAt.Sub(stream.requestedAt).String()
		}
		fmt.Printf(
			"  reqID=%d %s@%s firstTick=%s updates=%d priceCallbacks=%d sizeCallbacks=%d bid=%s ask=%s last=%s\n",
			reqID,
			stream.contract.Symbol,
			stream.contract.Exchange,
			latency,
			stream.updates,
			stream.priceCallbacks,
			stream.sizeCallbacks,
			formatFloat(stream.ticker.bid),
			formatFloat(stream.ticker.ask),
			formatFloat(stream.ticker.last),
		)
	}
	if len(w.errors) == 0 {
		fmt.Println("  errors=none")
		return
	}
	fmt.Println("  errors:")
	for _, event := range w.errors {
		fmt.Printf("    %s reqID=%d code=%d %s\n", event.at.UTC().Format(time.RFC3339Nano), event.reqID, event.code, event.message)
	}
}

func (w *appWrapper) printTickerLocked(stream *streamSubscription) {
	if stream == nil {
		return
	}
	ts := time.Now().UTC().Format(time.RFC3339Nano)
	fmt.Printf("%s %s@%s bid=%s ask=%s last=%s bidSize=%d askSize=%d lastSize=%d\n",
		ts,
		stream.contract.Symbol,
		stream.contract.Exchange,
		formatFloat(stream.ticker.bid),
		formatFloat(stream.ticker.ask),
		formatFloat(stream.ticker.last),
		stream.ticker.bidSize,
		stream.ticker.askSize,
		stream.ticker.lastSize,
	)

	if w.writer == nil {
		return
	}
	_ = w.writer.Write([]string{
		ts,
		stream.contract.Symbol,
		stream.contract.Exchange,
		formatFloat(stream.ticker.bid),
		formatFloat(stream.ticker.ask),
		formatFloat(stream.ticker.last),
		fmt.Sprint(stream.ticker.bidSize),
		fmt.Sprint(stream.ticker.askSize),
		fmt.Sprint(stream.ticker.lastSize),
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
