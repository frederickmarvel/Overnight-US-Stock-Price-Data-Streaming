package main

import (
	"encoding/csv"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestParseArgsNormalizesChoices(t *testing.T) {
	cfg, err := parseArgs([]string{
		"--venue", "IEX",
		"--market-data-type", "Delayed",
		"--place-limit", "buy",
		"--limit-price", "100",
		"--route", "Overnight-Day",
		"--allow-trading",
		"--i-understand-live-trading",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := cfg.validate(); err != nil {
		t.Fatal(err)
	}
	if cfg.venue != "iex" || cfg.marketDataType != "delayed" || cfg.placeLimit != "BUY" || cfg.route != "overnight-day" {
		t.Fatalf("config was not normalized: %+v", cfg)
	}
}

func TestValidateBlocksOrdersWithoutAcknowledgements(t *testing.T) {
	cfg, err := parseArgs([]string{"--place-limit", "BUY", "--limit-price", "100"})
	if err != nil {
		t.Fatal(err)
	}
	err = cfg.validate()
	if err == nil || !strings.Contains(err.Error(), "order blocked") {
		t.Fatalf("expected order acknowledgement error, got %v", err)
	}
}

func TestValidateRequiresLimitPrice(t *testing.T) {
	cfg, err := parseArgs([]string{
		"--place-limit", "SELL",
		"--allow-trading",
		"--i-understand-live-trading",
	})
	if err != nil {
		t.Fatal(err)
	}
	err = cfg.validate()
	if err == nil || !strings.Contains(err.Error(), "--limit-price") {
		t.Fatalf("expected limit price error, got %v", err)
	}
}

func TestParseSymbolsTrimsAndDeduplicates(t *testing.T) {
	symbols, err := parseSymbols("005930, 000660,005930,,035720")
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"005930", "000660", "035720"}
	if strings.Join(symbols, ",") != strings.Join(want, ",") {
		t.Fatalf("unexpected symbols: got %v want %v", symbols, want)
	}
}

func TestValidateRejectsSymbolsForOrders(t *testing.T) {
	cfg, err := parseArgs([]string{
		"--symbols", "005930,000660",
		"--place-limit", "BUY",
		"--limit-price", "100",
		"--allow-trading",
		"--i-understand-live-trading",
	})
	if err != nil {
		t.Fatal(err)
	}
	err = cfg.validate()
	if err == nil || !strings.Contains(err.Error(), "--symbols") {
		t.Fatalf("expected --symbols order error, got %v", err)
	}
}

func TestValidateRejectsNegativeSubscribeInterval(t *testing.T) {
	cfg, err := parseArgs([]string{"--subscribe-interval-ms", "-1"})
	if err != nil {
		t.Fatal(err)
	}
	err = cfg.validate()
	if err == nil || !strings.Contains(err.Error(), "--subscribe-interval-ms") {
		t.Fatalf("expected subscribe interval error, got %v", err)
	}
}

func TestParseArgsSetsSubscribeInterval(t *testing.T) {
	cfg, err := parseArgs([]string{"--subscribe-interval-ms", "125"})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.subscribeInterval != 125*time.Millisecond {
		t.Fatalf("unexpected subscribe interval: %s", cfg.subscribeInterval)
	}
}

func TestBuildStockUppercasesContractFields(t *testing.T) {
	contract := buildStock("aapl", "smart", "usd", "nasdaq")
	if contract.Symbol != "AAPL" || contract.Exchange != "SMART" || contract.Currency != "USD" || contract.PrimaryExchange != "NASDAQ" {
		t.Fatalf("unexpected contract: %+v", contract)
	}
}

func TestBuildKoreanStocksOnKRX(t *testing.T) {
	symbols := []string{
		"005930", // Samsung Electronics
		"000660", // SK hynix
		"005380", // Hyundai Motor
		"035420", // NAVER
		"035720", // Kakao
	}

	for _, symbol := range symbols {
		t.Run(symbol, func(t *testing.T) {
			contract := buildStock(symbol, "krx", "krw", "")
			if contract.Symbol != symbol {
				t.Fatalf("unexpected symbol: %q", contract.Symbol)
			}
			if contract.Exchange != "KRX" || contract.Currency != "KRW" || contract.SecurityType != "STK" {
				t.Fatalf("unexpected KRX contract: %+v", contract)
			}
			if contract.PrimaryExchange != "" {
				t.Fatalf("expected empty primary exchange for KRX contract, got %q", contract.PrimaryExchange)
			}
		})
	}
}

func TestKRXVenueSelectsKRXExchange(t *testing.T) {
	cfg, err := parseArgs([]string{"--venue", "krx", "--currency", "krw", "--symbol", "005930"})
	if err != nil {
		t.Fatal(err)
	}
	if err := cfg.validate(); err != nil {
		t.Fatal(err)
	}
	if selectedExchange(cfg) != "KRX" || cfg.currency != "krw" {
		t.Fatalf("unexpected KRX config: exchange=%s currency=%s", selectedExchange(cfg), cfg.currency)
	}
}

func TestKRXVenueBuildsMultipleStreamSymbols(t *testing.T) {
	cfg, err := parseArgs([]string{"--venue", "krx", "--currency", "KRW", "--symbols", "005930,000660,005380"})
	if err != nil {
		t.Fatal(err)
	}
	if err := cfg.validate(); err != nil {
		t.Fatal(err)
	}
	symbols, err := streamSymbols(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(symbols, ",") != "005930,000660,005380" {
		t.Fatalf("unexpected symbols: %v", symbols)
	}
	for _, symbol := range symbols {
		contract := buildStock(symbol, selectedExchange(cfg), cfg.currency, cfg.primaryExchange)
		if contract.Exchange != "KRX" || contract.Currency != "KRW" {
			t.Fatalf("unexpected contract for %s: %+v", symbol, contract)
		}
	}
}

func TestOpenCSVWritesHeaderOnlyOnce(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ticks.csv")
	file, writer, err := openCSV(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := writer.Write([]string{"ts", "AAPL", "SMART", "1", "2", "3", "4", "5", "6"}); err != nil {
		t.Fatal(err)
	}
	writer.Flush()
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}

	file, writer, err = openCSV(path)
	if err != nil {
		t.Fatal(err)
	}
	writer.Flush()
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}

	handle, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer handle.Close()

	rows, err := csv.NewReader(handle).ReadAll()
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 {
		t.Fatalf("expected header and one data row, got %d rows", len(rows))
	}
	if rows[0][0] != "ts_utc" {
		t.Fatalf("unexpected header: %v", rows[0])
	}
}
