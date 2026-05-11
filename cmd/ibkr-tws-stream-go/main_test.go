package main

import (
	"encoding/csv"
	"os"
	"path/filepath"
	"strings"
	"testing"
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

func TestBuildStockUppercasesContractFields(t *testing.T) {
	contract := buildStock("aapl", "smart", "usd", "nasdaq")
	if contract.Symbol != "AAPL" || contract.Exchange != "SMART" || contract.Currency != "USD" || contract.PrimaryExchange != "NASDAQ" {
		t.Fatalf("unexpected contract: %+v", contract)
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
