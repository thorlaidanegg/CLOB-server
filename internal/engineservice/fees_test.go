package engineservice

import (
	"testing"

	"github.com/rs/zerolog"
	pgstore "github.com/thorlaidanegg/clob-server/internal/store/postgres"
	clobconfig "github.com/thorlaidanegg/clob/config"
	"github.com/thorlaidanegg/clob/fees"
	"github.com/thorlaidanegg/clob/types"
)

func TestParseFeeTiers_Valid(t *testing.T) {
	rows := []pgstore.FeeTierRow{
		{MinVolume: "0.00", MakerFeeRate: "0.0010", TakerFeeRate: "0.0030"},
		{MinVolume: "10000.00", MakerFeeRate: "0.0005", TakerFeeRate: "0.0020"},
		{MinVolume: "100000.00", MakerFeeRate: "-0.0001", TakerFeeRate: "0.0010"},
	}
	tiers, err := parseFeeTiers(rows, 2)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(tiers) != 3 {
		t.Fatalf("got %d tiers, want 3", len(tiers))
	}
	// MinVolume parsed at price precision 2.
	if tiers[1].MinVolume.String() != "10000.00" {
		t.Errorf("tier1 MinVolume = %s, want 10000.00", tiers[1].MinVolume.String())
	}
	// Rates at precision 4, negative maker (rebate) preserved.
	if tiers[2].MakerFeeRate.String() != "-0.0001" {
		t.Errorf("tier2 maker rate = %s, want -0.0001", tiers[2].MakerFeeRate.String())
	}
}

func TestParseFeeTiers_BadDecimal(t *testing.T) {
	_, err := parseFeeTiers([]pgstore.FeeTierRow{
		{MinVolume: "abc", MakerFeeRate: "0.0010", TakerFeeRate: "0.0030"},
	}, 2)
	if err == nil {
		t.Error("expected error for invalid minVolume")
	}
}

func TestRowToMarketConfig_Tiered(t *testing.T) {
	row := pgstore.MarketRow{
		MarketID: "BTC-USD", PricePrecision: 2, QtyPrecision: 2,
		TickSize: 1, LotSize: 1, Features: int(clobconfig.DefaultFeatures()),
		FeeModel: "tiered",
		FeeTiers: []pgstore.FeeTierRow{
			{MinVolume: "0.00", MakerFeeRate: "0.0010", TakerFeeRate: "0.0030"},
			{MinVolume: "10000.00", MakerFeeRate: "0.0005", TakerFeeRate: "0.0020"},
		},
		State: "open",
	}
	cfg, err := rowToMarketConfig(row)
	if err != nil {
		t.Fatalf("rowToMarketConfig: %v", err)
	}
	if cfg.FeeSchedule.FeeModel != clobconfig.FeeModelTiered {
		t.Errorf("FeeModel = %v, want tiered", cfg.FeeSchedule.FeeModel)
	}
	if len(cfg.FeeSchedule.Tiers) != 2 {
		t.Errorf("expected 2 tiers, got %d", len(cfg.FeeSchedule.Tiers))
	}
}

func TestRowToMarketConfig_TieredUnsortedRejected(t *testing.T) {
	// Tiers out of order must fail MarketConfig.Validate.
	row := pgstore.MarketRow{
		MarketID: "BTC-USD", PricePrecision: 2, QtyPrecision: 2,
		TickSize: 1, LotSize: 1, Features: int(clobconfig.DefaultFeatures()),
		FeeModel: "tiered",
		FeeTiers: []pgstore.FeeTierRow{
			{MinVolume: "100.00", MakerFeeRate: "0.0010", TakerFeeRate: "0.0030"},
			{MinVolume: "10.00", MakerFeeRate: "0.0005", TakerFeeRate: "0.0020"},
		},
		State: "open",
	}
	if _, err := rowToMarketConfig(row); err == nil {
		t.Error("expected validation error for unsorted tiers")
	}
}

func TestFeeCalculatorFor_Selection(t *testing.T) {
	flat := clobconfig.MarketConfig{
		MarketID: "FLAT", PricePrecision: 2, QtyPrecision: 2,
		FeeSchedule: clobconfig.FeeSchedule{FeeModel: clobconfig.FeeModelFlat},
	}
	if _, ok := FeeCalculatorFor(flat, nil).(fees.FlatRateFeeCalculator); !ok {
		t.Error("flat market should get FlatRateFeeCalculator")
	}

	tiered := clobconfig.MarketConfig{
		MarketID: "TIER", PricePrecision: 2, QtyPrecision: 2,
		FeeSchedule: clobconfig.FeeSchedule{
			FeeModel: clobconfig.FeeModelTiered,
			Tiers: []clobconfig.FeeTier{
				{MinVolume: types.NewDecimal(0, 2), MakerFeeRate: types.NewDecimal(10, 4), TakerFeeRate: types.NewDecimal(30, 4)},
			},
		},
	}
	vc := NewVolumeCache(nil, []clobconfig.MarketConfig{tiered}, zerolog.Nop())
	if _, ok := FeeCalculatorFor(tiered, vc).(fees.TieredFeeCalculator); !ok {
		t.Error("tiered market with volume cache should get TieredFeeCalculator")
	}

	// Tiered but nil cache → safe fallback to flat.
	if _, ok := FeeCalculatorFor(tiered, nil).(fees.FlatRateFeeCalculator); !ok {
		t.Error("tiered market with nil cache should fall back to flat")
	}
}
