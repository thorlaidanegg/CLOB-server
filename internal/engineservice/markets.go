package engineservice

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
	clobconfig "github.com/thorlaidanegg/clob/config"
	"github.com/thorlaidanegg/clob/types"

	pgstore "github.com/thorlaidanegg/clob-server/internal/store/postgres"
)

// LoadMarkets reads all markets from Postgres and converts them to MarketConfig structs.
func LoadMarkets(ctx context.Context, pool *pgxpool.Pool) ([]clobconfig.MarketConfig, error) {
	rows, err := pgstore.ListMarkets(ctx, pool)
	if err != nil {
		return nil, fmt.Errorf("load markets: %w", err)
	}

	cfgs := make([]clobconfig.MarketConfig, 0, len(rows))
	for _, r := range rows {
		if r.State != "open" && r.State != "halted" {
			continue // skip disabled markets
		}
		cfg, err := rowToMarketConfig(r)
		if err != nil {
			return nil, fmt.Errorf("load markets: market %s: %w", r.MarketID, err)
		}
		cfgs = append(cfgs, cfg)
	}
	return cfgs, nil
}

func rowToMarketConfig(r pgstore.MarketRow) (clobconfig.MarketConfig, error) {
	pp := r.PricePrecision
	qp := r.QtyPrecision

	cfg := clobconfig.MarketConfig{
		MarketID:       types.MarketID(r.MarketID),
		BaseAsset:      r.BaseAsset,
		QuoteAsset:     r.QuoteAsset,
		PricePrecision: pp,
		QtyPrecision:   qp,
		TickSize:       types.NewDecimal(r.TickSize, pp),
		LotSize:        types.NewDecimal(r.LotSize, qp),
		MinOrderQty:    types.NewDecimal(r.MinOrderQty, qp),
		MaxOrderQty:    types.NewDecimal(r.MaxOrderQty, qp),
		MaxOrderValue:  types.NewDecimal(r.MaxOrderValue, pp),
		MaxDepth:       r.MaxDepth,
		Features:       clobconfig.FeatureSet(r.Features),
		FeeSchedule: clobconfig.FeeSchedule{
			MakerFeeRate: types.NewDecimal(r.MakerFeeRate, 4),
			TakerFeeRate: types.NewDecimal(r.TakerFeeRate, 4),
			FeeCurrency:  r.FeeCurrency,
			FeeModel:     clobconfig.FeeModelFlat,
		},
	}

	if r.FeeModel == "tiered" {
		tiers, err := parseFeeTiers(r.FeeTiers, pp)
		if err != nil {
			return clobconfig.MarketConfig{}, fmt.Errorf("market %s: %w", r.MarketID, err)
		}
		cfg.FeeSchedule.FeeModel = clobconfig.FeeModelTiered
		cfg.FeeSchedule.Tiers = tiers
	}

	if r.STPMode != "" {
		switch r.STPMode {
		case "cancel_both":
			cfg.STPMode = clobconfig.STPCancelBoth
		case "cancel_maker":
			cfg.STPMode = clobconfig.STPCancelMaker
		case "cancel_taker":
			cfg.STPMode = clobconfig.STPCancelTaker
		case "decrement_cancel":
			cfg.STPMode = clobconfig.STPDecrementCancel
		}
	}

	if err := cfg.Validate(); err != nil {
		return clobconfig.MarketConfig{}, err
	}
	return cfg, nil
}

// parseFeeTiers converts stored tier rows into config.FeeTier values.
// MinVolume is parsed at the market's price precision (matching the volume
// the VolumeCache reports); rates are parsed at precision 4.
func parseFeeTiers(rows []pgstore.FeeTierRow, pricePrecision uint8) ([]clobconfig.FeeTier, error) {
	tiers := make([]clobconfig.FeeTier, 0, len(rows))
	for i, r := range rows {
		minVol, err := types.ParseDecimal(r.MinVolume, pricePrecision)
		if err != nil {
			return nil, fmt.Errorf("tier %d: invalid minVolume %q: %w", i, r.MinVolume, err)
		}
		makerRate, err := types.ParseDecimal(r.MakerFeeRate, 4)
		if err != nil {
			return nil, fmt.Errorf("tier %d: invalid makerFeeRate %q: %w", i, r.MakerFeeRate, err)
		}
		takerRate, err := types.ParseDecimal(r.TakerFeeRate, 4)
		if err != nil {
			return nil, fmt.Errorf("tier %d: invalid takerFeeRate %q: %w", i, r.TakerFeeRate, err)
		}
		tiers = append(tiers, clobconfig.FeeTier{
			MinVolume:    minVol,
			MakerFeeRate: makerRate,
			TakerFeeRate: takerRate,
		})
	}
	return tiers, nil
}
