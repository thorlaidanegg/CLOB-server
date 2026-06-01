package engineservice

import (
	clobconfig "github.com/thorlaidanegg/clob/config"
	"github.com/thorlaidanegg/clob/fees"
)

// FeeCalculatorFor returns the fee calculator a market should use.
// Tiered markets get a TieredFeeCalculator backed by the shared VolumeCache;
// all others get the flat-rate calculator. vc may be nil for flat-only setups.
func FeeCalculatorFor(mc clobconfig.MarketConfig, vc *VolumeCache) fees.FeeCalculator {
	if mc.FeeSchedule.FeeModel == clobconfig.FeeModelTiered && len(mc.FeeSchedule.Tiers) > 0 && vc != nil {
		return fees.TieredFeeCalculator{Volume: vc, MarketID: mc.MarketID}
	}
	return fees.FlatRateFeeCalculator{}
}
