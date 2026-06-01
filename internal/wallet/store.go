package wallet

import (
	"context"
	"errors"

	"github.com/thorlaidanegg/clob/types"
)

// ErrInsufficientCredits is returned when a reservation fails due to low balance.
var ErrInsufficientCredits = errors.New("wallet: insufficient credits")

// Store abstracts wallet persistence. Settlement workers use inline SQL inside
// transactions — they never call Release directly (idempotency invariant).
// Release is available for admin/hook paths only.
type Store interface {
	GetAvailable(ctx context.Context, userID string) (types.Decimal, error)
	Reserve(ctx context.Context, userID string, amount types.Decimal) error
	Release(ctx context.Context, userID string, amount types.Decimal) error
	Credit(ctx context.Context, userID string, amount types.Decimal) error
}
