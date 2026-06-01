package wallet_test

import (
	"context"
	"errors"
	"testing"

	"github.com/thorlaidanegg/clob-server/internal/testsupport"
	"github.com/thorlaidanegg/clob-server/internal/wallet"
	"github.com/thorlaidanegg/clob/types"
)

func dec(s string) types.Decimal { return types.MustDecimal(s, 2) }

func TestWalletStore_CreditAndGetAvailable(t *testing.T) {
	pool := testsupport.RequirePostgres(t)
	store := wallet.NewPgStore(pool, 2)
	ctx := context.Background()

	// Credit auto-creates the wallet.
	if err := store.Credit(ctx, "alice", dec("100.00")); err != nil {
		t.Fatalf("credit: %v", err)
	}
	avail, err := store.GetAvailable(ctx, "alice")
	if err != nil {
		t.Fatalf("get available: %v", err)
	}
	if avail.String() != "100.00" {
		t.Errorf("available = %s, want 100.00", avail.String())
	}

	// Crediting again accumulates.
	if err := store.Credit(ctx, "alice", dec("50.00")); err != nil {
		t.Fatal(err)
	}
	avail, _ = store.GetAvailable(ctx, "alice")
	if avail.String() != "150.00" {
		t.Errorf("available after second credit = %s, want 150.00", avail.String())
	}
}

func TestWalletStore_ReserveSuccessAndInsufficient(t *testing.T) {
	pool := testsupport.RequirePostgres(t)
	store := wallet.NewPgStore(pool, 2)
	ctx := context.Background()

	store.Credit(ctx, "bob", dec("100.00"))

	// Reserve within balance.
	if err := store.Reserve(ctx, "bob", dec("60.00")); err != nil {
		t.Fatalf("reserve: %v", err)
	}
	avail, _ := store.GetAvailable(ctx, "bob")
	if avail.String() != "40.00" {
		t.Errorf("available after reserve = %s, want 40.00", avail.String())
	}

	// Over-reserve must fail with ErrInsufficientCredits and not change balance.
	err := store.Reserve(ctx, "bob", dec("50.00"))
	if !errors.Is(err, wallet.ErrInsufficientCredits) {
		t.Errorf("over-reserve error = %v, want ErrInsufficientCredits", err)
	}
	avail, gaErr := store.GetAvailable(ctx, "bob")
	if gaErr != nil {
		t.Fatalf("GetAvailable after failed reserve errored: %v", gaErr)
	}
	if avail.String() != "40.00" {
		t.Errorf("available must be unchanged after failed reserve, got %s", avail.String())
	}
}

func TestWalletStore_ReleaseReturnsToAvailable(t *testing.T) {
	pool := testsupport.RequirePostgres(t)
	store := wallet.NewPgStore(pool, 2)
	ctx := context.Background()

	store.Credit(ctx, "carol", dec("100.00"))
	store.Reserve(ctx, "carol", dec("70.00"))

	if err := store.Release(ctx, "carol", dec("70.00")); err != nil {
		t.Fatalf("release: %v", err)
	}
	avail, _ := store.GetAvailable(ctx, "carol")
	if avail.String() != "100.00" {
		t.Errorf("available after release = %s, want 100.00", avail.String())
	}
}
