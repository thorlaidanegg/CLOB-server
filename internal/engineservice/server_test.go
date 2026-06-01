package engineservice

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/rs/zerolog"
	enginev1 "github.com/thorlaidanegg/clob-server/proto/engine/v1"
	clobconfig "github.com/thorlaidanegg/clob/config"
	"github.com/thorlaidanegg/clob/engine"
	"github.com/thorlaidanegg/clob/testutil"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

// startGRPC spins up the EngineServer over an in-memory bufconn (no real network)
// and returns a connected generated client.
func startGRPC(t *testing.T) (enginev1.EngineServiceClient, *engine.MultiEngine) {
	t.Helper()
	cfg := testutil.DefaultConfig("BTC-USD")
	multi := engine.NewMultiEngine()
	if err := multi.CreateMarket(cfg); err != nil {
		t.Fatalf("create market: %v", err)
	}
	t.Cleanup(func() { multi.Close() })
	if err := multi.Submit(engine.AdminResumeMarket{MarketID: cfg.MarketID}); err != nil {
		t.Fatalf("resume: %v", err)
	}
	time.Sleep(20 * time.Millisecond)

	lis := bufconn.Listen(1024 * 1024)
	srv := grpc.NewServer()
	enginev1.RegisterEngineServiceServer(srv, NewEngineServer(multi, []clobconfig.MarketConfig{cfg}, zerolog.Nop()))
	go srv.Serve(lis)
	t.Cleanup(srv.Stop)

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { conn.Close() })
	return enginev1.NewEngineServiceClient(conn), multi
}

func TestGRPCServer_GetStatsRoundTrip(t *testing.T) {
	client, _ := startGRPC(t)
	ctx := context.Background()

	// Place a resting bid (precision-0 qty market, price precision 2).
	_, err := client.PlaceOrder(ctx, &enginev1.PlaceOrderRequest{
		MarketId: "BTC-USD", OrderId: "ord_1", UserId: "alice",
		Side: "bid", OrderType: "limit", Price: "100.00", Qty: "5", Tif: "GTC",
	})
	if err != nil {
		t.Fatalf("place order: %v", err)
	}

	// Poll stats until the order is reflected.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		stats, err := client.GetStats(ctx, &enginev1.GetStatsRequest{MarketId: "BTC-USD"})
		if err != nil {
			t.Fatalf("get stats: %v", err)
		}
		if stats.OpenOrders == 1 && stats.BidLevels == 1 {
			if stats.MarketId != "BTC-USD" {
				t.Errorf("marketID = %q", stats.MarketId)
			}
			if stats.NodePoolCapacity == 0 {
				t.Error("nodePoolCapacity should be > 0")
			}
			if stats.State == "" {
				t.Error("state should be populated")
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("stats never reflected the resting order")
}

func TestGRPCServer_GetStatsUnknownMarket(t *testing.T) {
	client, _ := startGRPC(t)
	_, err := client.GetStats(context.Background(), &enginev1.GetStatsRequest{MarketId: "NOPE"})
	if err == nil {
		t.Error("expected NotFound error for unknown market")
	}
}

func TestGRPCServer_GetDepthRoundTrip(t *testing.T) {
	client, _ := startGRPC(t)
	ctx := context.Background()

	_, err := client.PlaceOrder(ctx, &enginev1.PlaceOrderRequest{
		MarketId: "BTC-USD", OrderId: "ord_d", UserId: "alice",
		Side: "bid", OrderType: "limit", Price: "100.00", Qty: "5", Tif: "GTC",
	})
	if err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := client.GetDepth(ctx, &enginev1.GetDepthRequest{MarketId: "BTC-USD", Levels: 10})
		if err != nil {
			t.Fatal(err)
		}
		if len(resp.Bids) == 1 {
			if resp.Bids[0].Price != "100.00" {
				t.Errorf("bid price over gRPC = %q, want 100.00", resp.Bids[0].Price)
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("depth never reflected the resting order")
}
