# Wallet & Settlement Model

**Status:** canonical spec for credit/position accounting in `clob-server`.
**Scope:** virtual credits only. No real money, no margin, no leverage.

This document is the source of truth for how credits move. The pre-order hook and
the settlement worker MUST match it exactly. It exists because the original
implementation handled maker/taker fills inconsistently (a resting buy refunded
its own reservation; a sell never received proceeds).

---

## 1. Model: long-only spot

- Each user holds a **credit wallet**: `available` and `reserved` (both ≥ 0).
- Each user holds a **position** per market: `quantity` (≥ 0 — long only),
  `avg_entry_price`, `realised_pnl`.
- **Credits are the only currency.** A position is acquired by spending credits
  (buying) and liquidated for credits (selling). There is no separate asset
  balance and **no shorting** — you cannot sell more than you hold.

This matches the portfolio worker, which records `realised_pnl = (sellPrice −
avgEntry) × qty` on sells and a volume-weighted average entry on buys.

---

## 2. Order placement (PreOrderHook)

The hook runs before an order reaches the matching engine.

### Buy (bid)

Reserve the maximum credits the order could cost, moving them `available →
reserved`:

| Order type | Reserved amount |
|------------|-----------------|
| limit      | `price × qty` (exact) |
| market     | `bestAsk × qty × 2` (BBO estimate with a 2× slippage buffer); if no BBO, accept and let settlement reconcile |

The per-unit reservation (`reserved ÷ qty`) is written to `orders.reserved_per_unit`
synchronously so settlement can release the exact amount. **Buys always have
`reserved_per_unit > 0`.**

Reject if `available < required`.

### Sell (ask)

A sell liquidates a position the user already holds. It reserves **no credits**.
Instead the hook validates the user holds enough quantity:

```
held = position.quantity for (user, market)
reject if held < qty        // long-only: cannot sell what you don't own
```

`reserved_per_unit` stays `0` for sells. This single fact (`reserved_per_unit > 0
⟺ a buy that reserved credits`) drives all release logic below.

---

## 3. Settlement on fill (TradeFill)

Settlement branches on **`fill.Side`**, never on maker/taker role. Maker and taker
buyers are accounted identically; the only difference is the fee already baked
into `fill.Fee`.

### Buyer (`fill.Side == bid`)

The reservation is consumed and the actual cost is paid:

```
reservationForFill = reserved_per_unit × filledQty   // what the hook locked
cost               = fillPrice × filledQty + fee

UPDATE wallets SET
  reserved  = reserved  − reservationForFill,
  available = available + reservationForFill − cost
WHERE user_id = buyer AND reserved >= reservationForFill
```

- Net change to total balance (`available + reserved`) = **−cost**. The buyer pays.
- `available += reservationForFill − cost` refunds any over-reservation —
  price improvement for a taker, or the unused 2× market-buy buffer.
- For a maker buyer, `fillPrice = reserved_per_unit` (they fill at their own
  resting price), so the refund is exactly `−fee`: total balance falls by
  `price×qty + fee`, the true cost. Correct.

### Seller (`fill.Side == ask`)

The seller never reserved credits; they receive the sale proceeds:

```
proceeds = fillPrice × filledQty − fee

UPDATE wallets SET
  available = available + proceeds
WHERE user_id = seller
```

- Net change to total balance = **+proceeds**. The seller is paid.
- `reserved` is untouched (sellers hold no reservation).
- Realised PnL is implicit and consistent: the seller paid `avgEntry × qty` when
  buying and now receives `fillPrice × qty`, so credit P&L = proceeds − original
  cost = the `realised_pnl` the portfolio worker records.

---

## 4. Cancel / reject / restart

A cancel, reject, or engine restart must return whatever the hook reserved — and
**only buys ever reserved**. Every release path is therefore guarded by
`reserved_per_unit > 0`:

```
if order.reserved_per_unit > 0:        // a buy
    release = reserved_per_unit × remainingQty (cancel/recovery) or origQty (reject)
    reserved  −= release
    available += release
// sells: reserved_per_unit == 0 → nothing to release
```

There is **no `price`-based fallback**. A sell limit order has `price > 0` but
reserved nothing; falling back to `price × qty` would wrongly credit the seller.

---

## 5. Invariants

1. `available ≥ 0` and `reserved ≥ 0` at all times (DB CHECK constraints).
2. `reserved_per_unit > 0` ⟺ the order is a buy that reserved credits.
3. A buy's total balance falls by exactly its fill cost; a sell's available rises
   by exactly its proceeds.
4. Settlement decides credit direction by `fill.Side`, never by maker/taker role.
5. All credit mutations in settlement happen via inline SQL inside the worker's
   single transaction (idempotent with `worker_offsets`); settlement never calls
   `wallet.Release`.
