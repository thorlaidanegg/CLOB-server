-- auction_clears_at marks when a market's opening call-auction will clear into
-- continuous trading. NULL for markets that opened continuously. It drives the
-- UI's auction phase (countdown + "opens at" reveal); it is informational only —
-- the engine owns the real PreOpen→Auction→Open transition.
ALTER TABLE markets ADD COLUMN IF NOT EXISTS auction_clears_at TIMESTAMPTZ;
