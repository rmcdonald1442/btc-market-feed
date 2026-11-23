import test from "node:test";
import assert from "node:assert/strict";
import {
  calculateMetrics,
  calculateTicketEstimate,
  MODEL_V2,
} from "./finance.mjs";

const books = (spotBid, spotAsk, futureBid, futureAsk) => [
  { id: "spot", bid_cents: spotBid, ask_cents: spotAsk },
  { id: "future", bid_cents: futureBid, ask_cents: futureAsk },
];
const fill = (market, lots, price_cents, fee_cents, clock_ms, side) => ({
  market,
  lots,
  price_cents,
  fee_cents,
  clock_ms,
  side,
});

test("v2 matches entry lots FIFO and excludes the differently priced unmatched spot lot", () => {
  const metrics = calculateMetrics(
    books(1_000_000, 1_000_000, 1_020_000, 1_020_000),
    [
      fill("spot", 100, 1_000_000, 1000, 0, "buy"),
      fill("spot", 100, 2_000_000, 2000, 0, "buy"),
      fill("future", 100, 1_020_000, 1000, 0, "sell"),
    ],
    0,
    MODEL_V2,
  );
  assert.equal(metrics.hedged_btc, 0.1);
  assert.equal(metrics.matched_basis_cents, 2000);
  assert.equal(metrics.matched_entry_fees_cents, 2000);
  assert.equal(metrics.financing_estimate_cents, 658);
  assert.equal(metrics.spot_exit_fee_estimate_cents, 30);
  assert.equal(metrics.projected_pnl_cents, -688);
  assert.equal(metrics.current_mid_mark_pnl_cents, -104000);
});

test("negative basis and entry costs can produce a projected loss", () => {
  const metrics = calculateMetrics(
    books(1_000_000, 1_000_000, 990_000, 990_000),
    [
      fill("spot", 100, 1_000_000, 100, 0, "buy"),
      fill("future", 100, 990_000, 100, 0, "sell"),
    ],
    0,
    MODEL_V2,
  );
  assert.equal(metrics.matched_basis_cents, -1000);
  assert.ok(metrics.projected_pnl_cents < 0);
});

test("ticket estimate uses remaining fixed-expiry financing and explicit exit fee", () => {
  const atExpiry = calculateTicketEstimate({
    spotPriceCents: 1_000_000,
    futurePriceCents: 1_010_000,
    lots: 100,
    clockMs: MODEL_V2.expiry_ms,
  });
  assert.equal(atExpiry.financing_estimate_cents, 0);
  assert.equal(atExpiry.spot_exit_fee_estimate_cents, 30);
  assert.ok(atExpiry.projected_pnl_cents < 1000);
});

test("net P&L rounds once after fractional-cent FIFO fee allocation", () => {
  const metrics = calculateMetrics(
    books(4_999_950, 5_000_050, 5_000_050, 5_000_150),
    [
      fill("spot", 2, 5_000_000, 1, MODEL_V2.expiry_ms, "buy"),
      fill("future", 1, 5_000_100, 2, MODEL_V2.expiry_ms, "sell"),
    ],
    MODEL_V2.expiry_ms,
    MODEL_V2,
  );
  assert.equal(metrics.matched_basis_cents, 0);
  assert.equal(metrics.matched_entry_fees_cents, 3);
  assert.equal(metrics.financing_estimate_cents, 0);
  assert.equal(metrics.spot_exit_fee_estimate_cents, 2);
  assert.equal(metrics.projected_pnl_cents, -4);
});
