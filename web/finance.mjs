export const MODEL_V2 = Object.freeze({
  id: "SIM-BTC-30D",
  version: 2,
  lot_btc: 0.001,
  tick_usd: 1,
  expiry_ms: 30 * 24 * 60 * 60 * 1000,
  annual_financing_rate: 0.08,
  financing_basis: "ACT/365 simple",
  maker_fee_bps: 1,
  taker_fee_bps: 3,
  assumed_spot_exit_fee_bps: 3,
  spot_exit_mark: "current spot midpoint",
  flags: [
    "synthetic market and fills",
    "financing estimate to fixed model expiry",
    "projected payoff assumes spot sale and futures cash settlement use the same final reference",
    "spot exit fee uses current midpoint as an estimate, not a maturity price forecast",
    "excludes margin, funding, slippage, taxes and counterparty risk",
  ],
});

const LOTS_PER_BTC = 1000;
const DAY_MS = 86_400_000;
const rustRound = (value) =>
  value < 0 ? -Math.floor(-value + 0.5) : Math.floor(value + 0.5);
const marketName = (fill) => String(fill.market).toLowerCase();

function legacyMetrics(books, fills) {
  const quantities = [0, 0],
    values = [0, 0],
    fees = [0, 0];
  for (const fill of fills) {
    const i = marketName(fill) === "spot" ? 0 : 1;
    quantities[i] += Number(fill.lots);
    values[i] += Number(fill.price_cents) * Number(fill.lots);
    fees[i] += Number(fill.fee_cents);
  }
  const averages = quantities.map((q, i) => (q ? values[i] / q : null));
  const hedged = Math.min(...quantities);
  let projected = null;
  if (averages[0] != null && averages[1] != null) {
    const matchedFees = quantities.reduce(
      (sum, q, i) => sum + (fees[i] * hedged) / q,
      0,
    );
    projected = rustRound(
      ((averages[1] - averages[0] - 66000) * hedged) / LOTS_PER_BTC -
        matchedFees,
    );
  }
  const spot = books.find((book) => book.id === "spot"),
    future = books.find((book) => book.id === "future");
  const indicated =
    spot?.ask_cents == null || future?.bid_cents == null
      ? null
      : Number(future.bid_cents) -
        Number(spot.ask_cents) -
        66000 -
        Math.trunc(
          ((Number(spot.ask_cents) + Number(future.bid_cents)) * 3) / 10000,
        );
  return {
    spot_btc: quantities[0] / LOTS_PER_BTC,
    future_short_btc: quantities[1] / LOTS_PER_BTC,
    hedged_btc: hedged / LOTS_PER_BTC,
    residual_btc: (quantities[0] - quantities[1]) / LOTS_PER_BTC,
    spot_average_cents: averages[0],
    future_average_cents: averages[1],
    projected_pnl_cents: projected,
    carry_cost_cents_per_btc: 66000,
    fees_paid_cents: fees[0] + fees[1],
    indicated_pnl_cents_per_btc: indicated,
  };
}

export function calculateMetrics(books, fills, clockMs, model) {
  if (!model) return legacyMetrics(books, fills);
  const quantities = { spot: 0, future: 0 },
    values = { spot: 0, future: 0 },
    fees = { spot: 0, future: 0 };
  const queues = { spot: [], future: [] };
  for (const fill of fills) {
    const market = marketName(fill),
      lots = Number(fill.lots),
      price = Number(fill.price_cents);
    quantities[market] += lots;
    values[market] += lots * price;
    fees[market] += Number(fill.fee_cents);
    queues[market].push({ fill, remaining: lots });
  }
  const averages = Object.fromEntries(
    ["spot", "future"].map((m) => [
      m,
      quantities[m] ? values[m] / quantities[m] : null,
    ]),
  );
  const hedgedLots = Math.min(quantities.spot, quantities.future);
  let matchedBasis = 0,
    matchedEntryFees = 0,
    financing = 0,
    exitFees = 0;
  const cursors = { spot: 0, future: 0 };
  while (
    cursors.spot < queues.spot.length &&
    cursors.future < queues.future.length
  ) {
    const spot = queues.spot[cursors.spot],
      future = queues.future[cursors.future];
    const lots = Math.min(spot.remaining, future.remaining),
      quantity = lots / LOTS_PER_BTC;
    const sp = spot.fill,
      fu = future.fill;
    matchedBasis +=
      (Number(fu.price_cents) - Number(sp.price_cents)) * quantity;
    matchedEntryFees +=
      (Number(sp.fee_cents) * lots) / Number(sp.lots) +
      (Number(fu.fee_cents) * lots) / Number(fu.lots);
    const remaining = Math.max(
      0,
      Number(model.expiry_ms) - Number(sp.clock_ms),
    );
    financing +=
      (Number(sp.price_cents) *
        quantity *
        Number(model.annual_financing_rate) *
        remaining) /
      (365 * DAY_MS);
    const spotBook = books.find((book) => book.id === "spot");
    const mid =
      spotBook?.bid_cents == null || spotBook?.ask_cents == null
        ? Number(sp.price_cents)
        : (Number(spotBook.bid_cents) + Number(spotBook.ask_cents)) / 2;
    exitFees +=
      (mid * quantity * Number(model.assumed_spot_exit_fee_bps)) / 10000;
    spot.remaining -= lots;
    future.remaining -= lots;
    if (!spot.remaining) cursors.spot++;
    if (!future.remaining) cursors.future++;
  }
  const mids = Object.fromEntries(
    ["spot", "future"].map((market) => {
      const book = books.find((item) => item.id === market);
      return [
        market,
        book?.bid_cents == null || book?.ask_cents == null
          ? null
          : (Number(book.bid_cents) + Number(book.ask_cents)) / 2,
      ];
    }),
  );
  let mark = -(fees.spot + fees.future);
  for (const fill of fills) {
    const market = marketName(fill),
      mid = mids[market] ?? Number(fill.price_cents);
    const move =
      String(fill.side).toLowerCase() === "buy"
        ? mid - Number(fill.price_cents)
        : Number(fill.price_cents) - mid;
    mark += (move * Number(fill.lots)) / LOTS_PER_BTC;
  }
  const spotBook = books.find((book) => book.id === "spot"),
    futureBook = books.find((book) => book.id === "future");
  const indicated =
    spotBook?.ask_cents == null || futureBook?.bid_cents == null
      ? null
      : Number(futureBook.bid_cents) - Number(spotBook.ask_cents);
  const round = (n) => rustRound(n);
  return {
    spot_btc: quantities.spot / LOTS_PER_BTC,
    future_short_btc: quantities.future / LOTS_PER_BTC,
    hedged_btc: hedgedLots / LOTS_PER_BTC,
    residual_btc: (quantities.spot - quantities.future) / LOTS_PER_BTC,
    spot_average_cents: averages.spot,
    future_average_cents: averages.future,
    matched_basis_cents: round(matchedBasis),
    matched_entry_fees_cents: round(matchedEntryFees),
    financing_estimate_cents: round(financing),
    spot_exit_fee_estimate_cents: round(exitFees),
    projected_pnl_cents: hedgedLots
      ? round(matchedBasis - matchedEntryFees - financing - exitFees)
      : null,
    current_mid_mark_pnl_cents: round(mark),
    fees_paid_cents: fees.spot + fees.future,
    indicated_basis_cents_per_btc: indicated,
  };
}

export function calculateTicketEstimate({
  spotPriceCents,
  futurePriceCents,
  lots,
  clockMs = 0,
  model = MODEL_V2,
  spotFeeBps = model.taker_fee_bps,
  futureFeeBps = model.taker_fee_bps,
}) {
  const quantity = Number(lots) / LOTS_PER_BTC;
  const gross = (Number(futurePriceCents) - Number(spotPriceCents)) * quantity;
  const entryFees =
    ((Number(spotPriceCents) * spotFeeBps +
      Number(futurePriceCents) * futureFeeBps) *
      quantity) /
    10000;
  const remaining = Math.max(0, Number(model.expiry_ms) - Number(clockMs));
  const financing =
    (Number(spotPriceCents) *
      quantity *
      model.annual_financing_rate *
      remaining) /
    (365 * DAY_MS);
  const exitFee =
    (Number(spotPriceCents) * quantity * model.assumed_spot_exit_fee_bps) /
    10000;
  return {
    gross_basis_cents: rustRound(gross),
    entry_fee_estimate_cents: rustRound(entryFees),
    financing_estimate_cents: rustRound(financing),
    spot_exit_fee_estimate_cents: rustRound(exitFee),
    projected_pnl_cents: rustRound(gross - entryFees - financing - exitFee),
  };
}
