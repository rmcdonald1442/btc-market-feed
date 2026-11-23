// The public inputs to this estimator are aggregate displayed quantity at our
// price and public prints. No order IDs other than our private ID are required.
export class L2QueueEstimator {
  constructor() {
    this.queues = new Map();
  }

  update(
    id,
    {
      beforeLots,
      afterLots,
      previousOwnLots,
      ownLots,
      tradedLots,
      makerFilled,
    },
  ) {
    const externalAfter = Math.max(0, afterLots - ownLots);
    const previousAhead =
      this.queues.get(id) ?? Math.max(0, beforeLots - previousOwnLots);
    const externalBefore = Math.max(0, beforeLots - previousOwnLots);
    // Executions consume the front. New joins go behind us. Allocate
    // unexplained net removals proportionally across remaining external volume.
    // Offsetting adds and cancels within a sampled book update are unobservable.
    const afterTrades = Math.max(0, externalBefore - tradedLots);
    const aheadAfterTrades = Math.max(0, previousAhead - tradedLots);
    const remainingFraction = afterTrades
      ? Math.min(1, externalAfter / afterTrades)
      : 0;
    this.queues.set(
      id,
      makerFilled
        ? 0
        : Math.min(externalAfter, aheadAfterTrades * remainingFraction),
    );
  }

  ahead(id) {
    return this.queues.get(id) ?? null;
  }
  forget(id) {
    this.queues.delete(id);
  }
}

const FLOW_WINDOW_MS = 3000;
// Matches the protective marketable limit used by Command::Hedge in market.rs.
const HEDGE_PROTECTION_CENTS = 10000;
const REASONS = {
  fixed_timer: "Wait for the fixed hedge deadline.",
  risk_exceeds_edge:
    "The recent price risk over the estimated wait exceeds the passive saving.",
  risk_deadline: "The exposure has reached its time limit.",
  insufficient_passive_size:
    "The resting order cannot cover the exposed quantity.",
  no_passive_edge: "Crossing now is at least as favorable after fees.",
  awaiting_queue_update: "Waiting for a queue observation.",
  no_observed_flow: "No recent aggressive flow on the required side.",
  queue_exceeds_budget:
    "At the recent trading pace, the queue would outlast the risk limit.",
  queue_within_budget:
    "At the recent trading pace, the queue could clear before the risk limit.",
};

function levels(book, side) {
  return (side === "sell" ? book?.asks : book?.bids) ?? [];
}

function atPrice(book, side, price) {
  const level = levels(book, side).find(
    (row) => Number(row.price_atoms) === price,
  );
  return Number(level?.qty_atoms ?? 0);
}

function sweepPrice(book, side, lots) {
  const opposite = side === "sell" ? "buy" : "sell";
  const rows = levels(book, opposite);
  const best = Number(rows[0]?.price_atoms);
  let remaining = lots;
  let value = 0;
  for (const row of rows) {
    const price = Number(row.price_atoms);
    if (Math.abs(price - best) > HEDGE_PROTECTION_CENTS) break;
    const take = Math.min(remaining, Number(row.qty_atoms));
    value += price * take;
    remaining -= take;
    if (!remaining) return value / lots;
  }
  return null;
}

function exposureStart(fills) {
  let balance = 0;
  let since = null;
  for (const fill of fills) {
    const previous = Math.sign(balance);
    balance += (fill.market === "spot" ? 1 : -1) * fill.lots;
    if (!balance) since = null;
    else if (Math.sign(balance) !== previous) since = fill.clock_ms;
  }
  return since;
}

function betterPriceLots(book, order) {
  if (!order) return null;
  let total = 0;
  for (const level of levels(book, order.side)) {
    const price = Number(level.price_atoms);
    const isBetter =
      order.side === "sell"
        ? price < order.price_cents
        : price > order.price_cents;
    if (isBetter) total += Number(level.qty_atoms);
  }
  return total;
}

// A risk preference, not an expected loss. Compare the saved spread and fees
// with one recent RMS price move scaled to the time our hedge would rest.
export function delayRisk(history, market, now, etaMs, btc) {
  if (etaMs === null) return null;
  const field = market === "spot" ? "spot_mid_cents" : "future_mid_cents";
  let previous;
  let squaredMoves = 0;
  let observedMs = 0;
  let intervals = 0;
  for (const point of history ?? []) {
    if (
      point.clock_ms < now - 10000 ||
      point.clock_ms > now ||
      !Number.isFinite(point[field])
    )
      continue;
    if (previous && point.clock_ms > previous.clock_ms) {
      squaredMoves += (point[field] - previous[field]) ** 2;
      observedMs += point.clock_ms - previous.clock_ms;
      intervals++;
    }
    previous = point;
  }
  return intervals >= 3
    ? btc * Math.sqrt((etaMs * squaredMoves) / observedMs)
    : null;
}

export class HedgePolicy {
  constructor(mode, deadlineMs) {
    if (!["l3", "l2", "timer"].includes(mode))
      throw new RangeError("Unknown hedge policy mode.");
    this.mode = mode;
    this.deadlineMs = deadlineMs;
    this.estimator = mode === "l2" ? new L2QueueEstimator() : null;
    this.trades = [];
    this.startMs = null;
  }

  observe(before, after, events) {
    this.startMs ??= before.clock_ms;
    const prints = events
      .filter((event) => event.type === "trade")
      .map(({ market, clock_ms, trade }) => ({
        market,
        clock_ms,
        side: trade.maker_side,
        price: trade.price_cents,
        lots: trade.lots,
      }));
    this.trades.push(...prints);
    this.trades = this.trades.filter(
      (print) => print.clock_ms > after.clock_ms - FLOW_WINDOW_MS,
    );
    if (!this.estimator) return;
    for (const order of after.orders) {
      if (!order.remaining_lots) {
        this.estimator.forget(order.id);
        continue;
      }
      const previous = before.orders.find((other) => other.id === order.id);
      const beforeBook = before.books.find((book) => book.id === order.market);
      const afterBook = after.books.find((book) => book.id === order.market);
      const tradedLots = prints.reduce(
        (total, print) =>
          total +
          (print.market === order.market &&
          print.side === order.side &&
          print.price === order.price_cents
            ? print.lots
            : 0),
        0,
      );
      this.estimator.update(order.id, {
        beforeLots: atPrice(beforeBook, order.side, order.price_cents),
        afterLots: atPrice(afterBook, order.side, order.price_cents),
        previousOwnLots: previous?.remaining_lots ?? 0,
        ownLots: order.remaining_lots,
        tradedLots,
        makerFilled: previous && order.filled_lots > previous.filled_lots,
      });
    }
  }

  assess(snapshot) {
    const residual = snapshot.metrics.residual_btc;
    if (!residual) return null;
    const now = snapshot.clock_ms;
    const market = residual > 0 ? "future" : "spot";
    const side = residual > 0 ? "sell" : "buy";
    const book = snapshot.books.find((item) => item.id === market);
    const order = snapshot.orders.find(
      (item) =>
        item.market === market && item.side === side && item.remaining_lots > 0,
    );
    const lots = Math.round(Math.abs(residual) / snapshot.model.lot_btc);
    const elapsed = now - (exposureStart(snapshot.fills) ?? now);
    const remainingMs = Math.max(0, this.deadlineMs - elapsed);
    const better = betterPriceLots(book, order);
    const ahead =
      !order || this.mode === "timer"
        ? null
        : this.estimator
          ? this.estimator.ahead(order.id)
          : (order.ahead_lots ?? null);
    const windowMs = Math.min(FLOW_WINDOW_MS, now - (this.startMs ?? now));
    const volume = this.trades.reduce(
      (total, print) =>
        total +
        (print.market === market &&
        print.side === side &&
        print.clock_ms > now - FLOW_WINDOW_MS
          ? print.lots
          : 0),
      0,
    );
    const pace = windowMs > 0 ? (volume * 1000) / windowMs : 0;
    // A constant-flow estimate of full completion, including better prices
    // and our exposed size. It is not a calibrated fill probability.
    const eta =
      ahead !== null && pace > 0
        ? (1000 * (better + ahead + lots)) / pace
        : null;
    const cross = sweepPrice(book, side, lots);
    const waitRisk = delayRisk(
      snapshot.history,
      market,
      now,
      eta,
      Math.abs(residual),
    );
    const passiveEdge =
      !order || cross === null
        ? null
        : Math.abs(residual) *
          ((side === "sell" ? 1 : -1) * (order.price_cents - cross) +
            (cross * snapshot.model.taker_fee_bps -
              order.price_cents * snapshot.model.maker_fee_bps) /
              10000);
    let action = "wait";
    let reason = "fixed_timer";
    if (remainingMs === 0) {
      action = "cross";
      reason = "risk_deadline";
    } else if (this.mode !== "timer") {
      if (!order || order.remaining_lots < lots) {
        action = "cross";
        reason = "insufficient_passive_size";
      } else if (passiveEdge !== null && passiveEdge <= 0) {
        action = "cross";
        reason = "no_passive_edge";
      } else if (ahead === null) {
        reason = "awaiting_queue_update";
      } else if (!pace) {
        action = "cross";
        reason = "no_observed_flow";
      } else if (eta > remainingMs) {
        action = "cross";
        reason = "queue_exceeds_budget";
      } else if (
        waitRisk !== null &&
        passiveEdge !== null &&
        waitRisk > passiveEdge
      ) {
        action = "cross";
        reason = "risk_exceeds_edge";
      } else {
        reason = "queue_within_budget";
      }
    }
    return {
      mode: this.mode,
      action,
      reason_code: reason,
      reason: REASONS[reason],
      market,
      side,
      clock_ms: now,
      ahead_lots: ahead,
      better_lots: better,
      remaining_lots: lots,
      flow_lots_per_second: pace,
      eta_ms: eta,
      elapsed_ms: elapsed,
      deadline_ms: this.deadlineMs,
      passive_edge_cents: passiveEdge,
      wait_risk_cents: waitRisk,
      estimated_cross_price_cents: cross,
    };
  }
}
