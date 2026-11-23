import { calculateTicketEstimate } from "./finance.mjs";
import { delayRisk } from "./hedge-policy.mjs";

const WINDOW_MS = 3000;
const YOUNG_MS = 1000;
const PROTECTION_CENTS = 10000;
const MARKETS = ["spot", "future"];
const ACTIONS = ["rest_both", "cross_spot", "cross_future"];
const restingSide = (market) => (market === "spot" ? "buy" : "sell");
const restingLevels = (book) => (book.id === "spot" ? book.bids : book.asks);
const topPrice = (book) =>
  book.id === "spot" ? book.bid_cents : book.ask_cents;

function sample() {
  return {
    lotMs: 0,
    youngLotMs: 0,
    youngRemoved: 0,
    cancels: 0,
    priority: 0,
    prints: 0,
  };
}

function levelAt(book, price) {
  return restingLevels(book)?.find((row) => Number(row.price_atoms) === price);
}

// Track residence in price-time priority, including while a level is off touch.
// Orders present when observation begins have a lower bound on their true age.
class VisibleOrders {
  constructor(book, clock) {
    this.orders = new Map();
    for (const level of restingLevels(book) ?? []) {
      for (const order of level.queue) {
        this.orders.set(String(order.id), {
          price: Number(level.price_atoms),
          lots: Number(order.qty_atoms),
          since: clock,
        });
      }
    }
  }

  youngLots(queue, clock) {
    let lots = 0;
    for (const row of queue ?? []) {
      const order = this.orders.get(String(row.id));
      if (order && clock - order.since < YOUNG_MS)
        lots += Number(row.qty_atoms);
    }
    return lots;
  }

  observe(book, level, events, start, end, record) {
    // Only priority volume present before the change could have been ahead of
    // an order joining then. Ignore churn of new orders behind that cohort.
    const cohort = new Set();
    for (const row of level?.queue ?? []) {
      const id = String(row.id);
      cohort.add(id);
      const existing = this.orders.get(id);
      if (!existing) throw new Error("Missing public L3 priority history.");
      const youngMs = Math.max(
        0,
        Math.min(end, existing.since + YOUNG_MS) - start,
      );
      record.youngLotMs += Number(row.qty_atoms) * youngMs;
    }
    for (const event of events) {
      if (event.type !== "book" || event.market !== book.id) continue;
      const change = event.change;
      if (change.type === "add") {
        const row = change.order;
        if (row.side === restingSide(book.id))
          this.orders.set(String(row.id), {
            price: row.price_cents,
            lots: row.lots,
            since: event.clock_ms,
          });
        continue;
      }
      const id = String(change.id);
      const old = this.orders.get(id);
      if (!old) continue; // opposite side
      const losesPriority =
        change.type === "amend" &&
        (change.price_cents !== old.price || change.lots > old.lots);
      const removed =
        change.type === "reduce"
          ? change.lots
          : losesPriority
            ? old.lots
            : old.lots - change.lots;
      // The public feed marks maker executions and also publishes their prints.
      // An execution contributes once to aggressor flow, never to cancel hazard.
      if (cohort.has(id) && event.cause !== "fill") {
        if (losesPriority) record.priority += removed;
        else record.cancels += removed;
        if (event.clock_ms - old.since < YOUNG_MS)
          record.youngRemoved += removed;
      }
      if (losesPriority) {
        old.since = event.clock_ms;
        cohort.delete(id);
      }
      if (change.type === "amend") {
        old.price = change.price_cents;
        old.lots = change.lots;
      } else {
        old.lots -= removed;
      }
      if (!old.lots) {
        this.orders.delete(id);
        cohort.delete(id);
      }
    }
  }
}

function sweep(book, lots) {
  const rows = book.id === "spot" ? book.asks : book.bids;
  let remaining = lots;
  let value = 0;
  const touch = Number(rows?.[0]?.price_atoms);
  for (const level of rows ?? []) {
    const price = Number(level.price_atoms);
    if (Math.abs(price - touch) > PROTECTION_CENTS) break;
    const take = Math.min(remaining, Number(level.qty_atoms));
    remaining -= take;
    value += take * price;
    if (!remaining)
      return { cross_limit_cents: price, cross_vwap_cents: value / lots };
  }
  return { cross_limit_cents: null, cross_vwap_cents: null };
}

function executableBook(book) {
  return (
    Number.isSafeInteger(book?.bid_cents) &&
    Number.isSafeInteger(book?.ask_cents) &&
    book.bid_cents > 0 &&
    book.bid_cents < book.ask_cents &&
    Number(book.bids?.[0]?.price_atoms) === book.bid_cents &&
    Number(book.asks?.[0]?.price_atoms) === book.ask_cents
  );
}

const reasons = {
  warmup: "Collecting three market seconds of queue and trade observations.",
  missing_quotes: "Both instruments need a live, two-sided book.",
  insufficient_depth:
    "Displayed depth within the protective limit cannot cover the full size.",
  no_flow: "No recent aggressive trades could fill the resting leg.",
  slow_queue: "The resting queue could outlast the exposure limit.",
  missing_risk:
    "Waiting for three observed midpoint changes to estimate price risk.",
  low_carry:
    "Estimated carry after fees and financing is below the entry threshold.",
  high_risk:
    "Estimated carry after the allowance for price risk is below the entry threshold.",
  rest_both:
    "Both passive queues fit the exposure budget at the best risk-adjusted carry.",
  cross_spot: "Buy the displayed spot offers and rest the futures sale.",
  cross_future: "Sell into the displayed futures bids and rest the spot buy.",
};

function evaluateRoute(action, legs, snapshot, settings, warm) {
  const crossSpot = action === "cross_spot",
    crossFuture = action === "cross_future";
  const { spot, future } = legs;
  const result = {
    action,
    viable: false,
    reason_code: null,
    reason: null,
    carry: null,
    score_cents: null,
    risk_cents: null,
    exposure_ms: null,
    spot_limit_cents: crossSpot
      ? (spot?.cross_limit_cents ?? null)
      : (spot?.price_cents ?? null),
    future_limit_cents: crossFuture
      ? (future?.cross_limit_cents ?? null)
      : (future?.price_cents ?? null),
  };
  function finish(code) {
    return {
      ...result,
      viable: code === action,
      reason_code: code,
      reason: reasons[code],
    };
  }
  if (!spot || !future) return finish("missing_quotes");
  const spotPrice = crossSpot ? spot.cross_vwap_cents : spot.price_cents;
  const futurePrice = crossFuture
    ? future.cross_vwap_cents
    : future.price_cents;
  const model = snapshot.model;
  if (spotPrice !== null && futurePrice !== null) {
    result.carry = calculateTicketEstimate({
      spotPriceCents: spotPrice,
      futurePriceCents: futurePrice,
      lots: settings.lots,
      clockMs: snapshot.clock_ms,
      model,
      spotFeeBps: crossSpot ? model.taker_fee_bps : model.maker_fee_bps,
      futureFeeBps: crossFuture ? model.taker_fee_bps : model.maker_fee_bps,
    });
  }
  if (!warm) return finish("warmup");
  // A full-size immediate fallback hedge should also be visible for any leg
  // we plan to rest. The displayed sweep is an estimate until it executes.
  if (spot.cross_limit_cents === null || future.cross_limit_cents === null)
    return finish("insufficient_depth");
  const resting = crossSpot ? [future] : crossFuture ? [spot] : [spot, future];
  if (resting.some((leg) => leg.fill_eta_ms === null)) return finish("no_flow");
  // Stress either passive leg filling first, including immediately on entry.
  result.exposure_ms = Math.max(...resting.map((leg) => leg.fill_eta_ms));
  const risks = resting.map((leg) =>
    delayRisk(
      snapshot.history,
      leg.market,
      snapshot.clock_ms,
      leg.fill_eta_ms,
      settings.lots * model.lot_btc,
    ),
  );
  if (risks.every((risk) => risk !== null)) {
    result.risk_cents = Math.max(...risks);
    result.score_cents = result.carry.projected_pnl_cents - result.risk_cents;
  }
  if (result.exposure_ms > settings.exposureBudgetMs)
    return finish("slow_queue");
  if (result.carry.projected_pnl_cents < settings.minNetCents)
    return finish("low_carry");
  if (result.risk_cents === null) return finish("missing_risk");
  if (result.score_cents < settings.minNetCents) return finish("high_risk");
  return finish(action);
}

export class EntryPolicy {
  constructor({
    mode = "l3",
    lots = 100,
    minNetCents = 100,
    exposureBudgetMs = 4000,
  } = {}) {
    if (
      !["l2", "l3"].includes(mode) ||
      !Number.isSafeInteger(lots) ||
      lots < 1 ||
      lots > 10000 ||
      !Number.isFinite(minNetCents) ||
      !Number.isFinite(exposureBudgetMs) ||
      exposureBudgetMs <= 0
    )
      throw new RangeError("Invalid entry policy configuration.");
    this.mode = mode;
    this.settings = { lots, minNetCents, exposureBudgetMs };
    this.intervals = [];
    this.visible = new Map();
    this.start = null;
    this.clock = null;
  }

  observe(before, after, events) {
    const start = before.clock_ms,
      end = after.clock_ms;
    if (
      end < start ||
      (this.clock !== null && this.clock !== start) ||
      events.some((ev) => ev.clock_ms < start || ev.clock_ms > end)
    )
      throw new RangeError("Entry observations must be contiguous and causal.");
    this.start ??= start;
    const frame = { start, end };
    for (const market of MARKETS) {
      const oldBook = before.books.find((book) => book.id === market),
        newBook = after.books.find((book) => book.id === market);
      const price = topPrice(oldBook),
        level = levelAt(oldBook, price);
      const visible = Number(level?.qty_atoms ?? 0);
      const record = sample();
      record.lotMs = visible * (end - start);
      let atTouch = 0;
      for (const event of events) {
        if (
          event.type !== "trade" ||
          event.market !== market ||
          event.trade.maker_side !== restingSide(market)
        )
          continue;
        record.prints += event.trade.lots;
        if (event.trade.price_cents === price) atTouch += event.trade.lots;
      }
      if (this.mode === "l3") {
        if (!this.visible.has(market))
          this.visible.set(market, new VisibleOrders(oldBook, start));
        this.visible
          .get(market)
          .observe(oldBook, level, events, start, end, record);
      } else {
        const afterLots = Number(levelAt(newBook, price)?.qty_atoms ?? 0);
        record.cancels = Math.max(0, visible - afterLots - atTouch);
      }
      frame[market] = record;
    }
    this.clock = end;
    this.intervals.push(frame);
    while (this.intervals.length && this.intervals[0].end <= end - WINDOW_MS)
      this.intervals.shift();
  }

  leg(book, now) {
    if (!executableBook(book)) return null;
    const total = sample();
    for (const frame of this.intervals) {
      if (frame.end <= now - WINDOW_MS) continue;
      const stats = frame[book.id];
      const fraction =
        frame.end === frame.start
          ? 0
          : (frame.end - Math.max(frame.start, now - WINDOW_MS)) /
            (frame.end - frame.start);
      total.lotMs += stats.lotMs * fraction;
      total.youngLotMs += stats.youngLotMs * fraction;
      total.cancels += stats.cancels;
      total.priority += stats.priority;
      total.youngRemoved += stats.youngRemoved;
      total.prints += stats.prints;
    }
    const duration = Math.min(WINDOW_MS, now - (this.start ?? now));
    const flow = duration > 0 ? (total.prints * 1000) / duration : 0;
    const touch = restingLevels(book)[0],
      ahead = Number(touch.qty_atoms);
    const exits = total.cancels + total.priority;
    const pooled = total.lotMs ? (exits * 1000) / total.lotMs : 0;
    let turnover = ahead * pooled;
    if (this.mode === "l3" && this.visible.has(book.id)) {
      const young = this.visible.get(book.id).youngLots(touch.queue, now);
      const oldMs = total.lotMs - total.youngLotMs;
      const youngHazard = total.youngLotMs
        ? (total.youngRemoved * 1000) / total.youngLotMs
        : pooled;
      const oldHazard =
        oldMs > 0 ? ((exits - total.youngRemoved) * 1000) / oldMs : pooled;
      turnover = young * youngHazard + (ahead - young) * oldHazard;
    }
    // Trade flow consumes the head; cancellation intensity falls as the
    // original ahead cohort shrinks. Only trades can execute our own order.
    const hazard = ahead ? turnover / ahead : 0;
    const clear = !flow
      ? null
      : hazard > 0
        ? (1000 * Math.log1p((hazard * ahead) / flow)) / hazard
        : (1000 * ahead) / flow;
    return {
      market: book.id,
      side: restingSide(book.id),
      price_cents: topPrice(book),
      visible_ahead_lots: ahead,
      trade_lots_per_second: flow,
      turnover_lots_per_second: turnover,
      queue_clear_ms: clear,
      fill_eta_ms:
        clear === null ? null : clear + (1000 * this.settings.lots) / flow,
      recent_trade_lots: total.prints,
      recent_cancel_lots: total.cancels,
      recent_priority_loss_lots: this.mode === "l3" ? total.priority : null,
      ...sweep(book, this.settings.lots),
    };
  }

  assess(snapshot) {
    const now = snapshot.clock_ms;
    if (this.clock !== null && now !== this.clock)
      throw new RangeError("Assess the latest observed entry checkpoint.");
    const warm = this.start !== null && now - this.start >= WINDOW_MS;
    const legs = Object.fromEntries(
      MARKETS.map((market) => [
        market,
        this.leg(
          snapshot.books.find((book) => book.id === market),
          now,
        ),
      ]),
    );
    const alternatives = ACTIONS.map((action) =>
      evaluateRoute(action, legs, snapshot, this.settings, warm),
    );
    const selected = alternatives
      .filter((route) => route.viable)
      .sort((a, b) => b.score_cents - a.score_cents)[0];
    const fallback =
      alternatives
        .filter((r) => r.score_cents !== null)
        .sort((a, b) => b.score_cents - a.score_cents)[0] ?? alternatives[0];
    const reason = selected ?? fallback;
    return {
      mode: this.mode,
      clock_ms: now,
      action: selected?.action ?? "skip",
      reason_code: reason.reason_code,
      reason: reason.reason,
      lots: this.settings.lots,
      spot_limit_cents: selected?.spot_limit_cents ?? null,
      future_limit_cents: selected?.future_limit_cents ?? null,
      estimate_cents: selected?.carry.projected_pnl_cents ?? null,
      carry: selected?.carry ?? null,
      legs,
      alternatives,
    };
  }
}
