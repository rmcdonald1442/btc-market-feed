import { calculateMetrics, MODEL_V2 } from "./finance.mjs";

const DAY_MS = 86_400_000;
const LOTS_PER_BTC = 1000;
const CARRY_COST_CENTS = 66_000;
const EVENT_LIMIT = 256;
const PRINT_LIMIT = 12;
const MARKETS = ["spot", "future"];
const MAX_ARTIFACT_EVENTS = 100_000;
const MAX_FINANCIAL_PRODUCT = Number.MAX_SAFE_INTEGER;
const validatedArtifacts = new WeakSet();
const clone = (value) => structuredClone(value);
const sideName = (side) => {
  if (side === "buy" || side === "sell") return side;
  throw new TypeError(`Unknown book side: ${side}`);
};
const marketName = (market) => {
  if (MARKETS.includes(market)) return market;
  throw new TypeError(`Unknown market: ${market}`);
};
const integer = (
  value,
  label,
  { positive = false, nonnegative = false } = {},
) => {
  const number = Number(value);
  if (
    !Number.isSafeInteger(number) ||
    (positive && number <= 0) ||
    (nonnegative && number < 0)
  ) {
    throw new TypeError(`Invalid ${label}.`);
  }
  return number;
};
const orderId = (value, label = "order ID") => {
  const id = String(value ?? "");
  if (!/^[1-9]\d{0,19}$/.test(id) || BigInt(id) > 18_446_744_073_709_551_615n)
    throw new TypeError(`Invalid ${label}.`);
  return id;
};
function freezeDeep(value) {
  if (!value || typeof value !== "object" || Object.isFrozen(value))
    return value;
  for (const child of Object.values(value)) freezeDeep(child);
  return Object.freeze(value);
}
function validateModel(model) {
  if (model === undefined) return;
  const fields = [
    "id",
    "version",
    "lot_btc",
    "tick_usd",
    "expiry_ms",
    "annual_financing_rate",
    "financing_basis",
    "maker_fee_bps",
    "taker_fee_bps",
    "assumed_spot_exit_fee_bps",
    "spot_exit_mark",
  ];
  if (
    !model ||
    typeof model !== "object" ||
    fields.some((key) => model[key] !== MODEL_V2[key]) ||
    !Array.isArray(model.flags) ||
    model.flags.some((flag) => typeof flag !== "string" || !flag.trim())
  )
    throw new TypeError("Unsupported or invalid artifact model contract.");
}
const rustRound = (value) =>
  Math.sign(value) * Math.floor(Math.abs(value) + 0.5);

function normalizeOrder(order) {
  if (!order || typeof order !== "object")
    throw new TypeError("Invalid book order.");
  const id = orderId(order.id, "book order ID");
  const normalized = {
    id,
    side: sideName(order.side),
    price_cents: integer(order.price_cents, "order price", { positive: true }),
    lots: integer(order.lots, "order lots", { positive: true }),
  };
  if (normalized.price_cents % 100 !== 0)
    throw new TypeError("Book order price must be a whole dollar.");
  if (normalized.price_cents * normalized.lots > MAX_FINANCIAL_PRODUCT)
    throw new TypeError("Book order value exceeds safe numeric precision.");
  return normalized;
}

function emptyBook(id, source) {
  const book = source?.find((item) => item.id === id);
  if (!book) throw new TypeError(`Initial snapshot is missing ${id} book.`);
  const orders = new Map();
  const queues = { buy: new Map(), sell: new Map() };
  let previousBid = Infinity,
    previousAsk = 0;
  for (const [sideKey, side] of [
    ["bids", "buy"],
    ["asks", "sell"],
  ]) {
    if (!Array.isArray(book[sideKey]))
      throw new TypeError(`Invalid ${id} ${sideKey}.`);
    for (const level of book[sideKey]) {
      const price = integer(level.price_atoms, "book price", {
        positive: true,
      });
      if (price % 100 !== 0)
        throw new TypeError("Book price must be a whole dollar.");
      if (side === "buy" ? price >= previousBid : price <= previousAsk)
        throw new TypeError(`Invalid ${id} ${sideKey} price order.`);
      if (side === "buy") previousBid = price;
      else previousAsk = price;
      if (!Array.isArray(level.queue))
        throw new TypeError("Invalid book queue.");
      const queue = queues[side].get(price) ?? [];
      for (const item of level.queue) {
        const order = {
          id: orderId(item.id, "book order ID"),
          side,
          price_cents: price,
          lots: integer(item.qty_atoms, "book lots", { positive: true }),
        };
        if (orders.has(order.id))
          throw new TypeError(`Duplicate book order ${order.id}.`);
        orders.set(order.id, order);
        queue.push(order.id);
      }
      const total = queue.reduce(
        (sum, orderId) => sum + orders.get(orderId).lots,
        0,
      );
      if (
        !Number.isSafeInteger(total) ||
        String(total) !== String(level.qty_atoms)
      )
        throw new TypeError(`Invalid aggregate quantity for ${id} book level.`);
      if (queue.length) queues[side].set(price, queue);
    }
  }
  const bid = best({ queues }, "buy"),
    ask = best({ queues }, "sell");
  if (bid != null && ask != null && bid >= ask)
    throw new TypeError(`Initial ${id} book is crossed.`);
  return { id, orders, queues };
}

function best(book, side) {
  const prices = [...book.queues[side].keys()];
  if (!prices.length) return null;
  return side === "buy" ? Math.max(...prices) : Math.min(...prices);
}

function removeFromQueue(book, order) {
  const queue = book.queues[order.side].get(order.price_cents);
  const index = queue?.indexOf(order.id) ?? -1;
  if (index < 0)
    throw new Error(`Order ${order.id} is missing from its FIFO level.`);
  queue.splice(index, 1);
  if (!queue.length) book.queues[order.side].delete(order.price_cents);
}

function bookChange(book, change) {
  if (!change || typeof change !== "object")
    throw new TypeError("Invalid book change.");
  if (change.type === "add") {
    const order = normalizeOrder(change.order);
    if (book.orders.has(order.id))
      throw new Error(`Duplicate order id ${order.id}.`);
    const oppositeBest = best(book, order.side === "buy" ? "sell" : "buy");
    if (
      oppositeBest != null &&
      (order.side === "buy"
        ? order.price_cents >= oppositeBest
        : order.price_cents <= oppositeBest)
    )
      throw new Error(
        `Add would cross the opposite book at order ${order.id}.`,
      );
    const queue = book.queues[order.side].get(order.price_cents) ?? [];
    queue.push(order.id);
    book.queues[order.side].set(order.price_cents, queue);
    book.orders.set(order.id, order);
    return order;
  }
  const id = orderId(change.id);
  const old = book.orders.get(id);
  if (!old) throw new Error(`Unknown order id ${id}.`);
  if (change.type === "reduce") {
    const lots = integer(change.lots, "reduction lots", { positive: true });
    if (lots > old.lots)
      throw new Error(`Cannot reduce order ${id} by more lots than it has.`);
    if (lots === old.lots) {
      removeFromQueue(book, old);
      book.orders.delete(id);
    } else old.lots -= lots;
    return old;
  }
  if (change.type === "amend") {
    const price = integer(change.price_cents, "amended price", {
      positive: true,
    });
    const lots = integer(change.lots, "amended lots", { positive: true });
    if (price % 100 !== 0)
      throw new Error("Amended price must be a positive whole dollar.");
    const oppositeBest = best(book, old.side === "buy" ? "sell" : "buy");
    const crosses =
      oppositeBest != null &&
      (old.side === "buy" ? price >= oppositeBest : price <= oppositeBest);
    if (crosses) throw new Error("Amend would cross the opposite book.");
    if (price === old.price_cents && lots <= old.lots) old.lots = lots;
    else {
      removeFromQueue(book, old);
      const updated = { ...old, price_cents: price, lots };
      const queue = book.queues[updated.side].get(price) ?? [];
      queue.push(id);
      book.queues[updated.side].set(price, queue);
      book.orders.set(id, updated);
    }
    return book.orders.get(id);
  }
  throw new TypeError(`Unknown book change type: ${change.type}`);
}

function marketIndex(market) {
  return market === "spot" ? 0 : 1;
}

function activityRow(
  sequence,
  clockMs,
  market,
  kind,
  order,
  lots,
  previousLots,
  previousPrice,
) {
  const row = {
    sequence,
    clock_ms: clockMs,
    market,
    kind,
    order_id: order.id,
    side: order.side,
    price_cents: order.price_cents,
    lots,
  };
  if (previousLots != null) row.previous_lots = previousLots;
  if (previousPrice != null) row.previous_price_cents = previousPrice;
  return row;
}

function essentialSnapshot(snapshot) {
  if (!snapshot || typeof snapshot !== "object")
    throw new TypeError("Invalid snapshot.");
  const out = {};
  for (const key of [
    "seed",
    "clock_ms",
    "elapsed_ms",
    "next_event_ms",
    "start_ms",
    "end_ms",
    "stage",
    "model",
    "books",
    "orders",
    "fills",
    "history",
    "metrics",
    "prints",
    "feed_stats",
    "visual_events",
  ]) {
    if (snapshot[key] !== undefined) out[key] = clone(snapshot[key]);
  }
  for (const key of [
    "books",
    "orders",
    "fills",
    "history",
    "prints",
    "visual_events",
  ])
    if (out[key] === undefined) out[key] = [];
  return out;
}

export class EventProjector {
  constructor(initialSnapshot, { trackHistoricalIds = false } = {}) {
    const initial = essentialSnapshot(initialSnapshot);
    this.clock = integer(initial.clock_ms ?? 0, "snapshot clock", {
      nonnegative: true,
    });
    this.elapsedMs = integer(
      initial.elapsed_ms ?? this.clock,
      "elapsed clock",
      { nonnegative: true },
    );
    this.nextEventMs = integer(
      initial.next_event_ms ?? this.elapsedMs + 100,
      "next event clock",
      { nonnegative: true },
    );
    this.seed = initial.seed;
    this.startMs = integer(initial.start_ms ?? this.clock, "start clock", {
      nonnegative: true,
    });
    this.endMs = integer(initial.end_ms ?? DAY_MS, "end clock", {
      nonnegative: true,
    });
    this.stage = initial.stage;
    validateModel(initial.model);
    this.model = initial.model === undefined ? undefined : clone(initial.model);
    this.books = Object.fromEntries(
      MARKETS.map((id) => [id, emptyBook(id, initial.books)]),
    );
    // Full-history ID retention is only needed for bounded artifact validation;
    // live and playback projectors track duplicates in the active books only.
    this.seenBookIds = trackHistoricalIds
      ? new Set(MARKETS.flatMap((id) => [...this.books[id].orders.keys()]))
      : null;
    this.own = new Map();
    for (const order of initial.orders) {
      validateOwnOrder(order, this.clock, "initial own order");
      const id = String(order.id);
      if (this.own.has(id)) throw new TypeError(`Duplicate own order ${id}.`);
      this.own.set(id, { ...clone(order), id });
    }
    this.fills = clone(initial.fills);
    this.history = clone(initial.history);
    this.prints = clone(initial.prints).slice(-PRINT_LIMIT);
    this.visualEvents = clone(initial.visual_events).slice(-EVENT_LIMIT);
    this.sequence = this.visualEvents.reduce(
      (max, row) => Math.max(max, Number(row.sequence) || 0),
      0,
    );
    this.feedStats = Object.fromEntries(
      MARKETS.map((id) => {
        const stats = initial.feed_stats?.[id] ?? {};
        return [
          id,
          {
            adds: Number(stats.adds ?? 0),
            amends: Number(stats.amends ?? 0),
            cancels: Number(stats.cancels ?? 0),
            maker_fills: Number(stats.maker_fills ?? 0),
            trade_prints: Number(stats.trade_prints ?? 0),
            startup: clone(
              stats.startup ?? {
                adds: 0,
                amends: 0,
                cancels: 0,
                maker_fills: 0,
                trade_prints: 0,
              },
            ),
          },
        ];
      }),
    );
    this.rng = null;
    this.fairCents = null;
    this.nextId = Math.max(
      1,
      ...MARKETS.flatMap((id) =>
        [...this.books[id].orders.keys()]
          .map(Number)
          .filter(Number.isFinite)
          .map((n) => n + 1),
      ),
      ...[...this.own.keys()]
        .map(Number)
        .filter(Number.isFinite)
        .map((n) => n + 1),
    );
    this.nextId = Math.max(
      this.nextId,
      ...initial.books.map((book) => Number(book.position) || 1),
    );
  }

  apply(event) {
    const clock = integer(event?.clock_ms, "event clock", {
      nonnegative: true,
    });
    if (clock < this.clock || clock > DAY_MS)
      throw new Error("Invalid market timestamp.");
    const type = event.type;
    this.clock = clock;
    if (type === "book") {
      const market = marketName(event.market);
      const change = event.change;
      const id =
        change?.type === "add" ? String(change.order?.id) : orderId(change?.id);
      if (
        change?.type === "add" &&
        this.seenBookIds?.has(orderId(id, "book order ID"))
      )
        throw new Error(`Reused book order ID ${id}.`);
      const previous = this.books[market].orders.get(id);
      const before = previous ? { ...previous } : undefined;
      const cause = event.cause ?? "market";
      const after = bookChange(this.books[market], change);
      if (change.type === "add") this.seenBookIds?.add(after.id);
      if (change.type === "add")
        this.nextId = Math.max(
          this.nextId,
          Number(after.id) + 1 || this.nextId,
        );
      let kind, visualOrder, lots, previousLots, previousPrice;
      if (change.type === "add") {
        kind = "add";
        visualOrder = after;
        lots = after.lots;
      } else if (change.type === "amend") {
        kind = "amend";
        visualOrder = before;
        lots = integer(change.lots, "amended lots", { positive: true });
        previousLots = before.lots;
        previousPrice = before.price_cents;
        visualOrder = { ...before, price_cents: change.price_cents };
      } else {
        kind = cause === "fill" ? "fill" : "cancel";
        visualOrder = before;
        lots = integer(change.lots, "reduction lots", { positive: true });
        previousLots = before.lots;
      }
      const stats = this.feedStats[market];
      if (clock === 0)
        stats.startup[
          kind === "fill"
            ? "maker_fills"
            : kind === "cancel"
              ? "cancels"
              : `${kind}s`
        ] += 1;
      else if (kind === "fill") stats.maker_fills += 1;
      else if (kind === "cancel") stats.cancels += 1;
      else if (kind === "amend") stats.amends += 1;
      else stats.adds += 1;
      if (clock > 0) {
        this.sequence += 1;
        this.visualEvents.push(
          activityRow(
            this.sequence,
            clock,
            market,
            kind,
            visualOrder,
            lots,
            previousLots,
            previousPrice,
          ),
        );
        if (this.visualEvents.length > EVENT_LIMIT) this.visualEvents.shift();
      }
    } else if (type === "trade") {
      const market = marketName(event.market),
        trade = event.trade;
      this.nextId = Math.max(
        this.nextId,
        Number(trade.taker_id) + 1 || this.nextId,
      );
      if (clock > 0) this.feedStats[market].trade_prints += 1;
      else this.feedStats[market].startup.trade_prints += 1;
      this.prints.push({
        market,
        clock_ms: clock,
        price_cents: integer(trade.price_cents, "trade price", {
          positive: true,
        }),
        lots: integer(trade.lots, "trade lots", { positive: true }),
        side:
          trade.maker_side === "sell"
            ? "buy"
            : trade.maker_side === "buy"
              ? "sell"
              : (() => {
                  throw new TypeError("Invalid maker side.");
                })(),
      });
      if (this.prints.length > PRINT_LIMIT) this.prints.shift();
    } else if (type === "acknowledgement") {
      const order = event.order;
      const id = orderId(order?.id, "acknowledgement order ID");
      if (this.own.has(id))
        throw new Error(`Duplicate own acknowledgement ${id}.`);
      this.nextId = Math.max(this.nextId, Number(id) + 1 || this.nextId);
      this.own.set(id, { ...clone(order), id });
    } else if (type === "fill") {
      const fill = clone(event.fill),
        id = orderId(fill?.order_id, "fill order ID");
      const order = this.own.get(id);
      if (!order) throw new Error("Missing own acknowledgement.");
      validateFillForOrder(fill, order, this.model);
      if (
        fill.market !== order.market ||
        fill.side !== order.side ||
        Number(fill.clock_ms) !== clock
      )
        throw new Error(
          "Private fill does not match its acknowledgement or event timestamp.",
        );
      const lots = integer(fill.lots, "private fill lots", { positive: true });
      if (["cancelled", "filled"].includes(order.status))
        throw new Error("Private fill follows a terminal own-order state.");
      if (lots > order.lots - order.filled_lots)
        throw new Error("Private fill exceeds order quantity.");
      order.filled_lots += lots;
      order.status = order.filled_lots === order.lots ? "filled" : "partial";
      this.fills.push(fill);
    } else if (type === "cancelled") {
      const id = orderId(event.order_id, "cancellation order ID"),
        order = this.own.get(id);
      if (!order) throw new Error("Unknown cancellation.");
      if (["cancelled", "filled"].includes(order.status))
        throw new Error("Duplicate cancellation for a terminal own order.");
      order.status = "cancelled";
    } else if (type === "tick") {
      this.rng = event.rng;
      this.fairCents = integer(event.fair_cents, "fair price", {
        positive: true,
      });
      this.elapsedMs = integer(event.elapsed_ms ?? clock, "elapsed clock", {
        nonnegative: true,
      });
      this.nextEventMs = integer(
        event.next_event_ms ?? this.nextEventMs,
        "next event clock",
        { nonnegative: true },
      );
      if (
        event.sample &&
        MARKETS.every(
          (market) =>
            best(this.books[market], "buy") != null &&
            best(this.books[market], "sell") != null,
        )
      ) {
        const spotMid = Math.trunc(
          (best(this.books.spot, "buy") + best(this.books.spot, "sell")) / 2,
        );
        const futureMid = Math.trunc(
          (best(this.books.future, "buy") + best(this.books.future, "sell")) /
            2,
        );
        const sample = {
          clock_ms: clock,
          spot_mid_cents: spotMid,
          future_mid_cents: futureMid,
          basis_cents: futureMid - spotMid,
        };
        if (this.history.at(-1)?.clock_ms === clock) this.history.pop();
        this.history.push(sample);
      }
    } else throw new TypeError(`Unknown event type: ${type}`);
    return this;
  }

  snapshot(clockMs) {
    const displayClock =
      clockMs === undefined
        ? this.clock
        : integer(clockMs, "snapshot clock", { nonnegative: true });
    const books = MARKETS.map((id) => {
      const book = this.books[id];
      const levels = (side) =>
        [...book.queues[side].keys()]
          .sort((a, b) => (side === "buy" ? b - a : a - b))
          .map((price) => {
            const queue = book.queues[side]
              .get(price)
              .map((orderId) => book.orders.get(orderId));
            return {
              price_atoms: String(price),
              qty_atoms: String(
                queue.reduce((sum, order) => sum + order.lots, 0),
              ),
              queue: queue.map((order) => ({
                id: order.id,
                qty_atoms: String(order.lots),
              })),
            };
          });
      return {
        id,
        price_scale: 100,
        qty_scale: 1000,
        exchange_ns: String(this.clock * 1_000_000),
        position: this.nextId,
        bid_cents: best(book, "buy"),
        ask_cents: best(book, "sell"),
        bids: levels("buy"),
        asks: levels("sell"),
      };
    });
    const orders = [...this.own.values()].map((order) => {
      const book = this.books[order.market];
      const live = book.orders.get(String(order.id));
      const remaining = live?.lots ?? 0;
      let ahead = 0;
      if (live)
        for (const id of book.queues[live.side].get(live.price_cents) ?? []) {
          if (id === live.id) break;
          ahead += book.orders.get(id).lots;
        }
      return {
        ...clone(order),
        remaining_lots: remaining,
        ahead_lots: remaining > 0 ? ahead : 0,
      };
    });
    const metrics = this.metrics(books);
    const counts = Object.fromEntries(
      MARKETS.map((market) => {
        const stats = this.feedStats[market];
        const records =
          stats.adds +
          stats.amends +
          stats.cancels +
          stats.maker_fills +
          stats.trade_prints;
        return [
          market,
          {
            ...stats,
            wire_records: records,
            events_per_second:
              this.elapsedMs > 0 ? records / (this.elapsedMs / 1000) : 0,
          },
        ];
      }),
    );
    const stride = Math.max(1, Math.ceil(this.history.length / 1000));
    const history = this.history
      .filter((_, index) => index % stride === 0)
      .map(clone);
    if (
      this.history.length &&
      history.at(-1)?.clock_ms !== this.history.at(-1).clock_ms
    )
      history.push(clone(this.history.at(-1)));
    return {
      ...(this.seed === undefined ? {} : { seed: this.seed }),
      clock_ms: displayClock,
      elapsed_ms: this.elapsedMs,
      next_event_ms: this.nextEventMs,
      start_ms: this.startMs,
      end_ms: this.endMs,
      stage: displayClock >= this.endMs ? "complete" : "running",
      books,
      orders,
      fills: clone(this.fills),
      history,
      metrics,
      prints: clone(this.prints),
      ...(this.model === undefined ? {} : { model: clone(this.model) }),
      feed_stats: { spot: counts.spot, future: counts.future },
      visual_events: clone(this.visualEvents),
    };
  }

  metrics(books = this.snapshot().books) {
    if (this.model !== undefined)
      return calculateMetrics(books, this.fills, this.clock, this.model);
    const quantities = [0, 0],
      values = [0, 0],
      fees = [0, 0];
    for (const fill of this.fills) {
      const index = marketIndex(marketName(fill.market));
      quantities[index] += Number(fill.lots);
      values[index] += Number(fill.price_cents) * Number(fill.lots);
      fees[index] += Number(fill.fee_cents);
    }
    const averages = quantities.map((lots, i) =>
      lots > 0 ? values[i] / lots : null,
    );
    const hedged = Math.min(...quantities);
    let projected = null;
    if (averages[0] != null && averages[1] != null) {
      const matchedFees = quantities.reduce(
        (sum, lots, i) => sum + (fees[i] * hedged) / lots,
        0,
      );
      projected = rustRound(
        ((averages[1] - averages[0] - CARRY_COST_CENTS) * hedged) /
          LOTS_PER_BTC -
          matchedFees,
      );
    }
    const spotAsk = best(this.books.spot, "sell"),
      futureBid = best(this.books.future, "buy");
    const indicated =
      spotAsk == null || futureBid == null
        ? null
        : futureBid -
          spotAsk -
          CARRY_COST_CENTS -
          Math.trunc(((spotAsk + futureBid) * 3) / 10_000);
    return {
      spot_btc: quantities[0] / LOTS_PER_BTC,
      future_short_btc: quantities[1] / LOTS_PER_BTC,
      hedged_btc: hedged / LOTS_PER_BTC,
      residual_btc: (quantities[0] - quantities[1]) / LOTS_PER_BTC,
      spot_average_cents: averages[0],
      future_average_cents: averages[1],
      projected_pnl_cents: projected,
      carry_cost_cents_per_btc: CARRY_COST_CENTS,
      fees_paid_cents: fees[0] + fees[1],
      indicated_pnl_cents_per_btc: indicated,
    };
  }
}

function validateSnapshot(snapshot, label) {
  if (
    !snapshot ||
    typeof snapshot !== "object" ||
    !Array.isArray(snapshot.books) ||
    !Array.isArray(snapshot.orders) ||
    !Array.isArray(snapshot.fills)
  )
    throw new TypeError(`Invalid ${label} snapshot shape.`);
  if (
    snapshot.books.length !== MARKETS.length ||
    new Set(snapshot.books.map((book) => book?.id)).size !== MARKETS.length
  )
    throw new TypeError(`Invalid ${label} book set.`);
  const clock = integer(snapshot.clock_ms ?? 0, `${label} clock`, {
    nonnegative: true,
  });
  if (clock > DAY_MS)
    throw new TypeError(`${label} clock exceeds the session.`);
  validateModel(snapshot.model);
  const books = Object.fromEntries(
    MARKETS.map((market) => [market, emptyBook(market, snapshot.books)]),
  );
  const own = new Map();
  for (const order of snapshot.orders) {
    validateOwnOrder(order, clock, `${label} own order`);
    const id = orderId(order.id, `${label} ID`);
    if (own.has(id)) throw new TypeError(`Duplicate ${label} own order ${id}.`);
    if (
      (order.market === "spot" && order.side !== "buy") ||
      (order.market === "future" && order.side !== "sell")
    )
      throw new TypeError(`Invalid ${label} cash-and-carry leg side.`);
    own.set(id, order);
    const resting = books[order.market].orders.get(id);
    if (["working", "partial"].includes(order.status) && !resting)
      throw new TypeError(`Missing resting ${label} own order ${id}.`);
    if (["filled", "cancelled"].includes(order.status) && resting)
      throw new TypeError(`Terminal ${label} own order ${id} remains in book.`);
    if (
      resting &&
      (resting.side !== order.side ||
        resting.price_cents !== Number(order.price_cents) ||
        (order.remaining_lots != null &&
          resting.lots !== Number(order.remaining_lots)))
    )
      throw new TypeError(`Inconsistent ${label} own order ${id} and book.`);
  }
  let lastFillClock = 0;
  const totals = Object.fromEntries(
    MARKETS.map((market) => [market, { lots: 0, value: 0, fees: 0 }]),
  );
  const filledByOrder = new Map();
  for (const fill of snapshot.fills) {
    validateFill(fill, clock);
    if (fill.clock_ms < lastFillClock)
      throw new TypeError(`Unordered ${label} fills.`);
    lastFillClock = fill.clock_ms;
    const fillId = orderId(fill.order_id, `${label} fill order ID`),
      order = own.get(fillId);
    if (!order || order.market !== fill.market || order.side !== fill.side)
      throw new TypeError(`Inconsistent ${label} fill ownership.`);
    validateFillForOrder(fill, order, snapshot.model);
    filledByOrder.set(
      fillId,
      (filledByOrder.get(fillId) ?? 0) + Number(fill.lots),
    );
    const total = totals[fill.market];
    total.lots += Number(fill.lots);
    total.value += Number(fill.price_cents) * Number(fill.lots);
    total.fees += Number(fill.fee_cents);
    if (![total.lots, total.value, total.fees].every(Number.isSafeInteger))
      throw new TypeError(`Unsafe financial totals in ${label} snapshot.`);
  }
  for (const [id, order] of own) {
    if ((filledByOrder.get(id) ?? 0) !== Number(order.filled_lots))
      throw new TypeError(
        `Inconsistent ${label} fill total for own order ${id}.`,
      );
    if (
      ["working", "partial"].includes(order.status) &&
      Number(order.remaining_lots) !==
        Number(order.lots) - Number(order.filled_lots)
    )
      throw new TypeError(
        `Inconsistent active ${label} remainder for own order ${id}.`,
      );
    if (
      order.status === "filled" &&
      Number(order.filled_lots) !== Number(order.lots)
    )
      throw new TypeError(`Incomplete filled ${label} own order ${id}.`);
  }
}

function validateOwnOrder(order, clock, label) {
  if (!order || typeof order !== "object" || !MARKETS.includes(order.market))
    throw new TypeError(`Invalid ${label}.`);
  orderId(order.id, `${label} ID`);
  sideName(order.side);
  if (
    (order.market === "spot" && order.side !== "buy") ||
    (order.market === "future" && order.side !== "sell")
  )
    throw new TypeError(`Invalid ${label} cash-and-carry leg side.`);
  const price = integer(order.price_cents, `${label} price`, {
    positive: true,
  });
  if (price % 100 !== 0)
    throw new TypeError(`${label} price must be a whole dollar.`);
  const lots = integer(order.lots, `${label} lots`, { positive: true });
  const filled = integer(order.filled_lots, `${label} filled lots`, {
    nonnegative: true,
  });
  if (
    price * lots > MAX_FINANCIAL_PRODUCT ||
    filled > lots ||
    !["working", "partial", "filled", "cancelled"].includes(order.status)
  )
    throw new TypeError(`Invalid ${label} status, quantity or value.`);
  const created = integer(order.created_ms, `${label} creation clock`, {
    nonnegative: true,
  });
  if (created > clock || created > DAY_MS)
    throw new TypeError(`${label} is dated after its snapshot.`);
  if (
    (order.status === "filled" && filled !== lots) ||
    (order.status === "partial" && (filled === 0 || filled === lots)) ||
    (order.status === "working" && filled !== 0)
  )
    throw new TypeError(`Inconsistent ${label} status and fills.`);
  if (order.remaining_lots != null)
    integer(order.remaining_lots, `${label} remaining lots`, {
      nonnegative: true,
    });
}

function validateFill(fill, eventClock) {
  if (!fill || fill.order_id == null || !MARKETS.includes(fill.market))
    throw new TypeError("Invalid fill event.");
  sideName(fill.side);
  const price = integer(fill.price_cents, "fill price", { positive: true });
  if (price % 100 !== 0)
    throw new TypeError("Fill price must be a whole dollar.");
  const lots = integer(fill.lots, "fill lots", { positive: true });
  integer(fill.fee_cents, "fill fee", { nonnegative: true });
  const clock = integer(fill.clock_ms, "fill clock", { nonnegative: true });
  if (
    clock > eventClock ||
    clock > DAY_MS ||
    price * lots > MAX_FINANCIAL_PRODUCT
  )
    throw new TypeError("Invalid fill timestamp or value.");
  if (fill.liquidity !== "maker" && fill.liquidity !== "taker")
    throw new TypeError("Invalid fill liquidity.");
}

function validateFillForOrder(fill, order, model) {
  const price = Number(fill.price_cents),
    clock = Number(fill.clock_ms);
  if (clock < Number(order.created_ms))
    throw new TypeError("Fill predates own-order creation.");
  if (
    order.side === "buy"
      ? price > Number(order.price_cents)
      : price < Number(order.price_cents)
  )
    throw new TypeError("Fill price violates the acknowledged limit.");
  if (fill.liquidity === "maker" && price !== Number(order.price_cents))
    throw new TypeError("Maker fill price differs from the resting limit.");
  if (model?.version === 2) {
    const bps =
      fill.liquidity === "maker" ? model.maker_fee_bps : model.taker_fee_bps;
    const expected = Number(
      (BigInt(price) * BigInt(fill.lots) * BigInt(bps) + 5_000_000n) /
        10_000_000n,
    );
    if (Number(fill.fee_cents) !== expected)
      throw new TypeError("Fill fee does not match the version 2 model.");
  }
}

function validateEvent(event, previousClock) {
  if (!event || typeof event !== "object" || Array.isArray(event))
    throw new TypeError("Invalid artifact event.");
  const clock = integer(event.clock_ms, "event clock", { nonnegative: true });
  if (clock < previousClock || clock > DAY_MS)
    throw new TypeError(
      "Artifact event timestamps must be ordered within the session.",
    );
  if (event.type === "book") {
    marketName(event.market);
    const change = event.change;
    if (!change || !["add", "reduce", "amend"].includes(change.type))
      throw new TypeError(`Unknown book change type: ${change?.type}`);
    if (change.type === "add") normalizeOrder(change.order);
    else {
      String(change.id);
      if (change.type === "reduce")
        integer(change.lots, "reduction lots", { positive: true });
      else {
        integer(change.price_cents, "amended price", { positive: true });
        integer(change.lots, "amended lots", { positive: true });
      }
    }
  } else if (event.type === "trade") {
    marketName(event.market);
    orderId(event.trade?.maker_id, "trade maker ID");
    orderId(event.trade?.taker_id, "trade taker ID");
    const price = integer(event.trade?.price_cents, "trade price", {
      positive: true,
    });
    if (price % 100 !== 0)
      throw new TypeError("Trade price must be a whole dollar.");
    integer(event.trade?.lots, "trade lots", { positive: true });
    sideName(event.trade?.maker_side);
  } else if (event.type === "acknowledgement") {
    validateOwnOrder(event.order, clock, "acknowledgement order");
  } else if (event.type === "fill") {
    validateFill(event.fill, clock);
  } else if (event.type === "cancelled") {
    orderId(event.order_id, "cancellation order ID");
  } else if (event.type === "tick") {
    integer(event.fair_cents, "fair price", { positive: true });
    integer(event.elapsed_ms ?? clock, "elapsed clock", { nonnegative: true });
    integer(event.next_event_ms ?? 0, "next event clock", {
      nonnegative: true,
    });
  } else throw new TypeError(`Unknown event type: ${event.type}`);
  return clock;
}

export function createArtifact(initialSnapshot, events, finalSnapshot) {
  validateSnapshot(initialSnapshot, "initial");
  if (!Array.isArray(events))
    throw new TypeError("Artifact events must be an array.");
  if (events.length > MAX_ARTIFACT_EVENTS)
    throw new TypeError(
      `Artifact exceeds the ${MAX_ARTIFACT_EVENTS} event limit.`,
    );
  let previous = integer(initialSnapshot.clock_ms ?? 0, "initial clock", {
    nonnegative: true,
  });
  for (const event of events) previous = validateEvent(event, previous);
  if (finalSnapshot) {
    validateSnapshot(finalSnapshot, "final");
    if (Number(finalSnapshot.clock_ms ?? 0) < previous)
      throw new TypeError("Final snapshot precedes the last event.");
    if (
      JSON.stringify(stable(initialSnapshot.model ?? null)) !==
      JSON.stringify(stable(finalSnapshot.model ?? null))
    )
      throw new TypeError(
        "Final snapshot model differs from the initial model.",
      );
  }
  const artifact = {
    schema_version: 1,
    format: "btc-execution-events",
    initial_snapshot: essentialSnapshot(initialSnapshot),
    events: clone(events),
    ...(finalSnapshot
      ? { final_snapshot: essentialSnapshot(finalSnapshot) }
      : {}),
  };
  const projector = new EventProjector(artifact.initial_snapshot, {
    trackHistoricalIds: true,
  });
  for (const event of artifact.events) projector.apply(event);
  const replayed = projector.snapshot();
  validateSnapshot(replayed, "replayed");
  if (finalSnapshot)
    compareFinal(
      projector.snapshot(Number(artifact.final_snapshot.clock_ms)),
      artifact.final_snapshot,
    );
  freezeDeep(artifact);
  validatedArtifacts.add(artifact);
  return artifact;
}

export function parseArtifact(text) {
  const data = JSON.parse(text);
  if (data?.schema_version !== 1 || data?.format !== "btc-execution-events")
    throw new TypeError("Unsupported browser artifact schema or format.");
  if (!data.initial_snapshot || !Array.isArray(data.events))
    throw new TypeError("Invalid browser artifact structure.");
  return createArtifact(
    data.initial_snapshot,
    data.events,
    data.final_snapshot,
  );
}

export class ArtifactPlayer {
  constructor(artifact) {
    this.artifact = validatedArtifacts.has(artifact)
      ? artifact
      : parseArtifact(JSON.stringify(artifact));
    this.startMs = Number(this.artifact.initial_snapshot.clock_ms ?? 0);
    this.endMs = Number(this.artifact.events.at(-1)?.clock_ms ?? this.startMs);
    if (this.artifact.final_snapshot)
      this.endMs = Math.max(
        this.endMs,
        Number(this.artifact.final_snapshot.clock_ms ?? this.endMs),
      );
    this.reset();
  }
  reset() {
    this.projector = new EventProjector(this.artifact.initial_snapshot);
    this.appliedRows = 0;
    this.clockMs = this.startMs;
    this.done = this.artifact.events.length === 0 && this.endMs <= this.startMs;
    return this.snapshot();
  }
  advanceTo(clockMs, { snapshot = true } = {}) {
    const target = Math.max(
      this.startMs,
      Math.min(
        this.endMs,
        integer(clockMs, "playback clock", { nonnegative: true }),
      ),
    );
    if (target < this.clockMs) this.reset();
    while (
      this.appliedRows < this.artifact.events.length &&
      this.artifact.events[this.appliedRows].clock_ms <= target
    ) {
      this.projector.apply(this.artifact.events[this.appliedRows]);
      this.appliedRows += 1;
    }
    this.clockMs = target;
    this.done =
      this.appliedRows === this.artifact.events.length && target >= this.endMs;
    return snapshot ? this.snapshot() : undefined;
  }
  snapshot() {
    return this.projector.snapshot(this.clockMs);
  }
}

function comparable(snapshot) {
  // History is deliberately excluded: server checkpoints decimate long sessions,
  // while replay history is rebuilt from the sampled Tick events.
  return {
    clock_ms: snapshot.clock_ms,
    elapsed_ms: snapshot.elapsed_ms,
    next_event_ms: snapshot.next_event_ms,
    model: snapshot.model,
    books: snapshot.books,
    orders: snapshot.orders,
    fills: snapshot.fills,
    metrics: snapshot.metrics,
    prints: snapshot.prints,
    feed_stats: snapshot.feed_stats,
    visual_events: snapshot.visual_events,
  };
}

function stable(value) {
  if (Array.isArray(value)) return value.map(stable);
  if (value && typeof value === "object")
    return Object.fromEntries(
      Object.keys(value)
        .sort()
        .map((key) => [key, stable(value[key])]),
    );
  return value;
}

function compareFinal(actualSnapshot, finalSnapshot) {
  const actual = comparable(actualSnapshot),
    expected = comparable(finalSnapshot);
  if (JSON.stringify(stable(actual)) !== JSON.stringify(stable(expected)))
    throw new Error(
      "Browser event projection differs from the canonical final snapshot.",
    );
}

export function assertArtifactParity(artifact) {
  const parsed =
    typeof artifact === "string"
      ? parseArtifact(artifact)
      : validatedArtifacts.has(artifact)
        ? artifact
        : parseArtifact(JSON.stringify(artifact));
  if (!parsed.final_snapshot)
    throw new Error("Artifact has no canonical final snapshot.");
  return true;
}
