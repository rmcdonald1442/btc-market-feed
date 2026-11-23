import test from "node:test";
import assert from "node:assert/strict";
import { EntryPolicy } from "./entry-policy.mjs";
import { MODEL_V2, calculateTicketEstimate } from "./finance.mjs";

const level = (price, lots, id) => ({
  price_atoms: String(price),
  qty_atoms: String(lots),
  queue: [{ id: String(id), qty_atoms: String(lots) }],
});
const touch = (book) => (book.id === "spot" ? book.bids[0] : book.asks[0]);
const side = (book) => (book.id === "spot" ? "buy" : "sell");

function initial({ spotLots = 100, futureLots = 150, premium = 90000 } = {}) {
  const spot = 10000000,
    future = spot + premium;
  return {
    clock_ms: 0,
    model: MODEL_V2,
    history: [
      {
        clock_ms: 0,
        spot_mid_cents: spot + 100,
        future_mid_cents: future - 100,
      },
    ],
    books: [
      {
        id: "spot",
        bid_cents: spot,
        ask_cents: spot + 200,
        bids: [level(spot, spotLots, 1)],
        asks: [level(spot + 200, 110, 2)],
      },
      {
        id: "future",
        bid_cents: future - 200,
        ask_cents: future,
        bids: [level(future - 200, 60, 3), level(future - 500, 110, 4)],
        asks: [level(future, futureLots, 5)],
      },
    ],
  };
}

// The policy-facing L2 view makes accidental access to MBO fields fail loudly.
function blind(snapshot) {
  const copy = structuredClone(snapshot);
  for (const book of copy.books)
    for (const row of [...book.bids, ...book.asks]) {
      Object.defineProperty(row, "queue", {
        get() {
          throw Error("L2 accessed public order IDs");
        },
      });
    }
  return copy;
}
function blindEvents(events) {
  return events.map((event) => {
    if (event.type === "book")
      return {
        type: "book",
        market: event.market,
        clock_ms: event.clock_ms,
        get change() {
          throw Error("L2 read individual order event");
        },
      };
    return {
      ...event,
      trade: {
        ...event.trade,
        get maker_id() {
          throw Error("L2 read maker identity");
        },
      },
    };
  });
}

function experiment({
  mode = "l3",
  churning = false,
  spotPrint = 50,
  futurePrint = 25,
  ...sizes
} = {}) {
  const policy = new EntryPolicy({ mode });
  let snapshot = initial(sizes),
    serial = 10;
  const asSeen = mode === "l2" ? blind : (s) => s;
  const decisions = [];
  for (let step = 1; step <= 6; step++) {
    const after = structuredClone(snapshot);
    const clock = (after.clock_ms = step * 500),
      events = [];
    for (const book of after.books) {
      const row = touch(book),
        price = Number(row.price_atoms),
        start = Number(row.qty_atoms);
      let left = book.id === "spot" ? spotPrint : futurePrint;
      const emit = (cause, change) =>
        events.push({
          type: "book",
          market: book.id,
          clock_ms: clock,
          cause,
          change,
        });
      while (left > 0) {
        const first = row.queue[0],
          executed = Math.min(left, Number(first.qty_atoms));
        emit("fill", { type: "reduce", id: first.id, lots: executed });
        events.push({
          type: "trade",
          market: book.id,
          clock_ms: clock,
          trade: {
            maker_id: first.id,
            maker_side: side(book),
            price_cents: price,
            lots: executed,
          },
        });
        first.qty_atoms = String(Number(first.qty_atoms) - executed);
        if (Number(first.qty_atoms) === 0) row.queue.shift();
        left -= executed;
      }
      if (churning && book.id === "future") {
        for (const order of row.queue)
          emit("cancel", {
            type: "reduce",
            id: order.id,
            lots: Number(order.qty_atoms),
          });
        row.queue = [];
      }
      const added =
        start - row.queue.reduce((sum, q) => sum + Number(q.qty_atoms), 0);
      if (added) {
        const id = String(serial++);
        row.queue.push({ id, qty_atoms: String(added) });
        emit("add", {
          type: "add",
          order: { id, side: side(book), price_cents: price, lots: added },
        });
      }
    }
    if (clock % 1000 === 0)
      after.history.push({ ...after.history.at(-1), clock_ms: clock });
    policy.observe(
      asSeen(snapshot),
      asSeen(after),
      mode === "l2" ? blindEvents(events) : events,
    );
    decisions.push(policy.assess(asSeen(after)));
    snapshot = after;
  }
  return { policy, snapshot, decision: decisions.at(-1), decisions };
}

test("equal touches, aggregate sizes and aggressive flow can yield different entry routes from identity turnover", () => {
  const stable = experiment().decision,
    churn = experiment({ churning: true }).decision;
  const l2Stable = experiment({ mode: "l2" }).decision,
    l2Churn = experiment({ mode: "l2", churning: true }).decision;
  assert.deepEqual(l2Stable, l2Churn);
  assert.equal(churn.action, "rest_both");
  for (const choice of [l2Churn, stable])
    assert.equal(choice.action, "cross_future");
  assert.equal(
    churn.legs.future.visible_ahead_lots,
    l2Churn.legs.future.visible_ahead_lots,
  );
  assert.equal(churn.legs.future.recent_trade_lots, 150);
  assert.equal(churn.legs.future.recent_cancel_lots, 750);
  assert.equal(l2Churn.legs.future.recent_cancel_lots, 0);
  assert.equal(l2Churn.legs.future.recent_priority_loss_lots, null);
  assert.equal(l2Churn.legs.future.fill_eta_ms, 5000);
  assert.ok(
    churn.legs.future.fill_eta_ms > 2000 &&
      churn.legs.future.fill_eta_ms < 4000,
  );
});

test("a cross uses the worst required displayed limit, VWAP and the appropriate maker/taker fees", () => {
  const { decision, snapshot } = experiment();
  const future = snapshot.books[1];
  const crossed = decision.alternatives.find(
    (r) => r.action === "cross_future",
  );
  assert.equal(crossed.future_limit_cents, Number(future.bids[1].price_atoms));
  const vwap =
    (60 * Number(future.bids[0].price_atoms) +
      40 * Number(future.bids[1].price_atoms)) /
    100;
  assert.equal(decision.legs.future.cross_vwap_cents, vwap);
  assert.deepEqual(
    crossed.carry,
    calculateTicketEstimate({
      lots: 100,
      clockMs: 3000,
      spotPriceCents: snapshot.books[0].bid_cents,
      futurePriceCents: vwap,
      spotFeeBps: MODEL_V2.maker_fee_bps,
      futureFeeBps: MODEL_V2.taker_fee_bps,
    }),
  );
  assert.equal(decision.estimate_cents, decision.carry.projected_pnl_cents);
  assert.equal(
    experiment({
      spotLots: 150,
      futureLots: 100,
      spotPrint: 25,
      futurePrint: 50,
    }).decision.action,
    "cross_spot",
  );
});

test("maker cancellations cannot execute our order and a crossed leg always needs a viable resting hedge", () => {
  const { decision } = experiment({
    churning: true,
    spotPrint: 0,
    futurePrint: 0,
  });
  assert.equal(decision.action, "skip");
  assert.equal(decision.legs.future.fill_eta_ms, null);
  assert.ok(decision.legs.future.recent_cancel_lots > 0);
  assert.ok(
    decision.alternatives.every(
      (a) => !a.viable && a.reason_code === "no_flow",
    ),
  );
});

test("risk, cost, depth, and observation guards can independently veto entry", () => {
  const { policy, snapshot, decisions } = experiment({ churning: true });
  assert.equal(decisions[0].action, "skip");
  assert.equal(decisions[0].reason_code, "warmup");
  const later = structuredClone(snapshot);
  later.history.push({
    clock_ms: 10000,
    spot_mid_cents: 1,
    future_mid_cents: 1,
  });
  assert.deepEqual(policy.assess(later), decisions.at(-1));
  later.history = later.history.slice(0, 2);
  assert.equal(policy.assess(later).action, "skip");
  later.history = snapshot.history.map((p, i) => ({
    ...p,
    spot_mid_cents: p.spot_mid_cents + (i % 2) * 200000,
    future_mid_cents: p.future_mid_cents + (i % 2) * 200000,
  }));
  const risky = policy.assess(later);
  assert.equal(risky.action, "skip");
  assert.ok(
    risky.alternatives.some(
      (r) => r.reason_code === "high_risk" && r.carry.projected_pnl_cents > 0,
    ),
  );
  assert.equal(experiment({ premium: -3000 }).decision.action, "skip");
  const thin = structuredClone(snapshot);
  thin.books[1].bids[1].price_atoms = String(thin.books[1].bid_cents - 10100);
  assert.equal(policy.assess(thin).reason_code, "insufficient_depth");
  assert.throws(
    () => policy.assess({ ...snapshot, clock_ms: 2999 }),
    /latest observed/,
  );
});

test("a price that moves off touch isn't treated as cancelled, and same-price increases lose priority", () => {
  const s0 = initial(),
    s1 = structuredClone(s0);
  s1.clock_ms = 500;
  const spot = s1.books[0],
    old = spot.bid_cents,
    price = old + 100;
  spot.bid_cents = price;
  spot.bids.unshift(level(price, 20, 17));
  const add = {
    type: "book",
    market: "spot",
    clock_ms: 500,
    cause: "add",
    change: {
      type: "add",
      order: { id: 17, side: "buy", price_cents: price, lots: 20 },
    },
  };
  const s2 = structuredClone(s1);
  s2.clock_ms = 750;
  s2.books[0].bids[0] = level(price, 40, 17);
  const amend = {
    type: "book",
    market: "spot",
    clock_ms: 750,
    cause: "amend",
    change: { type: "amend", id: 17, price_cents: price, lots: 40 },
  };
  const l3 = new EntryPolicy(),
    l2 = new EntryPolicy({ mode: "l2" });
  l3.observe(s0, s1, [add]);
  l3.observe(s1, s2, [amend]);
  l2.observe(blind(s0), blind(s1), blindEvents([add]));
  l2.observe(blind(s1), blind(s2), blindEvents([amend]));
  assert.equal(l3.assess(s2).legs.spot.recent_priority_loss_lots, 20);
  assert.equal(l3.assess(s2).legs.spot.recent_cancel_lots, 0);
  assert.equal(l2.assess(blind(s2)).legs.spot.recent_cancel_lots, 0);
});

test("even separately published L2 changes and order counts cannot identify which equal-sized maker cancelled", () => {
  function replay(cancelOldest) {
    const l2 = new EntryPolicy({ mode: "l2" }),
      l3 = new EntryPolicy();
    let current = initial({ futureLots: 250 });
    const price = current.books[1].ask_cents;
    touch(current.books[1]).queue = Array.from({ length: 10 }, (_, i) => ({
      id: String(20 + i),
      qty_atoms: "25",
    }));
    let serial = 30;
    const publicTape = [];
    function publish(clock, operation) {
      const after = structuredClone(current);
      after.clock_ms = clock;
      const row = touch(after.books[1]);
      const event = { type: "book", market: "future", clock_ms: clock };
      const events = [event];
      if (operation === "add") {
        const id = String(serial++);
        row.queue.push({ id, qty_atoms: "25" });
        event.cause = "add";
        event.change = {
          type: "add",
          order: { id, side: "sell", price_cents: price, lots: 25 },
        };
      } else {
        const [maker] = row.queue.splice(
          operation === "fill" || cancelOldest ? 0 : row.queue.length - 1,
          1,
        );
        event.cause = operation;
        event.change = { type: "reduce", id: maker.id, lots: 25 };
        if (operation === "fill")
          events.push({
            type: "trade",
            market: "future",
            clock_ms: clock,
            trade: {
              maker_id: maker.id,
              maker_side: "sell",
              price_cents: price,
              lots: 25,
            },
          });
      }
      row.qty_atoms = String(25 * row.queue.length);
      publicTape.push([
        clock,
        price,
        row.qty_atoms,
        row.queue.length,
        operation,
      ]);
      l2.observe(blind(current), blind(after), blindEvents(events));
      l3.observe(current, after, events);
      current = after;
    }
    for (let step = 1; step <= 6; step++) {
      publish(step * 500 - 30, "fill");
      publish(step * 500 - 20, "cancel");
      publish(step * 500 - 10, "add");
      publish(step * 500, "add");
    }
    return {
      publicTape,
      l2: l2.assess(blind(current)).legs.future,
      l3: l3.assess(current).legs.future,
    };
  }
  const front = replay(true),
    back = replay(false);
  assert.deepEqual(front.publicTape, back.publicTape);
  assert.deepEqual(front.l2, back.l2);
  assert.equal(front.l3.visible_ahead_lots, back.l3.visible_ahead_lots);
  assert.equal(front.l3.recent_cancel_lots, back.l3.recent_cancel_lots);
  assert.equal(front.l3.recent_trade_lots, back.l3.recent_trade_lots);
  assert.notEqual(
    front.l3.turnover_lots_per_second,
    back.l3.turnover_lots_per_second,
  );
  assert.notEqual(front.l3.fill_eta_ms, back.l3.fill_eta_ms);
});
