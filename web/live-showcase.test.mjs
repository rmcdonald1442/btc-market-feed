import test from "node:test";
import assert from "node:assert/strict";
import { LiveCarryPolicy, LiveShowcase } from "./live-showcase.mjs";
import { MODEL_V2 } from "./finance.mjs";
import { HedgePolicy, L2QueueEstimator } from "./hedge-policy.mjs";

function snapshot(premium = 80_000) {
  return {
    clock_ms: 0,
    model: MODEL_V2,
    orders: [],
    fills: [],
    metrics: {
      spot_btc: 0,
      future_short_btc: 0,
      hedged_btc: 0,
      residual_btc: 0,
    },
    books: [
      { id: "spot", bid_cents: 10_000_000, ask_cents: 10_000_200 },
      {
        id: "future",
        bid_cents: 10_000_000 + premium,
        ask_cents: 10_000_200 + premium,
      },
    ],
  };
}
test("live policy observes first and refuses an entry below estimated costs", () => {
  const policy = new LiveCarryPolicy(),
    state = snapshot();
  assert.equal(policy.decide(state).kind, "wait");
  state.clock_ms = 3000;
  assert.equal(policy.decide(state).kind, "place");
  const cheap = snapshot(60_000);
  cheap.clock_ms = 3000;
  assert.equal(policy.decide(cheap).kind, "wait");
});
test("live hedge deadline handles a partial entry and completes without forcing the 0.1 BTC target", () => {
  const policy = new LiveCarryPolicy(),
    state = snapshot();
  state.clock_ms = 3000;
  policy.commit(policy.decide(state), state);
  state.fills = [{ market: "spot", lots: 37, clock_ms: 3400 }];
  state.metrics.spot_btc = state.metrics.residual_btc = 0.037;
  state.orders = [{ remaining_lots: 63 }, { remaining_lots: 100 }];
  state.clock_ms = 7399;
  assert.equal(policy.decide(state).kind, "wait");
  state.clock_ms = 7400;
  const hedge = policy.decide(state);
  assert.equal(hedge.kind, "hedge");
  policy.commit(hedge, state);
  state.metrics.future_short_btc = state.metrics.hedged_btc = 0.037;
  state.metrics.residual_btc = 0;
  state.orders = [];
  assert.equal(policy.decide(state).kind, "complete");
});
test("a pending Kafka start excludes a second start and a broker failure is surfaced", async () => {
  let rejectRequest;
  const run = new LiveShowcase({
    fetchImpl: () =>
      new Promise((_, reject) => {
        rejectRequest = reject;
      }),
  });
  const first = run.start();
  await assert.rejects(run.start(), /fresh live run/);
  rejectRequest(new Error("broker unavailable"));
  await assert.rejects(first, /broker unavailable/);
  assert.equal(run.eventsProcessed, 0);
});
test("live demo rejects a local transport response instead of substituting a replay", async () => {
  const run = new LiveShowcase({
    fetchImpl: async () => ({
      ok: true,
      json: async () => ({ transport: "local", scenario: "carry_demo" }),
    }),
  });
  await assert.rejects(run.start(), /No replay fallback/);
  assert.equal(run.eventsProcessed, 0);
});

function level(price, lots) {
  return { price_atoms: String(price), qty_atoms: String(lots) };
}

function queueSnapshot(clock, total, ahead, placed = true) {
  const state = snapshot();
  state.scenario = "queue_demo";
  state.clock_ms = clock;
  const future = state.books[1];
  future.asks = [
    level(future.ask_cents, 20),
    level(future.ask_cents + 100, total),
  ];
  future.bids = [
    level(future.bid_cents, 50),
    level(future.bid_cents - 100, 50),
  ];
  if (placed) {
    state.orders = [
      {
        id: "ours",
        market: "future",
        side: "sell",
        price_cents: future.ask_cents + 100,
        filled_lots: 0,
        remaining_lots: 100,
        ahead_lots: ahead,
      },
    ];
  }
  if (clock >= 3200) {
    state.fills = [{ market: "spot", lots: 100, clock_ms: 3200 }];
    state.metrics.residual_btc = 0.1;
  }
  return state;
}

function evaluateCancellation(mode, ahead, history = []) {
  const policy = new HedgePolicy(mode, 4000);
  const pre = queueSnapshot(2000, 160, 0, false);
  const entered = queueSnapshot(3000, 260, 160);
  const joined = queueSnapshot(4000, 420, 160);
  const cancelled = queueSnapshot(5000, 340, ahead);
  cancelled.history = history;
  if (mode === "l2") {
    for (const state of [pre, entered, joined, cancelled]) {
      for (const row of state.books[1].asks)
        Object.defineProperty(row, "queue", {
          get() {
            throw new Error("L2 read private queue composition");
          },
        });
      for (const order of state.orders)
        Object.defineProperty(order, "ahead_lots", {
          get() {
            throw new Error("L2 read true rank");
          },
        });
    }
  }
  policy.observe(pre, entered, []);
  policy.observe(entered, joined, [
    {
      type: "trade",
      market: "future",
      clock_ms: 4000,
      trade: {
        maker_side: "sell",
        price_cents: joined.books[1].ask_cents,
        lots: 300,
      },
    },
  ]);
  policy.observe(joined, cancelled, []);
  return policy.assess(cancelled);
}

test("identical public L2 histories with cancellations ahead or behind can require opposite L3 decisions", () => {
  const near = evaluateCancellation("l3", 80);
  const far = evaluateCancellation("l3", 160);
  assert.equal(near.action, "wait");
  assert.equal(far.action, "cross");
  assert.equal(near.better_lots, 20);
  assert.equal(near.eta_ms, 2000);
  assert.equal(near.estimated_cross_price_cents, 10079950);
  assert.ok(near.passive_edge_cents > 0);
  const estimate = evaluateCancellation("l2", 80);
  assert.deepEqual(estimate, evaluateCancellation("l2", 160));
  assert.equal(estimate.ahead_lots, 120);
  assert.equal(estimate.eta_ms, 2400);
  assert.equal(estimate.reason_code, "queue_exceeds_budget");
  assert.equal(evaluateCancellation("timer", 160).action, "wait");
});

test("L2 queue accounts for public executions once and recognizes an own partial fill", () => {
  const queue = new L2QueueEstimator();
  queue.update("our", {
    beforeLots: 100,
    afterLots: 140,
    previousOwnLots: 0,
    ownLots: 40,
    tradedLots: 0,
  });
  queue.update("our", {
    beforeLots: 140,
    afterLots: 160,
    previousOwnLots: 40,
    ownLots: 40,
    tradedLots: 0,
  });
  queue.update("our", {
    beforeLots: 160,
    afterLots: 130,
    previousOwnLots: 40,
    ownLots: 40,
    tradedLots: 30,
  });
  assert.equal(queue.ahead("our"), 70);
  queue.update("our", {
    beforeLots: 130,
    afterLots: 55,
    previousOwnLots: 40,
    ownLots: 35,
    tradedLots: 75,
    makerFilled: true,
  });
  assert.equal(queue.ahead("our"), 0);
  queue.forget("our");
  assert.equal(queue.ahead("our"), null);
});

test("recent public price risk can exceed the passive saving even with time left to fill", () => {
  const history = [2000, 3000, 4000, 5000].map((clock_ms, index) => ({
    clock_ms,
    future_mid_cents: 10080000 + (index % 2) * 20000,
  }));
  const result = evaluateCancellation("l3", 80, history);
  assert.equal(result.reason_code, "risk_exceeds_edge");
  assert.ok(Math.abs(result.wait_risk_cents - 2000 * Math.sqrt(2)) < 0.0001);
  assert.ok(result.wait_risk_cents > result.passive_edge_cents);
  const future = { clock_ms: 6000, future_mid_cents: 20000000 };
  assert.deepEqual(
    evaluateCancellation("l3", 80, [...history, future]),
    result,
  );
  assert.equal(
    evaluateCancellation("l3", 80, history.slice(0, 3)).wait_risk_cents,
    null,
  );
});

test("a short residual uses the spot buy queue and offer sweep for the actual 37 lots", () => {
  const state = snapshot();
  const book = state.books[0];
  state.clock_ms = 5500;
  state.metrics.residual_btc = -0.037;
  state.fills = [{ market: "future", lots: 37, clock_ms: 1500 }];
  state.orders = [
    {
      id: "spot",
      market: "spot",
      side: "buy",
      price_cents: book.bid_cents - 100,
      remaining_lots: 100,
      ahead_lots: 4,
    },
  ];
  book.bids = [level(book.bid_cents, 12), level(book.bid_cents - 100, 104)];
  book.asks = [level(book.ask_cents, 7), level(book.ask_cents + 100, 30)];
  const decision = new HedgePolicy("l3", 4000).assess(state);
  assert.equal(decision.market, "spot");
  assert.equal(decision.remaining_lots, 37);
  assert.equal(decision.better_lots, 12);
  assert.equal(decision.reason_code, "risk_deadline");
  assert.equal(
    decision.estimated_cross_price_cents,
    (book.ask_cents * 7 + (book.ask_cents + 100) * 30) / 37,
  );
  book.asks = [level(book.ask_cents, 7), level(book.ask_cents + 10100, 30)];
  assert.equal(
    new HedgePolicy("l3", 4000).assess(state).estimated_cross_price_cents,
    null,
  );
});

test("queue comparison policies enter with identical orders at touch regardless of hedge information", () => {
  const state = queueSnapshot(3000, 160, 0, false);
  const decisions = ["timer", "l2", "l3"].map((mode) =>
    new LiveCarryPolicy({ mode }).decide(state),
  );
  assert.deepEqual(decisions[0], decisions[1]);
  assert.deepEqual(decisions[0], decisions[2]);
  assert.equal(decisions[0].future_limit_cents, state.books[1].ask_cents);
});

test("L2 observes cancellation and replenishment at source time even inside one delivery", () => {
  const price = 10080000;
  const add = (id, clock_ms) => ({
    type: "book",
    market: "future",
    clock_ms,
    cause: "add",
    change: {
      type: "add",
      order: { id, side: "sell", price_cents: price, lots: 100 },
    },
  });
  const run = new LiveShowcase({ mode: "l2", scenario: "queue_demo" });
  run.snapshot = {
    ...snapshot(),
    id: 12,
    revision: 0,
    transport: "kafka",
    books: [
      { id: "spot", bids: [], asks: [] },
      {
        id: "future",
        bids: [],
        asks: [
          { ...level(price, 100), queue: [{ id: "1", qty_atoms: "100" }] },
        ],
      },
    ],
  };
  const accept = (events) =>
    run.accept({
      id: 12,
      revision: run.snapshot.revision + 1,
      transport: "kafka",
      recent: [],
      stage: "running",
      clock_ms: events.at(-1).clock_ms,
      events,
    });
  accept([
    {
      type: "acknowledgement",
      clock_ms: 10,
      order: {
        id: "2",
        market: "future",
        side: "sell",
        price_cents: price,
        lots: 100,
        filled_lots: 0,
        status: "working",
        created_ms: 10,
      },
    },
    add(2, 10),
    add(3, 20),
  ]);
  assert.equal(run.policy.hedgePolicy.estimator.ahead("2"), 100);
  accept([
    {
      type: "book",
      market: "future",
      cause: "cancel",
      clock_ms: 30,
      change: { type: "reduce", id: 1, lots: 100 },
    },
    add(4, 40),
  ]);
  assert.equal(run.policy.hedgePolicy.estimator.ahead("2"), 50);
  assert.equal(run.snapshot.orders[0].ahead_lots, 0);
});

test("a partially executed marketable hedge retries the residual after the bounded interval", () => {
  const state = queueSnapshot(5000, 340, 80);
  const policy = new LiveCarryPolicy({ mode: "l3" });
  policy.commit({ kind: "hedge" }, state);
  state.clock_ms = 5999;
  assert.equal(policy.decide(state).kind, "wait");
  assert.equal(policy.latestDecision.reason_code, "hedge_retry_interval");
  state.clock_ms = 6000;
  const retry = policy.decide(state);
  assert.equal(retry.kind, "hedge");
  assert.equal(retry.assessment.reason_code, "finish_cross");
  assert.equal(retry.residual_btc, 0.1);
});

function entryRun(route, fillLots = 37) {
  const initial = {
    ...snapshot(),
    id: 17,
    revision: 1,
    transport: "kafka",
    scenario: "queue_demo",
    clock_ms: 3000,
    recent: [],
  };
  for (const [i, book] of initial.books.entries()) {
    for (const [j, [name, price]] of [
      ["bids", book.bid_cents],
      ["asks", book.ask_cents],
    ].entries()) {
      book[name] = [
        {
          ...level(price, 100),
          queue: [{ id: String(i * 2 + j + 1), qty_atoms: "100" }],
        },
      ];
    }
  }
  const calls = [];
  const observations = [];
  const run = new LiveShowcase({ entryMode: "l3" });
  // Stub the choice, independently exercised by entry-policy tests, to cover
  // each response including a partial or entirely unfilled protected IOC.
  run.policy.entryPolicy = {
    observe(before, after, events) {
      observations.push([
        before.clock_ms,
        after.clock_ms,
        events.map((event) => event.clock_ms),
      ]);
    },
    assess(state) {
      const [spot, future] = initial.books;
      return {
        mode: "l3",
        clock_ms: state.clock_ms,
        action: route,
        reason_code: route,
        reason: "Test route",
        lots: 100,
        spot_limit_cents:
          route === "skip"
            ? null
            : route === "cross_spot"
              ? spot.ask_cents
              : spot.bid_cents,
        future_limit_cents:
          route === "skip"
            ? null
            : route === "cross_future"
              ? future.bid_cents
              : future.ask_cents,
        estimate_cents: 500,
        carry: { projected_pnl_cents: 500 },
      };
    },
  };
  run.request = async (path, body) => {
    calls.push({ path, body });
    if (path === "/sessions") return initial;
    const clock = run.snapshot.clock_ms;
    const reply = {
      id: initial.id,
      revision: run.snapshot.revision + 1,
      transport: "kafka",
      stage: "running",
      recent: [],
      clock_ms: clock,
      events: [],
    };
    if (path.endsWith("/step"))
      return { ...reply, clock_ms: clock + body.milliseconds };
    assert.ok(path.endsWith("/entry") || path.endsWith("/orders"));
    const addEvent = (type, payload) =>
      reply.events.push({ type, clock_ms: clock, ...payload });
    const markets =
      body.cross_market === "future" ? ["future", "spot"] : ["spot", "future"];
    for (const market of markets) {
      const crossed = body.cross_market === market;
      const lots = !body.cross_market || crossed ? body.lots : fillLots;
      if (!lots) continue;
      const side = market === "spot" ? "buy" : "sell";
      const price = body[market + "_limit_cents"];
      const id = market === "spot" ? "11" : "12";
      addEvent("acknowledgement", {
        order: {
          id,
          market,
          side,
          price_cents: price,
          lots,
          created_ms: clock,
          status: "working",
          filled_lots: 0,
        },
      });
      if (!crossed) {
        addEvent("book", {
          market,
          cause: "add",
          change: {
            type: "add",
            order: { id, side, lots, price_cents: price },
          },
        });
      } else {
        if (fillLots) {
          const maker = market === "spot" ? 2 : 3;
          addEvent("book", {
            market,
            cause: "fill",
            change: { type: "reduce", id: maker, lots: fillLots },
          });
          addEvent("trade", {
            market,
            trade: {
              maker_id: maker,
              taker_id: +id,
              maker_side: side === "buy" ? "sell" : "buy",
              lots: fillLots,
              price_cents: price,
            },
          });
          addEvent("fill", {
            fill: {
              order_id: id,
              market,
              side,
              price_cents: price,
              lots: fillLots,
              clock_ms: clock,
              liquidity: "taker",
              fee_cents: Math.round((price * fillLots * 3) / 1e7),
            },
          });
        }
        if (fillLots < lots) addEvent("cancelled", { order_id: id });
      }
    }
    return reply;
  };
  return { run, calls, observations };
}

test("rest-both uses the paired limit endpoint and keeps the accepted pre-order assessment", async () => {
  const { run, calls } = entryRun("rest_both");
  await run.start();
  const frame = await run.tick(100, { pauseAtMilestones: true });
  assert.equal(frame.pauseReason, "placement");
  assert.equal(calls.at(-1).path, "/sessions/17/orders");
  assert.equal("cross_market" in calls.at(-1).body, false);
  assert.equal(run.latestEntryDecision.clock_ms, 3000);
  assert.equal(run.latestEntryDecision.action, "rest_both");
  assert.deepEqual(
    run.actions[0].entry_execution.orders.map((o) => o.remaining_lots),
    [100, 100],
  );
  assert.equal(run.entryTrace[0].status, "submitted");
});

test("either protected IOC holds both same-clock checkpoints and sizes the companion from actual fills", async () => {
  for (const market of ["spot", "future"]) {
    const { run, calls } = entryRun("cross_" + market);
    await run.start();
    const placement = await run.tick(100, { pauseAtMilestones: true });
    assert.equal(placement.pauseReason, "placement");
    assert.equal(
      placement.snapshot.metrics.residual_btc,
      market === "spot" ? 0.037 : -0.037,
    );
    assert.equal(calls.at(-1).path, "/sessions/17/entry");
    assert.equal(calls.at(-1).body.cross_market, market);
    const receipt = run.actions[0].entry_execution;
    assert.equal(receipt.ioc_filled_lots, 37);
    assert.equal(receipt.companion_lots, 37);
    assert.equal(
      receipt.orders.find((o) => o.market === market).status,
      "cancelled",
    );
    const exposure = await run.tick(100, { pauseAtMilestones: true });
    assert.equal(exposure.pauseReason, "exposure");
    assert.equal(exposure.snapshot, placement.snapshot);
    assert.equal(calls.length, 2);
    const resumed = await run.tick(100, { pauseAtMilestones: true });
    assert.equal(resumed.snapshot.clock_ms, 3100);
    const fast = entryRun("cross_" + market).run;
    await fast.start();
    await fast.tick(100);
    assert.deepEqual(resumed.snapshot, fast.snapshot);
    assert.deepEqual(run.actions, fast.actions);
  }
});

test("an unfilled IOC opens no companion or exposure and the next observation shows the cooldown", async () => {
  const { run } = entryRun("cross_spot", 0);
  await run.start();
  await run.tick(100);
  assert.equal(run.actions[0].entry_execution.companion_lots, 0);
  assert.equal(run.actions[0].entry_execution.orders.length, 1);
  assert.equal(run.snapshot.metrics.residual_btc, 0);
  assert.equal(run.policy.phase, "scanning");
  assert.equal(run.latestEntryDecision.eligible, false);
  assert.equal(run.latestEntryDecision.eligible_after_ms, 33000);
});

test("actionable skips coalesce in the decision trace and never send an order", async () => {
  const { run, calls } = entryRun("skip");
  await run.start();
  await run.tick(100);
  await run.tick(100);
  assert.deepEqual(
    calls.map((call) => call.path),
    ["/sessions", "/sessions/17/step", "/sessions/17/step"],
  );
  assert.equal(run.actions.length, 0);
  assert.equal(run.entryTrace.length, 1);
  assert.equal(run.entryTrace[0].evaluations, 2);
  assert.equal(run.entryTrace[0].through_clock_ms, 3100);
  assert.equal(run.latestEntryDecision.clock_ms, 3200);
});

test("entry warmup observes each source timestamp and then a quiet end-of-step interval", async () => {
  const { run, observations } = entryRun("skip");
  await run.start();
  const add = (clock_ms, id) => ({
    type: "book",
    clock_ms,
    market: "spot",
    cause: "add",
    change: {
      type: "add",
      order: { id, side: "buy", price_cents: 9999900, lots: 5 },
    },
  });
  run.accept({
    id: 17,
    revision: 2,
    transport: "kafka",
    clock_ms: 3100,
    recent: [],
    stage: "running",
    events: [add(3020, 20), add(3020, 21), add(3060, 22)],
  });
  assert.deepEqual(observations, [
    [3000, 3000, []],
    [3000, 3020, [3020, 3020]],
    [3020, 3060, [3060]],
    [3060, 3100, []],
  ]);
});
