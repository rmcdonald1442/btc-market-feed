import test from "node:test";
import assert from "node:assert/strict";
import {
  ArtifactPlayer,
  EventProjector,
  assertArtifactParity,
  createArtifact,
  parseArtifact,
} from "./browser-artifact.mjs";
import { MODEL_V2 } from "./finance.mjs";

function initialSnapshot() {
  const book = (id) => ({
    id,
    price_scale: 100,
    qty_scale: 1000,
    exchange_ns: "0",
    position: 1,
    bid_cents: null,
    ask_cents: null,
    bids: [],
    asks: [],
  });
  return {
    seed: 42,
    clock_ms: 0,
    elapsed_ms: 0,
    next_event_ms: 100,
    start_ms: 0,
    end_ms: 86_400_000,
    stage: "running",
    books: [book("spot"), book("future")],
    orders: [],
    fills: [],
    history: [],
    prints: [],
    visual_events: [],
    metrics: {},
    feed_stats: {
      spot: {
        adds: 0,
        amends: 0,
        cancels: 0,
        maker_fills: 0,
        trade_prints: 0,
        startup: {
          adds: 0,
          amends: 0,
          cancels: 0,
          maker_fills: 0,
          trade_prints: 0,
        },
      },
      future: {
        adds: 0,
        amends: 0,
        cancels: 0,
        maker_fills: 0,
        trade_prints: 0,
        startup: {
          adds: 0,
          amends: 0,
          cancels: 0,
          maker_fills: 0,
          trade_prints: 0,
        },
      },
    },
  };
}

function fixtureEvents() {
  return [
    {
      clock_ms: 100,
      type: "book",
      market: "spot",
      cause: "add",
      change: {
        type: "add",
        order: { id: 1, side: "buy", price_cents: 10_000, lots: 10 },
      },
    },
    {
      clock_ms: 100,
      type: "book",
      market: "spot",
      cause: "add",
      change: {
        type: "add",
        order: { id: 2, side: "sell", price_cents: 10_200, lots: 4 },
      },
    },
    {
      clock_ms: 100,
      type: "book",
      market: "future",
      cause: "add",
      change: {
        type: "add",
        order: { id: 3, side: "buy", price_cents: 10_400, lots: 6 },
      },
    },
    {
      clock_ms: 100,
      type: "book",
      market: "future",
      cause: "add",
      change: {
        type: "add",
        order: { id: 4, side: "sell", price_cents: 10_600, lots: 4 },
      },
    },
    {
      clock_ms: 100,
      type: "tick",
      rng: 99,
      fair_cents: 10_000,
      sample: true,
      elapsed_ms: 100,
      next_event_ms: 200,
    },
    {
      clock_ms: 110,
      type: "acknowledgement",
      order: {
        id: "5",
        market: "spot",
        side: "buy",
        price_cents: 10_000,
        lots: 8,
        filled_lots: 0,
        status: "working",
        created_ms: 110,
      },
    },
    {
      clock_ms: 110,
      type: "book",
      market: "spot",
      cause: "add",
      change: {
        type: "add",
        order: { id: 5, side: "buy", price_cents: 10_000, lots: 8 },
      },
    },
    {
      clock_ms: 120,
      type: "book",
      market: "spot",
      cause: "amend",
      change: { type: "amend", id: 1, price_cents: 10_000, lots: 6 },
    },
    {
      clock_ms: 130,
      type: "book",
      market: "spot",
      cause: "amend",
      change: { type: "amend", id: 5, price_cents: 9_900, lots: 9 },
    },
    {
      clock_ms: 140,
      type: "trade",
      market: "spot",
      trade: {
        maker_id: 2,
        taker_id: 6,
        price_cents: 10_200,
        lots: 2,
        maker_side: "sell",
      },
    },
    {
      clock_ms: 150,
      type: "book",
      market: "spot",
      cause: "fill",
      change: { type: "reduce", id: 5, lots: 2 },
    },
    {
      clock_ms: 150,
      type: "fill",
      fill: {
        order_id: "5",
        market: "spot",
        side: "buy",
        price_cents: 10_000,
        lots: 2,
        clock_ms: 150,
        liquidity: "maker",
        fee_cents: 1,
      },
    },
    {
      clock_ms: 160,
      type: "book",
      market: "spot",
      cause: "cancel",
      change: { type: "reduce", id: 5, lots: 7 },
    },
    { clock_ms: 160, type: "cancelled", order_id: "5" },
    {
      clock_ms: 200,
      type: "tick",
      rng: 100,
      fair_cents: 10_050,
      sample: false,
      elapsed_ms: 200,
      next_event_ms: 300,
    },
  ];
}

test("projects book FIFO, own lifecycle, fills, prints, history and feed activity", () => {
  const projector = new EventProjector(initialSnapshot());
  let afterFill, afterPriorityReduction;
  for (const event of fixtureEvents()) {
    projector.apply(event);
    if (event.clock_ms === 120) afterPriorityReduction = projector.snapshot();
    if (event.clock_ms === 150) afterFill = projector.snapshot();
  }
  const snapshot = projector.snapshot();
  const spotBid = snapshot.books[0].bids.find(
    (level) => level.price_atoms === "10000",
  );
  assert.deepEqual(
    spotBid.queue.map((order) => order.id),
    ["1"],
  );
  assert.deepEqual(
    afterPriorityReduction.books[0].bids[0].queue.map((order) => order.id),
    ["1", "5"],
  );
  assert.equal(
    afterFill.books[0].bids.find((level) => level.price_atoms === "9900")
      .qty_atoms,
    "7",
  );
  assert.equal(snapshot.orders[0].status, "cancelled");
  assert.equal(snapshot.orders[0].filled_lots, 2);
  assert.equal(snapshot.orders[0].remaining_lots, 0);
  assert.equal(snapshot.fills.length, 1);
  assert.equal(snapshot.prints[0].side, "buy");
  assert.equal(snapshot.history.length, 1);
  assert.equal(snapshot.feed_stats.spot.adds, 3);
  assert.equal(snapshot.feed_stats.spot.amends, 2);
  assert.equal(snapshot.feed_stats.spot.maker_fills, 1);
  assert.equal(snapshot.visual_events.length, 9);
  assert.equal(snapshot.feed_stats.spot.cancels, 1);
  assert.equal(snapshot.metrics.spot_btc, 0.002);
});

test("artifact validates, serializes, pages by timestamp, resets and checks canonical parity", () => {
  const events = fixtureEvents();
  const projector = new EventProjector(initialSnapshot());
  for (const event of events) projector.apply(event);
  const artifact = createArtifact(
    initialSnapshot(),
    events,
    projector.snapshot(),
  );
  const parsed = parseArtifact(JSON.stringify(artifact));
  assert.equal(assertArtifactParity(parsed), true);
  const player = new ArtifactPlayer(parsed);
  player.advanceTo(115);
  assert.equal(player.appliedRows, 7);
  assert.equal(player.snapshot().clock_ms, 115);
  player.advanceTo(200);
  assert.equal(player.done, true);
  assert.equal(player.appliedRows, events.length);
  player.reset();
  assert.equal(player.appliedRows, 0);
  assert.equal(player.snapshot().clock_ms, 0);
});

test("advanceTo can apply artifact events without constructing a display snapshot", () => {
  const player = new ArtifactPlayer(
    createArtifact(initialSnapshot(), fixtureEvents()),
  );
  assert.equal(player.advanceTo(115, { snapshot: false }), undefined);
  assert.equal(player.appliedRows, 7);
  assert.equal(player.clockMs, 115);
  assert.equal(player.snapshot().clock_ms, 115);
  assert.equal(player.advanceTo(200).clock_ms, 200);
});

test("rejects malformed books, event types, invalid amendments and unordered timestamps", () => {
  const base = initialSnapshot();
  assert.throws(
    () =>
      parseArtifact(
        JSON.stringify({
          schema_version: 1,
          format: "btc-execution-events",
          initial_snapshot: { ...base, books: [] },
          events: [],
        }),
      ),
    /book set|missing spot book/,
  );
  assert.throws(
    () => createArtifact(base, [{ clock_ms: 1, type: "mystery" }]),
    /Unknown event type/,
  );
  const projector = new EventProjector(base);
  projector.apply({
    clock_ms: 1,
    type: "book",
    market: "spot",
    change: {
      type: "add",
      order: { id: 1, side: "buy", price_cents: 10_000, lots: 2 },
    },
  });
  projector.apply({
    clock_ms: 2,
    type: "book",
    market: "spot",
    change: {
      type: "add",
      order: { id: 2, side: "sell", price_cents: 10_200, lots: 2 },
    },
  });
  assert.throws(
    () =>
      projector.apply({
        clock_ms: 3,
        type: "book",
        market: "spot",
        change: { type: "amend", id: 1, price_cents: 10_200, lots: 1 },
      }),
    /cross/,
  );
  assert.throws(
    () =>
      createArtifact(base, [
        { clock_ms: 2, type: "tick", fair_cents: 1 },
        { clock_ms: 1, type: "tick", fair_cents: 1 },
      ]),
    /timestamps must be ordered/,
  );
});

test("semantically replays and rejects malformed no-checkpoint artifacts before playback", () => {
  const base = initialSnapshot();
  assert.throws(
    () =>
      createArtifact(base, [
        {
          clock_ms: 1,
          type: "book",
          market: "spot",
          change: { type: "reduce", id: 999, lots: 1 },
        },
      ]),
    /Unknown order id/,
  );
  assert.throws(
    () =>
      createArtifact(base, [
        {
          clock_ms: 1,
          type: "book",
          market: "spot",
          change: {
            type: "add",
            order: { id: 1, side: "buy", price_cents: 10_200, lots: 1 },
          },
        },
        {
          clock_ms: 1,
          type: "book",
          market: "spot",
          change: {
            type: "add",
            order: { id: 2, side: "sell", price_cents: 10_200, lots: 1 },
          },
        },
      ]),
    /cross/,
  );
  assert.throws(
    () =>
      createArtifact(base, [
        {
          clock_ms: 1,
          type: "book",
          market: "spot",
          change: {
            type: "add",
            order: { id: 1, side: "buy", price_cents: 10_000, lots: 1 },
          },
        },
        {
          clock_ms: 2,
          type: "book",
          market: "spot",
          change: { type: "reduce", id: 1, lots: 1 },
        },
        {
          clock_ms: 3,
          type: "book",
          market: "spot",
          change: {
            type: "add",
            order: { id: 1, side: "buy", price_cents: 10_000, lots: 1 },
          },
        },
      ]),
    /Reused book order ID/,
  );
  assert.throws(
    () => createArtifact({ ...base, model: { version: 99 } }, []),
    /model contract/,
  );
  assert.throws(
    () =>
      createArtifact(base, [
        { clock_ms: 86_400_001, type: "tick", fair_cents: 1 },
      ]),
    /timestamps must be ordered/,
  );
});

test("validates canonical checkpoint against non-book event state too", () => {
  const projector = new EventProjector(initialSnapshot());
  const events = fixtureEvents();
  for (const event of events) projector.apply(event);
  const final = projector.snapshot();
  final.feed_stats.spot.trade_prints += 1;
  assert.throws(
    () => createArtifact(initialSnapshot(), events, final),
    /canonical final snapshot/,
  );
});

test("freezes semantically validated artifacts and rejects malformed financial ownership", () => {
  const artifact = createArtifact(initialSnapshot(), fixtureEvents());
  assert.ok(Object.isFrozen(artifact));
  assert.ok(Object.isFrozen(artifact.events));
  assert.ok(Object.isFrozen(artifact.events[0]));
  assert.throws(() => {
    artifact.events[0].clock_ms = 0;
  }, TypeError);

  const base = initialSnapshot();
  assert.throws(
    () =>
      createArtifact(
        { ...base, model: { ...MODEL_V2, annual_financing_rate: Number.NaN } },
        [],
      ),
    /model contract/,
  );
  assert.throws(
    () =>
      createArtifact(
        {
          ...base,
          fills: [
            {
              order_id: "77",
              market: "spot",
              side: "buy",
              price_cents: 10_000,
              lots: 1,
              clock_ms: 0,
              liquidity: "maker",
              fee_cents: 1,
            },
          ],
        },
        [],
      ),
    /fill ownership/,
  );
});

test("version 2 fills must respect acknowledgement limit and model fee", () => {
  const base = { ...initialSnapshot(), model: structuredClone(MODEL_V2) };
  const ack = {
    clock_ms: 100,
    type: "acknowledgement",
    order: {
      id: "5",
      market: "spot",
      side: "buy",
      price_cents: 10_000,
      lots: 1,
      filled_lots: 0,
      status: "working",
      created_ms: 100,
    },
  };
  const invalidFill = {
    clock_ms: 110,
    type: "fill",
    fill: {
      order_id: "5",
      market: "spot",
      side: "buy",
      price_cents: 10_100,
      lots: 1,
      clock_ms: 110,
      liquidity: "taker",
      fee_cents: 0,
    },
  };
  assert.throws(() => createArtifact(base, [ack, invalidFill]), /limit/);
});
