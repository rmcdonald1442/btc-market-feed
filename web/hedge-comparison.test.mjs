import assert from "node:assert/strict";
import test from "node:test";
import {
  compareEntryPolicies,
  compareHedgePolicies,
} from "./hedge-comparison.mjs";
import { LiveShowcase } from "./live-showcase.mjs";

const sharedPath = { version: 2, arrivals: 4, digest: "a014cfe4e188384b" };
const differentPath = { ...sharedPath, digest: "35147e2043d61576" };

function sessions(t, { projectedPath, canonicalPath, afterStep } = {}) {
  const opened = [],
    closed = [];
  let active = 0;
  t.mock.method(LiveShowcase.prototype, "start", async function () {
    assert.equal(active++, 0, "Only one comparison session can be open");
    opened.push(this.mode);
    this.snapshot = {
      id: opened.length,
      seed: this.seed,
      clock_ms: 0,
      revision: 0,
      transport: "kafka",
      scenario: "queue_demo",
      metrics: { residual_btc: 0 },
    };
    this.actions = [
      {
        kind: "place",
        clock_ms: 0,
        lots: 100,
        spot_limit_cents: 9_999_800,
        future_limit_cents: 10_080_000,
      },
    ];
    return this.snapshot;
  });
  t.mock.method(LiveShowcase.prototype, "tick", async function (ms) {
    this.snapshot = {
      ...this.snapshot,
      clock_ms: this.snapshot.clock_ms + ms,
      revision: this.snapshot.revision + 1,
      market_path: projectedPath?.(this.mode) ?? sharedPath,
    };
    this.brokerRecords++;
    afterStep?.(this);
  });
  t.mock.method(LiveShowcase.prototype, "request", async function () {
    return {
      ...this.snapshot,
      market_path: canonicalPath?.(this.mode) ?? this.snapshot.market_path,
      fills: [
        { side: "buy", clock_ms: 15, lots: 50, liquidity: "maker" },
        { side: "sell", clock_ms: 45, lots: 20, liquidity: "taker" },
      ],
      metrics: {
        residual_btc: 0.03,
        hedged_btc: 0.02,
        projected_pnl_cents: 22,
      },
      model: { lot_btc: 0.001 },
      orders: [{ remaining_lots: 30 }],
    };
  });
  t.mock.method(LiveShowcase.prototype, "close", async function () {
    assert.equal(active--, 1);
    closed.push(this.mode);
  });
  return { opened, closed };
}

test("integrates partial private fills between policy evaluations through a common horizon", async (t) => {
  const { opened, closed } = sessions(t);
  const progress = [];
  const report = await compareHedgePolicies({
    horizonMs: 100,
    onProgress: (step) => progress.push(step.clockMs),
  });
  assert.deepEqual(opened, ["timer", "l2", "l3"]);
  assert.deepEqual(closed, opened);
  assert.deepEqual(progress, [0, 100, 0, 100, 0, 100]);
  assert.equal(report.pathDigest, sharedPath.digest);
  assert.equal(report.pathVersion, sharedPath.version);
  for (const row of report.rows) {
    // 0.050 BTC from 15–45 ms, then 0.030 BTC from 45 ms to the horizon.
    assert.equal(row.exposedMs, 85);
    assert.ok(
      Math.abs(row.exposureBtcSeconds - (0.05 * 0.03 + 0.03 * 0.055)) < 1e-12,
    );
    assert.equal(row.peakExposureBtc, 0.05);
    assert.equal(row.crossedBtc, 0.02);
    assert.equal(row.hedgedBtc, 0.02);
    assert.equal(row.residualBtc, 0.03);
    assert.equal(row.status, "exposed");
    assert.equal(row.completed, false);
    assert.equal(row.projectedCarryCents, 22);
  }
});

test("a different external path discards the comparison and closes both runs", async (t) => {
  const { opened, closed } = sessions(t, {
    projectedPath: (mode) => (mode === "l2" ? differentPath : sharedPath),
  });
  await assert.rejects(
    compareHedgePolicies({ horizonMs: 100 }),
    /External market paths differ for l2/,
  );
  assert.deepEqual(opened, ["timer", "l2"]);
  assert.deepEqual(closed, opened);
});

test("canonical and Kafka-projected path fingerprints must agree within each run", async (t) => {
  const { opened, closed } = sessions(t, {
    canonicalPath: () => differentPath,
  });
  await assert.rejects(
    compareHedgePolicies({ horizonMs: 100 }),
    /differs from the Kafka projection/,
  );
  assert.deepEqual(opened, ["timer"]);
  assert.deepEqual(closed, opened);
});

test("cancellation drains the in-flight step and closes the active session", async (t) => {
  const controller = new AbortController();
  const { opened, closed } = sessions(t, {
    afterStep: () => controller.abort(),
  });
  await assert.rejects(
    compareHedgePolicies({ horizonMs: 100, signal: controller.signal }),
    { name: "AbortError" },
  );
  assert.deepEqual(opened, ["timer"]);
  assert.deepEqual(closed, opened);
});

for (const variant of ["shared", "different book", "no order sent"]) {
  test(`only attributes divergent submitted actions to an identical pre-command state (${variant})`, async (t) => {
    sessions(t, {
      afterStep(run) {
        if (run.snapshot.clock_ms === 100) {
          Object.assign(run.snapshot, {
            metrics: { residual_btc: 0.03 },
            books: [
              {
                id: "future",
                bids: [],
                asks: [
                  {
                    price_atoms: "10080000",
                    queue: [
                      {
                        id: "12",
                        qty_atoms:
                          variant === "different book" && run.mode === "l3"
                            ? "40"
                            : "20",
                      },
                    ],
                  },
                ],
              },
            ],
            orders: [{ id: "1", remaining_lots: 30 }],
            fills: [{ side: "buy", lots: 30, clock_ms: 70 }],
          });
          run.policy.latestDecision = {
            mode: run.mode,
            market: "future",
            side: "sell",
            clock_ms: 100,
            action: run.mode === "l3" ? "cross" : "wait",
            ahead_lots: run.mode === "l3" ? 20 : 5,
          };
        } else if (run.mode === "l3" && variant !== "no order sent") {
          run.actions.push({
            kind: "hedge",
            residual_btc: 0.03,
            clock_ms: 100,
            assessment: run.latestDecision,
          });
        }
      },
    });
    const { firstQueueDifference: difference } = await compareHedgePolicies({
      horizonMs: 200,
    });
    if (variant !== "shared") {
      assert.equal(difference, null);
      return;
    }
    assert.equal(difference.clockMs, 100);
    assert.equal(difference.residualBtc, 0.03);
    assert.equal(difference.l2.action, "wait");
    assert.equal(difference.l2.ahead_lots, 5);
    assert.equal(difference.l3.action, "cross");
    assert.equal(difference.l3.ahead_lots, 20);
    assert.equal(difference.pathDigest, sharedPath.digest);
  });
}

function entrySessions(
  t,
  {
    l2Route = "skip",
    differentState = false,
    mismatchPath = false,
    abortAfterFirstTick,
  } = {},
) {
  const opened = [],
    closed = [];
  t.mock.method(LiveShowcase.prototype, "start", async function () {
    assert.equal(this.mode, "l3", "Only the entry information may vary");
    opened.push(this.entryMode);
    const isL2 = this.entryMode === "l2";
    const route = isL2 ? l2Route : "cross_spot";
    this.policy.latestEntryDecision = {
      clock_ms: 0,
      action: route,
      reason: "fixture",
      lots: 100,
      spot_limit_cents: route === "rest_both" ? 99 : 101,
      future_limit_cents: 109,
    };
    this.snapshot = {
      seed: this.seed,
      clock_ms: 0,
      revision: 0,
      id: opened.length,
      transport: "kafka",
      scenario: "queue_demo",
      books: [
        {
          id: "spot",
          bids: [
            {
              price_atoms: "99",
              qty_atoms: differentState && isL2 ? "8" : "9",
            },
          ],
          asks: [],
        },
      ],
      orders: [],
      fills: [],
      market_path: mismatchPath && isL2 ? differentPath : sharedPath,
      metrics: { hedged_btc: 0, residual_btc: 0, projected_pnl_cents: null },
      model: { lot_btc: 0.001 },
    };
    return this.snapshot;
  });
  t.mock.method(LiveShowcase.prototype, "tick", async function (ms) {
    const before = this.snapshot,
      info = this.policy.latestEntryDecision;
    const snapshots = {
      ...before,
      clock_ms: before.clock_ms + ms,
      revision: before.revision + 1,
    };
    const events = [];
    if (before.clock_ms === 0 && info.action !== "skip") {
      this.actions.push({
        kind: "place",
        clock_ms: before.clock_ms,
        entry_action: info.action,
        entry_assessment: info,
        entry_execution: {
          orders: [{ id: "entry-spot" }, { id: "entry-future" }],
        },
        lots: info.lots,
        spot_limit_cents: info.spot_limit_cents,
        future_limit_cents: info.future_limit_cents,
      });
      for (const id of ["entry-spot", "entry-future"])
        events.push({ type: "acknowledgement", order: { id } });
      snapshots.fills = [
        {
          order_id: "entry-spot",
          side: "buy",
          clock_ms: 0,
          lots: 100,
          liquidity: info.action === "cross_spot" ? "taker" : "maker",
        },
        {
          order_id: "entry-future",
          side: "sell",
          clock_ms: 25,
          lots: 75,
          liquidity: "maker",
        },
      ];
      snapshots.metrics = {
        hedged_btc: 0.075,
        residual_btc: 0.025,
        projected_pnl_cents: 70,
      };
    } else if (before.fills.length) {
      snapshots.fills = [
        ...before.fills,
        {
          order_id: "hedge-future",
          side: "sell",
          clock_ms: 100,
          lots: 25,
          liquidity: "taker",
        },
      ];
      snapshots.metrics = {
        hedged_btc: 0.1,
        residual_btc: 0,
        projected_pnl_cents: 93,
      };
    }
    this.snapshot = snapshots;
    this.brokerRecords++;
    abortAfterFirstTick?.();
    return { events };
  });
  t.mock.method(LiveShowcase.prototype, "request", async function () {
    return this.snapshot;
  });
  t.mock.method(LiveShowcase.prototype, "close", async function () {
    closed.push(this.entryMode);
  });
  return { opened, closed };
}

for (const l2Route of ["skip", "rest_both"]) {
  test(`holds hedge fixed and attributes ${l2Route} versus crossing spot to the same pre-entry book`, async (t) => {
    const { opened, closed } = entrySessions(t, { l2Route });
    const result = await compareEntryPolicies({ horizonMs: 200 });
    assert.deepEqual(opened, ["l2", "l3"]);
    assert.deepEqual(closed, opened);
    assert.equal(result.fixedHedgeMode, "l3");
    assert.equal(result.firstEntryDifference.clockMs, 0);
    assert.equal(result.firstEntryDifference.l2.action, l2Route);
    assert.equal(result.firstEntryDifference.l2.submitted, l2Route !== "skip");
    assert.equal(result.firstEntryDifference.l3.action, "cross_spot");
    const [l2, l3] = result.rows;
    assert.equal(l3.entryTakerBtc, 0.1);
    assert.equal(l3.hedgeTakerBtc, 0.025);
    assert.equal(l3.entryAction, "cross_spot");
    if (l2Route === "skip") {
      assert.equal(l2.status, "no entry");
      assert.equal(l2.projectedCarryCents, null);
      assert.equal(l2.hedgedBtc, 0);
      assert.equal(l2.placement, null);
    } else assert.equal(l2.placement.spotLimitCents, 99);
  });
}

test("different pre-entry books cannot establish a common-state route decision", async (t) => {
  entrySessions(t, { differentState: true });
  const result = await compareEntryPolicies({ horizonMs: 100 });
  assert.equal(result.firstEntryDifference, null);
});

test("entry path mismatch invalidates the pair and closes both sessions", async (t) => {
  const { opened, closed } = entrySessions(t, { mismatchPath: true });
  await assert.rejects(
    compareEntryPolicies({ horizonMs: 100 }),
    /External market paths differ/,
  );
  assert.deepEqual(closed, opened);
});

test("cancelling between entry branches cleans up both open sessions", async (t) => {
  const aborter = new AbortController();
  const { opened, closed } = entrySessions(t, {
    abortAfterFirstTick: () => aborter.abort(),
  });
  await assert.rejects(
    compareEntryPolicies({ horizonMs: 100, signal: aborter.signal }),
    { name: "AbortError" },
  );
  assert.deepEqual(closed, opened);
});
