import { LiveShowcase, isHedgeComplete } from "./live-showcase.mjs";

const MODES = ["timer", "l2", "l3"];
const STEP_MS = 100;
export const COMPARISON_HORIZON_MS = 20_000;

function samePath(a, b) {
  return (
    a?.version === b.version &&
    a.arrivals === b.arrivals &&
    a.digest === b.digest
  );
}

function executionState(snapshot) {
  // Drop session IDs and transport metadata. Equality here compares the full
  // visible FIFO books and private execution history before either action.
  return JSON.stringify([
    snapshot.market_path,
    snapshot.books.map(({ id, bids, asks }) => [id, bids, asks]),
    snapshot.orders,
    snapshot.fills,
  ]);
}

// Integrate the actual private fills at exchange time, including exposure still
// open at the common horizon. A partially filled order can change the exposure
// several times within a single 100 ms policy evaluation interval.
function fillOutcome(snapshot) {
  const lotBtc = snapshot.model.lot_btc;
  let clockMs = 0;
  let netLots = 0;
  let exposureLotMs = 0;
  let exposedMs = 0;
  let crossedLots = 0;
  let peakLots = 0;

  function advance(toMs) {
    if (
      !Number.isSafeInteger(toMs) ||
      toMs < clockMs ||
      toMs > snapshot.clock_ms
    )
      throw new Error("Private fill times are outside the comparison window.");
    const ms = toMs - clockMs;
    if (netLots !== 0 && ms > 0) {
      exposureLotMs += Math.abs(netLots) * ms;
      exposedMs += ms;
      peakLots = Math.max(peakLots, Math.abs(netLots));
    }
    clockMs = toMs;
  }
  for (const fill of snapshot.fills) {
    advance(fill.clock_ms);
    netLots += fill.side === "buy" ? fill.lots : -fill.lots;
    if (fill.liquidity === "taker") crossedLots += fill.lots;
  }
  advance(snapshot.clock_ms);
  peakLots = Math.max(peakLots, Math.abs(netLots));
  if (
    !Number.isFinite(lotBtc) ||
    lotBtc <= 0 ||
    Math.abs(netLots * lotBtc - snapshot.metrics.residual_btc) > 1e-9
  )
    throw new Error("Private fills disagree with the final BTC exposure.");
  return {
    fillCount: snapshot.fills.length,
    firstFillMs: snapshot.fills[0]?.clock_ms ?? null,
    crossedBtc: crossedLots * lotBtc,
    exposureBtcSeconds: (exposureLotMs * lotBtc) / 1000,
    exposedMs,
    peakExposureBtc: peakLots * lotBtc,
  };
}

function outcome(mode, snapshot, actions) {
  const { residual_btc: residualBtc, hedged_btc: hedgedBtc } = snapshot.metrics;
  const completed = isHedgeComplete(snapshot);
  const placement = actions.find((action) => action.kind === "place");
  const cross = actions.find(
    (action) => action.kind === "hedge" && action.residual_btc !== 0,
  );
  const working = snapshot.orders.some((order) => order.remaining_lots > 0);
  let status = "no entry";
  if (placement) status = "unfilled";
  if (working) status = "working";
  if (residualBtc !== 0) status = "exposed";
  if (completed) status = "hedged";
  return {
    mode,
    status,
    completed,
    hedgedBtc,
    residualBtc,
    projectedCarryCents:
      hedgedBtc > 0 ? snapshot.metrics.projected_pnl_cents : null,
    hedgeFilledMs: completed ? snapshot.fills.at(-1).clock_ms : null,
    firstCrossMs: cross?.clock_ms ?? null,
    crossReason: cross?.assessment?.reason ?? cross?.message ?? null,
    placement: placement
      ? {
          clockMs: placement.clock_ms,
          lots: placement.lots,
          spotLimitCents: placement.spot_limit_cents,
          futureLimitCents: placement.future_limit_cents,
        }
      : null,
    ...fillOutcome(snapshot),
  };
}

async function startRun(run, seed) {
  const initial = await run.start();
  if (
    initial.transport !== "kafka" ||
    initial.scenario !== "queue_demo" ||
    initial.seed !== seed ||
    initial.clock_ms !== 0
  )
    throw new Error("The broker did not start the requested comparison path.");
}

async function verifiedSnapshot(run, seed, horizonMs) {
  const snapshot = await run.request(
    `/sessions/${run.snapshot.id}?revision=${run.snapshot.revision}`,
    null,
    "GET",
  );
  if (
    snapshot.clock_ms !== horizonMs ||
    snapshot.revision !== run.snapshot.revision ||
    snapshot.id !== run.snapshot.id ||
    snapshot.transport !== "kafka" ||
    snapshot.scenario !== "queue_demo" ||
    snapshot.seed !== seed
  )
    throw new Error("The canonical snapshot does not match the Kafka run.");
  const path = snapshot.market_path;
  if (
    !Number.isSafeInteger(path?.version) ||
    path.version < 1 ||
    !Number.isSafeInteger(path.arrivals) ||
    path.arrivals < 1 ||
    typeof path.digest !== "string" ||
    !/^[0-9a-f]{16}$/.test(path.digest)
  )
    throw new Error("The market did not provide an external-flow fingerprint.");
  if (!samePath(run.snapshot.market_path, path) || run.brokerRecords < 1)
    throw new Error("Canonical market flow differs from the Kafka projection.");
  return snapshot;
}

function validateWindow(seed, horizonMs) {
  if (!Number.isSafeInteger(seed) || seed < 0)
    throw new Error("Seed must be a nonnegative safe integer.");
  if (
    !Number.isSafeInteger(horizonMs) ||
    horizonMs < STEP_MS ||
    horizonMs > 86_400_000 ||
    horizonMs % STEP_MS !== 0
  )
    throw new Error(
      "Use a horizon in 100 ms increments within the simulated day.",
    );
}

function closeOnNavigation(run, base) {
  const closeOnPageHide = () => {
    if (run.snapshot && !run.closed)
      void fetch(`${base}/sessions/${run.snapshot.id}`, {
        method: "DELETE",
        keepalive: true,
      }).catch(() => {});
  };
  globalThis.addEventListener?.("pagehide", closeOnPageHide);
  return () => globalThis.removeEventListener?.("pagehide", closeOnPageHide);
}

async function runPolicy({
  mode,
  seed,
  base,
  horizonMs,
  completedModes,
  onProgress,
  signal,
  recordDecision,
}) {
  const run = new LiveShowcase({ mode, seed, scenario: "queue_demo", base });
  let firstWaitAssessment = null;
  const progress = (clockMs) =>
    onProgress?.({ mode, completedModes, clockMs, horizonMs });
  const removeListener = closeOnNavigation(run, base);
  try {
    signal?.throwIfAborted();
    progress(0);
    await startRun(run, seed);
    while (run.snapshot.clock_ms < horizonMs) {
      // Let an acknowledged mutation finish before deleting an aborted session.
      signal?.throwIfAborted();
      const before = run.snapshot;
      const assessment = run.latestDecision;
      const actionCount = run.actions.length;
      // This is the information available just before the policy acts.
      if (
        !firstWaitAssessment &&
        before.metrics.residual_btc !== 0 &&
        assessment?.action === "wait"
      )
        firstWaitAssessment = assessment;
      await run.tick(STEP_MS);
      if (run.snapshot.clock_ms !== before.clock_ms + STEP_MS)
        throw new Error("Policy evaluation clocks diverged.");
      if (
        mode !== "timer" &&
        before.metrics.residual_btc !== 0 &&
        assessment?.clock_ms === before.clock_ms
      ) {
        const crossed = run.actions
          .slice(actionCount)
          .some(
            (action) => action.kind === "hedge" && action.residual_btc !== 0,
          );
        recordDecision(mode, before, {
          ...assessment,
          action: crossed ? "cross" : "wait",
        });
      }
      if (
        run.snapshot.clock_ms % 1000 === 0 ||
        run.snapshot.clock_ms === horizonMs
      )
        progress(run.snapshot.clock_ms);
    }
    signal?.throwIfAborted();
    const snapshot = await verifiedSnapshot(run, seed, horizonMs);
    const path = snapshot.market_path;
    const row = outcome(mode, snapshot, run.actions);
    row.firstWaitAssessment = firstWaitAssessment;
    row.firstCrossAssessment =
      run.actions.find(
        (action) => action.kind === "hedge" && action.residual_btc !== 0,
      )?.assessment ?? null;
    return { path, row };
  } finally {
    try {
      await run.close();
    } finally {
      removeListener();
    }
  }
}

// Compare independent positions on one external path. Each policy sees the
// same 100 ms evaluation clock and can change its own book through execution.
// Keep one comparison session open at a time alongside the visible demo.
export async function compareHedgePolicies({
  seed = 73,
  base = "/api",
  horizonMs = COMPARISON_HORIZON_MS,
  onProgress,
  signal,
} = {}) {
  validateWindow(seed, horizonMs);
  const rows = [];
  let path;
  const l2Decisions = new Map();
  let firstQueueDifference = null;
  function recordDecision(mode, snapshot, assessment) {
    if (mode === "l2") {
      l2Decisions.set(snapshot.clock_ms, {
        state: executionState(snapshot),
        assessment,
      });
      return;
    }
    const other = l2Decisions.get(snapshot.clock_ms);
    if (
      firstQueueDifference ||
      !other ||
      other.assessment.action === assessment.action
    )
      return;
    if (other.state !== executionState(snapshot)) return;
    firstQueueDifference = {
      clockMs: snapshot.clock_ms,
      market: assessment.market,
      side: assessment.side,
      residualBtc: snapshot.metrics.residual_btc,
      pathDigest: snapshot.market_path.digest,
      l2: other.assessment,
      l3: assessment,
    };
  }
  for (const mode of MODES) {
    const result = await runPolicy({
      mode,
      seed,
      base,
      horizonMs,
      completedModes: rows.length,
      onProgress,
      signal,
      recordDecision,
    });
    signal?.throwIfAborted();
    if (path && !samePath(result.path, path))
      throw new Error(
        `External market paths differ for ${mode}. Comparison discarded.`,
      );
    if (
      rows.length &&
      JSON.stringify(rows[0].placement) !== JSON.stringify(result.row.placement)
    )
      throw new Error(`Entry orders differ for ${mode}. Comparison discarded.`);
    path = result.path;
    rows.push(result.row);
  }
  return {
    seed,
    horizonMs,
    pathVersion: path.version,
    pathArrivals: path.arrivals,
    pathDigest: path.digest,
    firstQueueDifference,
    rows,
  };
}

function entryInstruction(assessment, placement) {
  // The assessment can recommend a route while the session is cooling down.
  // Only an acknowledged place action counts as a submitted entry.
  if (!assessment) return null;
  return {
    ...assessment,
    assessment_action: assessment.action,
    action: placement?.entry_action ?? "skip",
    submitted: Boolean(placement),
    order: placement
      ? {
          lots: placement.lots,
          spotLimitCents: placement.spot_limit_cents,
          futureLimitCents: placement.future_limit_cents,
        }
      : null,
  };
}

function differentInstruction(a, b) {
  return (
    a &&
    b &&
    (a.action !== b.action ||
      JSON.stringify(a.order) !== JSON.stringify(b.order))
  );
}

async function stepEntry(run, clockMs) {
  const candidate = run.latestEntryDecision;
  const assessment = candidate?.clock_ms === clockMs ? candidate : null;
  const actionCount = run.actions.length;
  await run.tick(STEP_MS);
  if (run.snapshot.clock_ms !== clockMs + STEP_MS)
    throw new Error("Entry policy evaluation clocks diverged.");
  const placement = run.actions
    .slice(actionCount)
    .find((action) => action.kind === "place");
  return entryInstruction(placement?.entry_assessment ?? assessment, placement);
}

function entryOutcome(run, snapshot) {
  const { entryMode } = run;
  const row = outcome(entryMode, snapshot, run.actions);
  const placements = run.actions.filter((action) => action.kind === "place");
  const entryIds = new Set(
    placements.flatMap((action) =>
      action.entry_execution.orders.map((order) => String(order.id)),
    ),
  );
  const takerLots = { entry: 0, hedge: 0 };
  for (const fill of snapshot.fills) {
    if (fill.liquidity === "taker") {
      const leg = entryIds.has(String(fill.order_id)) ? "entry" : "hedge";
      takerLots[leg] += fill.lots;
    }
  }
  const first = placements[0];
  return {
    ...row,
    entryMode,
    hedgeMode: "l3",
    entryAction: first?.entry_action ?? null,
    entryAssessment: first?.entry_assessment ?? null,
    entryExecution: first?.entry_execution ?? null,
    lastEntryAssessment: run.latestEntryDecision,
    entryTakerBtc: takerLots.entry * snapshot.model.lot_btc,
    hedgeTakerBtc: takerLots.hedge * snapshot.model.lot_btc,
  };
}

// Both branches use the same post-entry hedge. Keep them at the same clock so
// the attribution check can compare their pre-command books and private state
// directly, even on paths that spend a long time waiting to enter.
export async function compareEntryPolicies({
  seed = 73,
  base = "/api",
  horizonMs = COMPARISON_HORIZON_MS,
  onProgress,
  signal,
} = {}) {
  validateWindow(seed, horizonMs);
  const branches = ["l2", "l3"].map((entryMode) => ({
    entryMode,
    run: new LiveShowcase({
      mode: "l3",
      entryMode,
      seed,
      scenario: "queue_demo",
      base,
    }),
  }));
  const removeListeners = branches.map(({ run }) =>
    closeOnNavigation(run, base),
  );
  const progress = (clockMs) =>
    onProgress?.({
      experiment: "entry",
      mode: "l3",
      completedModes: 0,
      clockMs,
      horizonMs,
    });
  let firstEntryDifference = null;
  try {
    for (const { run } of branches) {
      signal?.throwIfAborted();
      await startRun(run, seed);
    }
    progress(0);
    for (let clockMs = 0; clockMs < horizonMs; clockMs += STEP_MS) {
      const before = branches.map(({ run }) => run.snapshot);
      const decisions = [];
      for (const { run } of branches) {
        signal?.throwIfAborted();
        decisions.push(await stepEntry(run, clockMs));
      }
      const [l2, l3] = decisions;
      if (
        !firstEntryDifference &&
        differentInstruction(l2, l3) &&
        executionState(before[0]) === executionState(before[1])
      ) {
        firstEntryDifference = {
          clockMs,
          pathDigest: before[0].market_path.digest,
          l2,
          l3,
        };
      }
      const now = clockMs + STEP_MS;
      if (now % 1000 === 0 || now === horizonMs) progress(now);
    }
    const rows = [];
    let path;
    for (const { run } of branches) {
      signal?.throwIfAborted();
      const snapshot = await verifiedSnapshot(run, seed, horizonMs);
      if (path && !samePath(snapshot.market_path, path))
        throw new Error(
          "External market paths differ for the entry policies. Comparison discarded.",
        );
      path = snapshot.market_path;
      rows.push(entryOutcome(run, snapshot));
    }
    return {
      experiment: "entry",
      fixedHedgeMode: "l3",
      seed,
      horizonMs,
      pathVersion: path.version,
      pathArrivals: path.arrivals,
      pathDigest: path.digest,
      firstEntryDifference,
      rows,
    };
  } finally {
    // Attempt cleanup on both branches even if the first deletion fails.
    const failures = [];
    for (const { run } of branches) {
      if (!run.snapshot) continue;
      try {
        await run.close();
      } catch (error) {
        failures.push(error);
      }
    }
    removeListeners.forEach((remove) => remove());
    if (failures.length)
      throw new AggregateError(
        failures,
        "Entry comparison session cleanup failed.",
      );
  }
}
