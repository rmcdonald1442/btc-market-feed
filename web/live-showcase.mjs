import { EventProjector } from "./browser-artifact.mjs";
import { calculateTicketEstimate } from "./finance.mjs";
import { HedgePolicy } from "./hedge-policy.mjs";
import { EntryPolicy } from "./entry-policy.mjs";

const ENTRY_RETRY_MS = 30_000;

export const LIVE_DEMO_POLICY = Object.freeze({
  lots: 100,
  entry_after_ms: 3000,
  min_net_cents: 100,
  hedge_quote_ticks: 20,
  hedge_after_first_fill_ms: 4000,
  no_fill_timeout_ms: 15_000,
});

export function isHedgeComplete(snapshot) {
  return (
    snapshot.metrics.hedged_btc > 0 &&
    snapshot.metrics.residual_btc === 0 &&
    snapshot.orders.every((order) => order.remaining_lots === 0)
  );
}

export function estimateLiveEntry(snapshot) {
  const spot = snapshot.books.find((book) => book.id === "spot");
  const future = snapshot.books.find((book) => book.id === "future");
  if (spot?.bid_cents == null || future?.ask_cents == null) return null;
  const lots = LIVE_DEMO_POLICY.lots;
  const offsetTicks =
    snapshot.scenario === "queue_demo" ? 0 : LIVE_DEMO_POLICY.hedge_quote_ticks;
  const futureLimit =
    future.ask_cents + offsetTicks * snapshot.model.tick_usd * 100;
  const carry = calculateTicketEstimate({
    spotPriceCents: spot.bid_cents,
    futurePriceCents: futureLimit,
    lots,
    clockMs: snapshot.clock_ms,
    model: snapshot.model,
  });
  return {
    lots,
    spot_limit_cents: spot.bid_cents,
    future_limit_cents: futureLimit,
    estimate_cents: carry.projected_pnl_cents,
    carry,
  };
}

export class LiveCarryPolicy {
  constructor({ mode = "timer", entryMode } = {}) {
    this.hedgePolicy = new HedgePolicy(
      mode,
      LIVE_DEMO_POLICY.hedge_after_first_fill_ms,
    );
    this.latestDecision = null;
    this.entryPolicy =
      entryMode === undefined
        ? null
        : new EntryPolicy({
            mode: entryMode,
            lots: LIVE_DEMO_POLICY.lots,
            minNetCents: LIVE_DEMO_POLICY.min_net_cents,
            exposureBudgetMs: LIVE_DEMO_POLICY.hedge_after_first_fill_ms,
          });
    this.latestEntryDecision = null;
    this.phase = "scanning";
    this.entryMs = null;
    this.lastHedgeMs = -Infinity;
    this.hedgeAttempts = 0;
    this.retryAfterMs = 0;
  }
  refreshEntry(snapshot) {
    if (!this.entryPolicy || this.phase !== "scanning") return null;
    const eligibleAfterMs = Math.max(
      LIVE_DEMO_POLICY.entry_after_ms,
      this.retryAfterMs,
    );
    this.latestEntryDecision = {
      ...this.entryPolicy.assess(snapshot),
      eligible: snapshot.clock_ms >= eligibleAfterMs,
      eligible_after_ms: eligibleAfterMs,
    };
    return this.latestEntryDecision;
  }
  refreshAssessment(snapshot) {
    const assessment = this.hedgePolicy.assess(snapshot);
    if (assessment && this.phase === "hedging") {
      const retryReady = snapshot.clock_ms - this.lastHedgeMs >= 1000;
      assessment.action = retryReady ? "cross" : "wait";
      assessment.reason_code = retryReady
        ? "finish_cross"
        : "hedge_retry_interval";
      assessment.reason = retryReady
        ? "Cross the exposure left after the previous hedge."
        : "A partial hedge is working. Check its remainder after one market second.";
    }
    if (assessment) this.latestDecision = assessment;
    return assessment;
  }
  decide(snapshot) {
    const now = Number(snapshot.clock_ms);
    const working = snapshot.orders.filter((order) => order.remaining_lots > 0);
    if (this.phase === "complete") return { kind: "wait" };
    if (this.phase !== "scanning") {
      if (isHedgeComplete(snapshot))
        return {
          kind: "complete",
          message: "Filled inventory hedged. Books keep trading.",
        };
      if (this.phase === "hedging" && !working.length && !snapshot.fills.length)
        return {
          kind: "retry",
          message: "Untouched entry cancelled; wait before retrying.",
        };
      const firstFillMs = snapshot.fills.length
        ? Math.min(...snapshot.fills.map((fill) => fill.clock_ms))
        : null;
      const assessment = this.refreshAssessment(snapshot);
      const matchedPartialExpired =
        !assessment &&
        firstFillMs !== null &&
        now - firstFillMs >= LIVE_DEMO_POLICY.hedge_after_first_fill_ms;
      if (
        assessment?.action === "cross" ||
        matchedPartialExpired ||
        (!snapshot.fills.length &&
          now - this.entryMs >= LIVE_DEMO_POLICY.no_fill_timeout_ms)
      ) {
        if (now - this.lastHedgeMs < 1000) return { kind: "wait" };
        if (this.hedgeAttempts >= 5)
          return {
            kind: "stop",
            message:
              "Hedge could not complete within the retry limit. Restart to begin a new run.",
          };
        return {
          kind: "hedge",
          residual_btc: snapshot.metrics.residual_btc,
          ...(assessment ? { assessment } : {}),
          message: assessment
            ? "Cancel resting orders and cross the residual exposure."
            : "Cancel unfilled entry remainders.",
        };
      }
      return { kind: "wait" };
    }
    if (this.entryPolicy) {
      const entry = this.refreshEntry(snapshot);
      if (!entry.eligible) return { kind: "wait" };
      if (entry.action === "skip")
        return { kind: "wait", entry_assessment: entry };
      const crossMarket = {
        rest_both: null,
        cross_spot: "spot",
        cross_future: "future",
      }[entry.action];
      if (crossMarket === undefined)
        throw new Error("Unknown entry recommendation.");
      return {
        kind: "place",
        lots: entry.lots,
        spot_limit_cents: entry.spot_limit_cents,
        future_limit_cents: entry.future_limit_cents,
        estimate_cents: entry.estimate_cents,
        carry: entry.carry,
        entry_action: entry.action,
        entry_mode: entry.mode,
        entry_assessment: entry,
        cross_market: crossMarket,
        message: entry.reason,
      };
    }
    if (now < Math.max(LIVE_DEMO_POLICY.entry_after_ms, this.retryAfterMs))
      return { kind: "wait" };
    const entry = estimateLiveEntry(snapshot);
    if (!entry || entry.estimate_cents < LIVE_DEMO_POLICY.min_net_cents)
      return { kind: "wait" };
    return {
      kind: "place",
      ...entry,
      message:
        snapshot.scenario === "queue_demo"
          ? "Buy at the spot bid and sell at the futures ask."
          : "Buy spot at the bid; quote the futures hedge 20 ticks above its ask. Fills remain market-driven.",
    };
  }
  commit(decision, snapshot) {
    if (decision.kind === "place") {
      this.phase = "working";
      this.entryMs = snapshot.clock_ms;
      if (
        decision.cross_market &&
        decision.entry_execution?.ioc_filled_lots === 0
      ) {
        this.phase = "scanning";
        this.retryAfterMs = snapshot.clock_ms + ENTRY_RETRY_MS;
      }
    }
    if (decision.kind === "hedge") {
      this.phase = "hedging";
      this.lastHedgeMs = snapshot.clock_ms;
      this.hedgeAttempts++;
    }
    if (decision.kind === "complete") this.phase = "complete";
    if (decision.kind === "retry") {
      this.phase = "scanning";
      this.retryAfterMs = snapshot.clock_ms + ENTRY_RETRY_MS;
      this.hedgeAttempts = 0;
    }
  }
}

function entryReceipt(snapshot, events, crossMarket) {
  const ids = new Set(
    events
      .filter((event) => event.type === "acknowledgement")
      .map((event) => String(event.order.id)),
  );
  // Keep the private execution receipt independent of public L3 queue rank.
  const orders = snapshot.orders
    .filter((order) => ids.has(String(order.id)))
    .map(({ ahead_lots, ...receipt }) => receipt);
  const ioc = orders.find((order) => order.market === crossMarket);
  const companion = orders.find((order) => order.market !== crossMarket);
  return {
    orders,
    ioc_filled_lots: crossMarket ? (ioc?.filled_lots ?? 0) : null,
    companion_lots: crossMarket ? (companion?.lots ?? 0) : null,
  };
}

// A single request lane owns revisions. Neither stepping nor commands read a capture.
export class LiveShowcase {
  constructor({
    base = "/api",
    fetchImpl = globalThis.fetch.bind(globalThis),
    mode,
    entryMode,
    compareQueue = false,
    seed = 73,
    scenario = entryMode || (mode && mode !== "timer")
      ? "queue_demo"
      : "carry_demo",
  } = {}) {
    this.base = base;
    this.fetch = fetchImpl;
    this.snapshot = null;
    this.mode = mode ?? (scenario === "queue_demo" ? "l3" : "timer");
    this.entryMode = entryMode;
    this.seed = seed;
    this.scenario = scenario;
    this.policy = new LiveCarryPolicy({ mode: this.mode, entryMode });
    this.l2Hedge = compareQueue
      ? new HedgePolicy("l2", LIVE_DEMO_POLICY.hedge_after_first_fill_ms)
      : null;
    this.actions = [];
    this.entryTrace = [];
    this.omittedEntryTransitions = 0;
    this.eventsProcessed = 0;
    this.brokerRecords = 0;
    this.exposurePauseShown = false;
    this.pendingExposurePause = false;
    this.lastEntrySubmissionMs = null;
    this.busy = false;
    this.closed = false;
  }
  get latestDecision() {
    return this.policy.latestDecision;
  }
  get latestEntryDecision() {
    return this.policy.latestEntryDecision;
  }
  get comparisonDecision() {
    return this.snapshot ? (this.l2Hedge?.assess(this.snapshot) ?? null) : null;
  }
  recordEntry(assessment, status) {
    const previous = this.entryTrace.at(-1);
    if (
      status === "skipped" &&
      previous?.status === status &&
      previous.reason_code === assessment.reason_code
    ) {
      previous.through_clock_ms = assessment.clock_ms;
      previous.evaluations++;
      return;
    }
    // Coalesce repeated skip decisions and bound the trace over a full simulated day.
    // Each submitted command and its complete assessment remain in actions.
    if (this.entryTrace.length === 256) {
      this.entryTrace.shift();
      this.omittedEntryTransitions++;
    }
    this.entryTrace.push({
      clock_ms: assessment.clock_ms,
      through_clock_ms: assessment.clock_ms,
      evaluations: 1,
      mode: assessment.mode,
      action: assessment.action,
      status,
      reason_code: assessment.reason_code,
      reason: assessment.reason,
      spot_limit_cents: assessment.spot_limit_cents,
      future_limit_cents: assessment.future_limit_cents,
    });
  }
  async request(suffix, body, method = "POST") {
    const response = await this.fetch(this.base + suffix, {
      method,
      headers: { "content-type": "application/json" },
      body: body == null ? undefined : JSON.stringify(body),
    });
    const data = await response.json();
    if (!response.ok)
      throw new Error(
        data.error || `Kafka request failed (${response.status}).`,
      );
    return data;
  }
  async start() {
    if (this.snapshot || this.busy || this.closed)
      throw new Error("Create a fresh live run before starting.");
    this.busy = true;
    try {
      this.snapshot = await this.request("/sessions", {
        seed: this.seed,
        transport: "kafka",
        scenario: this.scenario,
      });
      if (
        this.snapshot.transport !== "kafka" ||
        this.snapshot.scenario !== this.scenario
      )
        throw new Error(
          "The live Kafka preset was not started. No replay fallback is used.",
        );
      this.brokerRecords = Number(
        this.snapshot.recent.at(-1)?.kafka?.records || 0,
      );
      this.policy.entryPolicy?.observe(this.snapshot, this.snapshot, []);
      this.policy.refreshEntry(this.snapshot);
      return this.snapshot;
    } finally {
      this.busy = false;
    }
  }
  accept(reply, { placing = false } = {}) {
    if (
      reply.revision !== this.snapshot.revision + 1 ||
      reply.transport !== "kafka" ||
      reply.id !== this.snapshot.id ||
      reply.clock_ms < this.snapshot.clock_ms
    )
      throw new Error(
        "Live session revision or transport changed unexpectedly.",
      );
    const before = this.snapshot;
    const projector = new EventProjector(before);
    // Reconcile public price/size and print updates at their source timestamp.
    // HTTP delivery batches can contain a cancellation and a later replenishment
    // with zero net change, which would hide information available to L2.
    const trackQueue =
      (this.mode === "l2" || this.l2Hedge !== null) &&
      (before.orders.some((order) => order.remaining_lots > 0) ||
        reply.events.some((event) => event.type === "acknowledgement"));
    const entryObserver =
      this.policy.phase !== "complete" && !before.fills.length
        ? this.policy.entryPolicy
        : null;
    const observe = (prior, after, events) => {
      this.policy.hedgePolicy.observe(prior, after, events);
      this.l2Hedge?.observe(prior, after, events);
      entryObserver?.observe(prior, after, events);
    };
    let previous = before;
    let groupStart = 0;
    for (const [index, event] of reply.events.entries()) {
      projector.apply(event);
      const next = reply.events[index + 1];
      if (
        (trackQueue || entryObserver) &&
        (next?.clock_ms ?? reply.clock_ms) !== event.clock_ms
      ) {
        const observation = projector.snapshot(event.clock_ms);
        observe(
          previous,
          observation,
          reply.events.slice(groupStart, index + 1),
        );
        previous = observation;
        groupStart = index + 1;
      }
    }
    this.eventsProcessed += reply.events.length;
    this.brokerRecords += Number(reply.recent.at(-1)?.kafka?.records || 0);
    const marketPath =
      reply.market_path ??
      reply.events.findLast((event) => event.market_path)?.market_path ??
      before.market_path;
    this.snapshot = {
      ...projector.snapshot(reply.clock_ms),
      id: reply.id,
      revision: reply.revision,
      transport: reply.transport,
      scenario: this.scenario,
      ...(marketPath ? { market_path: marketPath } : {}),
      topics: reply.topics,
      recent: reply.recent,
      stage: reply.stage,
    };
    observe(
      previous,
      this.snapshot,
      groupStart ? reply.events.slice(groupStart) : reply.events,
    );
    this.policy.refreshAssessment(this.snapshot);
    if (!placing) this.policy.refreshEntry(this.snapshot);
    return reply.events;
  }
  fillPause(before) {
    if (!isHedgeComplete(before) && isHedgeComplete(this.snapshot))
      return "hedged";
    if (
      !this.exposurePauseShown &&
      this.snapshot.metrics.residual_btc !== 0 &&
      this.snapshot.fills.length > before.fills.length
    ) {
      this.exposurePauseShown = true;
      return "exposure";
    }
    return null;
  }

  async tick(milliseconds = 100, { pauseAtMilestones = false } = {}) {
    if (this.busy || !this.snapshot || this.closed)
      throw new Error("Live run is unavailable or has a request in flight.");
    this.busy = true;
    const events = [];
    const before = this.snapshot;
    try {
      if (this.pendingExposurePause) {
        this.pendingExposurePause = false;
        if (pauseAtMilestones) {
          this.exposurePauseShown = true;
          return { snapshot: this.snapshot, events, pauseReason: "exposure" };
        }
      }
      const path = `/sessions/${this.snapshot.id}`;
      // A pause at an IOC acknowledgement holds this exact source timestamp.
      // Keep one policy action per decision clock in both the UI and comparative runs.
      const decision =
        this.lastEntrySubmissionMs === before.clock_ms
          ? { kind: "wait" }
          : this.policy.decide(this.snapshot);
      if (decision.kind === "stop") throw new Error(decision.message);
      if (decision.kind === "wait" && decision.entry_assessment) {
        this.recordEntry(decision.entry_assessment, "skipped");
      }
      if (decision.kind === "place" || decision.kind === "hedge") {
        const body = { revision: this.snapshot.revision };
        if (decision.kind === "place")
          Object.assign(body, {
            lots: decision.lots,
            spot_limit_cents: decision.spot_limit_cents,
            future_limit_cents: decision.future_limit_cents,
            ...(decision.cross_market
              ? { cross_market: decision.cross_market }
              : {}),
          });
        const endpoint =
          decision.kind === "hedge"
            ? "/hedge"
            : decision.cross_market
              ? "/entry"
              : "/orders";
        const reply = await this.request(path + endpoint, body);
        events.push(
          ...this.accept(reply, { placing: decision.kind === "place" }),
        );
        if (decision.entry_assessment) {
          this.lastEntrySubmissionMs = this.snapshot.clock_ms;
          decision.entry_execution = entryReceipt(
            this.snapshot,
            events,
            decision.cross_market,
          );
          this.recordEntry(decision.entry_assessment, "submitted");
        }
      }
      if (decision.kind !== "wait") {
        this.policy.commit(decision, this.snapshot);
        this.actions.push({ ...decision, clock_ms: this.snapshot.clock_ms });
      }
      if (pauseAtMilestones) {
        if (
          decision.kind === "place" &&
          decision.cross_market &&
          !this.exposurePauseShown &&
          this.snapshot.metrics.residual_btc !== 0 &&
          this.snapshot.fills.length > before.fills.length
        ) {
          this.pendingExposurePause = true;
        }
        const pauseReason =
          decision.kind === "place" ? "placement" : this.fillPause(before);
        if (pauseReason)
          return { snapshot: this.snapshot, events, pauseReason };
      }
      const reply = await this.request(path + "/step", {
        revision: this.snapshot.revision,
        milliseconds,
        events_only: true,
      });
      events.push(...this.accept(reply));
      const pauseReason = pauseAtMilestones ? this.fillPause(before) : null;
      return { snapshot: this.snapshot, events, pauseReason };
    } finally {
      this.busy = false;
    }
  }
  async close() {
    if (this.busy)
      throw new Error(
        "Wait for the accepted command before closing the live run.",
      );
    if (this.closed) return;
    this.busy = true;
    try {
      if (this.snapshot)
        await this.request(`/sessions/${this.snapshot.id}`, null, "DELETE");
      this.closed = true;
    } finally {
      this.busy = false;
    }
  }
}
