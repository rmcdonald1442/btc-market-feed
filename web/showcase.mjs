import {
  LiveShowcase,
  LIVE_DEMO_POLICY,
  estimateLiveEntry,
  isHedgeComplete,
} from "./live-showcase.mjs";
import { createMarketCharts } from "./market-charts.mjs";
import { narratedBookChange } from "./event-activity.mjs";
import {
  compareEntryPolicies,
  compareHedgePolicies,
} from "./hedge-comparison.mjs";

const $ = (id) => document.getElementById(id);
const money = (cents) =>
  `${cents >= 0 ? "+" : "−"}$${(Math.abs(cents) / 100).toFixed(2)}`;
const priceLabel = new Intl.NumberFormat("en-US", {
  style: "currency",
  currency: "USD",
});
const STEP_MS = 100;
const WALL_INTERVAL_MS = 200;
const ORDER_INTERVAL_MS = 400;
const parameters = new URLSearchParams(location.search);
const portfolio = parameters.get("view") !== "full";
const feedPortfolio = portfolio && parameters.get("story") !== "hedge";
const manualFeed = feedPortfolio && parameters.get("manual") === "1";
const historyWindow = { startMs: 0, endMs: feedPortfolio ? 5500 : 14_000 };
const portfolioControls = parameters.get("controls") !== "0";
const portfolioHolds = { placement: 2300, exposure: 2800, decision: 4400 };
const feedHoldMs = { before: 3200, amendment: 4200 };
const root = document.querySelector("main");
root.classList.toggle("portfolio", portfolio);
root.classList.toggle("feed-portfolio", feedPortfolio);
if (feedPortfolio) {
  document.title = "BTC Market Feed · Live Kafka";
  $("demoBrand").textContent = "BTC Market Feed";
  $("spotBookTitle").textContent = "BTC spot";
  $("futureBookTitle").textContent = "BTC futures";
  const evidence = $("portfolioEvidence");
  evidence.setAttribute("aria-label", "Market event evidence");
  evidence.dataset.empty = "true";
  evidence.hidden = false;
}
const seedParameter = parameters.get("seed");
const requestedSeed = seedParameter?.trim() ? Number(seedParameter) : NaN;
const demoSeed = feedPortfolio
  ? 75
  : portfolio
    ? 15
    : Number.isSafeInteger(requestedSeed) && requestedSeed >= 0
      ? requestedSeed
      : 3;
const entryMode =
  portfolio || parameters.get("entry") === "touch"
    ? undefined
    : parameters.get("entry") === "l2"
      ? "l2"
      : "l3";
const entryLabels = {
  rest_both: "Quote both legs",
  cross_spot: "Cross spot",
  cross_future: "Cross futures",
  skip: "Skip entry",
};
const policyLabels = {
  timer: "Fixed timer",
  l2: "L2 estimator",
  l3: "L3 queue",
};
const charts = createMarketCharts({
  spotElement: $("spotBook"),
  futureElement: $("futureBook"),
  spotBucket: $("spotBucket"),
  futureBucket: $("futureBucket"),
  historyElement: portfolio && !feedPortfolio ? null : $("historyChart"),
  historyWindow,
  compactHistory: feedPortfolio,
  focusOwnOrders: true,
  orderLabelSize: portfolio ? 13 : undefined,
  volumeAxisLabel: feedPortfolio
    ? "Open order volume (BTC)"
    : "Resting volume (BTC)",
});
let run = null,
  playing = false,
  timer = null,
  inFlight = null,
  restarting = false,
  pauseReason = null,
  entrySkipShown = false,
  portfolioDecisionShown = false,
  feedMoment = null,
  feedAmendmentShown = false,
  filledCarry = null,
  comparison = null;

function feedFinished() {
  return feedMoment?.event.kind === "fill" && feedMoment.applied;
}

function controls() {
  $("pauseDemo").disabled =
    restarting ||
    !run?.snapshot ||
    !!$("demoError").textContent ||
    run?.snapshot?.stage === "complete";
  $("pauseDemo").textContent = playing ? "Pause" : "Resume";
  $("pauseDemo").classList.toggle("resume-orders", pauseReason !== null);
  $("replayDemo").disabled = restarting;
  const next = $("portfolioNext");
  next.hidden =
    !manualFeed ||
    !portfolioControls ||
    feedFinished() ||
    !!$("demoError").textContent;
  next.disabled = restarting || playing || !!inFlight || !run?.snapshot;
  next.textContent = playing ? "Advancing…" : "Next";
  $("portfolioReplay").hidden =
    !portfolio ||
    !portfolioControls ||
    restarting ||
    playing ||
    (!$("demoError").textContent &&
      !(feedPortfolio
        ? feedFinished()
        : run?.snapshot && isHedgeComplete(run.snapshot)));
}
function wallInterval(snapshot) {
  // Give the two resting orders longer on screen while the market keeps moving.
  return snapshot.fills.length === 0 &&
    snapshot.orders.filter((order) => order.status === "working").length === 2
    ? ORDER_INTERVAL_MS
    : WALL_INTERVAL_MS;
}
function carryRow(id, gross, fees, financing, net) {
  const cells = $(id).cells;
  cells[1].textContent = money(gross);
  cells[2].textContent = money(-fees);
  cells[3].textContent = money(-financing);
  cells[4].textContent = money(net);
}

function renderCarry(snapshot, entry, matched, forecast) {
  const passive = forecast?.alternatives.find(
    (alternative) => alternative.action === "rest_both",
  );
  const quote =
    entry ??
    (forecast?.carry ? forecast : null) ??
    (passive?.carry ? passive : null) ??
    estimateLiveEntry(snapshot);
  if (!quote) return;
  const expected = quote.carry;
  const expectedSize = (quote.lots ?? forecast.lots) / 1000;
  const expectedFees =
    expected.entry_fee_estimate_cents + expected.spot_exit_fee_estimate_cents;
  const route = entry?.entry_action ?? forecast?.action;
  const assumedFills = route?.startsWith("cross_")
    ? "If companion fills"
    : "If both limits fill";
  $("beforeCarry").cells[0].textContent =
    `${assumedFills} · ${expectedSize.toFixed(3)} BTC`;
  carryRow(
    "beforeCarry",
    expected.gross_basis_cents,
    expectedFees,
    expected.financing_estimate_cents,
    expected.projected_pnl_cents,
  );

  const threshold = LIVE_DEMO_POLICY.min_net_cents;
  $("carryReason").textContent =
    forecast?.eligible === false && !entry
      ? "Observing both queues"
      : route === "skip"
        ? "Entry skipped"
        : expected.projected_pnl_cents >= threshold
          ? `Meets ${priceLabel.format(threshold / 100)} entry threshold`
          : `Below ${priceLabel.format(threshold / 100)} entry threshold`;

  if (matched && !filledCarry) {
    // Preserve the comparison at the hedge fill so later quotes don't move it.
    filledCarry = snapshot.metrics;
  }
  $("afterCarry").hidden = !filledCarry;
  const alternatives = $("entryAlternatives");
  alternatives.hidden = !forecast || !!filledCarry;
  if (forecast && !filledCarry)
    alternatives.textContent = forecast.alternatives
      .map(
        ({ action, carry, viable }) =>
          `${entryLabels[action]} ${carry ? money(carry.projected_pnl_cents) : "—"}${viable ? "" : " (blocked)"}`,
      )
      .join(" · ");
  if (!filledCarry) {
    $("carryNote").textContent =
      "Fees include the estimated spot sale at expiry.";
    return;
  }
  $("afterCarry").cells[0].textContent =
    `After hedging · ${filledCarry.hedged_btc.toFixed(3)} BTC`;
  const filledFees =
    filledCarry.matched_entry_fees_cents +
    filledCarry.spot_exit_fee_estimate_cents;
  carryRow(
    "afterCarry",
    filledCarry.matched_basis_cents,
    filledFees,
    filledCarry.financing_estimate_cents,
    filledCarry.projected_pnl_cents,
  );
  const delta = filledCarry.projected_pnl_cents - expected.projected_pnl_cents;
  const sameSize = filledCarry.hedged_btc === expectedSize;
  $("carryReason").textContent = sameSize
    ? `${money(delta)} after fills`
    : "Smaller quantity hedged";
  $("carryNote").textContent = sameSize
    ? `Spread ${money(filledCarry.matched_basis_cents - expected.gross_basis_cents)} · fee cost ${money(filledFees - expectedFees)}. Financing and spot exit are estimated.`
    : "The estimates use different sizes. Financing and the spot exit fee remain estimates.";
}

function entryEta(leg) {
  return Number.isFinite(leg?.fill_eta_ms)
    ? `${(leg.fill_eta_ms / 1000).toFixed(2)}s`
    : "unavailable";
}

function entryQueueText(leg) {
  if (!Number.isFinite(leg?.visible_ahead_lots)) return "";
  const queue = `${(leg.visible_ahead_lots / 1000).toFixed(3)} BTC at the touch`;
  const estimate = Number.isFinite(leg.fill_eta_ms)
    ? `~${entryEta(leg)} to fill`
    : "Fill time unavailable";
  const turnover =
    leg.turnover_lots_per_second > 0
      ? `${(leg.turnover_lots_per_second / 1000).toFixed(3)} BTC/s estimated queue turnover`
      : "";
  return [queue, estimate, turnover].filter(Boolean).join(" · ");
}

function entryReason(forecast) {
  const { action, reason, legs, alternatives } = forecast;
  if (forecast.eligible === false) return reason;
  const market =
    action === "cross_spot"
      ? "spot"
      : action === "cross_future"
        ? "future"
        : null;
  if (!market) return `${entryLabels[action]} · ${reason}`;
  const passive = alternatives.find((route) => route.action === "rest_both");
  const eta = legs[market]?.fill_eta_ms;
  if (passive?.reason_code === "no_flow")
    return `${entryLabels[action]} · No recent trade flow for a passive ${market === "spot" ? "buy" : "sell"}.`;
  if (passive?.reason_code === "slow_queue" && Number.isFinite(eta))
    return `${entryLabels[action]} · ${entryEta(legs[market])} estimated for a passive fill, beyond the ${LIVE_DEMO_POLICY.hedge_after_first_fill_ms / 1000}s risk budget.`;
  return `${entryLabels[action]} · Best projected carry after the price-risk allowance.`;
}

function assessmentText(assessment, includeQueue = false) {
  const {
    reason,
    ahead_lots,
    better_lots,
    eta_ms,
    elapsed_ms,
    deadline_ms,
    wait_risk_cents,
    passive_edge_cents,
  } = assessment;
  const facts = [];
  if (includeQueue && Number.isFinite(ahead_lots))
    facts.push(`${(ahead_lots / 1000).toFixed(3)} BTC ahead`);
  if (Number.isFinite(better_lots) && better_lots > 0)
    facts.push(`${(better_lots / 1000).toFixed(3)} BTC at better prices`);
  if (Number.isFinite(eta_ms))
    facts.push(
      `~${(eta_ms / 1000).toFixed(1)}s to fill · ${(Math.max(0, deadline_ms - elapsed_ms) / 1000).toFixed(1)}s left`,
    );
  if (
    assessment.reason_code === "risk_exceeds_edge" &&
    Number.isFinite(wait_risk_cents) &&
    Number.isFinite(passive_edge_cents)
  )
    facts.push(
      `${priceLabel.format(wait_risk_cents / 100)} estimated price risk vs ${priceLabel.format(passive_edge_cents / 100)} passive saving`,
    );
  return [reason, facts.join(" · ")].filter(Boolean).join(" ");
}

function renderEvidence(columns) {
  const evidence = $("portfolioEvidence");
  evidence.hidden = false;
  evidence.dataset.empty = String(columns.length === 0);
  for (const [index, side] of ["Left", "Right"].entries()) {
    const [label = "", value = "", detail = ""] = columns[index] ?? [];
    $(`portfolio${side}Label`).textContent = label;
    $(`portfolio${side}Value`).textContent = value;
    $(`portfolio${side}Detail`).textContent = detail;
  }
}

function renderPortfolio(snapshot, phase, assessment) {
  const story = [
    [
      "opening",
      "Can the hedge fill in time?",
      "A BTC carry trade buys spot and sells a dated future.",
    ],
    [
      "quotes",
      "Two orders join the book",
      "The blue spot buy and amber futures sell are both resting.",
    ],
    [
      "exposure",
      "The futures sell fills first",
      "Until the spot buy fills, the position is short BTC.",
    ],
    [
      "hedged",
      "The spot buy closes the hedge",
      "The trade is matched and unhedged BTC is back to zero.",
    ],
  ];
  const [beat, title, caption] =
    pauseReason === "decision"
      ? [
          "decision",
          "The L3 queue changes the decision",
          `Only ${((assessment.deadline_ms - assessment.elapsed_ms) / 1000).toFixed(2)}s of the exposure budget remains.`,
        ]
      : story[phase];
  root.dataset.portfolioBeat = beat;
  $("sceneTitle").textContent = title;
  $("sceneDescription").textContent = caption;
  $("exposureCard").hidden = phase < 2;
  let columns = [];
  if (beat === "decision") {
    const estimated = run.comparisonDecision;
    columns = [
      ["L2 · Aggregate depth", estimated],
      ["L3 · Individual orders", assessment],
    ].map(([label, forecast]) => [
      label,
      forecast.action === "cross" ? "Cross now" : "Wait",
      `${(forecast.ahead_lots / 1000).toFixed(3)} BTC ahead · ${(forecast.eta_ms / 1000).toFixed(2)}s to fill`,
    ]);
  }
  if (beat === "hedged") {
    columns = [
      ["Matched size", `${snapshot.metrics.hedged_btc.toFixed(3)} BTC`, ""],
      [
        "Projected carry at expiry",
        money(snapshot.metrics.projected_pnl_cents),
        "",
      ],
    ];
  }
  renderEvidence(columns);
}

function renderFeedPortfolio() {
  const snapshot = feedMoment?.snapshot ?? run.snapshot;
  const event = feedMoment?.event;
  const applied = feedMoment?.applied;
  let beat = feedAmendmentShown ? "feed-resumes" : "feed";
  let title = feedAmendmentShown
    ? "An example strategy joins the book"
    : "Live L3 order books in the browser";
  let caption = feedAmendmentShown
    ? "Watch for the strategy’s highlighted order."
    : "Kafka messages update both books as orders change.";
  let columns = [];
  if (pauseReason === "placement") {
    beat = "placement";
    title = "The strategy places two orders";
    caption = "Open circles mark the orders on the price chart.";
  }
  if (event) {
    const isFill = event.kind === "fill";
    const btc = (lots) => (lots / 1000).toFixed(3);
    beat = applied ? event.kind : `${event.kind}-before`;
    if (isFill) {
      title = applied
        ? "The book and strategy update together"
        : "Follow the strategy’s order";
      caption = applied
        ? "Part of the order trades. The rest is still in the book."
        : "The order’s status is shown below the book.";
      const order = snapshot.orders.find((item) => item.id === event.order_id);
      if (order) {
        columns[event.market === "spot" ? 0 : 1] = [
          event.side === "buy" ? "Strategy buy" : "Strategy sell",
          `${btc(order.filled_lots)} BTC traded`,
          `${btc(order.remaining_lots)} BTC still waiting`,
        ];
      }
    } else {
      title = applied ? "One update shrinks the order" : "Follow one order";
      caption = applied
        ? "L3 data lets us follow the same order as it changes."
        : "The outline marks a single order in the book.";
      columns = applied
        ? [
            [
              "Order size",
              `${btc(event.previous_lots)} → ${btc(event.remaining_lots)} BTC`,
            ],
          ]
        : [["Order size", `${btc(event.previous_lots)} BTC`]];
    }
  }
  const beatChanged = root.dataset.portfolioBeat !== beat;
  Object.assign(root.dataset, {
    portfolioBeat: beat,
    highlightMarket: event?.market ?? "",
    clock: String(snapshot.clock_ms),
    transport: snapshot.transport,
  });
  $("sceneTitle").textContent = title;
  $("sceneDescription").textContent = caption;
  $("sourceClock").textContent =
    `${(snapshot.clock_ms / 1000).toFixed(1)}s market time`;
  $("exposureCard").hidden = true;
  renderEvidence(columns);
  if (
    beatChanged &&
    !window.matchMedia("(prefers-reduced-motion: reduce)").matches
  ) {
    const targets = [$("sceneTitle").parentElement];
    if (event && !applied) targets.push($("portfolioEvidence"));
    for (const target of targets) {
      target.animate(
        [
          { opacity: 0.35, transform: "translateY(3px)" },
          { opacity: 1, transform: "translateY(0)" },
        ],
        { duration: 280, easing: "ease-out" },
      );
    }
  }
  charts.update(snapshot, { highlightOrder: event });
}

function render() {
  if (feedPortfolio) {
    renderFeedPortfolio();
    return;
  }
  const snapshot = run.snapshot;
  const metrics = snapshot.metrics;
  const residual = Number(metrics.residual_btc);
  const working = snapshot.orders.filter((order) => order.remaining_lots > 0);
  const matched = isHedgeComplete(snapshot);
  const placement = run.actions.findLast((action) => action.kind === "place");
  const hedge = run.actions.findLast(
    (action) => action.kind === "hedge" && action.residual_btc !== 0,
  );
  const entry = run.policy.phase === "scanning" ? null : placement;
  const entryForecast = entry?.entry_assessment ?? run.latestEntryDecision;
  renderCarry(snapshot, entry, matched, entryForecast);
  const firstFillMs = snapshot.fills.length
    ? Math.min(...snapshot.fills.map((fill) => fill.clock_ms))
    : snapshot.clock_ms;
  const assessment = run.latestDecision;
  const phase = matched
    ? 3
    : residual !== 0 || run.policy.phase === "hedging"
      ? 2
      : working.length
        ? 1
        : 0;
  const hedgeMarket = residual > 0 ? "future" : "spot";
  const exposedMarket = residual > 0 ? "spot" : "future";
  const completedMarket = hedge
    ? hedge.residual_btc > 0
      ? "future"
      : "spot"
    : snapshot.fills.at(-1)?.market;
  $("sceneTitle").textContent = [
    "Watching the spread",
    "Orders placed",
    residual > 0 ? "Spot filled" : residual < 0 ? "Futures filled" : "Hedging",
    "Hedge filled",
  ][phase];
  if (pauseReason === "placement" && entry?.entry_action?.startsWith("cross_"))
    $("sceneTitle").textContent = "Orders placed";
  if (pauseReason === "entry-skip")
    $("sceneTitle").textContent = "Entry skipped";
  $("sceneDescription").textContent =
    pauseReason === "entry-skip"
      ? entryForecast.reason
      : (phase === 0 || pauseReason === "placement") && entryForecast
        ? entryReason(entryForecast)
        : phase === 2 && assessment
          ? assessment.action === "cross"
            ? "Cross the spread"
            : "Wait for a passive fill"
          : "";
  $("residualValue").textContent =
    `${residual > 0 ? "+" : ""}${residual.toFixed(3)}`;
  $("exposureCard").dataset.state = matched
    ? "hedged"
    : residual !== 0
      ? "exposed"
      : "flat";
  $("sourceClock").textContent =
    `${(snapshot.clock_ms / 1000).toFixed(1)}s market time`;
  for (const market of ["spot", "future"]) {
    const orders = snapshot.orders.filter((item) => item.market === market);
    const order =
      orders.find((item) => item.remaining_lots > 0) || orders.at(-1);
    const filled = Number(
      market === "spot" ? metrics.spot_btc : metrics.future_short_btc,
    );
    const targetLots =
      entry?.entry_execution?.companion_lots ||
      entry?.lots ||
      LIVE_DEMO_POLICY.lots;
    const target = Math.max(filled, targetLots / 1000);
    $(market + "Fill").textContent =
      `${filled.toFixed(3)} / ${target.toFixed(3)} BTC filled`;
    const average =
      market === "spot"
        ? metrics.spot_average_cents
        : metrics.future_average_cents;
    const price = order?.remaining_lots ? order.price_cents : average;
    $(market + "Price").textContent =
      price == null
        ? "—"
        : `${order?.remaining_lots ? "Limit" : "Avg fill"} ${priceLabel.format(price / 100)}`;
    $(market + "Price").closest(".order-summary").dataset.working = String(
      !!order?.remaining_lots,
    );
    $(market + "Status").textContent = !order
      ? "No order"
      : order.status === "filled"
        ? "Filled ✓"
        : order.status === "partial"
          ? "Partial fill"
          : order.status === "cancelled"
            ? "Remainder cancelled"
            : "Resting";
    $(market + "Queue").textContent = !order
      ? "Watching quotes"
      : order.remaining_lots
        ? `${(order.ahead_lots / 1000).toFixed(3)} BTC ahead`
        : "No working remainder";
    const explanation = $(market + "Explanation");
    let detail = "";
    if (phase === 0 && entryForecast) {
      detail = entryQueueText(entryForecast.legs[market]);
    } else if (pauseReason === "placement" && entryForecast) {
      const crossed = entry?.entry_execution?.ioc_filled_lots ?? 0;
      const limit = entryForecast[`${market}_limit_cents`];
      detail =
        entryForecast.action === `cross_${market}`
          ? `${(crossed / 1000).toFixed(3)} BTC crossed with a ${priceLabel.format(limit / 100)} protection limit.`
          : entryQueueText(entryForecast.legs[market]);
    } else if (pauseReason === "placement") {
      detail =
        market === "spot"
          ? "Buy at the best bid. Earlier orders at that price fill first."
          : "Sell at the best ask. Earlier orders at that price fill first.";
    } else if (phase === 2) {
      if (market === exposedMarket && pauseReason === "exposure") {
        const adverseMove = residual > 0 ? "fall" : "rise";
        detail = `A BTC price ${adverseMove} hurts while the hedge waits.`;
      } else if (market === hedgeMarket && assessment?.market === market) {
        detail = assessmentText(assessment);
      }
    } else if (matched && market === completedMarket) {
      if (hedge) {
        const limit =
          market === "spot" ? entry.spot_limit_cents : entry.future_limit_cents;
        const original = `Original limit ${priceLabel.format(limit / 100)}.`;
        detail = hedge.assessment?.reason
          ? `Crossed. ${assessmentText(hedge.assessment, true)}`
          : `Crossed the spread to close the exposure. ${original}`;
      } else {
        detail = "The resting order filled and closed the exposure.";
      }
    }
    explanation.hidden = !detail;
    explanation.textContent = detail;
  }
  $("placedAt").textContent = placement
    ? `${(placement.clock_ms / 1000).toFixed(1)}s`
    : "—";
  $("filledAt").textContent = snapshot.fills.length
    ? `${(firstFillMs / 1000).toFixed(1)}s`
    : "—";
  $("hedgedAt").textContent = matched
    ? `${(snapshot.fills.at(-1).clock_ms / 1000).toFixed(2)}s`
    : "—";
  $("holdNote").textContent =
    `${run.eventsProcessed.toLocaleString("en-GB")} events`;
  $("demoPace").textContent = !playing
    ? "Paused"
    : wallInterval(snapshot) === ORDER_INTERVAL_MS
      ? "¼×"
      : "½×";
  Object.assign(root.dataset, {
    session: String(snapshot.id),
    revision: String(snapshot.revision),
    clock: String(snapshot.clock_ms),
    events: String(run.eventsProcessed),
    brokerRecords: String(run.brokerRecords),
    phase: String(phase),
    transport: snapshot.transport,
    policy: run.mode,
    entryMode: run.entryMode ?? "touch",
    entryDecision: entryForecast?.action ?? "",
    hedgeDecision: assessment?.action ?? "",
    hedgeCommands: String(
      run.actions.filter((action) => action.kind === "hedge").length,
    ),
  });
  historyWindow.startMs = Math.max(0, snapshot.clock_ms - 15_000);
  historyWindow.endMs = Math.max(14_000, snapshot.clock_ms + 2000);
  if (portfolio) renderPortfolio(snapshot, phase, assessment);
  charts.update(snapshot);
}
function fail(error) {
  playing = false;
  clearTimeout(timer);
  $("demoError").hidden = false;
  $("demoError").textContent = error.message;
  $("holdNote").textContent = "Feed stopped · restart required";
  controls();
}
async function pump() {
  if (!playing || restarting || inFlight) return;
  if (
    portfolio &&
    !feedPortfolio &&
    !portfolioDecisionShown &&
    run.latestDecision?.action === "cross" &&
    run.latestDecision.clock_ms === run.snapshot.clock_ms &&
    run.comparisonDecision?.action === "wait"
  ) {
    portfolioDecisionShown = true;
    pauseReason = "decision";
    render();
    resumePortfolioAfter(portfolioHolds.decision);
    return;
  }
  const entry = run.latestEntryDecision;
  if (
    run.policy.phase === "scanning" &&
    !entrySkipShown &&
    entry?.eligible &&
    entry.action === "skip"
  ) {
    entrySkipShown = true;
    pauseReason = "entry-skip";
    playing = false;
    controls();
    render();
    return;
  }
  const started = performance.now();
  const interval = feedPortfolio
    ? feedAmendmentShown
      ? 100
      : 255
    : portfolio
      ? run.snapshot.clock_ms < 3000
        ? 65
        : 115
      : wallInterval(run.snapshot);
  const before = run.snapshot;
  inFlight = run.tick(STEP_MS, {
    pauseAtMilestones: !feedPortfolio || manualFeed,
  });
  controls();
  try {
    const result = await inFlight;
    if (manualFeed && result.pauseReason === "placement") {
      pauseReason = "placement";
      playing = false;
      render();
      return;
    }
    if (feedPortfolio && result.snapshot.clock_ms >= 1100) {
      const event = narratedBookChange(
        before,
        result.snapshot,
        result.events,
        feedAmendmentShown ? "fill" : "amend",
      );
      if (event) {
        if (event.kind === "amend") feedAmendmentShown = true;
        holdFeedMoment(event, before, result.snapshot);
        return;
      }
      if (result.snapshot.clock_ms >= 12000)
        throw new Error(
          "The selected market path did not contain both visible examples.",
        );
    }
    if (result.pauseReason) {
      pauseReason = result.pauseReason;
      if (!portfolio || result.pauseReason === "hedged") playing = false;
    }
    render();
    if (run.snapshot.stage === "complete") playing = false;
  } catch (error) {
    fail(error);
  } finally {
    inFlight = null;
    controls();
    if (playing && portfolio && portfolioHolds[pauseReason])
      resumePortfolioAfter(portfolioHolds[pauseReason]);
    else if (playing && !feedMoment)
      timer = setTimeout(
        pump,
        Math.max(0, interval - (performance.now() - started)),
      );
  }
}

function holdFeedMoment(event, before, after) {
  feedMoment = { event, snapshot: before, after, applied: false };
  if (manualFeed) playing = false;
  render();
  if (!manualFeed) timer = setTimeout(applyFeedMoment, feedHoldMs.before);
}

function applyFeedMoment() {
  feedMoment.snapshot = feedMoment.after;
  feedMoment.applied = true;
  if (feedFinished()) playing = false;
  render();
  controls();
  if (playing) timer = setTimeout(resumeFeed, feedHoldMs.amendment);
}

function resumeFeed() {
  feedMoment = null;
  pauseReason = null;
  playing = true;
  render();
  void pump();
}

function advanceFeed() {
  if (
    !manualFeed ||
    restarting ||
    playing ||
    inFlight ||
    !run?.snapshot ||
    feedFinished() ||
    $("demoError").textContent
  )
    return;
  if (feedMoment && !feedMoment.applied) applyFeedMoment();
  else resumeFeed();
}

function resumePortfolioAfter(delay) {
  timer = setTimeout(() => {
    pauseReason = null;
    void pump();
  }, delay);
}
function togglePlay() {
  pauseReason = null;
  playing = !playing;
  clearTimeout(timer);
  controls();
  if (playing && !inFlight) void pump();
  if (run?.snapshot) render();
}
async function restart() {
  if (restarting) return;
  restarting = true;
  playing = false;
  clearTimeout(timer);
  controls();
  try {
    if (inFlight) await inFlight.catch(() => {});
    if (run) await run.close();
    run = new LiveShowcase({
      mode: "l3",
      entryMode,
      compareQueue: portfolio && !feedPortfolio,
      seed: demoSeed,
      scenario: "queue_demo",
    });
    $("marketSource").textContent = portfolio
      ? "Synthetic BTC · Live Kafka"
      : entryMode
        ? `Synthetic BTC · ${entryMode.toUpperCase()} entry · Live Kafka`
        : "Synthetic BTC · Touch entry · Live Kafka";
    pauseReason = null;
    entrySkipShown = false;
    portfolioDecisionShown = false;
    feedMoment = null;
    feedAmendmentShown = false;
    if (feedPortfolio) delete root.dataset.portfolioBeat;
    filledCarry = null;
    $("demoError").hidden = true;
    $("demoError").textContent = "";
    $("sceneTitle").textContent = "Connecting";
    $("sceneDescription").textContent = "";
    $("demoPace").textContent = "";
    $("holdNote").textContent = "";
    await run.start();
    charts.clear();
    playing = !manualFeed;
    render();
  } catch (error) {
    fail(error);
  } finally {
    restarting = false;
    controls();
  }
  if (playing) void pump();
}

function renderComparison(report) {
  const comparesEntry = report.experiment === "entry";
  $("comparisonEntryColumn").hidden = !comparesEntry;
  $("comparisonRows").replaceChildren(
    ...report.rows.map((result) => {
      const row = document.createElement("tr");
      const policy = document.createElement("th");
      policy.scope = "row";
      policy.textContent = comparesEntry
        ? `${result.mode.toUpperCase()} entry`
        : policyLabels[result.mode];
      row.append(policy);
      if (comparesEntry) {
        const route = result.entryAction;
        const cell = row.insertCell();
        cell.textContent = route
          ? `${entryLabels[route]} · ${(result.placement.clockMs / 1000).toFixed(1)}s`
          : "No entry";
        cell.title =
          (result.entryAssessment ?? result.lastEntryAssessment)?.reason ?? "";
      }
      for (const value of [
        result.hedgedBtc.toFixed(3),
        result.residualBtc.toFixed(3),
        `${(result.exposedMs / 1000).toFixed(2)}s`,
        result.exposureBtcSeconds.toFixed(4),
        result.crossedBtc.toFixed(3),
        result.projectedCarryCents == null
          ? "—"
          : money(result.projectedCarryCents),
      ]) {
        row.insertCell().textContent = value;
      }
      row.cells[comparesEntry ? 6 : 5].title = comparesEntry
        ? `${result.entryTakerBtc.toFixed(3)} BTC at entry · ${result.hedgeTakerBtc.toFixed(3)} BTC hedging`
        : (result.crossReason ?? result.status);
      return row;
    }),
  );
  $("comparisonStatus").textContent =
    `Same external market path verified · ${report.rows.length} policies · ${report.horizonMs / 1000}s · seed ${report.seed}`;
  $("comparisonStatus").title = `Path digest ${report.pathDigest}`;
  const difference = comparesEntry
    ? report.firstEntryDifference
    : report.firstQueueDifference;
  const queueDecision = (forecast) =>
    `${(forecast.ahead_lots / 1000).toFixed(3)} BTC ahead → ${forecast.action}`;
  const differenceTime = difference
    ? (difference.clockMs / 1000).toFixed(1)
    : null;
  $("queueDifference").textContent = !difference
    ? ""
    : comparesEntry
      ? `Same book at ${differenceTime}s. L2 · ${entryLabels[difference.l2.action]} (spot ${entryEta(difference.l2.legs?.spot)}, futures ${entryEta(difference.l2.legs?.future)}). L3 · ${entryLabels[difference.l3.action]} (spot ${entryEta(difference.l3.legs?.spot)}, futures ${entryEta(difference.l3.legs?.future)}).`
      : `At ${differenceTime}s on the same book, L2 estimated ${queueDecision(difference.l2)}. L3 saw ${queueDecision(difference.l3)}.`;
  $("queueDifference").hidden = !difference;
  $("comparisonResults").hidden = false;
  $("comparisonNote").hidden = false;
  $("comparisonMethod").textContent =
    "Both entry policies use the same L3 hedge rule. Crossed BTC includes entry and hedge fills.";
  $("comparisonMethod").hidden = !comparesEntry;
  $("playComparisonPath").href =
    `/demo?seed=${report.seed}&entry=${comparesEntry ? "l3" : "touch"}`;
  $("playComparisonPath").hidden = false;
}

async function runComparison(seed, experiment) {
  if (comparison) return;
  const controller = new AbortController();
  comparison = controller;
  $("comparePolicies").disabled = true;
  $("runComparison").disabled = true;
  $("comparisonSeed").disabled = true;
  $("comparisonExperiment").disabled = true;
  $("comparisonResults").hidden = true;
  $("queueDifference").hidden = true;
  $("comparisonNote").hidden = true;
  $("comparisonMethod").hidden = true;
  $("playComparisonPath").hidden = true;
  $("comparisonStatus").textContent = "Starting live Kafka runs…";
  $("comparisonStatus").title = "";
  $("comparisonTitle").textContent =
    experiment === "entry"
      ? "Entry policy comparison"
      : "Hedge policy comparison";
  try {
    const compare =
      experiment === "entry" ? compareEntryPolicies : compareHedgePolicies;
    const report = await compare({
      seed,
      signal: controller.signal,
      onProgress: ({ mode, clockMs, horizonMs }) => {
        if (!controller.signal.aborted)
          $("comparisonStatus").textContent =
            `${experiment === "entry" ? "L2 and L3 entry" : policyLabels[mode]} · ${(clockMs / 1000).toFixed(1)} / ${horizonMs / 1000}s of market time`;
      },
    });
    if (!controller.signal.aborted) renderComparison(report);
  } catch (error) {
    if (!controller.signal.aborted)
      $("comparisonStatus").textContent = `Comparison failed. ${error.message}`;
  } finally {
    comparison = null;
    $("comparePolicies").disabled = false;
    $("runComparison").disabled = false;
    $("comparisonSeed").disabled = false;
    $("comparisonExperiment").disabled = false;
  }
}

$("pauseDemo").onclick = togglePlay;
$("replayDemo").onclick = restart;
$("portfolioNext").onclick = advanceFeed;
$("portfolioReplay").onclick = restart;
if (manualFeed) {
  window.addEventListener("keydown", (event) => {
    if (
      event.key !== "ArrowRight" ||
      event.repeat ||
      event.altKey ||
      event.ctrlKey ||
      event.metaKey
    )
      return;
    event.preventDefault();
    advanceFeed();
  });
}
$("comparePolicies").onclick = () => {
  $("comparisonSeed").value = demoSeed;
  const experiment = entryMode ? "entry" : "hedge";
  $("comparisonExperiment").value = experiment;
  $("policyComparison").showModal();
  void runComparison(demoSeed, experiment);
};
$("comparisonForm").onsubmit = (event) => {
  event.preventDefault();
  void runComparison(
    $("comparisonSeed").valueAsNumber,
    $("comparisonExperiment").value,
  );
};
$("policyComparison").addEventListener("close", () => comparison?.abort());
window.addEventListener("pagehide", () => {
  comparison?.abort();
  playing = false;
  clearTimeout(timer);
  if (run?.snapshot)
    void fetch(`/api/sessions/${run.snapshot.id}`, {
      method: "DELETE",
      keepalive: true,
    }).catch(() => {});
});
void restart();
