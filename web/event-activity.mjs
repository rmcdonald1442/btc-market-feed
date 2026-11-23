const esc = (value) =>
  String(value ?? "").replace(
    /[&<>"']/g,
    (c) =>
      ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" })[
        c
      ],
  );
const KIND = { add: "ADD", amend: "AMEND", cancel: "CANCEL", fill: "FILL" };
const KIND_CLASS = {
  add: "add",
  amend: "amend",
  cancel: "cancel",
  fill: "fill",
};

function nearTouchLots(snapshot, event) {
  const book = snapshot.books.find((book) => book.id === event.market);
  const levels = event.side === "buy" ? book?.bids : book?.asks;
  const level = levels
    ?.slice(0, 5)
    .find((row) => Number(row.price_atoms) === event.price_cents);
  const order = level?.queue.find((row) => String(row.id) === event.order_id);
  return order ? Number(order.qty_atoms) : null;
}

/** Select a change whose exact before and after quantities survive the live batch. */
export function narratedBookChange(before, after, events, kind) {
  const previousSequence = before.visual_events.at(-1)?.sequence ?? 0;
  for (const event of after.visual_events) {
    if (event.sequence <= previousSequence || event.kind !== kind) continue;
    if (kind === "amend" && event.price_cents !== event.previous_price_cents)
      continue;
    const remainingLots =
      kind === "fill" ? event.previous_lots - event.lots : event.lots;
    if (
      remainingLots < 10 ||
      event.previous_lots - remainingLots < 15 ||
      nearTouchLots(before, event) !== event.previous_lots ||
      nearTouchLots(after, event) !== remainingLots
    )
      continue;
    if (kind === "fill") {
      const receipt = events.find(
        (record) =>
          record.type === "fill" &&
          record.fill.liquidity === "maker" &&
          record.fill.market === event.market &&
          record.fill.order_id === event.order_id &&
          record.fill.clock_ms === event.clock_ms &&
          record.fill.price_cents === event.price_cents &&
          record.fill.lots === event.lots,
      );
      if (!receipt) continue;
    }
    return { ...event, remaining_lots: remainingLots };
  }
  return null;
}

/** Compact, read-only view of the most recent public market events. */
export function createEventActivity(container) {
  if (!container) throw new Error("An event activity container is required.");
  container.classList.add("event-activity");
  container.innerHTML =
    '<div class="event-activity-strip" aria-label="Event activity"></div><ol class="event-activity-list" aria-live="polite"></ol>';
  const strip = container.querySelector(".event-activity-strip");
  const list = container.querySelector(".event-activity-list");

  function update(events = [], stats = {}, playing = false) {
    const rows = [...events].slice(-8).reverse();
    list.innerHTML = rows
      .map((event) => {
        const kind = String(event.kind ?? "").toLowerCase();
        const side = [0, "bid", "buy"].includes(event.side)
          ? "BID"
          : [1, "ask", "sell"].includes(event.side)
            ? "ASK"
            : String(event.side ?? "—").toUpperCase();
        const clock = Number(event.clock_ms);
        const time = Number.isFinite(clock)
          ? new Date(clock).toISOString().slice(11, 23)
          : "—";
        const id = event.order_id == null ? "" : `#${esc(event.order_id)}`;
        const price =
          event.price_cents == null
            ? "—"
            : `$${(Number(event.price_cents) / 100).toLocaleString("en-US", { minimumFractionDigits: 2, maximumFractionDigits: 2 })}`;
        const lots = event.lots == null ? "" : `${esc(event.lots)} lots`;
        return `<li class="event-activity-row kind-${KIND_CLASS[kind] ?? "other"}"><time>${time}</time><b>${KIND[kind] ?? esc(kind || "EVENT")}</b><span>${esc(String(event.market ?? "").toUpperCase())} ${side}</span><strong>${price}</strong><span>${lots}</span><small>${id}</small></li>`;
      })
      .join("");
    const bins = Array.isArray(stats.bins) ? stats.bins.slice(-24) : [];
    const peak = Math.max(1, ...bins.map((v) => Number(v) || 0));
    const rate = Number(stats.rate ?? stats.events_per_second);
    strip.innerHTML = `<span class="event-activity-status">${playing ? (stats.recorded ? "REPLAY" : "LIVE") : "PAUSED"}</span><span class="event-activity-rate">${Number.isFinite(rate) ? `${rate.toFixed(1)} market rows/s` : `${events.length} events`}</span><span class="event-activity-bars" aria-hidden="true">${bins.map((value) => `<i style="--bar:${Math.max(4, (100 * (Number(value) || 0)) / peak)}%"></i>`).join("")}</span>`;
    container.dataset.playing = String(Boolean(playing));
  }

  function clear() {
    list.replaceChildren();
    strip.innerHTML =
      '<span class="event-activity-status">PAUSED</span><span class="event-activity-rate">0 events</span><span class="event-activity-bars" aria-hidden="true"></span>';
    delete container.dataset.playing;
  }

  clear();
  return { update, clear };
}
