const theme = {
  bid: '#3f8279',
  ask: '#a85e69',
  spot: '#76baff',
  future: '#ffc66e',
  text: '#aabdc9',
  grid: '#253440',
  highlight: '#d1a9ff',
};
const priceFormat = new Intl.NumberFormat('en-US', {
  minimumFractionDigits: 2,
  maximumFractionDigits: 2,
});
const fixedPrice = (cents) => priceFormat.format(cents / 100);

function drawingSurface(host, paint) {
  const canvas = document.createElement('canvas');
  canvas.style.cssText = 'display:block;width:100%;height:100%';
  canvas.setAttribute('aria-hidden', 'true');
  host.append(canvas);
  const ctx = canvas.getContext('2d');
  let model;

  function draw() {
    const width = host.clientWidth;
    const height = host.clientHeight;
    if (width < 2 || height < 2) return;
    const scale = window.devicePixelRatio || 1;
    const pixelWidth = Math.round(width * scale);
    const pixelHeight = Math.round(height * scale);
    if (canvas.width !== pixelWidth) canvas.width = pixelWidth;
    if (canvas.height !== pixelHeight) canvas.height = pixelHeight;
    ctx.setTransform(scale, 0, 0, scale, 0, 0);
    ctx.clearRect(0, 0, width, height);
    if (!model) return;
    ctx.font = '11px ui-monospace, monospace';
    ctx.textBaseline = 'middle';
    paint(ctx, width, height, model);
  }

  new ResizeObserver(draw).observe(host);
  return (next) => {
    model = next;
    draw();
  };
}

function line(ctx, x1, y1, x2, y2, color) {
  ctx.strokeStyle = color;
  ctx.beginPath();
  ctx.moveTo(x1, y1);
  ctx.lineTo(x2, y2);
  ctx.stroke();
}

function visibleLevels(levels, limit, focusedPrices) {
  const nearTouch = levels.slice(0, limit);
  for (const price of focusedPrices) {
    if (nearTouch.some((row) => Number(row.price_atoms) === price)) continue;
    const level = levels.find((row) => Number(row.price_atoms) === price);
    if (level) nearTouch.push(level);
  }
  return nearTouch;
}

function hatch(ctx, x, y, width, height) {
  ctx.save();
  ctx.beginPath();
  ctx.rect(x, y, width, height);
  ctx.clip();
  for (let dx = -height; dx < width; dx += 6) {
    line(ctx, x + dx, y + height, x + dx + height, y, '#ffffff86');
  }
  ctx.restore();
}

function renderBook(ctx, width, height, { book, orders, highlight, axisTitle }) {
  const left = 94;
  const right = width - 22;
  if (right <= left || height < 74) return;
  const shownPerSide = Math.max(2, Math.floor((height - 55) / 32));
  const open = orders.filter((order) => order.market === book.id && order.remaining_lots > 0);
  const owned = new Set(open.map((order) => String(order.id)));
  const focus = (side) => [
    ...open.filter((order) => order.side === side).map((order) => order.price_cents),
    ...(highlight?.market === book.id && highlight.side === side ? [highlight.price_cents] : []),
  ];
  const asks = visibleLevels(book.asks, shownPerSide, focus('sell'));
  const bids = visibleLevels(book.bids, shownPerSide, focus('buy'));
  const rows = [
    ...asks.map((row) => ({ row, color: theme.ask })),
    { row: null },
    ...bids.map((row) => ({ row, color: theme.bid })),
  ].sort((a, b) => {
    const middle = (Number(book.bid_cents) + Number(book.ask_cents)) / 2;
    return Number(b.row?.price_atoms ?? middle) - Number(a.row?.price_atoms ?? middle);
  });
  const maxLots = Math.max(1, ...bids.concat(asks).map((row) => Number(row.qty_atoms)));
  const rawStep = maxLots / 4;
  const magnitude = 10 ** Math.floor(Math.log10(rawStep));
  const step = Math.max(1, Math.ceil(rawStep / magnitude) * magnitude);
  const pxPerLot = (right - left) / (step * 4);
  const bottom = height - 38;
  const rowHeight = (bottom - 8) / rows.length;
  const barHeight = Math.min(17, rowHeight - 3);
  if (barHeight <= 0) return;
  for (let i = 0; i <= 4; i += 1) {
    const x = left + ((right - left) * i) / 4;
    line(ctx, x, 6, x, bottom, theme.grid);
    ctx.textAlign = 'center';
    ctx.fillStyle = theme.text;
    ctx.fillText((i * step / book.qty_scale).toFixed(3), x, height - 25);
  }
  ctx.textAlign = 'center';
  ctx.fillText(axisTitle, (left + right) / 2, height - 9);

  for (const [index, { row, color }] of rows.entries()) {
    const y = 8 + (index + 0.5) * rowHeight;
    if (!row) {
      line(ctx, 8, y, right, y, theme.grid);
      continue;
    }
    ctx.textAlign = 'right';
    ctx.fillStyle = theme.text;
    ctx.fillText(fixedPrice(Number(row.price_atoms)), left - 9, y);
    let x = left;
    for (const order of row.queue) {
      const span = Number(order.qty_atoms) * pxPerLot;
      const barWidth = span * (span > 4 ? 0.95 : 1);
      const own = owned.has(String(order.id));
      const selected = highlight?.market === book.id && highlight.order_id === String(order.id);
      ctx.fillStyle = selected ? theme.highlight : own ? theme[book.id] : color;
      ctx.fillRect(x, y - barHeight / 2, barWidth, barHeight);
      if (own) hatch(ctx, x, y - barHeight / 2, barWidth, barHeight);
      if (selected && barWidth > 1) {
        ctx.strokeStyle = '#f4eaff';
        ctx.strokeRect(x, y - barHeight / 2, barWidth, barHeight);
      }
      x += span;
    }
  }
}

function renderHistory(ctx, width, height, { snapshot, historyWindow }) {
  const left = 101;
  const right = width - 18;
  if (right <= left || height < 36) return;
  const end = Math.max(historyWindow.endMs, snapshot.clock_ms);
  const start = end - (historyWindow.endMs - historyWindow.startMs);
  const xAt = (clock) => left + (clock - start) * (right - left) / (end - start);
  const visible = (clock) => clock >= start && clock <= snapshot.clock_ms;
  const rows = snapshot.history.filter((sample) => visible(sample.clock_ms));
  const panelHeight = (height - 16) / 2;
  for (const [index, market] of ['spot', 'future'].entries()) {
    const series = rows.flatMap((sample) => {
      const value = sample[`${market}_mid_cents`];
      return Number.isFinite(value) ? [{ clock: sample.clock_ms, price: value }] : [];
    });
    const book = snapshot.books.find(({ id }) => id === market);
    if (book?.bid_cents != null && book.ask_cents != null) {
      series.push({ clock: snapshot.clock_ms, price: (book.bid_cents + book.ask_cents) / 2 });
    }
    if (!series.length) continue;
    const placed = snapshot.orders.filter((order) => order.market === market && visible(order.created_ms));
    const filled = snapshot.fills.filter((fill) => fill.market === market && visible(fill.clock_ms));
    const prices = [...series.map((p) => p.price), ...placed.concat(filled).map((p) => p.price_cents)];
    const low = Math.min(...prices) - 100;
    const high = Math.max(...prices) + 100;
    const top = index * panelHeight + 7;
    const bottom = (index + 1) * panelHeight - 5;
    const yAt = (price) => bottom - (price - low) * (bottom - top) / (high - low);
    const color = theme[market];
    line(ctx, left, bottom, right, bottom, theme.grid);
    ctx.textAlign = 'right';
    ctx.fillStyle = color;
    ctx.fillText(fixedPrice(series.at(-1).price), left - 9, (top + bottom) / 2);
    ctx.strokeStyle = color;
    ctx.beginPath();
    ctx.moveTo(xAt(series[0].clock), yAt(series[0].price));
    for (const point of series.slice(1)) ctx.lineTo(xAt(point.clock), yAt(point.price));
    ctx.stroke();
    for (const marker of [...placed, ...filled]) {
      ctx.beginPath();
      ctx.arc(xAt(marker.created_ms ?? marker.clock_ms), yAt(marker.price_cents), 3.5, 0, 2 * Math.PI);
      if (marker.created_ms != null) ctx.stroke();
      else ctx.fill();
    }
  }
  ctx.fillStyle = theme.text;
  ctx.textAlign = 'left';
  ctx.fillText(`${(start / 1000).toFixed(1)}s`, left, height - 7);
  ctx.textAlign = 'right';
  ctx.fillText(`${(end / 1000).toFixed(1)}s`, right, height - 7);
}

export function createMarketCharts({
  spotElement, futureElement, historyElement, historyWindow, volumeAxisLabel,
}) {
  const renderers = {
    spot: drawingSurface(spotElement, renderBook),
    future: drawingSurface(futureElement, renderBook),
  };
  const history = historyElement && drawingSurface(historyElement, renderHistory);
  return {
    update(snapshot, { highlightOrder } = {}) {
      for (const book of snapshot.books) {
        renderers[book.id]({ book, orders: snapshot.orders, highlight: highlightOrder, axisTitle: volumeAxisLabel });
      }
      history?.({ snapshot, historyWindow });
    },
    clear() {
      for (const draw of Object.values(renderers)) draw(null);
      history?.(null);
    },
  };
}
