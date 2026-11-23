use std::collections::{BTreeMap, VecDeque};

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::book::{Book, BookEvent, Order, Side, Trade};

#[path = "queue_market.rs"]
mod queue_market;
pub use queue_market::MarketPath;

const DAY: u64 = 86_400_000;
const TICK: i64 = 100;
const LOTS_PER_BTC: f64 = 1000.0;
const MODEL_ID: &str = "SIM-BTC-30D";
const MODEL_VERSION: u32 = 2;
const EXPIRY_MS: u64 = 30 * DAY;
const FINANCING_RATE: f64 = 0.08;
const MAKER_FEE_BPS: f64 = 1.0;
const TAKER_FEE_BPS: f64 = 3.0;
const SPOT_EXIT_FEE_BPS: f64 = 3.0;
const MAX_OWN_ORDERS: usize = 128;
const VISUAL_EVENT_LIMIT: usize = 256;

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MarketId {
    Spot,
    Future,
}

impl MarketId {
    const BOTH: [Self; 2] = [Self::Spot, Self::Future];

    fn index(self) -> usize {
        self as usize
    }

    fn side(self) -> Side {
        match self {
            Self::Spot => Side::Buy,
            Self::Future => Side::Sell,
        }
    }

    fn other(self) -> Self {
        match self {
            Self::Spot => Self::Future,
            Self::Future => Self::Spot,
        }
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct OwnOrder {
    pub id: String,
    pub market: MarketId,
    pub side: Side,
    pub price_cents: i64,
    pub lots: u32,
    pub filled_lots: u32,
    pub status: String,
    pub created_ms: u64,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct OwnFill {
    order_id: String,
    market: MarketId,
    side: Side,
    price_cents: i64,
    lots: u32,
    clock_ms: u64,
    liquidity: String,
    fee_cents: i64,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Message {
    Book {
        market: MarketId,
        change: BookEvent,
        #[serde(default = "default_source")]
        cause: String,
    },
    Trade {
        market: MarketId,
        trade: Trade,
    },
    Acknowledgement {
        order: OwnOrder,
    },
    Fill {
        fill: OwnFill,
    },
    Cancelled {
        order_id: String,
    },
    Tick {
        rng: u64,
        fair_cents: i64,
        #[serde(default)]
        basis_cents: i64,
        sample: bool,
        #[serde(default)]
        elapsed_ms: u64,
        #[serde(default)]
        next_event_ms: u64,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        market_path: Option<MarketPath>,
    },
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct Event {
    pub clock_ms: u64,
    #[serde(flatten)]
    pub message: Message,
}

fn default_source() -> String {
    "market".into()
}

fn model_config() -> Value {
    json!({
        "id": MODEL_ID, "version": MODEL_VERSION, "lot_btc": 0.001, "tick_usd": 1.0,
        "expiry_ms": EXPIRY_MS, "annual_financing_rate": FINANCING_RATE,
        "financing_basis": "ACT/365 simple", "maker_fee_bps": MAKER_FEE_BPS,
        "taker_fee_bps": TAKER_FEE_BPS, "assumed_spot_exit_fee_bps": SPOT_EXIT_FEE_BPS,
        "spot_exit_mark": "current spot midpoint",
        "flags": ["synthetic market and fills", "financing estimate to fixed model expiry",
            "projected payoff assumes spot sale and futures cash settlement use the same final reference",
            "spot exit fee uses current midpoint as an estimate, not a maturity price forecast",
            "excludes margin, funding, slippage, taxes and counterparty risk"]
    })
}

impl Event {
    pub fn is_private(&self) -> bool {
        matches!(
            self.message,
            Message::Acknowledgement { .. } | Message::Fill { .. } | Message::Cancelled { .. }
        )
    }
}

pub enum Command {
    Initialize,
    Advance(u32),
    AdvanceMillis(u32),
    Place {
        lots: u32,
        spot_limit_cents: i64,
        future_limit_cents: i64,
    },
    Enter {
        lots: u32,
        spot_limit_cents: i64,
        future_limit_cents: i64,
        cross_market: MarketId,
    },
    Cancel(u64),
    Cross(u64),
    Hedge,
}

#[derive(Clone, Serialize)]
struct Sample {
    clock_ms: u64,
    spot_mid_cents: i64,
    future_mid_cents: i64,
    basis_cents: i64,
}

#[derive(Clone)]
pub struct Session {
    scenario: &'static str,
    seed: u64,
    rng: u64,
    next_id: u64,
    clock_ms: u64,
    elapsed_ms: u64,
    next_event_ms: u64,
    market_path: Option<MarketPath>,
    fair_cents: i64,
    basis_cents: i64,
    books: [Book; 2],
    own: BTreeMap<u64, OwnOrder>,
    fills: Vec<OwnFill>,
    history: Vec<Sample>,
    prints: VecDeque<Value>,
    visual_events: VecDeque<Value>,
    feed_stats: [FeedStats; 2],
    activity_sequence: u64,
    startup_counts: [FeedStats; 2],
}

#[derive(Clone, Default)]
struct FeedStats {
    adds: u64,
    amends: u64,
    cancels: u64,
    maker_fills: u64,
    trade_prints: u64,
}

impl Session {
    pub fn new(seed: u64) -> Self {
        Self {
            scenario: "reference",
            seed,
            rng: seed.wrapping_add(0x9e3779b97f4a7c15),
            next_id: 1,
            clock_ms: 0,
            elapsed_ms: 0,
            next_event_ms: 100,
            market_path: None,
            fair_cents: 10_000_000,
            basis_cents: 65_753,
            books: Default::default(),
            own: BTreeMap::new(),
            fills: Vec::new(),
            history: Vec::new(),
            prints: VecDeque::new(),
            visual_events: VecDeque::new(),
            feed_stats: Default::default(),
            activity_sequence: 0,
            startup_counts: Default::default(),
        }
    }

    /// Explicit illustrative starting conditions; the normal flow and matcher are unchanged.
    pub fn carry_demo(seed: u64) -> Self {
        let mut session = Self::new(seed);
        session.scenario = "carry_demo";
        session.basis_cents = 80_000;
        session
    }

    pub fn queue_demo(seed: u64) -> Self {
        let mut session = Self::new(seed);
        session.scenario = "queue_demo";
        queue_market::configure(&mut session);
        session
    }

    /// Plan against a temporary state. Only broker-read events advance the live projection.
    pub fn plan(&self, command: Command) -> Result<Vec<Event>, String> {
        let command = match command {
            Command::Advance(seconds) => Command::AdvanceMillis(seconds.saturating_mul(1000)),
            other => other,
        };
        let mut working = self.clone();
        let mut out = Vec::new();
        match command {
            Command::Initialize => {
                if self.next_id != 1 {
                    return Err("Session already initialized.".into());
                }
                if working.market_path.is_some() {
                    working.initialize_queue_market(&mut out)?;
                } else {
                    working.replenish(&mut out)?;
                }
                working.emit(
                    Message::Tick {
                        rng: working.rng,
                        fair_cents: working.fair_cents,
                        basis_cents: working.basis_cents,
                        sample: true,
                        elapsed_ms: working.elapsed_ms,
                        next_event_ms: working.next_event_ms,
                        market_path: working.market_path,
                    },
                    &mut out,
                )?;
            }
            Command::AdvanceMillis(milliseconds) => {
                if !(1..=300_000).contains(&milliseconds) {
                    return Err("Advance between 1 and 300000 milliseconds.".into());
                }
                if self.elapsed_ms >= DAY {
                    return Err("The trading day has ended.".into());
                }
                let target = (self.elapsed_ms + u64::from(milliseconds)).min(DAY);
                while working.next_event_ms <= target {
                    let scheduled = working.next_event_ms;
                    working.next_event_ms = scheduled + match working.market_path {
                        Some(path) => queue_market::arrival_interval(working.seed, path.arrivals + 1),
                        None => 70 + working.random(61),
                    };
                    working.elapsed_ms = scheduled;
                    // Commit the next arrival before doing work so request boundaries
                    // never change the stream or expose an event from the future.
                    working.clock_ms = scheduled;
                    let last_sample = working.history.last().map_or(0, |sample| sample.clock_ms);
                    let sample = scheduled / 1000 > last_sample / 1000;
                    if working.market_path.is_some() {
                        working.queue_market_arrival(&mut out)?;
                    } else if working.scenario == "carry_demo" {
                        working.demo_tick(&mut out)?;
                    } else if sample {
                        working.market_tick(&mut out)?;
                    } else {
                        working.micro_tick(&mut out)?;
                    }
                    working.emit(
                        Message::Tick {
                            rng: working.rng,
                            fair_cents: working.fair_cents,
                            basis_cents: working.basis_cents,
                            sample,
                            elapsed_ms: working.elapsed_ms,
                            next_event_ms: working.next_event_ms,
                            market_path: working.market_path,
                        },
                        &mut out,
                    )?;
                }
                if working.elapsed_ms < target {
                    working.elapsed_ms = target;
                    working.clock_ms = working.clock_ms.max(target);
                    working.emit(
                        Message::Tick {
                            rng: working.rng,
                            fair_cents: working.fair_cents,
                            basis_cents: working.basis_cents,
                            sample: false,
                            elapsed_ms: working.elapsed_ms,
                            next_event_ms: working.next_event_ms,
                            market_path: working.market_path,
                        },
                        &mut out,
                    )?;
                }
            }
            Command::Advance(_) => unreachable!("Advance is normalized to milliseconds above"),
            Command::Place {
                lots,
                spot_limit_cents,
                future_limit_cents,
            } => {
                self.validate_entry(lots, spot_limit_cents, future_limit_cents)?;
                for (market, price) in [
                    (MarketId::Spot, spot_limit_cents),
                    (MarketId::Future, future_limit_cents),
                ] {
                    working.submit_owned(market, lots, price, &mut out)?;
                }
            }
            Command::Enter {
                lots,
                spot_limit_cents,
                future_limit_cents,
                cross_market,
            } => {
                self.validate_entry(lots, spot_limit_cents, future_limit_cents)?;
                let prices = [spot_limit_cents, future_limit_cents];
                let rest_market = cross_market.other();
                let rest_price = prices[rest_market.index()];
                let resting_opposite = self.books[rest_market.index()]
                    .best(rest_market.side().opposite())
                    .ok_or("Market unavailable.")?;
                let would_cross = match rest_market.side() {
                    Side::Buy => rest_price >= resting_opposite,
                    Side::Sell => rest_price <= resting_opposite,
                };
                if would_cross {
                    return Err("The companion limit must rest on the book.".into());
                }
                let order = working.acknowledge_owned(
                    cross_market,
                    lots,
                    prices[cross_market.index()],
                    &mut out,
                )?;
                let id = order.id;
                working.match_order(cross_market, order, false, &mut out)?;
                let executed = working.own[&id].filled_lots;
                if executed < lots {
                    // The IOC never added a public resting order to remove.
                    working.emit(Message::Cancelled { order_id: id.to_string() }, &mut out)?;
                }
                if executed > 0 {
                    working.submit_owned(rest_market, executed, rest_price, &mut out)?;
                }
            }
            Command::Cancel(id) => {
                self.validate_trading()?;
                working.cancel_owned(id, &mut out)?;
            }
            Command::Hedge => {
                self.validate_trading()?;
                let mut residual: i64 = 0;
                for order in self.own.values() {
                    residual += i64::from(order.filled_lots)
                        * if order.market == MarketId::Spot { 1 } else { -1 };
                }
                if residual != 0 && self.own.len() >= MAX_OWN_ORDERS {
                    return Err("Session order limit reached.".into());
                }
                // Cancel both unfilled entry remainders before sizing the hedge.
                // Planning is atomic: another market step cannot add exposure in between.
                let active: Vec<u64> = self.own.iter()
                    .filter(|(_, order)| matches!(order.status.as_str(), "working" | "partial"))
                    .map(|(id, _)| *id).collect();
                for id in active { working.cancel_owned(id, &mut out)?; }
                if residual != 0 {
                    let market = if residual > 0 { MarketId::Future } else { MarketId::Spot };
                    let opposite = if market == MarketId::Spot { Side::Sell } else { Side::Buy };
                    let best = working.books[market.index()].best(opposite).ok_or("No opposing liquidity.")?;
                    let limit = if market == MarketId::Spot { best + 10_000 } else { best - 10_000 };
                    working.submit_owned(market, residual.unsigned_abs() as u32, limit, &mut out)?;
                }
            }
            Command::Cross(id) => {
                self.validate_trading()?;
                if self.own.len() >= MAX_OWN_ORDERS {
                    return Err("Session order limit reached.".into());
                }
                let order = self.own.get(&id).ok_or("Unknown own order.")?;
                let market = order.market;
                let remaining = order.lots - order.filled_lots;
                working.cancel_owned(id, &mut out)?;
                let book = &working.books[market.index()];
                let opposite = if order.side == Side::Buy {
                    Side::Sell
                } else {
                    Side::Buy
                };
                let best = book.best(opposite).ok_or("No opposing liquidity.")?;
                // A protective limit bounds the sweep. Any unfilled quantity rests at that limit.
                let limit = if order.side == Side::Buy {
                    best + 10_000
                } else {
                    best - 10_000
                };
                working.submit_owned(market, remaining, limit, &mut out)?;
            }
        }
        Ok(out)
    }

    fn validate_trading(&self) -> Result<(), String> {
        if self.clock_ms >= DAY {
            return Err("The trading day has ended.".into());
        }
        Ok(())
    }

    fn validate_entry(&self, lots: u32, spot: i64, future: i64) -> Result<(), String> {
        self.validate_trading()?;
        if !(1..=10_000).contains(&lots) {
            return Err("Order size must be 0.001 to 10 BTC.".into());
        }
        if self.own.len() + 2 > MAX_OWN_ORDERS {
            return Err("Session order limit reached. Start a new session.".into());
        }
        for (market, price) in [(MarketId::Spot, spot), (MarketId::Future, future)] {
            if price <= 0 || price % TICK != 0 {
                return Err("Limit prices must be positive whole dollars.".into());
            }
            let touch = self.books[market.index()]
                .best(market.side())
                .ok_or("Market unavailable.")?;
            if price.abs_diff(touch) > 100_000 {
                return Err("Limit price must be within $1,000 of the market.".into());
            }
        }
        Ok(())
    }

    fn random(&mut self, bound: u64) -> u64 {
        self.rng = self.rng.wrapping_add(0x9e3779b97f4a7c15);
        let mut z = self.rng;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d049bb133111eb);
        (z ^ (z >> 31)) % bound
    }

    fn emit(&mut self, message: Message, out: &mut Vec<Event>) -> Result<(), String> {
        let event = Event {
            clock_ms: self.clock_ms,
            message,
        };
        self.apply_one(&event)?;
        out.push(event);
        Ok(())
    }

    fn submit_owned(
        &mut self,
        market: MarketId,
        lots: u32,
        price: i64,
        out: &mut Vec<Event>,
    ) -> Result<(), String> {
        let order = self.acknowledge_owned(market, lots, price, out)?;
        self.submit(market, order, out)
    }

    fn acknowledge_owned(
        &mut self,
        market: MarketId,
        lots: u32,
        price: i64,
        out: &mut Vec<Event>,
    ) -> Result<Order, String> {
        let id = if self.market_path.is_some() {
            queue_market::PRIVATE_ID_BASE + self.own.len() as u64
        } else {
            self.next_id
        };
        let own = OwnOrder {
            id: id.to_string(),
            market,
            side: market.side(),
            price_cents: price,
            lots,
            filled_lots: 0,
            status: "working".into(),
            created_ms: self.clock_ms,
        };
        self.emit(Message::Acknowledgement { order: own }, out)?;
        Ok(Order {
            id,
            side: market.side(),
            price_cents: price,
            lots,
        })
    }

    fn submit(
        &mut self,
        market: MarketId,
        incoming: Order,
        out: &mut Vec<Event>,
    ) -> Result<(), String> {
        self.match_order(market, incoming, true, out)
    }

    fn match_order(
        &mut self,
        market: MarketId,
        incoming: Order,
        rest_remainder: bool,
        out: &mut Vec<Event>,
    ) -> Result<(), String> {
        let (changes, trades) = self.books[market.index()].plan(incoming)?;
        for change in changes {
            if !rest_remainder && matches!(change, BookEvent::Add { .. }) {
                continue;
            }
            let source = match &change {
                BookEvent::Add { .. } => "add",
                BookEvent::Reduce { id, .. }
                    if trades.iter().any(|trade| trade.maker_id == *id) =>
                {
                    "fill"
                }
                _ => "external_cancel",
            };
            self.emit(
                Message::Book {
                    market,
                    change,
                    cause: if source == "external_cancel" {
                        "cancel"
                    } else {
                        source
                    }
                    .into(),
                },
                out,
            )?;
        }
        for trade in trades {
            self.emit(
                Message::Trade {
                    market,
                    trade: trade.clone(),
                },
                out,
            )?;
            for (id, liquidity) in [(trade.maker_id, "maker"), (trade.taker_id, "taker")] {
                if let Some(own) = self.own.get(&id) {
                    let rate = if liquidity == "maker" { 1 } else { 3 };
                    let fee =
                        (trade.price_cents * i64::from(trade.lots) * rate + 5_000_000) / 10_000_000;
                    let fill = OwnFill {
                        order_id: id.to_string(),
                        market,
                        side: own.side,
                        price_cents: trade.price_cents,
                        lots: trade.lots,
                        clock_ms: self.clock_ms,
                        liquidity: liquidity.into(),
                        fee_cents: fee,
                    };
                    self.emit(Message::Fill { fill }, out)?;
                }
            }
        }
        Ok(())
    }

    fn cancel_owned(&mut self, id: u64, out: &mut Vec<Event>) -> Result<(), String> {
        let order = self.own.get(&id).ok_or("Unknown own order.")?;
        if order.status != "working" && order.status != "partial" {
            return Err("This order is no longer working.".into());
        }
        let market = order.market;
        let change = self.books[market.index()].cancel(id)?;
        self.emit(
            Message::Book {
                market,
                change,
                cause: "cancel".into(),
            },
            out,
        )?;
        self.emit(
            Message::Cancelled {
                order_id: id.to_string(),
            },
            out,
        )
    }

    fn micro_tick(&mut self, out: &mut Vec<Event>) -> Result<(), String> {
        for market in MarketId::BOTH {
            self.market_micro_tick(market, out)?;
        }
        Ok(())
    }

    fn market_micro_tick(&mut self, market: MarketId, out: &mut Vec<Event>) -> Result<(), String> {
        // Most updates are small quote changes/cancels. Occasional aggressive orders
        // execute through the same FIFO matching path used by user orders.
        if self.random(5) == 0 {
            let eligible: Vec<_> = self.books[market.index()]
                .orders()
                .into_iter()
                .filter(|o| !self.own.contains_key(&o.id))
                .collect();
            if !eligible.is_empty() {
                let old = &eligible[self.random(eligible.len() as u64) as usize];
                let lots = if self.random(3) == 0 || old.lots == 1 {
                    old.lots + 1 + self.random(30) as u32
                } else {
                    (old.lots * 2 / 3).max(1)
                };
                let price_cents = if self.random(5) == 0 {
                    let best = self.books[market.index()]
                        .best(old.side)
                        .unwrap_or(old.price_cents);
                    let distance = self.random(6) as i64 * TICK;
                    if old.side == Side::Buy {
                        best - distance
                    } else {
                        best + distance
                    }
                } else {
                    old.price_cents
                };
                let change = BookEvent::Amend {
                    id: old.id,
                    price_cents,
                    lots,
                };
                self.emit(
                    Message::Book {
                        market,
                        change,
                        cause: "amend".into(),
                    },
                    out,
                )?;
            }
            return Ok(());
        }
        if self.random(10) == 0 {
            let eligible: Vec<_> = self.books[market.index()]
                .orders()
                .into_iter()
                .filter(|o| !self.own.contains_key(&o.id))
                .collect();
            if !eligible.is_empty() {
                let old = &eligible[self.random(eligible.len() as u64) as usize];
                let change = self.books[market.index()].cancel(old.id)?;
                self.emit(
                    Message::Book {
                        market,
                        change,
                        cause: "cancel".into(),
                    },
                    out,
                )?;
            }
            return Ok(());
        }
        let side = if self.random(2) == 0 {
            Side::Buy
        } else {
            Side::Sell
        };
        let passive = self.random(3) != 0;
        let reference_side = if passive {
            side
        } else if side == Side::Buy {
            Side::Sell
        } else {
            Side::Buy
        };
        if let Some(touch) = self.books[market.index()].best(reference_side) {
            let depth = self.random(5) as i64 * TICK;
            let price = if passive {
                if side == Side::Buy {
                    touch - depth
                } else {
                    touch + depth
                }
            } else {
                touch
            };
            let id = self.next_id;
            let lots = self.random_size();
            self.submit(
                market,
                Order {
                    id,
                    side,
                    price_cents: price,
                    lots,
                },
                out,
            )?;
            if !passive && self.books[market.index()].get(id).is_some() {
                let change = self.books[market.index()].cancel(id)?;
                self.emit(
                    Message::Book {
                        market,
                        change,
                        cause: "cancel".into(),
                    },
                    out,
                )?;
            }
        }
        Ok(())
    }

    fn random_size(&mut self) -> u32 {
        // Right-skewed discrete size mix: frequent small clips and rare block flow.
        match self.random(100) {
            0..=2 => 300 + self.random(1700) as u32,
            3..=17 => 40 + self.random(260) as u32,
            _ => 1 + self.random(39) as u32,
        }
    }

    fn demo_tick(&mut self, out: &mut Vec<Event>) -> Result<(), String> {
        // Maintain one book per arrival. Chart sampling does not drive order flow.
        let spot_move = self.random(51) as i64 - 25;
        let basis_move = self.random(81) as i64 - 40;
        self.fair_cents = (self.fair_cents + spot_move).clamp(8_000_000, 12_000_000);
        self.basis_cents = (self.basis_cents + basis_move
            + (80_000 - self.basis_cents) / 1_000).clamp(-200_000, 300_000);
        let market = MarketId::BOTH[self.random(2) as usize];
        self.market_micro_tick(market, out)?;
        let mid = self.fair_cents + if market == MarketId::Future { self.basis_cents } else { 0 };
        let anchor = ((mid + TICK / 2) / TICK) * TICK;
        for side in [Side::Buy, Side::Sell] {
            let touch = if side == Side::Buy { anchor - TICK } else { anchor + TICK };
            let levels = self.books[market.index()].levels(side);
            let displaced = levels.iter().find_map(|(price, orders)| {
                let depth = if side == Side::Buy { touch - *price } else { *price - touch };
                if (0..32 * TICK).contains(&depth) { return None; }
                orders.iter().find(|order| !self.own.contains_key(&order.id)).map(|order| order.id)
            });
            if let Some(id) = displaced {
                let change = self.books[market.index()].cancel(id)?;
                self.emit(Message::Book { market, change, cause: "cancel".into() }, out)?;
            }
            let depth = if self.random(3) == 0 { self.random(32) } else { self.random(6) };
            let price = if side == Side::Buy { touch - depth as i64 * TICK }
                else { touch + depth as i64 * TICK };
            let book = &self.books[market.index()];
            let price = match (side, book.best(side.opposite())) {
                (Side::Buy, Some(ask)) => price.min(ask - TICK),
                (Side::Sell, Some(bid)) => price.max(bid + TICK),
                (_, None) => price,
            };
            if book.at_price(side, price).len() < 3 {
                let lots = 10 + self.random(30) as u32;
                self.submit(market, Order { id: self.next_id, side, price_cents: price, lots }, out)?;
            }
        }
        // Independent, finite taker flow occasionally consumes the touch queue.
        if self.random(4) == 0 {
            let side = if self.random(2) == 0 { Side::Buy } else { Side::Sell };
            if let Some(price_cents) = self.books[market.index()].best(side.opposite()) {
                let lots = 40 + self.random(161) as u32;
                let id = self.next_id;
                self.submit(market, Order { id, side, price_cents, lots }, out)?;
                if self.books[market.index()].get(id).is_some() {
                    let change = self.books[market.index()].cancel(id)?;
                    self.emit(Message::Book { market, change, cause: "cancel".into() }, out)?;
                }
            }
        }
        Ok(())
    }

    fn market_tick(&mut self, out: &mut Vec<Event>) -> Result<(), String> {
        let hour = self.clock_ms / 3_600_000;
        let active = (13..=16).contains(&hour);
        let scale = if active { 70 } else { 25 };
        let noise = self.random(21) as i64 - 10;
        let pull = (10_000_000 - self.fair_cents) / 2000;
        self.fair_cents = (self.fair_cents + noise * scale + pull).clamp(8_000_000, 12_000_000);
        // A seeded mean-reverting basis process centered near 30 days of 8% simple financing.
        // This is an illustrative process, not a fitted venue model.
        let financing_basis = (self.fair_cents as f64 * FINANCING_RATE * 30.0 / 365.0).round() as i64;
        // A slow basis factor, separate from spot: at most $5 innovation per
        // one-second reference update, with an illustrative one-hour pullback.
        let basis_noise = self.random(1_001) as i64 - 500;
        self.basis_cents = (self.basis_cents + (financing_basis - self.basis_cents) / 3_600 + basis_noise)
            .clamp(-200_000, 300_000);
        self.replenish(out)?;
        for market in MarketId::BOTH {
            // Regular aggressive flow consumes real FIFO queues, including the trader's orders.
            let side = if self.random(2) == 0 {
                Side::Buy
            } else {
                Side::Sell
            };
            let opposite = if side == Side::Buy {
                Side::Sell
            } else {
                Side::Buy
            };
            let touch = self.books[market.index()]
                .best(opposite)
                .ok_or("Empty market after replenishment.")?;
            let lots = 30 + self.random(if active { 700 } else { 300 }) as u32;
            let depth = if self.random(10) == 0 { 8 } else { 2 };
            let limit = if side == Side::Buy {
                touch + depth * TICK
            } else {
                touch - depth * TICK
            };
            let id = self.next_id;
            self.submit(
                market,
                Order {
                    id,
                    side,
                    price_cents: limit,
                    lots,
                },
                out,
            )?;
            // Expire any unfilled external aggressive remainder.
            if self.books[market.index()].get(id).is_some() {
                let change = self.books[market.index()].cancel(id)?;
                self.emit(
                    Message::Book {
                        market,
                        change,
                        cause: "cancel".into(),
                    },
                    out,
                )?;
            }
            let candidates: Vec<_> = self.books[market.index()]
                .orders()
                .into_iter()
                .filter(|order| !self.own.contains_key(&order.id))
                .collect();
            for _ in 0..4 {
                if candidates.is_empty() {
                    break;
                }
                let order = &candidates[self.random(candidates.len() as u64) as usize];
                if let Some(live) = self.books[market.index()].get(order.id) {
                    let lots = live.lots;
                    self.emit(
                        Message::Book {
                            market,
                            change: BookEvent::Reduce { id: order.id, lots },
                            cause: "cancel".into(),
                        },
                        out,
                    )?;
                }
            }
        }
        Ok(())
    }

    fn replenish(&mut self, out: &mut Vec<Event>) -> Result<(), String> {
        // Basis can be positive or negative and mean-reverts around the model financing basis.
        for market in MarketId::BOTH {
            let target_levels = 24 + self.random(17) as i64;
            let fair = self.fair_cents
                + if market == MarketId::Future {
                    self.basis_cents
                } else {
                    0
                };
            let spread = (2 + self.random(4) as i64) * TICK;
            let bid = (fair / TICK) * TICK - (spread / TICK / 2) * TICK;
            let ask = bid + spread;
            // Repricing removes displaced external quotes but never sweeps resting orders.
            let old: Vec<_> = self.books[market.index()]
                .orders()
                .into_iter()
                .filter(|order| !self.own.contains_key(&order.id))
                .filter(|order| match order.side {
                    Side::Buy => {
                        order.price_cents < bid - target_levels * TICK || order.price_cents > bid
                    }
                    Side::Sell => {
                        order.price_cents > ask + target_levels * TICK || order.price_cents < ask
                    }
                })
                .collect();
            for order in old {
                self.emit(
                    Message::Book {
                        market,
                        change: BookEvent::Reduce {
                            id: order.id,
                            lots: order.lots,
                        },
                        cause: "cancel".into(),
                    },
                    out,
                )?;
            }
            for depth in 0..target_levels {
                for side in [Side::Buy, Side::Sell] {
                    let price = if side == Side::Buy {
                        bid - depth * TICK
                    } else {
                        ask + depth * TICK
                    };
                    // New background quotes stay passive even if our orders became crossed
                    // when the synthetic reference moved. Only sampled aggressors can fill us.
                    let price = match (side, self.books[market.index()].best(side.opposite())) {
                        (Side::Buy, Some(opposite)) => price.min(opposite - TICK),
                        (Side::Sell, Some(opposite)) => price.max(opposite + TICK),
                        (_, None) => price,
                    };
                    let present = self.books[market.index()].at_price(side, price).len();
                    let target = 2 + self.random(8) as usize;
                    for _ in present..target {
                        let lots = self.random_size();
                        let id = self.next_id;
                        self.submit(
                            market,
                            Order {
                                id,
                                side,
                                price_cents: price,
                                lots,
                            },
                            out,
                        )?;
                    }
                }
            }
        }
        Ok(())
    }

    pub fn apply(&mut self, events: Vec<Event>) -> Result<(), String> {
        for event in &events {
            self.apply_one(event)?;
        }
        Ok(())
    }

    fn apply_one(&mut self, event: &Event) -> Result<(), String> {
        if event.clock_ms < self.clock_ms || event.clock_ms > DAY {
            return Err("Invalid market timestamp.".into());
        }
        self.clock_ms = event.clock_ms;
        match &event.message {
            Message::Book {
                market,
                change,
                cause,
            } => {
                if let BookEvent::Add { order } = change {
                    self.next_id = self.next_id.max(order.id + 1);
                }
                self.record_book_activity(*market, change, cause, event.clock_ms);
                self.books[market.index()].apply(change)?;
            }
            Message::Trade { market, trade } => {
                self.next_id = self.next_id.max(trade.taker_id + 1);
                if event.clock_ms > 0 {
                    self.feed_stats[market.index()].trade_prints += 1;
                } else {
                    self.startup_counts[market.index()].trade_prints += 1;
                }
                if self.prints.len() == 12 {
                    self.prints.pop_front();
                }
                self.prints.push_back(json!({
                    "market": market, "clock_ms": event.clock_ms, "price_cents": trade.price_cents,
                    "lots": trade.lots, "side": if trade.maker_side == Side::Sell { Side::Buy } else { Side::Sell },
                }));
            }
            Message::Acknowledgement { order } => {
                let id: u64 = order.id.parse().map_err(|_| "Invalid own ID.")?;
                self.next_id = self.next_id.max(id + 1);
                self.own.insert(id, order.clone());
            }
            Message::Fill { fill } => {
                let id: u64 = fill.order_id.parse().map_err(|_| "Invalid fill ID.")?;
                let order = self
                    .own
                    .get_mut(&id)
                    .ok_or("Missing own acknowledgement.")?;
                if fill.lots == 0 || fill.lots > order.lots - order.filled_lots {
                    return Err("Private fill exceeds order quantity.".into());
                }
                order.filled_lots += fill.lots;
                order.status = if order.filled_lots == order.lots {
                    "filled"
                } else {
                    "partial"
                }
                .into();
                self.fills.push(fill.clone());
            }
            Message::Cancelled { order_id } => {
                let id = order_id.parse().map_err(|_| "Invalid cancel ID.")?;
                self.own.get_mut(&id).ok_or("Unknown cancellation.")?.status = "cancelled".into();
            }
            Message::Tick {
                rng,
                fair_cents,
                basis_cents,
                sample,
                elapsed_ms,
                next_event_ms,
                market_path,
            } => {
                self.rng = *rng;
                self.fair_cents = *fair_cents;
                self.basis_cents = *basis_cents;
                self.elapsed_ms = *elapsed_ms;
                self.next_event_ms = *next_event_ms;
                self.market_path = *market_path;
                if *sample
                    && self.books.iter().all(|book| {
                        book.best(Side::Buy).is_some() && book.best(Side::Sell).is_some()
                    })
                {
                    let mids = self.books.each_ref().map(|book| {
                        (book.best(Side::Buy).unwrap() + book.best(Side::Sell).unwrap()) / 2
                    });
                    let sample = Sample {
                        clock_ms: event.clock_ms,
                        spot_mid_cents: mids[0],
                        future_mid_cents: mids[1],
                        basis_cents: mids[1] - mids[0],
                    };
                    if self
                        .history
                        .last()
                        .is_some_and(|last| last.clock_ms == sample.clock_ms)
                    {
                        self.history.pop();
                    }
                    self.history.push(sample);
                }
            }
        }
        Ok(())
    }

    fn record_book_activity(
        &mut self,
        market: MarketId,
        change: &BookEvent,
        cause: &str,
        at_ms: u64,
    ) {
        let index = market.index();
        let old = match change {
            BookEvent::Add { order } => {
                if at_ms == 0 {
                    self.startup_counts[index].adds += 1;
                } else {
                    self.feed_stats[index].adds += 1;
                }
                self.push_visual(
                    at_ms,
                    market,
                    "add",
                    order,
                    None,
                    None,
                );
                return;
            }
            BookEvent::Amend {
                id,
                price_cents,
                lots,
            } => {
                if let Some(order) = self.books[index].get(*id) {
                    let old_side = order.side;
                    let previous_price = order.price_cents;
                    let previous_lots = order.lots;
                    if at_ms == 0 {
                        self.startup_counts[index].amends += 1;
                    } else {
                        self.feed_stats[index].amends += 1;
                    }
                    let amended = Order {
                        id: *id,
                        side: old_side,
                        price_cents: *price_cents,
                        lots: *lots,
                    };
                    self.push_visual(
                        at_ms,
                        market,
                        "amend",
                        &amended,
                        Some(previous_lots),
                        Some(previous_price),
                    );
                    return;
                }
                None
            }
            BookEvent::Reduce { id, lots } => {
                self.books[index].get(*id).map(|o| (o.clone(), *lots))
            }
        };
        if let Some((order, lots)) = old {
            let kind = if cause == "fill" { "fill" } else { "cancel" };
            if at_ms == 0 {
                if kind == "fill" {
                    self.startup_counts[index].maker_fills += 1;
                } else {
                    self.startup_counts[index].cancels += 1;
                }
            } else if kind == "fill" {
                self.feed_stats[index].maker_fills += 1;
            } else {
                self.feed_stats[index].cancels += 1;
            }
            let visual_order = Order {
                lots,
                ..order.clone()
            };
            self.push_visual(
                at_ms,
                market,
                kind,
                &visual_order,
                Some(order.lots),
                None,
            );
        }
    }

    fn push_visual(
        &mut self,
        at_ms: u64,
        market: MarketId,
        kind: &str,
        order: &Order,
        previous_lots: Option<u32>,
        previous_price: Option<i64>,
    ) {
        if at_ms == 0 {
            return;
        }
        self.activity_sequence += 1;
        let mut row = json!({ "sequence": self.activity_sequence, "clock_ms": at_ms, "market": market,
            "kind": kind, "order_id": order.id.to_string(), "side": order.side,
            "price_cents": order.price_cents, "lots": order.lots });
        if let Some(value) = previous_lots {
            row["previous_lots"] = json!(value);
        }
        if let Some(value) = previous_price {
            row["previous_price_cents"] = json!(value);
        }
        if self.visual_events.len() == VISUAL_EVENT_LIMIT {
            self.visual_events.pop_front();
        }
        self.visual_events.push_back(row);
    }

    fn metrics(&self) -> Value {
        let mut quantities = [0_u32; 2];
        let mut values = [0_i64; 2];
        let mut fees = [0_i64; 2];
        for fill in &self.fills {
            let index = fill.market.index();
            quantities[index] += fill.lots;
            values[index] += fill.price_cents * i64::from(fill.lots);
            fees[index] += fill.fee_cents;
        }
        let averages = [0, 1].map(|i| {
            if quantities[i] > 0 {
                Some(values[i] as f64 / f64::from(quantities[i]))
            } else {
                None
            }
        });
        let hedged_lots = quantities[0].min(quantities[1]);
        let (mut matched_basis, mut matched_entry_fees, mut financing, mut exit_fees) =
            (0.0_f64, 0.0_f64, 0.0_f64, 0.0_f64);
        let mut spot_queue: VecDeque<(&OwnFill, u32)> = self.fills.iter()
            .filter(|fill| fill.market == MarketId::Spot)
            .map(|fill| (fill, fill.lots)).collect();
        let mut future_queue: VecDeque<(&OwnFill, u32)> = self.fills.iter()
            .filter(|fill| fill.market == MarketId::Future)
            .map(|fill| (fill, fill.lots)).collect();
        while let (Some((spot, spot_left)), Some((future, future_left))) =
            (spot_queue.front().copied(), future_queue.front().copied())
        {
            let lots = spot_left.min(future_left);
            let quantity = f64::from(lots) / LOTS_PER_BTC;
            matched_basis += (future.price_cents - spot.price_cents) as f64 * quantity;
            matched_entry_fees += spot.fee_cents as f64 * f64::from(lots) / f64::from(spot.lots)
                + future.fee_cents as f64 * f64::from(lots) / f64::from(future.lots);
            let remaining_ms = EXPIRY_MS.saturating_sub(spot.clock_ms);
            financing += spot.price_cents as f64 * quantity * FINANCING_RATE
                * remaining_ms as f64 / (365.0 * DAY as f64);
            let spot_mid = self.books[0].best(Side::Buy).unwrap_or(spot.price_cents)
                + (self.books[0].best(Side::Sell).unwrap_or(spot.price_cents)
                    - self.books[0].best(Side::Buy).unwrap_or(spot.price_cents)) / 2;
            exit_fees += spot_mid as f64 * quantity * SPOT_EXIT_FEE_BPS / 10_000.0;
            if lots == spot_left { spot_queue.pop_front(); }
            else { *spot_queue.front_mut().unwrap() = (spot, spot_left - lots); }
            if lots == future_left { future_queue.pop_front(); }
            else { *future_queue.front_mut().unwrap() = (future, future_left - lots); }
        }
        let round = |value: f64| value.round() as i64;
        let basis_cents = round(matched_basis);
        let entry_fee_cents = round(matched_entry_fees);
        let financing_cents = round(financing);
        let exit_fee_cents = round(exit_fees);
        let projected = if hedged_lots > 0 {
            // Net rounds once from full-precision components; separately reported
            // components are rounded only for display and need not add to this value.
            Some(round(matched_basis - matched_entry_fees - financing - exit_fees))
        } else { None };
        let mut current_mid_mark = -(fees.iter().sum::<i64>() as f64);
        for fill in &self.fills {
            let book = &self.books[fill.market.index()];
            let bid = book.best(Side::Buy).unwrap_or(fill.price_cents);
            let ask = book.best(Side::Sell).unwrap_or(fill.price_cents);
            let mid = bid + (ask - bid) / 2;
            let move_cents = match fill.side {
                Side::Buy => mid - fill.price_cents,
                Side::Sell => fill.price_cents - mid,
            };
            current_mid_mark += move_cents as f64 * f64::from(fill.lots) / LOTS_PER_BTC;
        }
        let indicated = match (self.books[0].best(Side::Sell), self.books[1].best(Side::Buy)) {
            (Some(spot_ask), Some(future_bid)) => Some(future_bid - spot_ask),
            _ => None,
        };
        json!({
            "spot_btc": f64::from(quantities[0]) / LOTS_PER_BTC,
            "future_short_btc": f64::from(quantities[1]) / LOTS_PER_BTC,
            "hedged_btc": f64::from(hedged_lots) / LOTS_PER_BTC,
            "residual_btc": (i64::from(quantities[0]) - i64::from(quantities[1])) as f64 / LOTS_PER_BTC,
            "spot_average_cents": averages[0], "future_average_cents": averages[1],
            "projected_pnl_cents": projected, "matched_basis_cents": basis_cents,
            "matched_entry_fees_cents": entry_fee_cents, "financing_estimate_cents": financing_cents,
            "spot_exit_fee_estimate_cents": exit_fee_cents,
            "current_mid_mark_pnl_cents": round(current_mid_mark),
            "fees_paid_cents": fees.iter().sum::<i64>(),
            "indicated_basis_cents_per_btc": indicated,
        })
    }

    pub fn snapshot(&self) -> Value {
        let books = MarketId::BOTH.map(|market| {
            let book = &self.books[market.index()];
            let levels = |side| book.levels(side).into_iter().map(|(price, queue)| json!({
                "price_atoms": price.to_string(),
                "qty_atoms": queue.iter().map(|order| order.lots).sum::<u32>().to_string(),
                "queue": queue.into_iter().map(|order| json!({"id":order.id.to_string(),"qty_atoms":order.lots.to_string()})).collect::<Vec<_>>(),
            })).collect::<Vec<_>>();
            json!({
                "id":market, "price_scale":100,"qty_scale":1000,
                "exchange_ns":(self.clock_ms * 1_000_000).to_string(), "position": self.next_id,
                "bid_cents":book.best(Side::Buy),"ask_cents":book.best(Side::Sell),
                "bids":levels(Side::Buy),"asks":levels(Side::Sell),
            })
        });
        let orders: Vec<_> = self
            .own
            .iter()
            .map(|(id, order)| {
                let mut result = json!(order);
                let remaining = self.books[order.market.index()]
                    .get(*id)
                    .map_or(0, |entry| entry.lots);
                let ahead: u32 = self.books[order.market.index()]
                    .at_price(order.side, order.price_cents)
                    .into_iter()
                    .take_while(|entry| entry.id != *id)
                    .map(|entry| entry.lots)
                    .sum();
                result["remaining_lots"] = json!(remaining);
                result["ahead_lots"] = json!(if remaining > 0 { ahead } else { 0 });
                result
            })
            .collect();
        let stride = self.history.len().div_ceil(1000).max(1);
        let mut history: Vec<_> = self.history.iter().step_by(stride).cloned().collect();
        if let Some(last) = self.history.last()
            && history
                .last()
                .is_none_or(|sample| sample.clock_ms != last.clock_ms)
        {
            history.push(last.clone());
        }
        let stats = MarketId::BOTH.map(|market| {
            let s = &self.feed_stats[market.index()];
            let startup = &self.startup_counts[market.index()];
            let records = s.adds + s.amends + s.cancels + s.maker_fills + s.trade_prints;
            let elapsed_seconds = self.elapsed_ms as f64 / 1000.0;
            (match market { MarketId::Spot => "spot", MarketId::Future => "future" }, json!({
                "adds": s.adds, "amends": s.amends, "cancels": s.cancels,
                "maker_fills": s.maker_fills, "trade_prints": s.trade_prints,
                "wire_records": records,
                "events_per_second": if elapsed_seconds > 0.0 { records as f64 / elapsed_seconds } else { 0.0 },
                "startup": { "adds": startup.adds, "amends": startup.amends, "cancels": startup.cancels,
                    "maker_fills": startup.maker_fills, "trade_prints": startup.trade_prints }
            }))
        });
        let mut snapshot = json!({
            "seed":self.seed,"clock_ms":self.clock_ms,"elapsed_ms":self.elapsed_ms,
            "next_event_ms":self.next_event_ms,"start_ms":0,"end_ms":DAY,
            "stage":if self.clock_ms == DAY {"complete"} else {"running"},
            "model":model_config(),
            "scenario":self.scenario,
            "books":books,"orders":orders,"fills":self.fills,"history":history,
            "metrics":self.metrics(),"prints":self.prints,
            "feed_stats": { "spot": stats[0].1, "future": stats[1].1 },
            "visual_events": self.visual_events,
        });
        if let Some(path) = self.market_path {
            snapshot["market_path"] = json!(path);
        }
        snapshot
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn init(seed: u64) -> Session {
        let mut session = Session::new(seed);
        session
            .apply(session.plan(Command::Initialize).unwrap())
            .unwrap();
        session
    }

    fn metric_session(spot_mid: i64, future_mid: i64) -> Session {
        let mut session = Session::new(1);
        for (market, mid) in [(MarketId::Spot, spot_mid), (MarketId::Future, future_mid)] {
            let book = &mut session.books[market.index()];
            book.apply(&BookEvent::Add { order: Order {
                id: 1, side: Side::Buy, price_cents: mid - 50, lots: 1,
            }}).unwrap();
            book.apply(&BookEvent::Add { order: Order {
                id: 2, side: Side::Sell, price_cents: mid + 50, lots: 1,
            }}).unwrap();
        }
        session
    }

    fn test_fill(market: MarketId, side: Side, lots: u32, price_cents: i64, fee_cents: i64, clock_ms: u64) -> OwnFill {
        OwnFill {
            order_id: "test".into(), market, side, lots, price_cents, fee_cents, clock_ms,
            liquidity: "maker".into(),
        }
    }

    #[test]
    fn reference_repricing_does_not_trade_against_our_resting_order() {
        let mut session = init(41);
        let mut events = Vec::new();
        session.submit_owned(MarketId::Spot, 100_000, 9_950_000, &mut events).unwrap();
        let id = session.next_id - 1;
        assert_eq!(session.own[&id].filled_lots, 0);

        // Move the synthetic reference far enough that the new external ask
        // would cross the resting buy. Repricing alone must not act as a trade.
        session.fair_cents = 9_000_000;
        session.clock_ms = 1_000;
        session.replenish(&mut events).unwrap();
        assert_eq!(session.own[&id].filled_lots, 0);
        assert_eq!(session.books[MarketId::Spot.index()].get(id).unwrap().lots, 100_000);
        assert!(!events.iter().any(|event| matches!(&event.message, Message::Fill { .. })));
    }

    #[test]
    fn v2_finance_metrics_fifo_match_only_the_opening_spot_lots() {
        let mut session = metric_session(1_000_000, 1_020_000);
        session.fills = vec![
            test_fill(MarketId::Spot, Side::Buy, 100, 1_000_000, 1_000, 0),
            test_fill(MarketId::Spot, Side::Buy, 100, 2_000_000, 2_000, 0),
            test_fill(MarketId::Future, Side::Sell, 100, 1_020_000, 1_000, 0),
        ];
        let metrics = session.metrics();
        assert_eq!(metrics["hedged_btc"], 0.1);
        assert_eq!(metrics["matched_basis_cents"], 2_000);
        assert_eq!(metrics["matched_entry_fees_cents"], 2_000);
        assert_eq!(metrics["financing_estimate_cents"], 658);
        assert_eq!(metrics["spot_exit_fee_estimate_cents"], 30);
        assert_eq!(metrics["projected_pnl_cents"], -688);
        assert_eq!(metrics["current_mid_mark_pnl_cents"], -104_000);
    }

    #[test]
    fn v2_finance_metrics_allow_negative_basis_and_negative_net_edge() {
        let mut session = metric_session(1_000_000, 990_000);
        session.fills = vec![
            test_fill(MarketId::Spot, Side::Buy, 100, 1_000_000, 100, 0),
            test_fill(MarketId::Future, Side::Sell, 100, 990_000, 100, 0),
        ];
        let metrics = session.metrics();
        assert_eq!(metrics["matched_basis_cents"], -1_000);
        assert_eq!(metrics["projected_pnl_cents"], -1_888);
    }

    #[test]
    fn v2_financing_reaches_zero_at_the_model_expiry() {
        let mut session = metric_session(1_000_000, 1_010_000);
        session.fills = vec![
            test_fill(MarketId::Spot, Side::Buy, 100, 1_000_000, 0, EXPIRY_MS),
            test_fill(MarketId::Future, Side::Sell, 100, 1_010_000, 0, EXPIRY_MS),
        ];
        let metrics = session.metrics();
        assert_eq!(metrics["financing_estimate_cents"], 0);
        assert_eq!(metrics["spot_exit_fee_estimate_cents"], 30);
        assert_eq!(metrics["projected_pnl_cents"], 970);
    }

    #[test]
    fn v2_net_rounds_once_after_allocating_fractional_cents() {
        let mut session = metric_session(5_000_000, 5_000_100);
        session.fills = vec![
            test_fill(MarketId::Spot, Side::Buy, 2, 5_000_000, 1, EXPIRY_MS),
            test_fill(MarketId::Future, Side::Sell, 1, 5_000_100, 2, EXPIRY_MS),
        ];
        let metrics = session.metrics();
        assert_eq!(metrics["matched_basis_cents"], 0);
        assert_eq!(metrics["matched_entry_fees_cents"], 3);
        assert_eq!(metrics["financing_estimate_cents"], 0);
        assert_eq!(metrics["spot_exit_fee_estimate_cents"], 2);
        // Full precision: 0.1 - 2.5 - 0 - 1.5 = -3.9, rounded once to -4.
        assert_eq!(metrics["projected_pnl_cents"], -4);
    }

    #[test]
    fn non_tick_events_are_identical_across_advance_chunk_boundaries() {
        let mut one_batch = init(73);
        let mut chunks = init(73);
        let whole = one_batch.plan(Command::AdvanceMillis(3_000)).unwrap();
        one_batch.apply(whole.clone()).unwrap();

        let mut chunked_events = Vec::new();
        for milliseconds in [777, 1_033, 1_190] {
            let batch = chunks.plan(Command::AdvanceMillis(milliseconds)).unwrap();
            chunks.apply(batch.clone()).unwrap();
            chunked_events.extend(batch);
        }
        let market_events = |events: Vec<Event>| events.into_iter()
            .filter(|event| !matches!(&event.message, Message::Tick { .. }))
            .collect::<Vec<_>>();
        assert_eq!(market_events(whole), market_events(chunked_events));
        assert_eq!(one_batch.snapshot(), chunks.snapshot());
    }

    #[test]
    fn books_are_deep_uncrossed_and_deterministic() {
        let mut a = init(73);
        let mut b = init(73);
        let events = a.plan(Command::Advance(60)).unwrap();
        a.apply(events).unwrap();
        b.apply(b.plan(Command::Advance(60)).unwrap()).unwrap();
        assert_eq!(a.snapshot(), b.snapshot());
        for book in a.books {
            assert!(book.best(Side::Buy) < book.best(Side::Sell));
            assert!(book.levels(Side::Buy).len() >= 20);
            assert!(book.levels(Side::Sell).len() >= 20);
        }
    }

    #[test]
    fn both_legs_rest_cancel_and_cross_using_actual_book() {
        let mut session = init(73);
        let bid = session.books[0].best(Side::Buy).unwrap();
        let ask = session.books[1].best(Side::Sell).unwrap();
        session
            .apply(
                session
                    .plan(Command::Place {
                        lots: 100,
                        spot_limit_cents: bid,
                        future_limit_cents: ask,
                    })
                    .unwrap(),
            )
            .unwrap();
        let ids: Vec<_> = session.own.keys().copied().collect();
        assert_eq!(session.own.len(), 2);
        assert!(ids.iter().all(|id| session.own[id].filled_lots == 0));
        session
            .apply(session.plan(Command::Cross(ids[0])).unwrap())
            .unwrap();
        assert_eq!(session.metrics()["spot_btc"], 0.1);
        assert_eq!(session.own[&ids[0]].status, "cancelled");
        session
            .apply(session.plan(Command::Cancel(ids[1])).unwrap())
            .unwrap();
        assert_eq!(session.own[&ids[1]].status, "cancelled");
        assert_eq!(session.metrics()["hedged_btc"], 0.0);
    }

    #[test]
    fn invalid_second_leg_never_changes_state_or_publishes_first() {
        let session = init(73);
        let before = session.snapshot();
        assert!(
            session
                .plan(Command::Place {
                    lots: 100,
                    spot_limit_cents: 10_000_000,
                    future_limit_cents: -1,
                })
                .is_err()
        );
        assert_eq!(session.snapshot(), before);
    }

    #[test]
    fn entry_ioc_cancels_unfilled_qty_and_rests_only_the_fill_on_either_leg() {
        for market in MarketId::BOTH {
            let mut session = init(73);
            let opposing = market.side().opposite();
            let touch = session.books[market.index()].best(opposing).unwrap();
            let available: u32 = session.books[market.index()]
                .at_price(opposing, touch)
                .iter()
                .map(|order| order.lots)
                .sum();
            let mut prices = [session.books[0].best(Side::Buy).unwrap(),
                session.books[1].best(Side::Sell).unwrap()];
            prices[market.index()] = touch;
            let events = session.plan(Command::Enter {
                lots: available + 1,
                spot_limit_cents: prices[0],
                future_limit_cents: prices[1],
                cross_market: market,
            }).unwrap();
            session.apply(events).unwrap();
            let crossed = session.own.values().find(|order| order.market == market).unwrap();
            assert_eq!(crossed.filled_lots, available);
            assert_eq!(crossed.status, "cancelled");
            assert!(session.books[market.index()].get(crossed.id.parse().unwrap()).is_none());
            let companion = session.own.values().find(|order| order.market == market.other()).unwrap();
            assert_eq!(companion.lots, available);
            assert_eq!(companion.filled_lots, 0);
            assert_eq!(companion.status, "working");
            assert!(session.fills.iter().all(|fill| fill.market == market && fill.liquidity == "taker"));
        }
    }

    #[test]
    fn entry_ioc_with_no_fills_leaves_no_companion_and_requires_post_only_limit() {
        let mut session = init(73);
        let spot_bid = session.books[0].best(Side::Buy).unwrap();
        let future_ask = session.books[1].best(Side::Sell).unwrap();
        let future_bid = session.books[1].best(Side::Buy).unwrap();
        let before = session.snapshot();
        assert!(session.plan(Command::Enter {
            lots: 100, spot_limit_cents: spot_bid, future_limit_cents: future_bid,
            cross_market: MarketId::Spot,
        }).is_err());
        assert_eq!(session.snapshot(), before);
        let events = session.plan(Command::Enter {
            lots: 100, spot_limit_cents: spot_bid, future_limit_cents: future_ask,
            cross_market: MarketId::Spot,
        }).unwrap();
        session.apply(events).unwrap();
        assert_eq!(session.own.len(), 1);
        assert_eq!(session.own.values().next().unwrap().status, "cancelled");
        assert!(session.fills.is_empty());
        let after = session.snapshot();
        for index in 0..2 {
            assert_eq!(after["books"][index]["bids"], before["books"][index]["bids"]);
            assert_eq!(after["books"][index]["asks"], before["books"][index]["asks"]);
        }
    }

    #[test]
    fn hedge_cancels_entry_remainders_and_trades_only_actual_partial_exposure() {
        let mut session = init(73);
        let mut events = Vec::new();
        let ask = session.books[0].best(Side::Sell).unwrap();
        let bid = session.books[0].best(Side::Buy).unwrap();
        let future_ask = session.books[1].best(Side::Sell).unwrap();
        session.submit_owned(MarketId::Spot, 37, ask, &mut events).unwrap();
        session.submit_owned(MarketId::Spot, 63, bid - 1000, &mut events).unwrap();
        session.submit_owned(MarketId::Future, 100, future_ask + 1000, &mut events).unwrap();
        assert_eq!(session.metrics()["residual_btc"], 0.037);
        session.apply(session.plan(Command::Hedge).unwrap()).unwrap();
        assert_eq!(session.metrics()["spot_btc"], 0.037);
        assert_eq!(session.metrics()["future_short_btc"], 0.037);
        assert_eq!(session.metrics()["residual_btc"], 0.0);
        assert_eq!(session.own.values().last().unwrap().lots, 37);
        assert!(session.own.values().all(|order| !matches!(order.status.as_str(), "working" | "partial")));
    }

    #[test]
    fn hedge_reduces_a_future_first_fill_without_adding_extra_inventory() {
        let mut session = init(73);
        let mut events = Vec::new();
        let future_bid = session.books[1].best(Side::Buy).unwrap();
        let spot_bid = session.books[0].best(Side::Buy).unwrap();
        session.submit_owned(MarketId::Future, 37, future_bid, &mut events).unwrap();
        session.submit_owned(MarketId::Spot, 100, spot_bid - 1000, &mut events).unwrap();
        assert_eq!(session.metrics()["residual_btc"], -0.037);
        session.apply(session.plan(Command::Hedge).unwrap()).unwrap();
        assert_eq!(session.metrics()["hedged_btc"], 0.037);
        assert_eq!(session.metrics()["residual_btc"], 0.0);
        let count = session.own.len();
        session.apply(session.plan(Command::Hedge).unwrap()).unwrap();
        assert_eq!(session.own.len(), count);
    }

    #[test]
    fn carry_demo_is_an_explicit_starting_preset_not_the_reference_day() {
        assert_eq!(Session::new(73).basis_cents, 65_753);
        assert_eq!(Session::carry_demo(73).basis_cents, 80_000);
        assert_eq!(Session::carry_demo(73).scenario, "carry_demo");
    }

    #[test]
    fn playback_chunk_size_preserves_the_market_path() {
        let mut once = init(73);
        let mut split = init(73);
        once.apply(once.plan(Command::Advance(60)).unwrap())
            .unwrap();
        for seconds in [31, 29] {
            split
                .apply(split.plan(Command::Advance(seconds)).unwrap())
                .unwrap();
        }
        assert_eq!(once.snapshot(), split.snapshot());
    }

    #[test]
    fn millisecond_steps_keep_the_same_seeded_path() {
        let mut once = init(99);
        let mut split = init(99);
        once.apply(once.plan(Command::Advance(1)).unwrap()).unwrap();
        for milliseconds in [37, 113, 250, 600] {
            split
                .apply(split.plan(Command::AdvanceMillis(milliseconds)).unwrap())
                .unwrap();
        }
        assert_eq!(once.snapshot(), split.snapshot());
    }
}
