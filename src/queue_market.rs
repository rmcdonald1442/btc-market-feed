//! Common external order flow for the hedge-policy experiment.
//! Each quote slot and IOC receives its ID before it encounters the FIFO book.
use super::{BookEvent, Event, MarketId, Message, Order, Session, Side, TICK};
use serde::{Deserialize, Serialize};

const SLOTS: u64 = 64;
const GROUPS: u64 = 4;
const PATH_VERSION: u8 = 2;
const IOC_ID_BASE: u64 = 1_000_000_000_000;
pub(super) const PRIVATE_ID_BASE: u64 = 2_000_000_000_000;

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq)]
pub struct MarketPath {
    version: u8,
    pub(super) arrivals: u64,
    #[serde(with = "hex_digest")]
    digest: u64,
}

mod hex_digest {
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(value: &u64, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&format!("{value:016x}"))
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<u64, D::Error> {
        let value = String::deserialize(deserializer)?;
        u64::from_str_radix(&value, 16).map_err(serde::de::Error::custom)
    }
}

impl MarketPath {
    fn fingerprint(&mut self, words: &[u64]) {
        for word in words {
            for byte in word.to_le_bytes() {
                self.digest = (self.digest ^ u64::from(byte)).wrapping_mul(0x100000001b3);
            }
        }
    }
}

// Independent random substreams indexed by arrival and purpose. Matching outcomes
// can never advance these generators. Rejection of one quote cannot resample it.
fn mix(mut value: u64) -> u64 {
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d049bb133111eb);
    value ^ (value >> 31)
}

struct Draws(u64);

impl Draws {
    fn new(seed: u64, index: u64, domain: u64) -> Self {
        let stream = mix(seed ^ domain.wrapping_mul(0xd6e8feb86659fd93));
        Self(mix(
            stream.wrapping_add(index.wrapping_mul(0x9e3779b97f4a7c15))
        ))
    }

    fn below(&mut self, bound: u64) -> u64 {
        self.0 = self.0.wrapping_add(0x9e3779b97f4a7c15);
        mix(self.0) % bound
    }
}

pub(super) fn arrival_interval(seed: u64, arrival: u64) -> u64 {
    // Short bursts alternate with quieter spells. The clock runs independently
    // of chart sampling and of the number of accepted market messages.
    let burst = Draws::new(seed, arrival / 48, 1).below(3) == 0;
    let mut draws = Draws::new(seed, arrival, 2);
    if burst {
        14 + draws.below(35)
    } else {
        25 + draws.below(70)
    }
}

pub(super) fn configure(session: &mut Session) {
    let mut draws = Draws::new(session.seed, 0, 3);
    session.basis_cents = 76_000 + draws.below(7_500) as i64;
    session.next_event_ms = arrival_interval(session.seed, 0);
    let mut path = MarketPath {
        version: PATH_VERSION,
        arrivals: 0,
        digest: 0xcbf29ce484222325,
    };
    path.fingerprint(&[
        u64::from(PATH_VERSION),
        session.seed,
        session.fair_cents as u64,
        session.basis_cents as u64,
        session.next_event_ms,
    ]);
    session.market_path = Some(path);
}

#[derive(Clone, Copy)]
enum Operation {
    Replace,
    Amend,
    Cancel,
    Take,
}

struct Intent {
    op: Operation,
    market: MarketId,
    side: Side,
    id: u64,
    previous: u64,
    price: i64,
    lots: u32,
}

fn quote_id(epoch: u64, group: u64, slot: u64) -> u64 {
    1 + (epoch * GROUPS + group) * SLOTS + slot
}

fn clip_size(draws: &mut Draws, aggressive: bool) -> u32 {
    let bucket = draws.below(100);
    // Sizes are lots of 0.001 BTC. The mixture includes small orders, round
    // clips and occasional sweeps large enough to consume multiple makers.
    (if aggressive {
        match bucket {
            0..=52 => 1 + draws.below(24),
            53..=84 => 25 + draws.below(95),
            85..=96 => 120 + draws.below(180),
            _ => 300 + draws.below(400),
        }
    } else {
        match bucket {
            0..=52 => 1 + draws.below(15),
            53..=79 => 10 + draws.below(45),
            80..=92 => [25, 50, 100][draws.below(3) as usize],
            _ => 65 + draws.below(135),
        }
    }) as u32
}

fn quote_intent(session: &Session, group: u64, slot: u64, cycle: u64) -> Intent {
    let market = if group < 2 {
        MarketId::Spot
    } else {
        MarketId::Future
    };
    let side = if group.is_multiple_of(2) {
        Side::Buy
    } else {
        Side::Sell
    };
    let mut draws = Draws::new(
        session.seed,
        cycle * GROUPS * SLOTS + group * SLOTS + slot,
        4,
    );
    let fair = session.fair_cents
        + if market == MarketId::Future {
            session.basis_cents
        } else {
            0
        };
    let width = if market == MarketId::Spot {
        1 + i64::from(draws.below(4) == 0)
    } else {
        7 + draws.below(6) as i64
    };
    let bid = (fair / TICK - width / 2) * TICK;
    let at_touch = if market == MarketId::Spot { 25 } else { 12 };
    let depth = if slot < at_touch {
        draws.below(4).min(draws.below(4))
    } else if slot < 47 {
        1 + draws.below(if market == MarketId::Spot { 14 } else { 20 })
    } else {
        // Persistent far quotes leave natural price gaps between occupied levels.
        11 + (slot - 47) + draws.below(15)
    } as i64
        * TICK;
    let price = if side == Side::Buy {
        bid - depth
    } else {
        bid + width * TICK + depth
    };
    // Stagger refresh and amendment phases across slots so the whole book
    // never switches between replenishment and cancellation in lockstep.
    let phase = cycle + (mix(session.seed ^ (group * SLOTS + slot)) & 1);
    let epoch = phase / 2;
    let replace = cycle == 0 || phase.is_multiple_of(2);
    let op = if replace {
        Operation::Replace
    } else if draws.below(6) == 0 {
        Operation::Cancel
    } else {
        Operation::Amend
    };
    let size = clip_size(&mut draws, false);
    Intent {
        op,
        market,
        side,
        price,
        lots: if market == MarketId::Spot {
            size
        } else {
            (3 * size).div_ceil(4)
        },
        id: quote_id(epoch, group, slot),
        previous: if cycle > 0 && replace {
            quote_id(epoch - 1, group, slot)
        } else {
            0
        },
    }
}

fn slot_at(seed: u64, group: u64, arrival: u64) -> (u64, u64) {
    // Every cycle visits all slots once in a seeded permutation. A slot's
    // previous quote ID remains known even after a counterfactual fill.
    let index = arrival - 1;
    let cycle = 1 + index / SLOTS;
    let mut draws = Draws::new(seed, cycle * GROUPS + group, 5);
    let odd = 2 * draws.below(SLOTS / 2) + 1;
    ((odd * (index % SLOTS) + draws.below(SLOTS)) % SLOTS, cycle)
}

fn maker_arrival(seed: u64, group: u64, arrival: u64) -> Option<u64> {
    if group < 2 {
        return Some(arrival);
    }
    // Each futures side refreshes on one randomly selected arrival in each
    // pair. Its own ordinal keeps slot lifetimes independent of the other side.
    let pair = (arrival - 1) / 2;
    let selected = Draws::new(seed, pair, 12 + group).below(2);
    ((arrival - 1) % 2 == selected).then_some(pair + 1)
}

impl Session {
    pub(super) fn initialize_queue_market(&mut self, out: &mut Vec<Event>) -> Result<(), String> {
        for group in 0..GROUPS {
            for slot in 0..SLOTS {
                self.apply_queue_intent(quote_intent(self, group, slot, 0), out)?;
            }
        }
        Ok(())
    }

    fn apply_queue_intent(&mut self, intent: Intent, out: &mut Vec<Event>) -> Result<(), String> {
        let Intent {
            op,
            market,
            side,
            id,
            previous,
            price,
            lots,
        } = intent;
        self.market_path
            .as_mut()
            .expect("queue scenario")
            .fingerprint(&[
                op as u64,
                market as u64,
                side as u64,
                id,
                previous,
                price as u64,
                u64::from(lots),
            ]);
        let index = market.index();
        let cancel = match op {
            Operation::Cancel => id,
            Operation::Replace => previous,
            _ => 0,
        };
        if cancel != 0 && self.books[index].get(cancel).is_some() {
            let change = self.books[index].cancel(cancel)?;
            self.emit(
                Message::Book {
                    market,
                    change,
                    cause: "cancel".into(),
                },
                out,
            )?;
        }
        if matches!(op, Operation::Take) {
            return self.match_order(
                market,
                Order {
                    id,
                    side,
                    price_cents: price,
                    lots,
                },
                false,
                out,
            );
        }
        // Maker intents are post-only. A missing target makes an amendment or
        // cancellation a no-op. An IOC instead matches available visible depth.
        if matches!(op, Operation::Cancel)
            || self.books[index]
                .best(side.opposite())
                .is_some_and(|opposite| {
                    if side == Side::Buy {
                        price >= opposite
                    } else {
                        price <= opposite
                    }
                })
        {
            return Ok(());
        }
        match op {
            Operation::Replace => self.submit(
                market,
                Order {
                    id,
                    side,
                    price_cents: price,
                    lots,
                },
                out,
            ),
            Operation::Amend if self.books[index].get(id).is_some() => {
                let change = BookEvent::Amend {
                    id,
                    price_cents: price,
                    lots,
                };
                self.emit(
                    Message::Book {
                        market,
                        change,
                        cause: "amend".into(),
                    },
                    out,
                )
            }
            _ => Ok(()),
        }
    }

    pub(super) fn queue_market_arrival(&mut self, out: &mut Vec<Event>) -> Result<(), String> {
        let arrival = self.market_path.expect("queue scenario").arrivals + 1;
        let mut factors = Draws::new(self.seed, arrival, 6);
        let mut regime = Draws::new(self.seed, self.clock_ms / 900, 7);
        let pressure = regime.below(41) as i64 - 20;
        let volatility = if regime.below(5) == 0 { 2 } else { 1 };
        let spot_shock = (factors.below(81) as i64 - factors.below(81) as i64) * volatility;
        self.fair_cents = (self.fair_cents + spot_shock + pressure).clamp(4_000_000, 20_000_000);
        // The carried BTC factor is shared. Basis has its own innovations and
        // mean reverts towards simple funding plus an illustrative risk premium.
        let maturity = (super::EXPIRY_MS - self.clock_ms) as f64 / (365.0 * super::DAY as f64);
        let basis_target =
            (self.fair_cents as f64 * super::FINANCING_RATE * maturity) as i64 + 14_000;
        let basis_shock = factors.below(50) as i64 - factors.below(50) as i64;
        self.basis_cents += (basis_target - self.basis_cents) / 750 + basis_shock;
        let path = self.market_path.as_mut().expect("queue scenario");
        path.arrivals = arrival;
        path.fingerprint(&[
            arrival,
            self.clock_ms,
            self.next_event_ms,
            self.fair_cents as u64,
            self.basis_cents as u64,
            pressure as u64,
            volatility as u64,
        ]);
        for group in 0..GROUPS {
            if let Some(ordinal) = maker_arrival(self.seed, group, arrival) {
                let (slot, cycle) = slot_at(self.seed, group, ordinal);
                self.apply_queue_intent(quote_intent(self, group, slot, cycle), out)?;
            }
        }
        for market in MarketId::BOTH {
            let mut flow = Draws::new(self.seed, arrival, 8 + market.index() as u64);
            let frequency = if market == MarketId::Spot { 35 } else { 18 };
            if flow.below(100) >= frequency {
                continue;
            }
            let side = if (flow.below(100) as i64) < 50 + pressure {
                Side::Buy
            } else {
                Side::Sell
            };
            let fair = self.fair_cents
                + if market == MarketId::Future {
                    self.basis_cents
                } else {
                    0
                };
            let spread_allowance = if market == MarketId::Spot { 2 } else { 6 };
            let reach = (spread_allowance + flow.below(8) as i64) * TICK;
            let limit = (fair / TICK) * TICK + if side == Side::Buy { reach } else { -reach };
            let intent = Intent {
                op: Operation::Take,
                market,
                side,
                id: IOC_ID_BASE + 2 * arrival + market.index() as u64,
                previous: 0,
                price: limit,
                lots: clip_size(&mut flow, true),
            };
            self.apply_queue_intent(intent, out)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::super::Command;
    use super::*;

    fn apply(session: &mut Session, command: Command) {
        let events = session.plan(command).expect("plan");
        session.apply(events).expect("apply");
    }

    #[test]
    fn queue_market_path_is_independent_of_orders_and_step_boundaries() {
        let mut control = Session::queue_demo(73);
        apply(&mut control, Command::Initialize);
        let mut traded = control.clone();
        apply(&mut control, Command::AdvanceMillis(3_000));
        for _ in 0..30 {
            apply(&mut traded, Command::AdvanceMillis(100));
        }
        assert_eq!(control.market_path, traded.market_path);
        let initial_path = control.market_path;
        let spot_ask = traded.books[0].best(Side::Sell).unwrap();
        let futures_bid = traded.books[1].best(Side::Buy).unwrap();
        apply(
            &mut traded,
            Command::Place {
                lots: 300,
                spot_limit_cents: spot_ask + 2_000,
                future_limit_cents: futures_bid - 2_000,
            },
        );
        assert!(!traded.fills.is_empty());
        assert_ne!(control.books[0].orders(), traded.books[0].orders());
        assert_eq!(control.market_path, traded.market_path);
        apply(&mut traded, Command::AdvanceMillis(6_000));
        for ms in [100, 700, 1200, 4000] {
            apply(&mut control, Command::AdvanceMillis(ms));
        }
        assert_eq!(control.market_path, traded.market_path);
        assert_eq!(
            (
                control.fair_cents,
                control.basis_cents,
                control.next_event_ms
            ),
            (traded.fair_cents, traded.basis_cents, traded.next_event_ms)
        );
        assert_ne!(initial_path, traded.market_path);
        let mut different = Session::queue_demo(74);
        apply(&mut different, Command::Initialize);
        apply(&mut different, Command::AdvanceMillis(9_000));
        assert_ne!(control.market_path, different.market_path);
    }
}
