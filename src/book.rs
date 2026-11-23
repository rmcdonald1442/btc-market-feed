use std::collections::{BTreeMap, VecDeque};

use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq, PartialOrd, Ord, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Side {
    Buy,
    Sell,
}

impl Side {
    pub(crate) fn opposite(self) -> Self {
        match self {
            Self::Buy => Self::Sell,
            Self::Sell => Self::Buy,
        }
    }
}

fn plan_price_level(
    incoming: &Order,
    price: i64,
    queue: &VecDeque<u64>,
    orders: &BTreeMap<u64, Order>,
    remaining: &mut u32,
    events: &mut Vec<BookEvent>,
    trades: &mut Vec<Trade>,
) -> Result<bool, String> {
    let crosses = match incoming.side {
        Side::Buy => price <= incoming.price_cents,
        Side::Sell => price >= incoming.price_cents,
    };
    if !crosses {
        return Ok(false);
    }

    for maker_id in queue {
        if *remaining == 0 {
            break;
        }
        let maker = orders
            .get(maker_id)
            .ok_or_else(|| "Book level references a missing order.".to_string())?;
        let fill = (*remaining).min(maker.lots);
        events.push(BookEvent::Reduce {
            id: *maker_id,
            lots: fill,
        });
        trades.push(Trade {
            maker_id: *maker_id,
            taker_id: incoming.id,
            price_cents: price,
            lots: fill,
            maker_side: incoming.side.opposite(),
        });
        *remaining -= fill;
    }
    Ok(true)
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct Order {
    pub id: u64,
    pub side: Side,
    pub price_cents: i64,
    pub lots: u32,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum BookEvent {
    Add {
        order: Order,
    },
    Reduce {
        id: u64,
        lots: u32,
    },
    Amend {
        id: u64,
        price_cents: i64,
        lots: u32,
    },
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub struct Trade {
    pub maker_id: u64,
    pub taker_id: u64,
    pub price_cents: i64,
    pub lots: u32,
    pub maker_side: Side,
}

#[derive(Clone, Debug, Default)]
struct SideBook {
    // Prices are sorted, and orders at each price retain FIFO insertion order.
    levels: BTreeMap<i64, VecDeque<u64>>,
}

#[derive(Clone, Debug, Default)]
pub struct Book {
    buys: SideBook,
    sells: SideBook,
    orders: BTreeMap<u64, Order>,
}

impl Book {
    fn side_book(&self, side: Side) -> &SideBook {
        match side {
            Side::Buy => &self.buys,
            Side::Sell => &self.sells,
        }
    }

    fn side_book_mut(&mut self, side: Side) -> &mut SideBook {
        match side {
            Side::Buy => &mut self.buys,
            Side::Sell => &mut self.sells,
        }
    }

    fn validate_order(order: &Order) -> Result<(), String> {
        if order.price_cents <= 0 {
            return Err("Order price must be positive.".into());
        }
        if order.lots == 0 {
            return Err("Order lots must be positive.".into());
        }
        Ok(())
    }

    /// Plans a limit order without changing this book. Fills execute at maker prices.
    pub fn plan(&self, incoming: Order) -> Result<(Vec<BookEvent>, Vec<Trade>), String> {
        Self::validate_order(&incoming)?;
        if self.orders.contains_key(&incoming.id) {
            return Err(format!("Duplicate order id {}.", incoming.id));
        }

        let opposite = incoming.side.opposite();
        let crosses_best = self
            .best(opposite)
            .is_some_and(|price| match incoming.side {
                Side::Buy => price <= incoming.price_cents,
                Side::Sell => price >= incoming.price_cents,
            });
        if !crosses_best {
            return Ok((vec![BookEvent::Add { order: incoming }], Vec::new()));
        }

        let mut remaining = incoming.lots;
        let mut events = Vec::new();
        let mut trades = Vec::new();
        match opposite {
            Side::Sell => {
                for (price, queue) in &self.sells.levels {
                    if remaining == 0
                        || !plan_price_level(
                            &incoming,
                            *price,
                            queue,
                            &self.orders,
                            &mut remaining,
                            &mut events,
                            &mut trades,
                        )?
                    {
                        break;
                    }
                }
            }
            Side::Buy => {
                for (price, queue) in self.buys.levels.iter().rev() {
                    if remaining == 0
                        || !plan_price_level(
                            &incoming,
                            *price,
                            queue,
                            &self.orders,
                            &mut remaining,
                            &mut events,
                            &mut trades,
                        )?
                    {
                        break;
                    }
                }
            }
        }

        if remaining > 0 {
            events.push(BookEvent::Add {
                order: Order {
                    lots: remaining,
                    ..incoming
                },
            });
        }
        Ok((events, trades))
    }

    /// Applies one book projection event, rejecting malformed or inconsistent input.
    pub fn apply(&mut self, event: &BookEvent) -> Result<(), String> {
        match event {
            BookEvent::Add { order } => {
                Self::validate_order(order)?;
                if self.orders.contains_key(&order.id) {
                    return Err(format!("Duplicate order id {}.", order.id));
                }
                if self.best(order.side.opposite()).is_some_and(|best| match order.side {
                    Side::Buy => order.price_cents >= best,
                    Side::Sell => order.price_cents <= best,
                }) {
                    return Err("Added order would cross the opposite book.".into());
                }
                self.side_book_mut(order.side)
                    .levels
                    .entry(order.price_cents)
                    .or_default()
                    .push_back(order.id);
                self.orders.insert(order.id, order.clone());
            }
            BookEvent::Reduce { id, lots } => {
                if *lots == 0 {
                    return Err("Reduction lots must be positive.".into());
                }
                let order = self
                    .orders
                    .get(id)
                    .ok_or_else(|| format!("Unknown order id {id}."))?;
                if *lots > order.lots {
                    return Err(format!(
                        "Cannot reduce order {id} by more lots than it has."
                    ));
                }
                let side = order.side;
                let price = order.price_cents;
                if *lots == order.lots {
                    // Check both indexes before changing either one. A malformed event
                    // must not leave the order map and price-level map out of sync.
                    let position = self.side_book(side).levels
                        .get(&price)
                        .ok_or_else(|| "Order has no price level.".to_string())?
                        .iter()
                        .position(|candidate| candidate == id)
                        .ok_or_else(|| "Order is missing from its FIFO level.".to_string())?;
                    self.orders.remove(id);
                    let levels = &mut self.side_book_mut(side).levels;
                    let queue = levels.get_mut(&price).expect("validated price level");
                    queue.remove(position);
                    if queue.is_empty() {
                        levels.remove(&price);
                    }
                } else {
                    self.orders.get_mut(id).expect("order was checked").lots -= *lots;
                }
            }
            BookEvent::Amend {
                id,
                price_cents,
                lots,
            } => {
                if *price_cents <= 0 || *price_cents % 100 != 0 {
                    return Err("Amended price must be a positive whole dollar.".into());
                }
                if *lots == 0 {
                    return Err("Amended lots must be positive.".into());
                }
                let old = self
                    .orders
                    .get(id)
                    .cloned()
                    .ok_or_else(|| format!("Unknown order id {id}."))?;
                let opposite = old.side.opposite();
                let would_cross = self.best(opposite).is_some_and(|best| match old.side {
                    Side::Buy => *price_cents >= best,
                    Side::Sell => *price_cents <= best,
                });
                if would_cross {
                    return Err("Amend would cross the opposite book.".into());
                }
                // A same-price reduction retains time priority. Every increase or price
                // change leaves the old queue position and joins the new queue tail.
                if *price_cents == old.price_cents && *lots <= old.lots {
                    self.orders.get_mut(id).expect("order was checked").lots = *lots;
                } else {
                    self.remove_from_level(&old)?;
                    let updated = Order {
                        price_cents: *price_cents,
                        lots: *lots,
                        ..old
                    };
                    self.side_book_mut(updated.side)
                        .levels
                        .entry(updated.price_cents)
                        .or_default()
                        .push_back(*id);
                    self.orders.insert(*id, updated);
                }
            }
        }
        Ok(())
    }

    fn remove_from_level(&mut self, order: &Order) -> Result<(), String> {
        let levels = &mut self.side_book_mut(order.side).levels;
        let queue = levels
            .get_mut(&order.price_cents)
            .ok_or_else(|| "Order has no price level.".to_string())?;
        let position = queue
            .iter()
            .position(|candidate| *candidate == order.id)
            .ok_or_else(|| "Order is missing from its FIFO level.".to_string())?;
        queue.remove(position);
        if queue.is_empty() {
            levels.remove(&order.price_cents);
        }
        Ok(())
    }

    pub fn best(&self, side: Side) -> Option<i64> {
        match side {
            Side::Buy => self.buys.levels.last_key_value().map(|(price, _)| *price),
            Side::Sell => self.sells.levels.first_key_value().map(|(price, _)| *price),
        }
    }

    pub fn orders(&self) -> Vec<Order> {
        [Side::Buy, Side::Sell]
            .into_iter()
            .flat_map(|side| self.levels(side).into_iter().flat_map(|(_, orders)| orders))
            .collect()
    }

    pub fn get(&self, id: u64) -> Option<&Order> {
        self.orders.get(&id)
    }

    pub fn at_price(&self, side: Side, price: i64) -> Vec<Order> {
        self.side_book(side)
            .levels
            .get(&price)
            .into_iter()
            .flatten()
            .filter_map(|id| self.orders.get(id).cloned())
            .collect()
    }

    pub fn levels(&self, side: Side) -> Vec<(i64, Vec<Order>)> {
        let mut levels: Vec<_> = self
            .side_book(side)
            .levels
            .iter()
            .map(|(price, ids)| {
                (
                    *price,
                    ids.iter()
                        .filter_map(|id| self.orders.get(id).cloned())
                        .collect(),
                )
            })
            .collect();
        if side == Side::Buy {
            levels.reverse();
        }
        levels
    }

    pub fn cancel(&self, id: u64) -> Result<BookEvent, String> {
        let order = self
            .orders
            .get(&id)
            .ok_or_else(|| format!("Unknown order id {id}."))?;
        Ok(BookEvent::Reduce {
            id,
            lots: order.lots,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn order(id: u64, side: Side, price_cents: i64, lots: u32) -> Order {
        Order {
            id,
            side,
            price_cents,
            lots,
        }
    }

    #[test]
    fn fifo_partial_fill_and_sweep_best_price_first() {
        let mut book = Book::default();
        for resting in [
            order(1, Side::Sell, 101, 2),
            order(2, Side::Sell, 101, 3),
            order(3, Side::Sell, 102, 4),
        ] {
            book.apply(&BookEvent::Add { order: resting }).unwrap();
        }
        let (events, trades) = book.plan(order(9, Side::Buy, 102, 6)).unwrap();
        assert_eq!(
            trades,
            vec![
                Trade {
                    maker_id: 1,
                    taker_id: 9,
                    price_cents: 101,
                    lots: 2,
                    maker_side: Side::Sell
                },
                Trade {
                    maker_id: 2,
                    taker_id: 9,
                    price_cents: 101,
                    lots: 3,
                    maker_side: Side::Sell
                },
                Trade {
                    maker_id: 3,
                    taker_id: 9,
                    price_cents: 102,
                    lots: 1,
                    maker_side: Side::Sell
                },
            ]
        );
        for event in &events {
            book.apply(event).unwrap();
        }
        assert_eq!(book.get(3).unwrap().lots, 3);
    }

    #[test]
    fn non_crossing_order_rests_and_cancel_removes_it() {
        let mut book = Book::default();
        book.apply(&BookEvent::Add {
            order: order(1, Side::Sell, 105, 2),
        })
        .unwrap();
        let (events, trades) = book.plan(order(2, Side::Buy, 104, 3)).unwrap();
        assert!(trades.is_empty());
        assert_eq!(
            events,
            vec![BookEvent::Add {
                order: order(2, Side::Buy, 104, 3)
            }]
        );
        assert!(book.get(2).is_none(), "planning must not mutate the book");
        for event in events {
            book.apply(&event).unwrap();
        }
        assert_eq!(book.best(Side::Buy), Some(104));
        let cancel = book.cancel(2).unwrap();
        book.apply(&cancel).unwrap();
        assert!(book.get(2).is_none());
    }

    #[test]
    fn malformed_reduction_is_rejected() {
        let mut book = Book::default();
        book.apply(&BookEvent::Add {
            order: order(1, Side::Buy, 100, 2),
        })
        .unwrap();
        assert!(book.apply(&BookEvent::Reduce { id: 1, lots: 3 }).is_err());
        assert!(book.apply(&BookEvent::Reduce { id: 1, lots: 0 }).is_err());
        assert_eq!(book.get(1).unwrap().lots, 2);
    }

    #[test]
    fn malformed_full_reduction_does_not_mutate_order_index() {
        let mut book = Book::default();
        book.apply(&BookEvent::Add { order: order(1, Side::Buy, 10_000, 2) }).unwrap();
        book.buys.levels.get_mut(&10_000).unwrap().clear();
        assert!(book.apply(&BookEvent::Reduce { id: 1, lots: 2 }).is_err());
        assert_eq!(book.get(1).unwrap().lots, 2);
    }

    #[test]
    fn added_order_cannot_cross_book() {
        let mut book = Book::default();
        book.apply(&BookEvent::Add { order: order(1, Side::Sell, 10_100, 2) }).unwrap();
        assert!(book.apply(&BookEvent::Add { order: order(2, Side::Buy, 10_100, 1) }).is_err());
        assert!(book.get(2).is_none());
    }

    #[test]
    fn amend_reduction_keeps_priority_but_increase_requeues() {
        let mut book = Book::default();
        for id in [1, 2] {
            book.apply(&BookEvent::Add {
                order: order(id, Side::Buy, 10_000, 10),
            })
            .unwrap();
        }
        book.apply(&BookEvent::Add {
            order: order(3, Side::Sell, 10_200, 4),
        })
        .unwrap();
        book.apply(&BookEvent::Amend {
            id: 1,
            price_cents: 10_000,
            lots: 6,
        })
        .unwrap();
        assert_eq!(
            book.at_price(Side::Buy, 10_000)
                .iter()
                .map(|o| o.id)
                .collect::<Vec<_>>(),
            vec![1, 2]
        );
        book.apply(&BookEvent::Amend {
            id: 1,
            price_cents: 10_000,
            lots: 8,
        })
        .unwrap();
        assert_eq!(
            book.at_price(Side::Buy, 10_000)
                .iter()
                .map(|o| o.id)
                .collect::<Vec<_>>(),
            vec![2, 1]
        );
        book.apply(&BookEvent::Amend {
            id: 1,
            price_cents: 10_100,
            lots: 8,
        })
        .unwrap();
        assert_eq!(
            book.at_price(Side::Buy, 10_100)
                .iter()
                .map(|o| o.id)
                .collect::<Vec<_>>(),
            vec![1]
        );
        assert!(
            book.apply(&BookEvent::Amend {
                id: 1,
                price_cents: 10_200,
                lots: 8
            })
            .is_err()
        );
    }
}
