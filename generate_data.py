#!/usr/bin/env python3
"""
Retail data generator for the Retail Lakehouse Capstone.

Creates one set of files per simulated day:
    landing/pos/pos_<date>.csv            point-of-sale transaction lines
    landing/products/products_<date>.csv  product master snapshot (prices change)
    landing/stores/stores_<date>.csv      store master snapshot (a store opens)

The data is deliberately MESSY, because real retail data is. Each problem
below is something you will handle later in the pipeline:

    PROBLEM                      WHERE IT SHOWS UP                    HANDLED IN
    ---------------------------  -----------------------------------  ----------
    Duplicate rows (~1%)         same line appears twice in a file    Silver
    Late-arriving rows           stores S003/S007 sync next day       Silver
    Returns (~2%)                negative qty, txn_type = RETURN      Gold logic
    Bad rows (~0.5%)             missing product_id / negative price  Silver DQ
    Schema drift                 'promo_id' column appears on day 3   Bronze
    Price + category changes     products change on days 2 and 4      SCD2
    New store                    a store appears on day 4             SCD2

Same --seed gives the same data every time, so you can re-run safely.

Usage examples:
    python generate_data.py                       # 7 days from 2026-10-01
    python generate_data.py --days 3              # only days 0-2
    python generate_data.py --first-day 3 --days 1   # only day index 3
    python generate_data.py --scale 20 --hot-share 0.3   # bigger + skewed (week 2)

Only the Python standard library is used, so there is nothing to install.
"""
import argparse
import csv
import os
import random
from datetime import date, datetime, timedelta

N_STORES = 20
N_PRODUCTS = 200
N_CUSTOMERS = 5000
OFFLINE_STORES = {"S003", "S007"}  # half of their transactions arrive a day late
DRIFT_DAY_INDEX = 3                # promo_id column appears from this day on
NEW_STORE_DAY_INDEX = 4
PRICE_CHANGE_DAYS = (2, 4)

CATEGORIES = {
    "Dairy": ["Milk 1L", "Yogurt 500g", "Cheese 200g", "Butter 250g"],
    "Bakery": ["Bread loaf", "Croissant", "Bagel 4pk", "Muffin 2pk"],
    "Produce": ["Bananas 1kg", "Apples 1kg", "Tomatoes 500g", "Spinach 250g"],
    "Beverages": ["Cola 2L", "Orange juice 1L", "Water 6pk", "Green tea 20 bags"],
    "Snacks": ["Potato chips", "Chocolate bar", "Granola bars", "Salted nuts"],
    "Household": ["Dish soap", "Paper towels", "Laundry detergent", "Trash bags"],
}
BRANDS = ["FreshCo", "ValueMark", "Golden", "Everyday", "Premium Select"]
CITIES = [("Bengaluru", "South"), ("Hyderabad", "South"), ("Mumbai", "West"),
          ("Delhi", "North"), ("Chennai", "South"), ("Pune", "West"),
          ("Kolkata", "East")]
PAYMENTS = ["CARD", "CASH", "UPI", "WALLET"]

POS_COLUMNS = ["txn_id", "line_no", "store_id", "product_id", "customer_id",
               "txn_type", "qty", "unit_price", "discount_pct",
               "payment_type", "txn_ts"]


# ---------------------------------------------------------------- masters
def build_products(seed, day_idx):
    """Product master as of a given day (prices/categories drift over time)."""
    rng = random.Random(seed)
    names = [(c, n) for c, items in CATEGORIES.items() for n in items]
    products = []
    for i in range(N_PRODUCTS):
        cat, name = names[i % len(names)]
        brand = rng.choice(BRANDS)
        products.append({
            "product_id": f"P{i + 1:04d}",
            "product_name": f"{brand} {name}",
            "category": cat,
            "brand": brand,
            "unit_price": round(rng.uniform(0.5, 15.0), 2),
        })
    # apply cumulative changes up to and including day_idx
    for d in PRICE_CHANGE_DAYS:
        if d <= day_idx:
            crng = random.Random(f"{seed}-change-{d}")
            for i in crng.sample(range(N_PRODUCTS), 5):
                products[i]["unit_price"] = round(
                    products[i]["unit_price"] * crng.uniform(0.9, 1.15), 2)
    if day_idx >= 4:  # product hierarchy change
        products[10]["category"] = "Snacks"
    return products


def build_stores(seed, day_idx, start):
    """Store master as of a given day (a new store opens on day 4)."""
    rng = random.Random(seed + 1)
    n = N_STORES + (1 if day_idx >= NEW_STORE_DAY_INDEX else 0)
    stores = []
    for i in range(1, n + 1):
        city, region = CITIES[(i - 1) % len(CITIES)]
        fmt = rng.choice(["Hyper", "Super", "Express"])
        if i <= N_STORES:
            opened = date(2015, 1, 1) + timedelta(days=rng.randint(0, 3000))
        else:
            opened = start + timedelta(days=NEW_STORE_DAY_INDEX)
        stores.append({
            "store_id": f"S{i:03d}",
            "store_name": f"Store {i:03d} {city}",
            "city": city,
            "region": region,
            "format": fmt,
            "opened_date": opened.isoformat(),
        })
    return stores


# ------------------------------------------------------------ transactions
def gen_day(seed, day_idx, start, scale, hot_share):
    """All transaction lines that HAPPENED on this day, as (delayed, row)."""
    day = start + timedelta(days=day_idx)
    rng = random.Random(f"{seed}-pos-{day.isoformat()}")
    products = build_products(seed, day_idx)
    stores = build_stores(seed, day_idx, start)
    pids = [p["product_id"] for p in products]
    price = {p["product_id"]: p["unit_price"] for p in products}
    out = []
    for s in stores:
        sid = s["store_id"]
        n_txn = int(rng.randint(100, 200) * scale)
        if sid == "S001" and hot_share > 0:      # hot store -> data skew
            n_txn = int(n_txn * (1 + hot_share * 20))
        for t in range(1, n_txn + 1):
            txn_id = f"{sid}-{day:%Y%m%d}-{t:05d}"
            cust = f"C{rng.randint(1, N_CUSTOMERS):06d}" if rng.random() < 0.6 else ""
            ts = datetime.combine(day, datetime.min.time()) + timedelta(
                hours=8, seconds=rng.randint(0, 14 * 3600 - 1))
            pay = rng.choice(PAYMENTS)
            delayed = sid in OFFLINE_STORES and rng.random() < 0.5
            for ln in range(1, rng.randint(1, 6) + 1):
                pid = "P0001" if rng.random() < hot_share else rng.choice(pids)
                is_return = rng.random() < 0.02
                qty = rng.randint(1, 4)
                row = {
                    "txn_id": txn_id,
                    "line_no": ln,
                    "store_id": sid,
                    "product_id": pid,
                    "customer_id": cust,
                    "txn_type": "RETURN" if is_return else "SALE",
                    "qty": -qty if is_return else qty,
                    "unit_price": price[pid],
                    "discount_pct": rng.choice([0, 0, 0, 5, 10, 20]),
                    "payment_type": pay,
                    "txn_ts": ts.strftime("%Y-%m-%d %H:%M:%S"),
                    "promo_id": f"PR{rng.randint(1, 30):03d}" if rng.random() < 0.2 else "",
                }
                out.append((delayed, row))
    return out


def build_pos_file(seed, day_idx, start, scale, hot_share):
    """Rows that ARRIVE in this day's file (today's + yesterday's delayed)."""
    day = start + timedelta(days=day_idx)
    today = gen_day(seed, day_idx, start, scale, hot_share)
    rows = [r for delayed, r in today if not delayed]
    late_count = 0
    if day_idx > 0:
        prev = gen_day(seed, day_idx - 1, start, scale, hot_share)
        late = [r for delayed, r in prev if delayed]
        late_count = len(late)
        rows += late

    frng = random.Random(f"{seed}-file-{day.isoformat()}")
    bad_count = 0
    for r in rows:                                   # inject bad rows
        if frng.random() < 0.005:
            bad_count += 1
            if frng.random() < 0.5:
                r["product_id"] = ""
            else:
                r["unit_price"] = -abs(r["unit_price"])
    dups = [dict(r) for r in rows if frng.random() < 0.01]   # exact duplicates
    rows += dups
    frng.shuffle(rows)
    stats = {"rows": len(rows), "late": late_count, "dups": len(dups),
             "bad": bad_count,
             "returns": sum(1 for r in rows if r["txn_type"] == "RETURN")}
    return rows, stats


# -------------------------------------------------------------------- main
def write_csv(path, columns, rows):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=columns, extrasaction="ignore",
                           lineterminator="\n")
        w.writeheader()
        w.writerows(rows)


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--start", default="2026-10-01", help="date of day index 0")
    ap.add_argument("--first-day", type=int, default=0, help="first day index to write")
    ap.add_argument("--days", type=int, default=7, help="how many days to write")
    ap.add_argument("--out", default="./landing", help="output folder")
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--scale", type=float, default=1.0, help="volume multiplier")
    ap.add_argument("--hot-share", type=float, default=0.0,
                    help="0-1, share of lines on one hot SKU; also inflates store S001 (skew)")
    a = ap.parse_args()

    start = date.fromisoformat(a.start)
    for d in range(a.first_day, a.first_day + a.days):
        day = start + timedelta(days=d)
        tag = day.isoformat()

        rows, st = build_pos_file(a.seed, d, start, a.scale, a.hot_share)
        cols = POS_COLUMNS + (["promo_id"] if d >= DRIFT_DAY_INDEX else [])
        write_csv(f"{a.out}/pos/pos_{tag}.csv", cols, rows)

        write_csv(f"{a.out}/products/products_{tag}.csv",
                  ["product_id", "product_name", "category", "brand", "unit_price"],
                  build_products(a.seed, d))
        write_csv(f"{a.out}/stores/stores_{tag}.csv",
                  ["store_id", "store_name", "city", "region", "format", "opened_date"],
                  build_stores(a.seed, d, start))

        print(f"day {d} ({tag}): pos rows={st['rows']:>6}  late={st['late']:>4}  "
              f"dups={st['dups']:>3}  bad={st['bad']:>3}  returns={st['returns']:>4}  "
              f"promo_id column={'yes' if d >= DRIFT_DAY_INDEX else 'no'}")


if __name__ == "__main__":
    main()
