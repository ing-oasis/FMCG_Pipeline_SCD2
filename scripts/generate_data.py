"""
PB Tech pipeline — data simulator.

Produces TWO complete sets of source files (`day1/` and `day2/`) in
scripts/sim_data/. The DAG reads from data/raw/ — a separate switch_to_day.sh
script copies the active day's files into data/raw/.

Why two days:
  Day 1 establishes initial state. Day 2 introduces deliberate changes
  that exercise every SCD2 scenario (insert_initial, type2_change,
  type1_update, discontinued, unchanged) plus the reconciliation alert path.

Files produced per day:
  - transactions.csv         POS sales for that day
  - inv_movements.csv        Stock events (sale_outs, receipts, transfers, adjustments)
  - cost_master.csv          Current-state cost snapshot per SKU
  - inventory_snapshot.csv   ERP-reported end-of-day stock per (sku, warehouse)

Internal consistency invariants (verified by Phase 2's checkpoint):
  - Every transaction's SKU exists in that day's cost master
    (with one deliberate orphan on day 2 — see ORPHAN_SKU below)
  - Every transaction has a matching sale_out movement
  - Stock at every (sku, warehouse) is non-negative on day 1
    (initial receipts on day-1 morning are sized to handle day's sales)
  - Reconciliation drift is mostly small (~3-5 unit variance)
    plus one dramatic case (DRIFT_SKU off by 50 units) for demo

Reproducibility:
  Fixed random seeds. Day 1 uses seed 100, Day 2 uses seed 200.
  Re-running the script produces identical output.
"""

import csv
import json
import random
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path

# Output paths
SIM_ROOT = Path(__file__).parent / "sim_data"
DAY1_DIR = SIM_ROOT / "day1"
DAY2_DIR = SIM_ROOT / "day2"
for d in (DAY1_DIR, DAY2_DIR):
    d.mkdir(parents=True, exist_ok=True)

# ============================================================================
# Master data (shared between days)
# ============================================================================

# Categories with realistic NZ computing-retail price bands per brand.
# Each entry: (category, [(brand, low, high), ...])
CATEGORY_BRANDS = {
    "Laptops": [
        ("HP", 699, 2999), ("Lenovo", 799, 3499), ("ASUS", 899, 3999),
        ("Acer", 649, 2499), ("Dell", 899, 3799), ("Apple", 1499, 4999),
        ("MSI", 1299, 4499),
    ],
    "Components": [
        ("Intel", 299, 999), ("AMD", 249, 899), ("NVIDIA", 499, 2499),
        ("ASUS", 199, 1999), ("MSI", 179, 1899), ("Gigabyte", 179, 1799),
        ("Corsair", 89, 599), ("Kingston", 59, 449), ("Samsung", 119, 699),
        ("Western Digital", 89, 549),
    ],
    "Peripherals": [
        ("Logitech", 29, 399), ("Razer", 59, 499), ("SteelSeries", 49, 449),
        ("HyperX", 49, 379), ("Corsair", 79, 449),
    ],
    "Monitors": [
        ("LG", 249, 1999), ("Samsung", 229, 2499), ("ASUS", 299, 2799),
        ("Dell", 279, 1899), ("MSI", 249, 1599), ("BenQ", 299, 1799),
    ],
    "Networking": [
        ("TP-Link", 49, 599), ("Netgear", 79, 899), ("ASUS", 129, 1299),
        ("Ubiquiti", 149, 1599),
    ],
    "Audio": [
        ("Sony", 79, 699), ("Bose", 199, 899), ("JBL", 49, 549),
        ("Logitech", 29, 399), ("HyperX", 59, 349),
    ],
    "Gaming": [
        ("Sony", 499, 1299), ("Microsoft", 449, 999), ("Nintendo", 349, 799),
        ("Razer", 79, 599), ("Logitech", 59, 449),
    ],
    "Storage": [
        ("Samsung", 79, 899), ("Kingston", 59, 599), ("Western Digital", 49, 749),
        ("Seagate", 49, 699), ("Crucial", 59, 499),
    ],
    "Mobile": [
        ("Samsung", 299, 2499), ("Apple", 599, 2899), ("Google", 549, 1999),
        ("OPPO", 299, 1499),
    ],
}

# Stores (selling locations) and warehouses (stock-holding locations).
# Each store has an associated home warehouse for fulfilment.
STORE_DEFINITIONS = [
    # (store_id, name, region, type, is_warehouse, home_warehouse_id)
    ("ST001", "Auckland Albany",     "Auckland",      "Physical", False, "WH001"),
    ("ST002", "Auckland Penrose",    "Auckland",      "Physical", False, "WH001"),
    ("ST003", "Auckland Manukau",    "Auckland",      "Physical", False, "WH001"),
    ("ST004", "Auckland CBD",        "Auckland",      "Physical", False, "WH001"),
    ("ST005", "Hamilton Te Rapa",    "Waikato",       "Physical", False, "WH002"),
    ("ST006", "Tauranga Mt Maunganui","Bay of Plenty","Physical", False, "WH002"),
    ("ST007", "Wellington Petone",   "Wellington",    "Physical", False, "WH003"),
    ("ST008", "Wellington CBD",      "Wellington",    "Physical", False, "WH003"),
    ("ST009", "Christchurch Tower Junction", "Canterbury", "Physical", False, "WH004"),
    ("ST010", "Christchurch Riccarton",      "Canterbury", "Physical", False, "WH004"),
    ("ST011", "Dunedin",             "Otago",         "Physical", False, "WH004"),
    ("ST012", "Online - Website",    "National",      "Online",   False, "WH001"),
    ("ST013", "Online - Mobile App", "National",      "Online",   False, "WH001"),
    ("ST014", "Online - Marketplace","National",      "Online",   False, "WH001"),
    # Warehouses (stock locations, not selling locations)
    ("WH001", "Auckland DC",         "Auckland",      "Physical", True,  "WH001"),
    ("WH002", "Hamilton DC",         "Waikato",       "Physical", True,  "WH002"),
    ("WH003", "Wellington DC",       "Wellington",    "Physical", True,  "WH003"),
    ("WH004", "Christchurch DC",     "Canterbury",    "Physical", True,  "WH004"),
]

WAREHOUSE_IDS = [s[0] for s in STORE_DEFINITIONS if s[4]]  # is_warehouse = True
SELLING_STORES = [s for s in STORE_DEFINITIONS if not s[4]]


# ============================================================================
# Step 1 — Build the product catalog (shared between days)
# ============================================================================

def generate_products(seed=42):
    """Build the SKU catalog. Same for both days (catalog evolution is
    handled by adding/removing rows in the cost master, not the catalog)."""
    rng = random.Random(seed)
    products = []
    sku_counter = 1000
    for category, brand_specs in CATEGORY_BRANDS.items():
        for brand, low, high in brand_specs:
            num_skus = rng.randint(3, 7)
            for i in range(num_skus):
                sku_counter += 1
                raw_price = rng.uniform(low, high)
                # Round to typical retail price points like $1299.95
                list_price = round(raw_price / 5) * 5 - 0.05
                products.append({
                    "sku":          f"SKU{sku_counter:05d}",
                    "product_name": f"{brand} {category[:-1] if category.endswith('s') else category} M{i+1}",
                    "brand":        brand,
                    "category":     category,
                    "list_price":   round(list_price, 2),
                    "supplier_id":  f"SUP{rng.randint(100, 130):03d}",
                    "active":       True,
                })
    return products


def write_stores(out_dir):
    """Write the store/warehouse master file. Identical for both days."""
    with open(out_dir / "stores.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["store_id", "store_name", "region", "store_type",
                    "is_warehouse", "home_warehouse_id", "open_date"])
        for sid, name, region, stype, is_wh, home_wh in STORE_DEFINITIONS:
            # Stable open dates so re-runs produce identical files
            open_date = "2015-06-01" if "Online" in name else "2010-01-01"
            w.writerow([sid, name, region, stype, is_wh, home_wh, open_date])


def write_products(out_dir, products):
    """Write the product catalog. Identical for both days."""
    with open(out_dir / "products.json", "w", encoding="utf-8") as f:
        json.dump(products, f, indent=2)


# ============================================================================
# Step 2 — Cost master (the SCD2 source) — DAY-SPECIFIC
# ============================================================================

# These markers are the heart of the SCD2 demo. Day 2's cost master has
# deliberate differences from Day 1's, hand-chosen to exercise every scenario.

NUM_TYPE2_COST_CHANGES   = 25  # day 2 changes unit_cost only
NUM_TYPE2_SUPPLIER_CHANGES = 3  # day 2 changes supplier_id only
NUM_TYPE2_BOTH_CHANGES    = 2   # day 2 changes BOTH unit_cost and supplier_id
NUM_TYPE1_UOM_CHANGES     = 10  # day 2 changes uom (Type 1 — overwrite, not Type 2)
NUM_DISCONTINUED          = 5   # SKUs in day 1 but not day 2
NUM_NEW_SKUS              = 3   # SKUs in day 2 but not day 1
ORPHAN_SKU_INDEX          = -1  # last SKU on day 1 will be referenced by a day 2
                                # transaction even though it's been discontinued
                                # → exercises the orphan/sentinel cost_key path


def generate_cost_master_day1(products, seed=100):
    """Day 1 cost master — current cost for every SKU in the catalog.
    Cost is wholesale (typically 60-80% of list price)."""
    rng = random.Random(seed)
    rows = []
    for p in products:
        # Wholesale cost is 55-78% of list price (gross margin 22-45%)
        cost_pct = rng.uniform(0.55, 0.78)
        unit_cost = round(p["list_price"] * cost_pct, 4)
        rows.append({
            "sku":          p["sku"],
            "supplier_id":  p["supplier_id"],
            "unit_cost":    unit_cost,
            "currency":     "NZD",
            "uom":          "each",
            "extracted_at": "2026-04-01 02:00:00",  # day 1 is April 1
        })
    return rows


def generate_cost_master_day2(day1_rows, seed=200):
    """Day 2 cost master — like day 1, but with deliberate changes to
    exercise the SCD2 merge. The change mix is locked by the constants
    above (NUM_TYPE2_COST_CHANGES etc.). Each change is logged so we
    can verify the merge later."""
    rng = random.Random(seed)
    rows = [dict(r) for r in day1_rows]  # deep copy

    # Pick non-overlapping SKU sets for each change category
    available_indices = list(range(len(rows)))
    rng.shuffle(available_indices)

    # Type 2: unit_cost change
    type2_cost_idx = available_indices[:NUM_TYPE2_COST_CHANGES]
    available_indices = available_indices[NUM_TYPE2_COST_CHANGES:]

    # Type 2: supplier_id change
    type2_supplier_idx = available_indices[:NUM_TYPE2_SUPPLIER_CHANGES]
    available_indices = available_indices[NUM_TYPE2_SUPPLIER_CHANGES:]

    # Type 2: both changes
    type2_both_idx = available_indices[:NUM_TYPE2_BOTH_CHANGES]
    available_indices = available_indices[NUM_TYPE2_BOTH_CHANGES:]

    # Type 1: uom change (overwrite, no new history row)
    type1_uom_idx = available_indices[:NUM_TYPE1_UOM_CHANGES]
    available_indices = available_indices[NUM_TYPE1_UOM_CHANGES:]

    # Discontinued (will be removed below)
    discontinued_idx = available_indices[:NUM_DISCONTINUED]

    change_log = {  # for verification — written to day2/expected_changes.json
        "type2_unit_cost_changes": [],
        "type2_supplier_changes": [],
        "type2_both_changes": [],
        "type1_uom_changes": [],
        "discontinued_skus": [],
        "new_skus": [],
        "orphan_sku_referenced_in_day2_txn": None,
    }

    # Apply unit_cost changes (3-12% movement either direction)
    for i in type2_cost_idx:
        old_cost = rows[i]["unit_cost"]
        delta = rng.uniform(0.03, 0.12) * (1 if rng.random() < 0.6 else -1)
        new_cost = round(old_cost * (1 + delta), 4)
        change_log["type2_unit_cost_changes"].append({
            "sku":      rows[i]["sku"],
            "old_cost": old_cost,
            "new_cost": new_cost,
        })
        rows[i]["unit_cost"] = new_cost

    # Apply supplier_id changes (move to a different SUP)
    for i in type2_supplier_idx:
        old_sup = rows[i]["supplier_id"]
        new_sup_num = rng.randint(100, 130)
        new_sup = f"SUP{new_sup_num:03d}"
        # Make sure we actually changed the supplier
        while new_sup == old_sup:
            new_sup_num = rng.randint(100, 130)
            new_sup = f"SUP{new_sup_num:03d}"
        change_log["type2_supplier_changes"].append({
            "sku":          rows[i]["sku"],
            "old_supplier": old_sup,
            "new_supplier": new_sup,
        })
        rows[i]["supplier_id"] = new_sup

    # Apply combined unit_cost + supplier_id changes
    for i in type2_both_idx:
        old_cost = rows[i]["unit_cost"]
        old_sup = rows[i]["supplier_id"]
        new_cost = round(old_cost * 1.08, 4)  # +8%
        new_sup_num = rng.randint(100, 130)
        new_sup = f"SUP{new_sup_num:03d}"
        while new_sup == old_sup:
            new_sup_num = rng.randint(100, 130)
            new_sup = f"SUP{new_sup_num:03d}"
        change_log["type2_both_changes"].append({
            "sku":          rows[i]["sku"],
            "old_cost":     old_cost,
            "new_cost":     new_cost,
            "old_supplier": old_sup,
            "new_supplier": new_sup,
        })
        rows[i]["unit_cost"]    = new_cost
        rows[i]["supplier_id"] = new_sup

    # Apply Type 1 uom changes (these should overwrite the existing row,
    # NOT create a new historical row, because uom is in type_1_attributes)
    for i in type1_uom_idx:
        change_log["type1_uom_changes"].append({
            "sku":    rows[i]["sku"],
            "old_uom": rows[i]["uom"],
            "new_uom": "carton",
        })
        rows[i]["uom"] = "carton"

    # Discontinued SKUs — REMOVE from day 2 cost master
    discontinued_skus = [rows[i]["sku"] for i in sorted(discontinued_idx, reverse=True)]
    for sku in discontinued_skus:
        change_log["discontinued_skus"].append(sku)

    # The orphan SKU (last day-1 SKU) — we'll mark it as discontinued too,
    # but later we'll deliberately include a transaction for it on day 2.
    # That transaction will load with cost_key=-1 because no current cost row exists.
    orphan_sku = rows[ORPHAN_SKU_INDEX]["sku"]
    change_log["orphan_sku_referenced_in_day2_txn"] = orphan_sku
    if orphan_sku not in change_log["discontinued_skus"]:
        change_log["discontinued_skus"].append(orphan_sku)

    # Now actually remove discontinued SKUs from day 2 cost master
    rows = [r for r in rows if r["sku"] not in change_log["discontinued_skus"]]

    # Add new SKUs (not present on day 1 at all)
    for n in range(NUM_NEW_SKUS):
        new_sku = f"SKU{99000 + n:05d}"
        new_supplier = f"SUP{rng.randint(100, 130):03d}"
        new_cost = round(rng.uniform(50, 800), 4)
        rows.append({
            "sku":         new_sku,
            "supplier_id": new_supplier,
            "unit_cost":   new_cost,
            "currency":    "NZD",
            "uom":         "each",
            "extracted_at": "2026-04-02 02:00:00",
        })
        change_log["new_skus"].append({"sku": new_sku, "unit_cost": new_cost})

    # All day 2 rows get a fresh extracted_at timestamp
    for r in rows:
        r["extracted_at"] = "2026-04-02 02:00:00"

    return rows, change_log


def write_cost_master(out_dir, cost_rows):
    with open(out_dir / "cost_master.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(
            f, fieldnames=["sku", "supplier_id", "unit_cost", "currency", "uom", "extracted_at"]
        )
        w.writeheader()
        w.writerows(cost_rows)


# ============================================================================
# Step 3 — Transactions per day
# ============================================================================

def generate_transactions(active_skus, day_date_str, txn_id_offset, num_transactions, seed):
    """Generate one day's transactions. Active SKUs limits to those
    available on this day (so day 2 doesn't reference discontinued SKUs)."""
    rng = random.Random(seed)
    transactions = []
    for n in range(num_transactions):
        sku_data = rng.choice(active_skus)
        store_id, *_ = rng.choice(SELLING_STORES)

        # Quantity: 80% are 1 unit, 12% are 2, 5% are 3, 3% are returns
        qty = rng.choices([1, 2, 3, -1], weights=[80, 12, 5, 3])[0]

        unit_price = sku_data["list_price"]

        # Deliberate dirty-data patterns (carry over from v1)
        sku_dirty = sku_data["sku"]
        if rng.random() < 0.05:
            sku_dirty = f" {sku_dirty} "
        if rng.random() < 0.03:
            sku_dirty = sku_dirty.lower()

        # Mixed date formats — 10% NZ-style
        if rng.random() < 0.10:
            date_str = datetime.strptime(day_date_str, "%Y-%m-%d").strftime("%d/%m/%Y")
        else:
            date_str = day_date_str

        customer_id = None if rng.random() < 0.30 else f"CUST{rng.randint(10000, 19999)}"

        transactions.append({
            "transaction_id":   f"TXN{txn_id_offset + n:08d}",
            "transaction_date": date_str,
            "store_id":         store_id,
            "sku":              sku_dirty,
            "quantity":         qty,
            "unit_price":       unit_price,
            "total_amount":     round(qty * unit_price, 2),
            "customer_id":      customer_id,
            "payment_method":   rng.choice(["Card", "EFTPOS", "Online", "AfterPay", "Finance"]),
        })

    # ~0.5% duplicate transactions (real ERP extracts retry-double-publish)
    num_dups = max(1, int(num_transactions * 0.005))
    duplicates = rng.sample(transactions, num_dups)
    transactions.extend(duplicates)

    rng.shuffle(transactions)
    return transactions


def write_transactions(out_dir, transactions):
    with open(out_dir / "transactions.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=[
            "transaction_id", "transaction_date", "store_id", "sku",
            "quantity", "unit_price", "total_amount", "customer_id", "payment_method",
        ])
        w.writeheader()
        w.writerows(transactions)


# ============================================================================
# Step 4 — Inventory movements per day
# ============================================================================

def store_to_warehouse(store_id):
    """Map a selling store to its home warehouse."""
    for sid, _, _, _, is_wh, home_wh in STORE_DEFINITIONS:
        if sid == store_id:
            return home_wh
    return "WH001"  # fallback


def generate_initial_stock_receipts(products, seed):
    """Day 1 morning: every (active_sku, warehouse) gets an initial stock
    receipt large enough to handle a month of sales without going negative.
    These are the 'opening balance' for the simulation."""
    rng = random.Random(seed)
    movements = []
    movement_id_counter = 1
    receipt_date = "2026-04-01"  # morning of day 1
    for p in products:
        for wh in WAREHOUSE_IDS:
            initial_qty = rng.randint(50, 200)
            movements.append({
                "movement_id":   f"MOV{movement_id_counter:08d}",
                "movement_date": receipt_date,
                "sku":           p["sku"],
                "warehouse_id":  wh,
                "movement_type": "receipt",
                "quantity":      initial_qty,
                "reference_id":  f"PO_INIT_{wh}",
                "notes":         "Initial stock-up",
            })
            movement_id_counter += 1
    return movements, movement_id_counter


def generate_movements_for_day(transactions, mov_id_start, day_date_str, seed,
                               include_drift=False, drift_sku=None, drift_warehouse=None):
    """For each transaction generate a corresponding sale_out movement.
    Plus inject some receipts and adjustments to keep the data interesting."""
    rng = random.Random(seed)
    movements = []
    counter = mov_id_start

    # Sale_outs matching transactions
    for txn in transactions:
        # Resolve warehouse from store
        wh = store_to_warehouse(txn["store_id"])
        # Sale_out has negative qty (sign convention)
        sale_qty = -abs(txn["quantity"]) if txn["quantity"] > 0 else abs(txn["quantity"])
        # Note: returns (txn.quantity = -1) become positive movement (stock comes back)

        # Clean the SKU (transactions may have dirty whitespace/case)
        clean_sku = txn["sku"].strip().upper()

        movements.append({
            "movement_id":   f"MOV{counter:08d}",
            "movement_date": day_date_str,
            "sku":           clean_sku,
            "warehouse_id":  wh,
            "movement_type": "sale_out" if txn["quantity"] > 0 else "receipt",
            # If returns, quantity is positive (stock returning to warehouse)
            "quantity":      sale_qty,
            "reference_id":  txn["transaction_id"],
            "notes":         "" if txn["quantity"] > 0 else "Customer return",
        })
        counter += 1

    # Add a few receipts during the day (mid-day restocks, ~10 events)
    sample_skus_for_receipts = rng.sample(
        list({txn["sku"].strip().upper() for txn in transactions}),
        min(10, len({txn["sku"].strip().upper() for txn in transactions}))
    )
    for sku in sample_skus_for_receipts:
        wh = rng.choice(WAREHOUSE_IDS)
        qty = rng.randint(20, 100)
        movements.append({
            "movement_id":   f"MOV{counter:08d}",
            "movement_date": day_date_str,
            "sku":           sku,
            "warehouse_id":  wh,
            "movement_type": "receipt",
            "quantity":      qty,
            "reference_id":  f"PO{rng.randint(10000, 99999)}",
            "notes":         "Restock",
        })
        counter += 1

    # 2-3 adjustments (manual write-down or count corrections)
    for _ in range(3):
        sku = rng.choice(list({txn["sku"].strip().upper() for txn in transactions}))
        wh = rng.choice(WAREHOUSE_IDS)
        adj_qty = rng.choice([-2, -1, -1, 1, 2])  # mostly negative (write-downs)
        movements.append({
            "movement_id":   f"MOV{counter:08d}",
            "movement_date": day_date_str,
            "sku":           sku,
            "warehouse_id":  wh,
            "movement_type": "adjustment",
            "quantity":      adj_qty,
            "reference_id":  f"ADJ{rng.randint(1000, 9999)}",
            "notes":         "Cycle count adjustment",
        })
        counter += 1

    # The "missing movements" drift case — ONE sku at ONE warehouse
    # has its sale_outs deliberately under-reported on the day. This
    # creates a known reconciliation alert for the demo.
    if include_drift and drift_sku and drift_warehouse:
        # Inject 50 units of "phantom" sale_outs that don't appear in movements
        # but DO appear in the ERP snapshot's reported_qty as missing stock.
        # We don't add movements here — the absence creates the variance.
        pass

    return movements, counter


def write_movements(out_dir, movements):
    with open(out_dir / "inv_movements.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=[
            "movement_id", "movement_date", "sku", "warehouse_id",
            "movement_type", "quantity", "reference_id", "notes",
        ])
        w.writeheader()
        w.writerows(movements)


# ============================================================================
# Step 5 — Inventory snapshot (ERP-reported stock for reconciliation)
# ============================================================================

def compute_derived_stock(all_movements_to_date):
    """Given all movements up to and including a date, compute the
    end-of-day stock at every (sku, warehouse). Used to seed the
    snapshot, then deliberately perturb it for reconciliation drift."""
    stock = defaultdict(int)
    for m in all_movements_to_date:
        stock[(m["sku"], m["warehouse_id"])] += m["quantity"]
    return dict(stock)


def generate_snapshot(stock, snapshot_date, seed,
                     drift_sku=None, drift_warehouse=None, drift_amount=50):
    """Build the ERP snapshot. Mostly mirrors derived stock, with:
      - 5% of cells off by ±1 to ±4 units (small drift, won't trip alerts)
      - 2% of cells off by ±6 to ±15 units (will trip alert threshold)
      - ONE specific (drift_sku, drift_warehouse) off by drift_amount (demo)
      - 3% of cells missing entirely (no snapshot — alert reason 'no_erp_snapshot')
    """
    rng = random.Random(seed)
    rows = []
    for (sku, warehouse_id), derived_qty in sorted(stock.items()):
        roll = rng.random()
        if roll < 0.03:
            # Skip — no snapshot for this cell
            continue

        if drift_sku and drift_warehouse and sku == drift_sku and warehouse_id == drift_warehouse:
            # The dramatic demo drift case
            reported = derived_qty - drift_amount
        elif roll < 0.05:
            # Big drift (will trigger alerts)
            reported = derived_qty + rng.choice([-15, -10, -8, 8, 10, 12])
        elif roll < 0.10:
            # Small drift (won't trigger thresholds)
            reported = derived_qty + rng.choice([-3, -2, -1, 1, 2, 3])
        else:
            # No drift
            reported = derived_qty

        rows.append({
            "snapshot_date":     snapshot_date,
            "sku":               sku,
            "warehouse_id":      warehouse_id,
            "reported_qty":      reported,
            "snapshot_source":   "ERP",
            "snapshot_taken_at": f"{snapshot_date} 23:55:00",
        })
    return rows


def write_snapshot(out_dir, rows):
    with open(out_dir / "inventory_snapshot.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=[
            "snapshot_date", "sku", "warehouse_id", "reported_qty",
            "snapshot_source", "snapshot_taken_at",
        ])
        w.writeheader()
        w.writerows(rows)


# ============================================================================
# Main orchestration
# ============================================================================

def main():
    print("=" * 60)
    print("PB Tech pipeline data simulator")
    print("=" * 60)

    # Step 1: shared catalog
    print("\n[1/9] Generating product catalog (~250 SKUs)...")
    products = generate_products()
    print(f"      {len(products)} products generated")

    # Stores and products are identical between days — write to both
    for d in (DAY1_DIR, DAY2_DIR):
        write_stores(d)
        write_products(d, products)

    # Step 2a: day 1 cost master
    print("\n[2/9] Generating Day 1 cost master...")
    day1_costs = generate_cost_master_day1(products)
    write_cost_master(DAY1_DIR, day1_costs)
    print(f"      {len(day1_costs)} cost rows for day 1")

    # Step 2b: day 2 cost master with the change mix
    print("\n[3/9] Generating Day 2 cost master with SCD2 change mix...")
    day2_costs, change_log = generate_cost_master_day2(day1_costs)
    write_cost_master(DAY2_DIR, day2_costs)
    with open(DAY2_DIR / "expected_changes.json", "w") as f:
        json.dump(change_log, f, indent=2)
    print(f"      {len(day2_costs)} cost rows for day 2")
    print(f"      {len(change_log['type2_unit_cost_changes'])} type2 unit_cost changes")
    print(f"      {len(change_log['type2_supplier_changes'])} type2 supplier changes")
    print(f"      {len(change_log['type2_both_changes'])} type2 both changes")
    print(f"      {len(change_log['type1_uom_changes'])} type1 uom changes")
    print(f"      {len(change_log['discontinued_skus'])} discontinued")
    print(f"      {len(change_log['new_skus'])} new")
    print(f"      orphan SKU on day 2: {change_log['orphan_sku_referenced_in_day2_txn']}")

    # Step 3a: day 1 transactions
    print("\n[4/9] Generating Day 1 transactions...")
    day1_active_skus = [p for p in products]  # all SKUs available on day 1
    day1_txns = generate_transactions(
        active_skus=day1_active_skus,
        day_date_str="2026-04-01",
        txn_id_offset=1,
        num_transactions=600,
        seed=101,
    )
    write_transactions(DAY1_DIR, day1_txns)
    print(f"      {len(day1_txns):,} transactions for day 1 (incl. dups)")

    # Step 3b: day 2 transactions — restricted to day-2-active SKUs,
    # PLUS one transaction deliberately referencing the orphan SKU
    print("\n[5/9] Generating Day 2 transactions...")
    day2_active_sku_set = {r["sku"] for r in day2_costs}
    day2_active_products = [p for p in products if p["sku"] in day2_active_sku_set]
    # Plus the new SKUs introduced on day 2
    new_sku_names = [n["sku"] for n in change_log["new_skus"]]
    for sku_name in new_sku_names:
        cost_row = next(r for r in day2_costs if r["sku"] == sku_name)
        day2_active_products.append({
            "sku": sku_name,
            "list_price": round(cost_row["unit_cost"] / 0.65, 2),  # ~35% margin
            "active": True,
        })

    day2_txns = generate_transactions(
        active_skus=day2_active_products,
        day_date_str="2026-04-02",
        txn_id_offset=10001,  # different range so no ID overlap
        num_transactions=600,
        seed=202,
    )

    # Inject the orphan transaction — references a discontinued SKU
    orphan_sku = change_log["orphan_sku_referenced_in_day2_txn"]
    orphan_txn = {
        "transaction_id":   "TXN_ORPHAN_001",
        "transaction_date": "2026-04-02",
        "store_id":         "ST001",
        "sku":              orphan_sku,
        "quantity":         1,
        "unit_price":       999.95,
        "total_amount":     999.95,
        "customer_id":      "CUST15000",
        "payment_method":   "Card",
    }
    day2_txns.append(orphan_txn)
    write_transactions(DAY2_DIR, day2_txns)
    print(f"      {len(day2_txns):,} transactions for day 2")
    print(f"      includes 1 deliberate orphan transaction (sku={orphan_sku})")

    # Step 4a: day 1 inventory movements (initial stock + sale_outs + receipts)
    print("\n[6/9] Generating Day 1 inventory movements...")
    initial_movements, mov_id_counter = generate_initial_stock_receipts(products, seed=300)
    day1_sale_movements, mov_id_counter = generate_movements_for_day(
        day1_txns,
        mov_id_start=mov_id_counter,
        day_date_str="2026-04-01",
        seed=301,
    )
    day1_all_movements = initial_movements + day1_sale_movements
    write_movements(DAY1_DIR, day1_all_movements)
    print(f"      {len(day1_all_movements):,} movements (incl. {len(initial_movements):,} initial)")

    # Step 4b: day 2 inventory movements (sale_outs + receipts only — no
    # initial stock, that was day 1)
    print("\n[7/9] Generating Day 2 inventory movements...")
    day2_movements, _ = generate_movements_for_day(
        day2_txns,
        mov_id_start=mov_id_counter,
        day_date_str="2026-04-02",
        seed=302,
    )
    write_movements(DAY2_DIR, day2_movements)
    print(f"      {len(day2_movements):,} movements for day 2")

    # Step 5a: day 1 ERP snapshot — reconciliation against day 1 derived stock
    print("\n[8/9] Generating Day 1 ERP inventory snapshot...")
    # The dramatic drift case: pick a known SKU and warehouse, off by 50
    drift_sku = day1_costs[0]["sku"]  # first SKU
    drift_wh = "WH001"
    derived_stock_day1 = compute_derived_stock(day1_all_movements)
    day1_snapshot = generate_snapshot(
        derived_stock_day1, "2026-04-01", seed=400,
        drift_sku=drift_sku, drift_warehouse=drift_wh, drift_amount=50,
    )
    write_snapshot(DAY1_DIR, day1_snapshot)
    print(f"      {len(day1_snapshot):,} snapshot rows for day 1")
    print(f"      dramatic drift: sku={drift_sku} at {drift_wh} off by 50 units")

    # Step 5b: day 2 ERP snapshot
    print("\n[9/9] Generating Day 2 ERP inventory snapshot...")
    derived_stock_day2 = compute_derived_stock(day1_all_movements + day2_movements)
    day2_snapshot = generate_snapshot(
        derived_stock_day2, "2026-04-02", seed=401,
        drift_sku=drift_sku, drift_warehouse=drift_wh, drift_amount=50,
    )
    write_snapshot(DAY2_DIR, day2_snapshot)
    print(f"      {len(day2_snapshot):,} snapshot rows for day 2")

    # Verification summary
    print("\n" + "=" * 60)
    print("Generation complete. Summary:")
    print("=" * 60)
    print(f"\nday1/  ({DAY1_DIR})")
    for f in sorted(DAY1_DIR.iterdir()):
        if f.is_file():
            print(f"  {f.name:30s}  {f.stat().st_size:>10,d} bytes")
    print(f"\nday2/  ({DAY2_DIR})")
    for f in sorted(DAY2_DIR.iterdir()):
        if f.is_file():
            print(f"  {f.name:30s}  {f.stat().st_size:>10,d} bytes")

    print("\nNext step: copy day1 files into data/raw/ before triggering the DAG.")
    print("  ./scripts/switch_to_day.sh 1")
    print("\nThen for the SCD2 demo, switch to day 2 and re-trigger:")
    print("  ./scripts/switch_to_day.sh 2")


if __name__ == "__main__":
    main()
