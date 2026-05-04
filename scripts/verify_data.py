"""
Phase 2 verification — check internal consistency of the generated data.

Run after `python scripts/generate_data.py` to confirm the output is valid.
"""

import csv
import json
from collections import Counter, defaultdict
from pathlib import Path

ROOT = Path(__file__).parent / "sim_data"
PASS = []
FAIL = []


def check(condition, label):
    (PASS if condition else FAIL).append(label)
    print(f"  {'✓' if condition else '✗'}  {label}")


def load_csv(path):
    with open(path) as f:
        return list(csv.DictReader(f))


def verify_day(day_dir):
    print(f"\n{'='*60}\nVerifying {day_dir.name}/\n{'='*60}")
    txns = load_csv(day_dir / "transactions.csv")
    movements = load_csv(day_dir / "inv_movements.csv")
    cost_master = load_csv(day_dir / "cost_master.csv")
    snapshot = load_csv(day_dir / "inventory_snapshot.csv")

    cost_skus = {r["sku"] for r in cost_master}

    # Check 1: every transaction's SKU (cleaned) is in cost master
    # OR is the orphan SKU on day 2
    expected_orphan = None
    if (day_dir / "expected_changes.json").exists():
        with open(day_dir / "expected_changes.json") as f:
            change_log = json.load(f)
            expected_orphan = change_log.get("orphan_sku_referenced_in_day2_txn")

    txn_skus = {t["sku"].strip().upper() for t in txns}
    missing_skus = txn_skus - cost_skus
    if expected_orphan:
        missing_skus.discard(expected_orphan)
    check(
        len(missing_skus) == 0,
        f"All transaction SKUs present in cost master "
        f"(found {len(missing_skus)} unexpected orphans: {list(missing_skus)[:5]})"
    )

    # Check 2: every transaction has a matching sale_out movement
    txn_ids = {t["transaction_id"] for t in txns}
    sale_movement_refs = {
        m["reference_id"] for m in movements
        if m["movement_type"] in ("sale_out", "receipt") and m["reference_id"].startswith("TXN")
    }
    unmatched_txns = txn_ids - sale_movement_refs
    # Note: returns become "receipt" movements, sales become "sale_out" — both valid
    # Duplicates won't have a movement (same ref_id collapsed)
    # Allow up to ~5 unmatched (the duplicates)
    check(
        len(unmatched_txns) <= 10,
        f"Transactions matched to movements (unmatched: {len(unmatched_txns)}, "
        f"expected ~3-5 from duplicates)"
    )

    # Check 3: movement quantity sign convention
    sale_outs_with_pos_qty = [
        m for m in movements
        if m["movement_type"] == "sale_out" and int(m["quantity"]) > 0
    ]
    check(
        len(sale_outs_with_pos_qty) == 0,
        f"All sale_out movements have negative quantity "
        f"(violations: {len(sale_outs_with_pos_qty)})"
    )

    receipts_with_neg_qty = [
        m for m in movements
        if m["movement_type"] == "receipt" and int(m["quantity"]) < 0
    ]
    check(
        len(receipts_with_neg_qty) == 0,
        f"All receipt movements have non-negative quantity "
        f"(violations: {len(receipts_with_neg_qty)})"
    )

    # Check 4: cost master has unique SKUs
    sku_counts = Counter(r["sku"] for r in cost_master)
    duplicates = [s for s, c in sku_counts.items() if c > 1]
    check(
        len(duplicates) == 0,
        f"Cost master has unique SKUs (duplicates: {duplicates})"
    )

    # Check 5: snapshot SKUs and warehouses are valid
    valid_warehouses = {"WH001", "WH002", "WH003", "WH004"}
    bad_warehouses = [r for r in snapshot if r["warehouse_id"] not in valid_warehouses]
    check(
        len(bad_warehouses) == 0,
        f"Snapshot warehouses are valid (bad: {len(bad_warehouses)})"
    )

    # Check 6: Day 2 specifically — verify orphan transaction exists
    if expected_orphan:
        orphan_txns = [t for t in txns if t["sku"] == expected_orphan]
        check(
            len(orphan_txns) >= 1,
            f"Day 2 has the deliberate orphan transaction (sku={expected_orphan}, "
            f"count={len(orphan_txns)})"
        )

    # Check 7: cost values are positive
    bad_costs = [r for r in cost_master if float(r["unit_cost"]) <= 0]
    check(
        len(bad_costs) == 0,
        f"All cost values are positive (violations: {len(bad_costs)})"
    )


def verify_day2_changes():
    """Verify the day 2 expected_changes.json matches what's actually in
    the day 2 cost_master vs day 1."""
    print(f"\n{'='*60}\nVerifying SCD2 change mix between day 1 and day 2\n{'='*60}")
    day1_costs = {r["sku"]: r for r in load_csv(ROOT / "day1" / "cost_master.csv")}
    day2_costs = {r["sku"]: r for r in load_csv(ROOT / "day2" / "cost_master.csv")}
    with open(ROOT / "day2" / "expected_changes.json") as f:
        log = json.load(f)

    # Type2 unit_cost changes
    actual_cost_changes = 0
    for change in log["type2_unit_cost_changes"]:
        sku = change["sku"]
        if sku in day1_costs and sku in day2_costs:
            if day1_costs[sku]["unit_cost"] != day2_costs[sku]["unit_cost"]:
                actual_cost_changes += 1
    check(
        actual_cost_changes == len(log["type2_unit_cost_changes"]),
        f"All declared type2 unit_cost changes are real "
        f"(declared {len(log['type2_unit_cost_changes'])}, actual {actual_cost_changes})"
    )

    # Discontinued SKUs
    actual_discontinued = [s for s in log["discontinued_skus"] if s not in day2_costs]
    check(
        len(actual_discontinued) == len(log["discontinued_skus"]),
        f"All declared discontinued SKUs are absent from day 2 cost master"
    )

    # New SKUs
    actual_new = [n["sku"] for n in log["new_skus"]
                  if n["sku"] in day2_costs and n["sku"] not in day1_costs]
    check(
        len(actual_new) == len(log["new_skus"]),
        f"All declared new SKUs appear in day 2 but not day 1"
    )


verify_day(ROOT / "day1")
verify_day(ROOT / "day2")
verify_day2_changes()

print(f"\n{'='*60}\n{len(PASS)} checks passed, {len(FAIL)} failed\n{'='*60}")
exit(0 if not FAIL else 1)
