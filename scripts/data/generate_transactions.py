#!/usr/bin/env python3
import json
import random
import uuid
import argparse
from datetime import datetime, timedelta
from pathlib import Path

def generate(seed: int, rows: int, out_dir: str, duplicate_ratio: float = 0.02, late_arrival_ratio: float = 0.05):
    random.seed(seed)
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)

    base_time = datetime.utcnow() - timedelta(days=3)
    transactions = []
    ids_used = []

    for i in range(rows):
        tid = str(uuid.uuid4())
        ids_used.append(tid)
        event_ts = base_time + timedelta(seconds=random.randint(0, 86400))
        customer_id = f"C{random.randint(1, int(rows * 0.1))}"  # smaller dimension space
        amount = round(random.uniform(5.0, 500.0), 2)
        transactions.append({
            "transaction_id": tid,
            "customer_id": customer_id,
            "event_timestamp": event_ts.isoformat(),
            "amount": amount,
            "currency": "EUR"
        })

    # Duplicates (later timestamp overwrites)
    dup_count = int(rows * duplicate_ratio)
    for _ in range(dup_count):
        original = random.choice(transactions)
        new_event_time = datetime.fromisoformat(original["event_timestamp"]) + timedelta(minutes=random.randint(1,120))
        transactions.append({
            **original,
            "event_timestamp": new_event_time.isoformat()
        })

    # Late arrivals (future timestamp)
    late_count = int(rows * late_arrival_ratio)
    for _ in range(late_count):
        original = random.choice(transactions)
        future_time = datetime.utcnow() + timedelta(minutes=random.randint(1, 60))
        transactions.append({
            **original,
            "event_timestamp": future_time.isoformat()
        })

    # Write JSON lines
    file_path = out / "transactions.json"
    with file_path.open("w") as f:
        for t in transactions:
            f.write(json.dumps(t) + "\n")

    print(f"Generated {len(transactions)} records (base={rows}, duplicates={dup_count}, late={late_count}) at {file_path}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--rows", type=int, default=1000)
    parser.add_argument("--out-dir", type=str, default="data/raw_seed")
    parser.add_argument("--duplicate-ratio", type=float, default=0.02)
    parser.add_argument("--late-arrival-ratio", type=float, default=0.05)
    args = parser.parse_args()
    generate(**vars(args))