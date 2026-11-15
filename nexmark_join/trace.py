"""Seeded exponential arrivals. Six independent streams, five events per second."""

from __future__ import annotations

import random

NAMESPACES = 6
RATE = 5.0
MEASURE_S = 60.0
WARMUP_S = 10.0
SEED = 20261115


def generate(seed: int = SEED, namespaces: int = NAMESPACES, rate: float = RATE, warmup_s: float = WARMUP_S, measure_s: float = MEASURE_S) -> dict:
    rows = []
    horizon = warmup_s + measure_s
    for index in range(namespaces):
        rng = random.Random(seed + index)
        clock = 0.0
        persons = 0
        auctions = 0
        seq = 0
        while True:
            clock += rng.expovariate(rate)
            if clock >= horizon:
                break
            if persons == 0 or rng.random() < 0.5:
                kind = "person"
                person_id = f"p{index}-{persons}"
                event = {"kind": kind, "id": person_id, "name": f"person-{person_id}", "ts": int(clock * 1000)}
                persons += 1
            else:
                kind = "auction"
                seller = f"p{index}-{rng.randrange(persons)}"
                event = {
                    "kind": kind,
                    "id": f"a{index}-{auctions}",
                    "seller": seller,
                    "reserve": 100 + auctions,
                    "ts": int(clock * 1000),
                }
                auctions += 1
            rows.append({
                "namespace": index,
                "seq": seq,
                "predecessor": None if seq == 0 else seq - 1,
                "scheduled_s": clock,
                "warmup": clock < warmup_s,
                "event": event,
            })
            seq += 1
    rows.sort(key=lambda row: (row["scheduled_s"], row["namespace"], row["seq"]))
    return {
        "seed": seed,
        "namespaces": namespaces,
        "rate_per_namespace": rate,
        "warmup_s": warmup_s,
        "measure_s": measure_s,
        "rows": rows,
    }
