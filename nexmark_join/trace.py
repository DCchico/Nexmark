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
        "semantic_rate_per_namespace": rate,
        "warmup_s": warmup_s,
        "measure_s": measure_s,
        "rows": rows,
    }


def generate_load(
    seed: int = SEED,
    namespaces: int = NAMESPACES,
    wall_rate: float = RATE,
    semantic_rate: float = RATE,
    warmup_s: float = WARMUP_S,
    measure_s: float = MEASURE_S,
) -> dict:
    """Wall-clock release and event time use separate clocks.

    Event timestamps always advance at semantic_rate, so a faster wall-clock
    delivery does not compress the 12-hour window. The k-th event in a
    namespace is the same for every wall rate.
    """
    rows = []
    horizon = warmup_s + measure_s
    for index in range(namespaces):
        semantic = random.Random(seed + index)
        wall = random.Random(seed + 10_000 + index)
        event_clock = 0.0
        wall_clock = 0.0
        persons = 0
        auctions = 0
        seq = 0
        while True:
            wall_clock += wall.expovariate(wall_rate)
            if wall_clock >= horizon:
                break
            event_clock += semantic.expovariate(semantic_rate)
            if persons == 0 or semantic.random() < 0.5:
                person_id = f"p{index}-{persons}"
                event = {"kind": "person", "id": person_id, "name": f"person-{person_id}", "ts": int(event_clock * 1000)}
                persons += 1
            else:
                seller = f"p{index}-{semantic.randrange(persons)}"
                event = {
                    "kind": "auction",
                    "id": f"a{index}-{auctions}",
                    "seller": seller,
                    "reserve": 100 + auctions,
                    "ts": int(event_clock * 1000),
                }
                auctions += 1
            rows.append({
                "namespace": index,
                "seq": seq,
                "predecessor": None if seq == 0 else seq - 1,
                "scheduled_s": wall_clock,
                "event_s": event_clock,
                "warmup": wall_clock < warmup_s,
                "event": event,
            })
            seq += 1
    rows.sort(key=lambda row: (row["scheduled_s"], row["namespace"], row["seq"]))
    return {
        "seed": seed,
        "namespaces": namespaces,
        "rate_per_namespace": wall_rate,
        "semantic_rate_per_namespace": semantic_rate,
        "warmup_s": warmup_s,
        "measure_s": measure_s,
        "rows": rows,
    }
