#!/usr/bin/env python3
"""Summarize one Beam Q8 campaign directory and redraw its figures.

The headline is the unweighted mean of the three repetition means.
Percentiles use the nearest-rank method on that repetition's measured joins.
"""

from __future__ import annotations

import json
import math
from collections import Counter
from pathlib import Path

POLICIES = [
    ("rr", "RR", "Vanilla RR / Redis object-state"),
    ("faast", "Faast", "Faa$T-style"),
    ("cloudburst", "Cloudburst", "Cloudburst-style"),
    ("datacentric", "Data-centric", "Pheromone-style"),
    ("stateflow", "Stateflow", "STateFlow"),
]
COLORS = {
    "rr": "#4c78a8",
    "faast": "#f58518",
    "cloudburst": "#54a24b",
    "datacentric": "#e45756",
    "data-centric": "#e45756",
    "stateflow": "#72b7b2",
}
ROOT = Path("/proj/faasedge-PG0/bootstrap-20261114-DCchico-325947")
CAMPAIGN = ROOT / "nexmark-beam-q8-20261115"
OLDER = {
    "BFS apply": ROOT / "figures-20261114/bfs-apply-summary-20261114.json",
    "SocialNet update_home": ROOT / "figures-20261114/social-update-home-summary-20261114.json",
    "FEMNIST apply_update": ROOT / "figures-20261114/fedagg-apply-summary-20261115.json",
}


def percentile(values: list[float], p: float) -> float:
    ordered = sorted(values)
    rank = max(0, math.ceil(p / 100.0 * len(ordered)) - 1)
    return ordered[rank]


def mean(values: list[float]) -> float:
    return sum(values) / len(values)


def component(summary: dict, key: str) -> float:
    block = summary["mean"]
    aliases = {
        "e2e_s": ("e2e_s",),
        "orchestration_s": ("orchestration_s", "O"),
        "compute_s": ("compute_s", "C"),
        "state_s": ("state_s", "S"),
        "stall_s": ("stall_s", "W"),
    }
    for name in aliases[key]:
        if name in block:
            return block[name]
    raise KeyError(key)


def load_cell(path: Path) -> dict:
    data = json.loads(path.read_text())
    if not data.get("complete"):
        raise SystemExit(f"incomplete cell {path}")
    rows = data["records"]
    residual = max(abs(row["e2e_s"] - (row["orchestration_s"] + row["compute_s"] + row["state_s"] + row["stall_s"])) for row in rows)
    if residual > 1e-4:
        raise SystemExit(f"timing residual {residual} in {path}")
    return data


def summarize() -> dict:
    policies = []
    for slug, policy, label in POLICIES:
        repetitions = []
        for rep in range(3):
            data = load_cell(CAMPAIGN / f"r{rep}" / slug / "result.json")
            rows = data["records"]
            repetitions.append({
                "run_id": data["run_id"],
                "n": len(rows),
                "wall_s": data["wall_s"],
                "e2e_s": mean([row["e2e_s"] for row in rows]),
                "orchestration_s": mean([row["orchestration_s"] for row in rows]),
                "compute_s": mean([row["compute_s"] for row in rows]),
                "state_s": mean([row["state_s"] for row in rows]),
                "stall_s": mean([row["stall_s"] for row in rows]),
                "driver_queue_s": mean([row["driver_queue_s"] for row in rows]),
                "http_s": mean([row["http_s"] for row in rows]),
                "publish_bytes": mean([row["publish_bytes"] for row in rows]),
                "state_bytes": mean([row["state_bytes"] for row in rows]),
                "p50": percentile([row["e2e_s"] for row in rows], 50),
                "p95": percentile([row["e2e_s"] for row in rows], 95),
                "p99": percentile([row["e2e_s"] for row in rows], 99),
                "access": dict(Counter(row["access"] for row in rows)),
                "direct_zero_publish": sum(1 for row in rows if row["access"] == "direct" and row["publish_bytes"] == 0),
            })
        policies.append({
            "id": slug,
            "policy": policy,
            "label": label,
            "repetitions": repetitions,
            "mean": {key: mean([item[key] for item in repetitions]) for key in (
                "n", "wall_s", "e2e_s", "orchestration_s", "compute_s", "state_s", "stall_s",
                "driver_queue_s", "http_s", "publish_bytes", "state_bytes", "p50", "p95", "p99",
            )},
        })
    resources = parse_resources(CAMPAIGN / "resources.txt")
    summary = {
        "query": "Apache Beam Nexmark Q8 person-seller window join",
        "operation": "join_update",
        "timing_schema": "stateful-e2e-v1",
        "aggregation": "unweighted mean of three repetition means; each repetition has the same measured join count",
        "percentile": "nearest rank within a repetition; the reported percentile is the mean of the three repetition percentiles",
        "seed": 20261115,
        "namespaces": 6,
        "rate_per_namespace": 5.0,
        "measure_s": 60,
        "warmup_s": 10,
        "window_s": 12 * 3600,
        "policies": policies,
        "resources": resources,
        "attempt_ledger": [
            {
                "directory": "nexmark-beam-q8-20261115-attempt1-stateflow-500",
                "image": "docker.io/dcchico/nexmark-q8:beam-q8-metrics-20261115",
                "kept": False,
                "reason": "Stateflow returned 409/500 after a direct update left shared memory stale. Four earlier cells on that image were not reused.",
            }
        ],
    }
    return summary


def parse_resources(path: Path) -> dict:
    if not path.exists():
        return {"present": False}
    cpu_max = set()
    cpuset = set()
    partition = set()
    memory_max = set()
    current = []
    for line in path.read_text().splitlines():
        if line.startswith("cpu.max "):
            cpu_max.add(line.split(" ", 1)[1])
        elif line.startswith("cpuset.cpus "):
            cpuset.add(line.split(" ", 1)[1])
        elif line.startswith("cpuset.cpus.partition "):
            partition.add(line.split(" ", 1)[1])
        elif line.startswith("memory.max "):
            memory_max.add(line.split(" ", 1)[1])
        elif line.startswith("memory.current "):
            current.append(int(line.split()[1]))
    return {
        "present": True,
        "cpu_max": sorted(cpu_max),
        "cpuset_cpus": sorted(cpuset),
        "cpuset_partition": sorted(partition),
        "memory_max": sorted(memory_max),
        "memory_current_max": max(current) if current else None,
        "memory_current_min": min(current) if current else None,
        "samples": len(current),
    }


def bar_svg(title: str, series: list[tuple[str, str, float]], unit: str, width: int = 720, height: int = 420) -> str:
    left, right, top, bottom = 70, 20, 50, 70
    plot_w = width - left - right
    plot_h = height - top - bottom
    peak = max(value for _slug, _label, value in series) or 1
    peak = peak * 1.15
    parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">',
        '<rect width="100%" height="100%" fill="white"/>',
        f'<text x="{width/2}" y="28" text-anchor="middle" font-family="sans-serif" font-size="16">{title}</text>',
        f'<text x="16" y="{top + plot_h/2}" transform="rotate(-90 16,{top + plot_h/2})" text-anchor="middle" font-family="sans-serif" font-size="12">{unit}</text>',
    ]
    slot = plot_w / len(series)
    for index, (slug, label, value) in enumerate(series):
        bar_h = plot_h * value / peak
        x = left + index * slot + slot * 0.2
        y = top + plot_h - bar_h
        parts.append(f'<rect x="{x:.1f}" y="{y:.1f}" width="{slot*0.6:.1f}" height="{bar_h:.1f}" fill="{COLORS[slug]}"/>')
        parts.append(f'<text x="{x + slot*0.3:.1f}" y="{y - 6:.1f}" text-anchor="middle" font-family="sans-serif" font-size="11">{value:.2f}</text>')
        parts.append(f'<text x="{x + slot*0.3:.1f}" y="{top + plot_h + 18}" text-anchor="middle" font-family="sans-serif" font-size="11">{label}</text>')
    parts.append(f'<line x1="{left}" y1="{top+plot_h}" x2="{width-right}" y2="{top+plot_h}" stroke="#222"/>')
    parts.append("</svg>")
    return "\n".join(parts)


def stacked_svg(summary: dict) -> str:
    width, height = 760, 460
    left, top, bottom = 70, 50, 90
    plot_h = height - top - bottom
    series = []
    for policy in summary["policies"]:
        series.append((policy["id"], policy["label"].split()[0], [
            policy["mean"]["orchestration_s"] * 1000,
            policy["mean"]["compute_s"] * 1000,
            policy["mean"]["state_s"] * 1000,
            policy["mean"]["stall_s"] * 1000,
        ]))
    peak = max(sum(parts) for _a, _b, parts in series) * 1.15
    names = ["O", "C", "S", "W"]
    fills = ["#4c78a8", "#f58518", "#54a24b", "#b279a2"]
    parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">',
        '<rect width="100%" height="100%" fill="white"/>',
        '<text x="380" y="28" text-anchor="middle" font-family="sans-serif" font-size="16">Beam Q8 join_update components, mean of repetition means (ms)</text>',
    ]
    slot = (width - left - 20) / len(series)
    for index, (slug, label, values) in enumerate(series):
        x = left + index * slot + slot * 0.22
        y = top + plot_h
        for value, fill in zip(values, fills):
            bar_h = plot_h * value / peak
            y -= bar_h
            parts.append(f'<rect x="{x:.1f}" y="{y:.1f}" width="{slot*0.56:.1f}" height="{bar_h:.1f}" fill="{fill}"/>')
        parts.append(f'<text x="{x + slot*0.28:.1f}" y="{top + plot_h + 20}" text-anchor="middle" font-family="sans-serif" font-size="12">{label}</text>')
    for name, fill in zip(names, fills):
        parts.append(f'<rect x="{70 + names.index(name)*80}" y="{height-36}" width="14" height="14" fill="{fill}"/>')
        parts.append(f'<text x="{88 + names.index(name)*80}" y="{height-24}" font-family="sans-serif" font-size="12">{name}</text>')
    parts.append("</svg>")
    return "\n".join(parts)


def combined_svg(nexmark: dict) -> str:
    panels = []
    for title, path in OLDER.items():
        data = json.loads(path.read_text())
        panels.append((title, [(item["id"], component(item, "e2e_s") * 1000) for item in data["policies"]]))
    panels.append(("Nexmark Q8 join_update", [(item["id"], item["mean"]["e2e_s"] * 1000) for item in nexmark["policies"]]))
    width, height = 1100, 720
    parts = [
        f'<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}" viewBox="0 0 {width} {height}">',
        '<rect width="100%" height="100%" fill="white"/>',
        '<text x="550" y="28" text-anchor="middle" font-family="sans-serif" font-size="16">Four applications, mean end-to-end of the primary operation (ms)</text>',
        '<text x="550" y="48" text-anchor="middle" font-family="sans-serif" font-size="11">Older panels are the published 15 November summaries. BFS is 15 accepted of 16 attempts. Redis-object BFS C still includes untimed encode. Nexmark is Beam Q8, not that BFS residual.</text>',
    ]
    for index, (title, series) in enumerate(panels):
        col = index % 2
        row = index // 2
        origin_x = 40 + col * 540
        origin_y = 80 + row * 310
        peak = max(value for _slug, value in series) * 1.2
        parts.append(f'<text x="{origin_x}" y="{origin_y + 16}" font-family="sans-serif" font-size="14">{title}</text>')
        slot = 480 / len(series)
        base = origin_y + 250
        for bar_index, (slug, value) in enumerate(series):
            bar_h = 200 * value / peak
            x = origin_x + bar_index * slot + 16
            y = base - bar_h
            parts.append(f'<rect x="{x:.1f}" y="{y:.1f}" width="{slot*0.62:.1f}" height="{bar_h:.1f}" fill="{COLORS[slug]}"/>')
            parts.append(f'<text x="{x + slot*0.31:.1f}" y="{y - 4:.1f}" text-anchor="middle" font-family="sans-serif" font-size="11">{value:.1f}</text>')
            parts.append(f'<text x="{x + slot*0.31:.1f}" y="{base + 16}" text-anchor="middle" font-family="sans-serif" font-size="11">{slug}</text>')
    parts.append("</svg>")
    return "\n".join(parts)


def draw_bars(path_png: Path, path_pdf: Path, title: str, series: list[tuple[str, float]], ylabel: str) -> None:
    from PIL import Image, ImageDraw, ImageFont
    image = Image.new("RGB", (900, 520), "white")
    draw = ImageDraw.Draw(image)
    font = ImageFont.load_default()
    draw.text((180, 16), title, fill="black", font=font)
    draw.text((8, 240), ylabel, fill="black", font=font)
    peak = max(value for _label, value in series) * 1.2
    left, base = 80, 430
    slot = 150
    for index, (label, value) in enumerate(series):
        bar_h = 340 * value / peak
        x = left + index * slot
        draw.rectangle([x, base - bar_h, x + 80, base], fill="#4c78a8")
        draw.text((x, base - bar_h - 14), f"{value:.2f}", fill="black", font=font)
        draw.text((x, base + 8), label, fill="black", font=font)
    image.save(path_png)
    image.save(path_pdf, "PDF")


def draw_stacked(path_png: Path, path_pdf: Path, summary: dict) -> None:
    from PIL import Image, ImageDraw, ImageFont
    image = Image.new("RGB", (900, 520), "white")
    draw = ImageDraw.Draw(image)
    font = ImageFont.load_default()
    draw.text((120, 16), "Beam Q8 join_update O/C/S/W mean (ms)", fill="black", font=font)
    fills = ["#4c78a8", "#f58518", "#54a24b", "#b279a2"]
    series = []
    for policy in summary["policies"]:
        series.append((policy["label"].split()[0], [
            policy["mean"]["orchestration_s"] * 1000,
            policy["mean"]["compute_s"] * 1000,
            policy["mean"]["state_s"] * 1000,
            policy["mean"]["stall_s"] * 1000,
        ]))
    peak = max(sum(values) for _label, values in series) * 1.15
    left, base, slot = 80, 430, 150
    for index, (label, values) in enumerate(series):
        x = left + index * slot
        y = base
        for value, fill in zip(values, fills):
            bar_h = 340 * value / peak
            y -= bar_h
            draw.rectangle([x, y, x + 80, y + bar_h], fill=fill)
        draw.text((x, base + 8), label, fill="black", font=font)
    image.save(path_png)
    image.save(path_pdf, "PDF")


def draw_combined(path_png: Path, path_pdf: Path, nexmark: dict) -> None:
    from PIL import Image, ImageDraw, ImageFont
    image = Image.new("RGB", (1100, 720), "white")
    draw = ImageDraw.Draw(image)
    font = ImageFont.load_default()
    draw.text((180, 12), "Four applications, primary-operation mean E2E (ms)", fill="black", font=font)
    panels = []
    for title, path in OLDER.items():
        data = json.loads(path.read_text())
        panels.append((title, [(item["id"], component(item, "e2e_s") * 1000) for item in data["policies"]]))
    panels.append(("Nexmark Q8 join_update", [(item["id"], item["mean"]["e2e_s"] * 1000) for item in nexmark["policies"]]))
    for index, (title, series) in enumerate(panels):
        origin_x = 40 + (index % 2) * 540
        origin_y = 50 + (index // 2) * 330
        draw.text((origin_x, origin_y), title, fill="black", font=font)
        peak = max(value for _slug, value in series) * 1.2
        base = origin_y + 270
        slot = 90
        for bar_index, (slug, value) in enumerate(series):
            bar_h = 200 * value / peak
            x = origin_x + bar_index * slot
            draw.rectangle([x, base - bar_h, x + 50, base], fill=COLORS[slug])
            draw.text((x, base - bar_h - 12), f"{value:.0f}", fill="black", font=font)
            draw.text((x, base + 4), slug[:6], fill="black", font=font)
    image.save(path_png)
    image.save(path_pdf, "PDF")


def write_report(summary: dict, dest: Path) -> None:
    lines = [
        "# Beam Nexmark Q8 instrumented campaign, 15 November 2026",
        "",
        "The measured operation is `join_update`: a 12-hour event-time window over Person and Auction, joining `person.id` to `auction.seller`, emitting person id, name, auction id, and reserve. This is the README SQL / Apache Beam Q8. It is not the cloned per-auction bid list and not the seller-equals-bidder counter.",
        "",
        "Six independent namespaces, seed 20261115, exponential inter-arrival at 5 events/s/namespace. Warmup 10 s, measured arrivals 60 s. Every cell measured 1807 joins. The 12-hour window is event time. A 70-second trace does not expire records. Published objects are a few kilobytes, not the historical 2.2 MB observation. That size was not a target.",
        "",
        "Headline latency is the unweighted mean of the three repetition means. Components are means of the same rows. T = O + C + S + W held within 0.1 ms on every accepted row.",
        "",
        "| Policy | E2E ms | O | C | S | W | p50 | p95 | p99 | wall s | publish bytes |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    for policy in summary["policies"]:
        item = policy["mean"]
        lines.append(
            "| {label} | {e2e:.2f} | {o:.2f} | {c:.3f} | {s:.2f} | {w:.3f} | {p50:.2f} | {p95:.2f} | {p99:.2f} | {wall:.2f} | {pub:.0f} |".format(
                label=policy["label"],
                e2e=item["e2e_s"] * 1000,
                o=item["orchestration_s"] * 1000,
                c=item["compute_s"] * 1000,
                s=item["state_s"] * 1000,
                w=item["stall_s"] * 1000,
                p50=item["p50"] * 1000,
                p95=item["p95"] * 1000,
                p99=item["p99"] * 1000,
                wall=item["wall_s"],
                pub=item["publish_bytes"],
            )
        )
    lines.extend([
        "",
        "Repetition means, in milliseconds of end-to-end join latency:",
        "",
    ])
    for policy in summary["policies"]:
        reps = ", ".join(f"{item['e2e_s']*1000:.2f}" for item in policy["repetitions"])
        access = policy["repetitions"][0]["access"]
        lines.append(f"- {policy['label']}: {reps}. First-cell access counts {access}.")
    lines.extend([
        "",
        "Driver queue is about zero because each namespace has its own thread and the scheduled wait is outside the release-to-completion interval. O is the orchestrator, provider, and transport outside the handler. C is the join itself. S is load, decode, encode, commit, the direct-object copy, and the Data-centric commit semaphore. W is in-handler predecessor wait and is zero. Data-centric readiness waiting happens in the provider before the handler, so it is inside O.",
        "",
        "STateFlow keeps the live object in the container when the scheduler selects direct, so those rows publish zero bytes. When the scheduler then selects shared memory, the same container writes the object so the next shared-memory read is not stale. That write is why a handful of STateFlow rows have nonzero publish bytes.",
        "",
        "One replica per worker, 1000m and 1Gi, `com.openfaas.cpu.dedicated=true`. Samples show `cpu.max` 100000 100000, `cpuset.cpus.partition` member, and `memory.max` 1073741824. Affinity is not exclusive ownership. Live memory in the samples stayed between 21 MB and 25 MB.",
        "",
        "Achieved measured rate is 1807 joins / 60 s = 30.12 aggregate joins/s, the seeded trace, on every accepted cell. Workflow wall time includes the 10 s warmup and the completion of the last arrival.",
        "",
        "Attempt ledger: `nexmark-beam-q8-20261115-attempt1-stateflow-500` finished rep 0 of RR, Faa$T, Cloudburst, and Pheromone on `beam-q8-metrics-20261115`, then STateFlow returned HTTP 500. The provider had turned a function 409 into 500: a shared-memory schedule read an object that direct reuse had not published. Those four cells are preserved and are not in the table above. The accepted cohort is `beam-q8-coherent-20261115`, 15 of 15 cells, after one replaced attempt.",
        "",
        "The historical Cloudburst large-window value remains unresolved at 442.98 ms versus 42.98 ms. These measurements do not choose between them.",
        "",
    ])
    dest.write_text("\n".join(lines))


def main() -> None:
    summary = summarize()
    out = Path("/users/DCchico/Nexmark/docs/analysis")
    out.mkdir(parents=True, exist_ok=True)
    (out / "campaign-20261115.json").write_text(json.dumps(summary, indent=2) + "\n")
    (CAMPAIGN / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    headline = [(item["id"], item["label"].split()[0], item["mean"]["e2e_s"] * 1000) for item in summary["policies"]]
    (out / "nexmark-q8-headline-20261115.svg").write_text(bar_svg("Beam Q8 join_update mean E2E (ms)", headline, "milliseconds"))
    (out / "nexmark-q8-breakdown-20261115.svg").write_text(stacked_svg(summary))
    combined = combined_svg(summary)
    (out / "four-apps-headline-20261115.svg").write_text(combined)
    draw_bars(
        out / "nexmark-q8-headline-20261115.png",
        out / "nexmark-q8-headline-20261115.pdf",
        "Beam Q8 join_update mean E2E (ms)",
        [(label, value) for _slug, label, value in headline],
        "ms",
    )
    draw_stacked(out / "nexmark-q8-breakdown-20261115.png", out / "nexmark-q8-breakdown-20261115.pdf", summary)
    draw_combined(out / "four-apps-headline-20261115.png", out / "four-apps-headline-20261115.pdf", summary)
    write_report(summary, out / "campaign-20261115.md")
    print("SUMMARY", out / "campaign-20261115.json")
    for policy in summary["policies"]:
        item = policy["mean"]
        print(policy["policy"], f"{item['e2e_s']*1000:.2f}", f"O {item['orchestration_s']*1000:.2f}", f"C {item['compute_s']*1000:.3f}", f"S {item['state_s']*1000:.2f}", f"W {item['stall_s']*1000:.3f}")


if __name__ == "__main__":
    main()
