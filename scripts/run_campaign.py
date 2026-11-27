#!/usr/bin/env python3
"""Beam Q8 campaign. One join update per seeded event. Six streams stay independent."""

from __future__ import annotations

import json
import os
import subprocess
import sys
import threading
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SCRIPTS = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(SCRIPTS))

FUNCTION = "nexmark_join"
IMAGE = os.environ.get("NEXMARK_IMAGE", "docker.io/dcchico/nexmark-q8:beam-q8-coherent-20261115")
ORCH = os.environ.get("ORCHESTRATOR", "http://127.0.0.1:8088")


def ssh_to(host: str, remote: str) -> None:
    command = ["sudo", "ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=10", "-o", "IdentitiesOnly=yes", "-o", "StrictHostKeyChecking=yes"]
    key = os.environ.get("EXPERIMENT_SSH_KEY", "")
    known = os.environ.get("EXPERIMENT_KNOWN_HOSTS", "")
    if key:
        command.extend(["-i", key])
    if known:
        command.extend(["-o", f"UserKnownHostsFile={known}"])
    command.append(f"root@{host}")
    command.append(remote)
    subprocess.check_call(command)


def oracle(trace: dict) -> dict:
    from nexmark_join.window import apply_event, empty_state
    states = [empty_state() for _ in range(trace["namespaces"])]
    expected = {}
    for row in trace["rows"]:
        result = apply_event(states[row["namespace"]], row["event"], row["seq"], row["predecessor"])
        if not row["warmup"]:
            expected[(row["namespace"], row["seq"])] = result["matches"]
    return expected


def post(path: str, body: dict, headers: dict | None = None) -> dict:
    import urllib.request
    raw = json.dumps(body).encode()
    request = urllib.request.Request(ORCH + path, data=raw, headers={"Content-Type": "application/json", **(headers or {})}, method="POST")
    with urllib.request.urlopen(request, timeout=120) as response:
        return json.loads(response.read())


def register(run_id: str, redis_addr: str, backend: str) -> None:
    vdags = [f"nx{run_id}s{index}" for index in range(6)]
    body = {
        "id": f"nexmark-{run_id}",
        "vDAGs": vdags,
        "stDependencies": [{
            "key": "nx",
            "senderFunction": FUNCTION,
            "senderStream": vdag,
            "receiverFunction": FUNCTION,
            "receiverStream": vdag,
            "sequenceOffset": 1,
        } for vdag in vdags],
        "functionSequence": [{
            "functionName": FUNCTION,
            "downstream": "",
            "parameters": {
                "service": FUNCTION,
                "image": IMAGE,
                "labels": {},
                "annotations": {"com.openfaas.scale.min": "1", "com.openfaas.cpu.dedicated": "true"},
                "envVars": {
                    "REDIS_ADDR": redis_addr,
                    "NEXMARK_BACKEND": backend,
                    "NEXMARK_PREFIX": "sn",
                },
                "limits": {"cpu": "1000m", "memory": "1Gi"},
                "requests": {"cpu": "1000m", "memory": "1Gi"},
            },
        }],
    }
    post("/workflow", body)


def invoke(run_id: str, namespace: int, sequence: int, body: dict) -> dict:
    import base64
    payload = base64.b64encode(json.dumps(body).encode()).decode()
    vdag = f"nx{run_id}s{namespace}"
    return post("/invoke", {"FunctionName": FUNCTION, "Payload": payload}, {
        "workflowID": f"nexmark-{run_id}",
        "vDAGID": vdag,
        "sequence": str(sequence),
    })


def run_stream(run_id: str, rows: list[dict], t0: float, sink: list, lock: threading.Lock) -> None:
    from nexmark_join.timing import close_operation
    for row in rows:
        while time.perf_counter() < t0 + row["scheduled_s"]:
            time.sleep(0.001)
        release = time.perf_counter()
        dispatch = time.perf_counter()
        response = invoke(run_id, row["namespace"], row["seq"], {
            "namespace": f"nx{run_id}s{row['namespace']}",
            "seq": row["seq"],
            "predecessor": row["predecessor"],
            "event": row["event"],
            "backend": os.environ.get("NEXMARK_CELL_BACKEND", "memory"),
        })
        completion = time.perf_counter()
        inner = response.get("timings") if isinstance(response, dict) else None
        record = dict(response) if isinstance(response, dict) else {"ok": False, "error": str(response)}
        record.update(close_operation(release=release, dispatch=dispatch, completion=completion, handler=inner))
        record["scheduled_s"] = row["scheduled_s"]
        record["warmup"] = row["warmup"]
        record["namespace_index"] = row["namespace"]
        record["seq"] = row["seq"]
        with lock:
            sink.append(record)


def worker_hosts() -> list[str]:
    yaml = Path(os.environ.get("STATEFLOW_REPO", str(Path.home() / "STateFlow"))) / "configs" / "stateflow.yaml"
    ips = []
    seen = False
    for line in yaml.read_text().splitlines():
        if line.startswith("fecores:"):
            seen = True
            continue
        if seen and line.strip().startswith("- ip:"):
            ips.append(line.split(":", 1)[1].strip())
        elif seen and line.strip() and not line.strip().startswith("port:"):
            break
    return ips


def wait_ready() -> None:
    deadline = time.time() + 90
    while time.time() < deadline:
        count = 0
        for host in worker_hosts():
            command = ["sudo", "ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=8", "-o", "IdentitiesOnly=yes", "-o", "StrictHostKeyChecking=yes"]
            if os.environ.get("EXPERIMENT_SSH_KEY"):
                command.extend(["-i", os.environ["EXPERIMENT_SSH_KEY"]])
            if os.environ.get("EXPERIMENT_KNOWN_HOSTS"):
                command.extend(["-o", f"UserKnownHostsFile={os.environ['EXPERIMENT_KNOWN_HOSTS']}"])
            command.extend([f"root@{host}", "ctr -n faasedge-fn tasks ls"])
            out = subprocess.check_output(command, text=True)
            count += sum(1 for line in out.splitlines() if line.startswith("nexmark_join-"))
        if count >= 6:
            print("REPLICAS", count, flush=True)
            return
        time.sleep(1)
    raise SystemExit(f"nexmark replicas not ready, saw {count}")


def cleanup() -> None:
    import urllib.request
    try:
        post_delete = urllib.request.Request(ORCH + "/delete", data=json.dumps({"functionName": FUNCTION}).encode(), headers={"Content-Type": "application/json"}, method="DELETE")
        urllib.request.urlopen(post_delete, timeout=60).read()
    except Exception:
        pass
    redis = os.environ.get("REDIS_ADDR", "")
    if redis:
        host, port = redis.rsplit(":", 1)
        script = (
            f"import socket\n"
            f"s=socket.create_connection(({host!r},{int(port)}),5)\n"
            f"def call(*a):\n"
            f"  p=b''.join([f'*{{len(a)}}\\r\\n'.encode()]+[f'${{len(x if isinstance(x,bytes) else str(x).encode())}}\\r\\n'.encode()+(x if isinstance(x,bytes) else str(x).encode())+b'\\r\\n' for x in a])\n"
            f"  s.sendall(p); return s.recv(65536)\n"
            f"raw=call('KEYS','nexmark:q8:*')\n"
            f"keys=[]\n"
            f"if raw.startswith(b'*'):\n"
            f"  parts=raw.split(b'\\r\\n'); keys=[parts[i] for i in range(2,len(parts),2) if parts[i] and not parts[i].startswith(b'$') and not parts[i].startswith(b'*')]\n"
            f"if keys: call('UNLINK',*keys)\n"
            f"s.close()\n"
        )
        subprocess.run([sys.executable, "-c", script], check=False)
    remote = r'''
set -eu
pass=0
while [ "$pass" -lt 2 ]; do
  ids=$(ctr -n faasedge-fn tasks ls | awk 'NR>1 && $1 ~ /^nexmark_join-/ {print $1}')
  for id in $ids; do
    ctr -n faasedge-fn tasks kill -s SIGKILL "$id" >/dev/null 2>&1 || true
    ctr -n faasedge-fn tasks delete "$id" >/dev/null 2>&1 || true
  done
  cids=$(ctr -n faasedge-fn containers ls | awk 'NR>1 && $1 ~ /^nexmark_join-/ {print $1}')
  for id in $cids; do
    ctr -n faasedge-fn containers delete "$id" >/dev/null 2>&1 || true
  done
  pass=$((pass + 1))
done
rm -f /dev/shm/sn_nx* /dev/shm/sem.nx_* || true
left=$(ctr -n faasedge-fn tasks ls | awk 'NR>1 && $1 ~ /^nexmark_join-/ {print $1}')
if [ -n "$left" ]; then echo TASKS_REMAIN $left; exit 3; fi
echo TASKS_CLEAR
'''
    yaml = Path(os.environ.get("STATEFLOW_REPO", str(Path.home() / "STateFlow"))) / "configs" / "stateflow.yaml"
    ips = []
    seen = False
    for line in yaml.read_text().splitlines():
        if line.startswith("fecores:"):
            seen = True
            continue
        if seen and line.strip().startswith("- ip:"):
            ips.append(line.split(":", 1)[1].strip())
        elif seen and line.strip() and not line.strip().startswith("port:"):
            break
    for ip in ips:
        ssh_to(ip, remote)


def cell(run_id: str, policy: str, dest: Path, trace: dict) -> dict:
    redis_addr = os.environ["REDIS_ADDR"]
    backend = "redis-object" if policy == "RR" else "memory"
    os.environ["NEXMARK_CELL_BACKEND"] = backend
    register(run_id, redis_addr, backend)
    wait_ready()
    expected = oracle(trace)
    grouped = [[] for _ in range(trace["namespaces"])]
    for row in trace["rows"]:
        grouped[row["namespace"]].append(row)
    sink: list[dict] = []
    lock = threading.Lock()
    t0 = time.perf_counter()
    errors: list[BaseException] = []

    def guarded(rows):
        try:
            run_stream(run_id, rows, t0, sink, lock)
        except BaseException as exc:
            errors.append(exc)

    threads = [threading.Thread(target=guarded, args=(rows,)) for rows in grouped]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    if errors:
        raise errors[0]
    wall = time.perf_counter() - t0
    measured = [row for row in sink if not row.get("warmup")]
    mismatches = []
    for row in measured:
        if not row.get("ok"):
            mismatches.append({"seq": row.get("seq"), "error": row.get("error")})
            continue
        got = row.get("matches")
        want = expected[(row["namespace_index"], row["seq"])]
        if got != want:
            mismatches.append({"namespace": row["namespace_index"], "seq": row["seq"], "got": got, "want": want})
    timing_bad = [
        row for row in measured
        if row.get("timing_complete") is not True
        or abs(row["e2e_s"] - (row["orchestration_s"] + row["compute_s"] + row["state_s"] + row["stall_s"])) > 1e-4
    ]
    evidence = {
        "run_id": run_id,
        "policy": policy,
        "backend": backend,
        "query": "Beam Nexmark Q8 person-seller window join",
        "complete": not mismatches and not timing_bad and len(measured) == len([row for row in trace["rows"] if not row["warmup"]]),
        "mismatches": mismatches[:8],
        "mismatch_count": len(mismatches),
        "timing_bad": len(timing_bad),
        "measured": len(measured),
        "wall_s": wall,
        "records": measured,
    }
    dest.mkdir(parents=True, exist_ok=True)
    (dest / "result.json").write_text(json.dumps(evidence) + "\n")
    print("CELL", run_id, policy, "measured", len(measured), "mismatches", len(mismatches), "timing_bad", len(timing_bad), flush=True)
    if not evidence["complete"]:
        raise SystemExit(f"cell failed {run_id}")
    return evidence


def prepare_one(policy: str, dest: Path) -> None:
    dest.mkdir(parents=True, exist_ok=True)
    subprocess.check_call([os.environ.get("GRAPHALYTICS_PREPARE", str(Path.home() / "Graphalytics-STateFlow/scripts/prepare_cell.sh")), policy, str(dest)])


def live_cell(plan: dict, trace: dict) -> dict:
    os.environ["REDIS_ADDR"] = plan["redis"]
    dest = Path(plan["output"])
    prepare_one(plan["policy"], dest / "prepare")
    try:
        return cell(plan["run_id"], plan["policy"], dest, trace)
    finally:
        cleanup()


def main(argv: list[str] | None = None) -> int:
    args = list(sys.argv[1:] if argv is None else argv)
    if not args or args[0] in {"-h", "--help", "cell"} or args[0].startswith("-"):
        from nexmark_join.trace import generate
        from one_cell import dispatch
        return dispatch(args, generate=generate, executor=live_cell)
    if len(args) < 2:
        print("probe and campaign need an output directory")
        return 2
    mode = args[0]
    out = Path(args[1])
    from nexmark_join.trace import generate
    out.mkdir(parents=True, exist_ok=True)
    trace = generate()
    (out / "trace.json").write_text(json.dumps({"seed": trace["seed"], "rows": len(trace["rows"]), "measured": sum(1 for row in trace["rows"] if not row["warmup"])}) + "\n")
    known = ["RR", "Faast", "Cloudburst", "Data-centric", "Stateflow"]
    policies = args[2:] or known
    unknown = [item for item in policies if item not in known]
    if unknown:
        raise SystemExit(f"unknown policies {unknown}")
    if mode == "probe":
        short = generate(measure_s=5.0, warmup_s=1.0)
        for policy in policies:
            dest = out / policy
            cleanup()
            subprocess.check_call([os.environ.get("GRAPHALYTICS_PREPARE", str(Path.home() / "Graphalytics-STateFlow/scripts/prepare_cell.sh")), policy, str(dest / "prepare")])
            try:
                cell(f"probe{policy[:2].lower()}", policy, dest, short)
            finally:
                cleanup()
        print("PROBE_DONE")
        return 0
    if mode != "campaign":
        raise SystemExit("mode must be probe or campaign")
    order = []
    for rep in range(3):
        rotated = policies[rep:] + policies[:rep]
        for policy in rotated:
            order.append((rep, policy))
    slug = {"RR": "rr", "Faast": "faast", "Cloudburst": "cloudburst", "Data-centric": "datacentric", "Stateflow": "stateflow"}
    for rep, policy in order:
        run_id = f"n{rep}{slug[policy]}"
        dest = out / f"r{rep}" / slug[policy]
        if (dest / "result.json").exists() and json.loads((dest / "result.json").read_text()).get("complete"):
            print("SKIP", run_id)
            continue
        cleanup()
        subprocess.check_call([os.environ.get("GRAPHALYTICS_PREPARE", str(Path.home() / "Graphalytics-STateFlow/scripts/prepare_cell.sh")), policy, str(dest / "prepare")])
        try:
            cell(run_id, policy, dest, trace)
        finally:
            cleanup()
    print("NEXMARK_CAMPAIGN_DONE")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
