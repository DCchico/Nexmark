# Beam Nexmark Q8 on the five-policy object substrate

The five-application entry is https://github.com/DCchico/STateFlow/blob/feature/social-integrated/docs/reproduce-all-applications.md. `prepare_cell.sh` comes from `GRAPHALYTICS_PREPARE` or `$HOME/Graphalytics-STateFlow/scripts/prepare_cell.sh`.

This runbook is the selected workload. The cloned handler in `nexmark-q8/handler.py` is a different program and is not what the campaign runs. See `docs/query-identity.md`.

## Workload

Apache Beam Nexmark Query 8, matching the SQL quoted in `nexmark-q8/README.md`:

- Person and Auction events.
- A 12-hour event-time window. The trace clock is the seeded logical arrival time, stored as milliseconds. It is not `time.time()`.
- Join `person.id` to `auction.seller`.
- Emit person id, name, auction id, and reserve.
- Six independent namespaces. A namespace is one Q8 instance, not an auction id. Sellers refer only to persons in the same namespace.
- Seed `20261115`, exponential inter-arrival, 5 events/s/namespace, 10 s warmup, 60 s measured arrivals.

A 70-second trace does not reach the 12-hour horizon, so the window does not drop records. That is the window rule, not padding. The cloned `initialize_12_hour_window` is not called. Objects in this trace average about 18 KB on the publish path. The historical 2.2 MB figure is not a target.

## Function and state

One function, `nexmark_join`. Six vDAGs, `nx{run}s0` through `nx{run}s5`. Each vDAG depends on itself with key `nx` and `sequenceOffset` 1, so the orchestrator treats the update as stateful.

```text
namespace stream ----> nexmark_join.join_update ----> matches
                         |
                         +-- object nexmark:q8:{namespace}
                             or /dev/shm/sn_{namespace}
```

The mutable object is one JSON document: version, last event time, persons, auctions, and the outputs already emitted for each sequence. Immutable inputs are the event in the invocation body. The match list is the transient output and is also stored so a duplicate sequence returns the same matches without rewriting.

Sequence 0 has predecessor null. Any later call whose stored version is not the predecessor fails and does not create an empty window. A repeated sequence returns the stored matches. An event time older than the last accepted time fails and does not write. Namespaces do not share persons.

`STATE_MODE=object` is the Redis path `NEXMARK_BACKEND=redis-object` and the memory path `NEXMARK_BACKEND=memory`. Redis GET, JSON decode, the shared join, and SET are one update. Memory policies transfer that same JSON object. Encode and decode are inside the measured state interval.

Direct reuse keeps the live object in the process when the scheduler's state location, or the dependent location, is `direct` and the cached version is the predecessor. Those rows publish zero bytes. If a later call asks for shared memory, the same process writes the object before returning, so the file is not stale. The Data-centric provider waits on `/nx_{vdag}_{seq}_sem`. The function posts that semaphore after a successful commit. That wait is before the handler, so it is orchestration time, not W.

The object prefix is `sn` because that is the existing 1 MiB cap and this object is far below it. No provider prefix was added. vDAG ids start with `nx`, so they do not match SocialNet or BFS names. Cleanup removes `/dev/shm/sn_nx*` and `/dev/shm/sem.nx_*` only.

## Resources

One replica of `nexmark_join` on each worker (`scale.min` 1), `1000m`, `1Gi`, annotation `com.openfaas.cpu.dedicated=true`. The annotation sets a cpuset. Sampled containers reported `cpu.max` `100000 100000`, `cpuset.cpus.partition` `member`, and `memory.max` 1073741824. That is affinity, not an exclusive core. The same budget is used for every policy. Run ids are `n{repetition}{policy}` because those ids place the six namespaces on six workers under the Data-centric hash.

Image: `docker.io/dcchico/nexmark-q8:beam-q8-coherent-20261115`.

Orchestrator: `/tmp/stateflow-timing-admission`. Provider: `/usr/local/bin/fecore` with `--pull-policy IfNotPresent`. Do not start `/tmp/stateflow` or `/tmp/stateflow-social`.

## Fresh cluster

From the control node, with the project share mounted and the six workers already running the pinned provider:

```bash
export STATEFLOW_BIN=/tmp/stateflow-timing-admission
export STATEFLOW_REPO=$HOME/STateFlow
export REDIS_ADDR=<redis host:6379>
export NEXMARK_IMAGE=docker.io/dcchico/nexmark-q8:beam-q8-coherent-20261115
python3 -m unittest tests.test_window
python3 -u scripts/run_campaign.py probe <probe-directory>
python3 -u scripts/run_campaign.py campaign <campaign-directory>
python3 scripts/analyze_campaign.py
```

Import the image into containerd namespace `faasedge-fn` on every worker before the first cell. `prepare_cell.sh` restarts the orchestrator onto `STATEFLOW_BIN` and rewrites `configs/stateflow.yaml` for the policy. That YAML contains this reservation's addresses and stays uncommitted.

Cleanup deletes the function `nexmark_join`, unlinks Redis keys `nexmark:q8:*`, and removes this app's shared-memory files. It does not run `FLUSHALL` and it does not touch SocialNet, BFS, or federation keys.

The campaign order is rep 0 RR, Faa$T, Cloudburst, Pheromone, STateFlow; rep 1 starts at Faa$T; rep 2 starts at Cloudburst. A cell with `result.json` and `complete: true` is skipped. Do not treat a skipped replacement as a clean 15 of 15. The attempt that failed before the coherent image is `nexmark-beam-q8-20261115-attempt1-stateflow-500`.
