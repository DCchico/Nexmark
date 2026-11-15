# Nexmark identity audit, 15 November 2026

Repository https://github.com/DCchico/Nexmark.git, branch `main` at `7fbe9b06dbf53d9d5124c97763635373e4848e29`. The untouched tree is also `codex/nexmark-q8-as-cloned-20261115` at that same commit. There is no Dockerfile and no generator in this checkout. The OpenFaaS stack file is `q8_redis.yaml`.

This is not Apache Beam Nexmark Q8. Beam Q8 joins `Person.id` to `Auction.seller` inside windows. The file is https://github.com/apache/beam/blob/master/sdks/java/testing/nexmark/src/main/java/org/apache/beam/sdk/nexmark/queries/Query8.java. The README in `nexmark-q8/README.md` quotes that SQL. The function that actually runs does not.

`NexmarkQuery8Streaming.process_bid_event` keys one pickled document by auction id (`nexmark:window:{auction_id}`). It loads the document, drops entries older than 12 hours of wall-clock time, appends one `bid` record, and stores the document. The person-seller loop looks for entry types `person` and `auction`. This method never writes those types, so `new_users` stays empty. `initialize_12_hour_window` writes 43,200 synthetic one-per-second entries. That is padding, not the query. Tests call `process_bid_event_ordered`, which is not defined. `compute_latency_test.py` instead counts bids whose bidder id equals a seller id, on one auction, with no window.

The manuscript's per-auction window, auction-id namespace, and bid TimeID are closer to `process_bid_event` than to Beam Q8, but the code uses processing time, not the bid timestamp, and it does not implement the join on the records it stores. Historical sizes near 708 KB / 2.2 MB / 4.5 MB are not reproduced here and are not targets. The 442.98 ms versus 42.98 ms Cloudburst disagreement is left unresolved.

The audit above left three concrete workloads:

1. The function that runs: per-auction bid window, 12-hour processing-time expiry, append one bid, empty join result, no synthetic padding.
2. The README SQL: 12-hour Person and Auction windows, join person id to seller, emit id, name, and reserve.
3. The latency-test variant: one auction, count bids where the bidder is the seller, no sliding window.

The selected workload is item 2, Apache Beam Q8. The implementation is `nexmark_join`, on branch `codex/nexmark-beam-q8-20261115`. The cloned `process_bid_event` remains on `codex/nexmark-q8-as-cloned-20261115` and is not the measured function. The manuscript's per-auction namespace is the rejected option. Here a namespace is one independent Q8 instance. The campaign report is `docs/analysis/campaign-20261115.md`.
