# D13 Benchmarks

| Run | Success | Throughput | p50 | p95 | p99 |
|---|---:|---:|---:|---:|---:|
| Devnet 10, 1s delay | 10/10 | 0.34 ops/s | 1681 ms | 3597 ms | 3597 ms |
| Devnet 100, no delay | 100/100 | 0.33 ops/s | 1137 ms | 11329 ms | 11877 ms |

- Network: Solana Devnet, RPC: `https://api.devnet.solana.com`
- Execution: `ExecutionEngine::execute()`, concurrency: 1
- Evidence: `devnet_10.json`, `devnet_100.json`
- Local fake benchmark: 100 operations, concurrency 10, ~817 ops/s, p50 11.5ms, p95 12.69ms, p99 12.95ms (10ms simulated RPC delay).
- Fault injection: 7 scenarios tested — slow RPC, submission errors, confirmation timeout, RPC unavailable, expired blockhash, cancellation, operation deadline.
- Limitation: public Devnet RPC showed periodic 10–12s status-query delays; measured throughput is not the engine's maximum capacity.

## Method and Reproducibility

Latency measures one `ExecutionEngine::execute()` call, including preparation,
submission, and confirmation, but excluding semaphore wait. Percentiles use nearest
rank on sorted latencies: `ceil(N * p) - 1`. Throughput is completed operations
divided by total batch wall time. The saved JSON confirms success counts and
percentiles; it does not contain the full batch duration needed to independently
verify throughput.

Devnet harness: [`examples/bench_devnet.rs`](examples/bench_devnet.rs).
Run from the repository root:

```sh
cargo run --locked --example bench_devnet
```

`bench_devnet` requires a funded Devnet keypair at
`devnet-payer.json` and sends real Devnet transfers to its hardcoded recipient.
Its current configuration uses 20 operations, concurrency 1, no inter-operation
delay, and writes `devnet_diag_20_v2.json`.

The exact harness versions and launch commands for the historical 10- and
100-operation runs were not preserved. The current example therefore does not
reproduce the table as-is. No CLI options for operation count or delay are provided.

The separate local-only `examples/bench_local.rs` runs with
`cargo run --locked --example bench_local`: 100 operations, concurrency 10,
a simulated 10 ms send delay, and immediate confirmation. Neither this local
harness nor its raw output is tracked in Git.
