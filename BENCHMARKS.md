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