# Solana Low Latency

Rust library for building, signing, sending, and confirming Solana transactions
through Solana RPC or Jito. Designed for integration into another application;
there is no standalone executable.

## Features

- Signed v0 transactions and RPC/Jito submission.
- Confirmation tracking, retry policy, timeouts, and cancellation.
- Metrics and tracing.
- Separate Jito bundle APIs, outside `ExecutionEngine`.

## Build and Check

Requires Rust with edition 2024 support.

```sh
cargo build --locked --lib
cargo test --locked --lib
cargo clippy --locked --lib --tests -- -D warnings
cargo fmt --all -- --check
```

## Documentation

- [Architecture](ARCHITECTURE.md)
- [Benchmarks and evidence](BENCHMARKS.md)

## Limitations

Default senders do not enable engine retries. Cancellation does not interrupt an
active send or confirmation. Ambiguous sends are not automatically reconciled;
exactly-once execution is not guaranteed. Jito bundle atomicity was not proven.
Network measurements used public Devnet RPC; dedicated RPC performance was not validated.
