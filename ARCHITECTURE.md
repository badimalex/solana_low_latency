# Architecture

## Overview

The project builds, sends, and confirms Solana transactions through Solana RPC or Jito.
`ExecutionEngine` receives blockhash and sender dependencies, a confirmation tracker,
retry policy, timeouts, and a cancellation token. `execute()` accepts instructions,
a payer keypair, and a route. It returns a signature and confirmation status, or an error.

## Execution Flow

1. Check cancellation; return `Cancelled` if signalled.
2. Fetch the latest blockhash and build a signed v0 transaction from the payer and
   instructions. Errors return `PreparationFailed`.
3. Save the signature immediately before calling the selected sender.
4. On error, classify it: `NonRetryable` returns `SubmissionFailed`; `Retryable`
   follows the retry policy. Exhausting attempts also returns `SubmissionFailed`.
5. On success, poll confirmation by signature. `NotFound` and `Processed` trigger
   another query after 500 ms; `Confirmed` or `Finalized` completes successfully.
   Transaction failure returns `ExecutionFailed`, RPC error returns
   `ConfirmationFailed`, and confirmation timeout returns `ConfirmationTimeout`.

The outer `execute()` applies `operation_timeout` to the entire flow. Expiration
before saving a submission signature returns `DeadlineExceeded`; afterward it
returns `OutcomeUnknown { signature }`. Other errors pass through unchanged.

## RPC / Jito Routes

The request selects `SolanaRpc` or `Jito`, changing only the sender: Solana RPC
`send_transaction` or Jito Block Engine `send_transaction`. Preparation and
confirmation are shared; `RpcStatusProvider` checks Solana RPC for either route.
`JitoClient` also supports bundles and their status queries, but bundle APIs are
not integrated into `ExecutionEngine`.

## Reliability

For retryable errors, `max_attempts` includes the first send. With attempts remaining,
the engine checks block height. An expired blockhash causes a fresh fetch and rebuild
with a new signature; otherwise the same signed transaction is retained. Preparation
errors return `PreparationFailed`. Retries wait with exponential backoff capped at
`max_delay`; the first send has no policy delay.

Both shipped senders classify all errors as `NonRetryable`, preventing automatic
engine retries after their errors. Custom senders can enable retries. Retaining the
transaction preserves its signature; rebuilding changes it. There is no exactly-once
guarantee.

`confirmation_timeout` bounds confirmation; `operation_timeout` bounds the whole
operation. Cancellation is checked only at entry and during retry delays, without
explicit priority. It does not interrupt an active send or confirmation. Neither
cancellation nor timeout undoes an already submitted transaction.

## Observability

Counters track execution starts/results, submission attempts, retries, and submission
errors. Duration histograms cover the whole execution, blockhash fetch, transaction
build, submission, and confirmation. Metrics carry the route; execution results also
carry the outcome. Tracing records stage boundaries, durations, attempts, error
classification, rebuild reasons, and final outcomes.

## Known Limitations

- A timeout or lost response can leave the transaction outcome uncertain. Only the
  overall timeout after a saved signature returns `OutcomeUnknown`; ordinary send
  errors return `SubmissionFailed` without exposing that signature.
- Normal confirmation queries signature history, but there is no reconciliation
  after ambiguous sends or tracking of previous attempt signatures. Custom retry
  classification can allow a rebuild before resolving the original transaction,
  risking repeated execution. Cancellation is limited to the points described above.
- Jito Testnet: two `sendBundle` calls returned HTTP 200 and bundle IDs. Tracking
  reported `Invalid` then `Failed`; `getBundleStatuses` returned null for both.
  Both transactions in each run finalized in the same slot; adjacent positions were
  verified in the second run. The discrepancy is unexplained; atomicity was not proven.
- Manual checks used Solana Devnet. Dedicated RPC performance has not been validated;
  production or Mainnet readiness is not established.
