use std::time::Duration;

use solana_client::rpc_client::SerializableTransaction;
use solana_keypair::Signature;

use crate::{
    blockhash::BlockhashSource,
    confirmation::{ConfirmationError, ConfirmationTracker, SignatureStatusProvider},
    execution::retry::{RetryClass, RetryPolicy, SubmissionError},
    transaction_builder,
};

use super::{
    sender::{JitoSender, RpcSender},
    types::{ExecutionError, ExecutionOutcome, ExecutionRequest, ExecutionRoute},
};

pub struct ExecutionEngine<B, R, J, P>
where
    P: SignatureStatusProvider,
{
    blockhash_source: B,
    rpc_sender: R,
    jito_sender: J,
    confirmation_tracker: ConfirmationTracker<P>,
    confirmation_timeout: Duration,

    retry_policy: super::retry::RetryPolicy,
    cancellation_token: tokio_util::sync::CancellationToken,
    operation_timeout: Duration,
}

impl<B, R, J, P> ExecutionEngine<B, R, J, P>
where
    B: BlockhashSource,
    R: RpcSender,
    J: JitoSender,
    P: SignatureStatusProvider,
{
    pub fn new(
        blockhash_source: B,
        rpc_sender: R,
        jito_sender: J,
        confirmation_tracker: ConfirmationTracker<P>,
        confirmation_timeout: Duration,
        retry_policy: RetryPolicy,
        cancellation_token: tokio_util::sync::CancellationToken,
        operation_timeout: Duration,
    ) -> Self {
        Self {
            blockhash_source,
            rpc_sender,
            jito_sender,
            confirmation_tracker,
            confirmation_timeout,
            retry_policy,
            cancellation_token,
            operation_timeout,
        }
    }

    pub async fn execute(
        &self,
        request: ExecutionRequest,
    ) -> Result<ExecutionOutcome, ExecutionError> {
        let mut submitted_signature: Option<Signature> = None;

        match tokio::time::timeout(
            self.operation_timeout,
            self.execute_inner(request, &mut submitted_signature),
        )
        .await
        {
            Ok(res) => res,
            Err(_) => match submitted_signature {
                Some(signature) => Err(ExecutionError::OutcomeUnknown { signature }),
                None => Err(ExecutionError::DeadlineExceeded),
            },
        }
    }

    async fn execute_inner(
        &self,
        request: ExecutionRequest,
        submitted_signature: &mut Option<Signature>,
    ) -> Result<ExecutionOutcome, ExecutionError> {
        if self.cancellation_token.is_cancelled() {
            return Err(ExecutionError::Cancelled);
        }
        let mut blockhash_info = self
            .blockhash_source
            .latest()
            .await
            .map_err(|_| ExecutionError::PreparationFailed)?;

        let instructions = request.instructions;
        let mut transaction = transaction_builder::build_v0_transaction(
            &request.payer,
            &instructions,
            blockhash_info.blockhash,
            vec![],
        )
        .map_err(|_| ExecutionError::PreparationFailed)?;

        let mut attempt = 1;

        let signature = loop {
            let signature_copy = transaction.signatures[0];
            *submitted_signature = Some(signature_copy.clone());

            let submission = match &request.route {
                ExecutionRoute::SolanaRpc => self
                    .rpc_sender
                    .send(&transaction)
                    .await
                    .map_err(|error| self.rpc_sender.classify_error(&error)),

                ExecutionRoute::Jito => self
                    .jito_sender
                    .send(&transaction)
                    .await
                    .map_err(|error| self.jito_sender.classify_error(&error)),
            };

            match submission {
                Ok(signature) => break signature,

                Err(error) => match error.class {
                    RetryClass::Retryable => {
                        if !self.retry_policy.can_retry(attempt) {
                            return Err(ExecutionError::SubmissionFailed);
                        }
                        let current_block_height = self
                            .blockhash_source
                            .current_block_height()
                            .await
                            .map_err(|_| ExecutionError::PreparationFailed)?;

                        if current_block_height > blockhash_info.last_valid_block_height {
                            blockhash_info = self
                                .blockhash_source
                                .latest()
                                .await
                                .map_err(|_| ExecutionError::PreparationFailed)?;

                            transaction = transaction_builder::build_v0_transaction(
                                &request.payer,
                                &instructions,
                                blockhash_info.blockhash,
                                vec![],
                            )
                            .map_err(|_| ExecutionError::PreparationFailed)?;
                        }
                    }
                    RetryClass::NonRetryable => {
                        return Err(ExecutionError::SubmissionFailed);
                    }
                },
            }

            tokio::select! {

                _ = tokio::time::sleep(self.retry_policy.delay_for(attempt)) => {
                    attempt += 1;
                }
                _=self.cancellation_token.cancelled() => {
                    return Err(ExecutionError::Cancelled)
                }

            }
        };

        let confirmation = self
            .confirmation_tracker
            .wait_until_confirmed(&signature, self.confirmation_timeout)
            .await;

        let confirmation = match confirmation {
            Ok(status) => status,

            Err(ConfirmationError::TransactionFailed(_)) => {
                return Err(ExecutionError::ExecutionFailed);
            }

            Err(ConfirmationError::Timeout) => {
                return Err(ExecutionError::ConfirmationTimeout);
            }

            Err(ConfirmationError::Rpc(_)) => {
                return Err(ExecutionError::ConfirmationFailed);
            }
        };

        Ok(ExecutionOutcome {
            signature,
            confirmation,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    use std::time::Duration;

    use solana_client::client_error::ClientError;
    use solana_hash::Hash;
    use solana_keypair::{Keypair, Signature};
    use solana_message::{AccountMeta, Instruction};
    use solana_pubkey::Pubkey;
    use solana_transaction::versioned::VersionedTransaction;
    use tokio_util::sync::CancellationToken;

    use crate::{
        blockhash::{BlockhashInfo, BlockhashSource},
        confirmation::{
            ConfirmationStatus::{self, Confirmed},
            ConfirmationTracker, SignatureStatusProvider,
        },
        execution::retry::{RetryClass, RetryPolicy, SubmissionError},
        jito::error::JitoError,
    };

    use super::*;
    use crate::execution::sender::{JitoSender, RpcSender};

    struct RetryOnceRpcSender {
        calls: Arc<AtomicUsize>,
    }

    impl RpcSender for RetryOnceRpcSender {
        async fn send(
            &self,
            _transaction: &VersionedTransaction,
        ) -> Result<Signature, ClientError> {
            let call = self.calls.fetch_add(1, Ordering::Relaxed);

            if call == 0 {
                return Err(solana_client::client_error::ClientError::from(
                    solana_client::client_error::ClientErrorKind::Custom(
                        "fake rpc error".to_string(),
                    ),
                ));
            }

            Ok(Signature::default())
        }

        fn classify_error(&self, error: &ClientError) -> SubmissionError {
            SubmissionError {
                class: RetryClass::Retryable,
                message: error.to_string(),
            }
        }
    }

    #[tokio::test]
    async fn retryable_submission_error_is_retried() {
        let rpc_calls = Arc::new(AtomicUsize::new(0));

        let engine = ExecutionEngine::new(
            FakeBlockhashSource,
            RetryOnceRpcSender {
                calls: Arc::clone(&rpc_calls),
            },
            FakeJitoSender {
                calls: Arc::new(AtomicUsize::new(0)),
            },
            ConfirmationTracker::new(FakeStatusProvider),
            Duration::from_secs(1),
            RetryPolicy::new(3, Duration::from_millis(1), Duration::from_millis(10)).unwrap(),
            CancellationToken::new(),
            Duration::from_secs(30),
        );

        let request = ExecutionRequest {
            instructions: vec![test_instruction()],
            payer: Keypair::new(),
            route: ExecutionRoute::SolanaRpc,
        };

        let result = engine.execute(request).await;

        assert!(result.is_ok());
        assert_eq!(rpc_calls.load(Ordering::Relaxed), 2);
    }

    struct AlwaysRetryableRpcSender {
        calls: Arc<AtomicUsize>,
    }

    impl RpcSender for AlwaysRetryableRpcSender {
        async fn send(
            &self,
            _transaction: &VersionedTransaction,
        ) -> Result<Signature, ClientError> {
            self.calls.fetch_add(1, Ordering::Relaxed);

            Err(ClientError::from(
                solana_client::client_error::ClientErrorKind::Custom("temporary error".to_string()),
            ))
        }

        fn classify_error(&self, error: &ClientError) -> SubmissionError {
            SubmissionError {
                class: RetryClass::Retryable,
                message: error.to_string(),
            }
        }
    }

    #[tokio::test]
    async fn retryable_error_stops_exactly_at_max_attempts() {
        let rpc_calls = Arc::new(AtomicUsize::new(0));

        let engine = ExecutionEngine::new(
            FakeBlockhashSource,
            AlwaysRetryableRpcSender {
                calls: Arc::clone(&rpc_calls),
            },
            FakeJitoSender {
                calls: Arc::new(AtomicUsize::new(0)),
            },
            ConfirmationTracker::new(FakeStatusProvider),
            Duration::from_secs(1),
            RetryPolicy::new(3, Duration::from_millis(1), Duration::from_millis(10)).unwrap(),
            CancellationToken::new(),
            Duration::from_secs(30),
        );

        let request = ExecutionRequest {
            instructions: vec![test_instruction()],
            payer: Keypair::new(),
            route: ExecutionRoute::SolanaRpc,
        };

        let result = engine.execute(request).await;

        assert!(matches!(result, Err(ExecutionError::SubmissionFailed)));

        assert_eq!(
            rpc_calls.load(Ordering::Relaxed),
            3,
            "max_attempts = 3 must produce exactly 3 submission attempts"
        );
    }

    struct NonRetryableRpcSender {
        calls: Arc<AtomicUsize>,
    }

    impl RpcSender for NonRetryableRpcSender {
        async fn send(
            &self,
            _transaction: &VersionedTransaction,
        ) -> Result<Signature, ClientError> {
            self.calls.fetch_add(1, Ordering::Relaxed);

            Err(ClientError::from(
                solana_client::client_error::ClientErrorKind::Custom(
                    "validation error".to_string(),
                ),
            ))
        }

        fn classify_error(&self, error: &ClientError) -> SubmissionError {
            SubmissionError {
                class: RetryClass::NonRetryable,
                message: error.to_string(),
            }
        }
    }

    #[tokio::test]
    async fn non_retryable_submission_error_is_not_retried() {
        let rpc_calls = Arc::new(AtomicUsize::new(0));

        let engine = ExecutionEngine::new(
            FakeBlockhashSource,
            NonRetryableRpcSender {
                calls: Arc::clone(&rpc_calls),
            },
            FakeJitoSender {
                calls: Arc::new(AtomicUsize::new(0)),
            },
            ConfirmationTracker::new(FakeStatusProvider),
            Duration::from_secs(1),
            RetryPolicy::new(3, Duration::from_millis(1), Duration::from_millis(10)).unwrap(),
            CancellationToken::new(),
            Duration::from_secs(30),
        );

        let request = ExecutionRequest {
            instructions: vec![test_instruction()],
            payer: Keypair::new(),
            route: ExecutionRoute::SolanaRpc,
        };

        let result = engine.execute(request).await;

        assert!(matches!(result, Err(ExecutionError::SubmissionFailed)));

        assert_eq!(rpc_calls.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn retry_stops_at_max_attempts() {
        let rpc_calls = Arc::new(AtomicUsize::new(0));

        let engine = ExecutionEngine::new(
            FakeBlockhashSource,
            AlwaysRetryableRpcSender {
                calls: Arc::clone(&rpc_calls),
            },
            FakeJitoSender {
                calls: Arc::new(AtomicUsize::new(0)),
            },
            ConfirmationTracker::new(FakeStatusProvider),
            Duration::from_secs(1),
            RetryPolicy::new(3, Duration::from_millis(1), Duration::from_millis(10)).unwrap(),
            CancellationToken::new(),
            Duration::from_secs(30),
        );

        let request = ExecutionRequest {
            instructions: vec![test_instruction()],
            payer: Keypair::new(),
            route: ExecutionRoute::SolanaRpc,
        };

        let result = engine.execute(request).await;

        assert!(matches!(result, Err(ExecutionError::SubmissionFailed)));

        assert_eq!(rpc_calls.load(Ordering::Relaxed), 3);
    }

    #[tokio::test]
    async fn execution_failure_is_never_retried() {
        let rpc_calls = Arc::new(AtomicUsize::new(0));

        let engine = ExecutionEngine::new(
            FakeBlockhashSource,
            FakeRpcSender {
                calls: Arc::clone(&rpc_calls),
            },
            FakeJitoSender {
                calls: Arc::new(AtomicUsize::new(0)),
            },
            ConfirmationTracker::new(FailedStatusProvider),
            Duration::from_secs(1),
            RetryPolicy::new(5, Duration::from_millis(1), Duration::from_millis(10)).unwrap(),
            CancellationToken::new(),
            Duration::from_secs(30),
        );

        let request = ExecutionRequest {
            instructions: vec![test_instruction()],
            payer: Keypair::new(),
            route: ExecutionRoute::SolanaRpc,
        };

        let result = engine.execute(request).await;

        assert!(matches!(result, Err(ExecutionError::ExecutionFailed)));

        assert_eq!(rpc_calls.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn valid_blockhash_can_reuse_same_prepared_transaction() {
        let blockhash_calls = Arc::new(AtomicUsize::new(0));
        let rpc_calls = Arc::new(AtomicUsize::new(0));

        let engine = ExecutionEngine::new(
            CountingBlockhashSource {
                calls: Arc::clone(&blockhash_calls),
            },
            RetryOnceRpcSender {
                calls: Arc::clone(&rpc_calls),
            },
            FakeJitoSender {
                calls: Arc::new(AtomicUsize::new(0)),
            },
            ConfirmationTracker::new(FakeStatusProvider),
            Duration::from_secs(1),
            RetryPolicy::new(3, Duration::from_millis(1), Duration::from_millis(10)).unwrap(),
            CancellationToken::new(),
            Duration::from_secs(30),
        );

        let request = ExecutionRequest {
            instructions: vec![test_instruction()],
            payer: Keypair::new(),
            route: ExecutionRoute::SolanaRpc,
        };

        let result = engine.execute(request).await;

        assert!(result.is_ok());

        // Первый send упал, второй прошёл.
        assert_eq!(rpc_calls.load(Ordering::Relaxed), 2);

        // Новый blockhash НЕ запрашивали.
        assert_eq!(blockhash_calls.load(Ordering::Relaxed), 1);
    }

    struct FakeBlockhashSource;

    impl BlockhashSource for FakeBlockhashSource {
        async fn latest(&self) -> Result<BlockhashInfo, ClientError> {
            Ok(BlockhashInfo::new(Hash::new_unique(), 1_000))
        }

        async fn current_block_height(&self) -> Result<u64, ClientError> {
            Ok(500)
        }
    }

    struct FakeRpcSender {
        calls: Arc<AtomicUsize>,
    }

    impl RpcSender for FakeRpcSender {
        async fn send(
            &self,
            _transaction: &VersionedTransaction,
        ) -> Result<Signature, ClientError> {
            self.calls.fetch_add(1, Ordering::Relaxed);

            Ok(Signature::default())
        }
    }

    struct FakeJitoSender {
        calls: Arc<AtomicUsize>,
    }

    impl JitoSender for FakeJitoSender {
        async fn send(&self, _transaction: &VersionedTransaction) -> Result<Signature, JitoError> {
            self.calls.fetch_add(1, Ordering::Relaxed);

            Ok(Signature::default())
        }
    }

    struct FakeStatusProvider;

    impl SignatureStatusProvider for FakeStatusProvider {
        async fn get_status(
            &self,
            _signature: &Signature,
        ) -> Result<ConfirmationStatus, ClientError> {
            Ok(ConfirmationStatus::Confirmed)
        }
    }

    fn test_instruction() -> Instruction {
        Instruction {
            program_id: Pubkey::new_unique(),
            accounts: vec![AccountMeta {
                pubkey: Pubkey::new_unique(),
                is_signer: false,
                is_writable: false,
            }],
            data: vec![1],
        }
    }

    #[tokio::test]
    async fn rpc_route_uses_rpc_sender() {
        let rpc_calls = Arc::new(AtomicUsize::new(0));
        let jito_calls = Arc::new(AtomicUsize::new(0));

        let engine = ExecutionEngine::new(
            FakeBlockhashSource,
            FakeRpcSender {
                calls: Arc::clone(&rpc_calls),
            },
            FakeJitoSender {
                calls: Arc::clone(&jito_calls),
            },
            ConfirmationTracker::new(FakeStatusProvider),
            Duration::from_secs(1),
            RetryPolicy::new(3, Duration::from_millis(1), Duration::from_millis(10)).unwrap(),
            CancellationToken::new(),
            Duration::from_secs(30),
        );

        let request = ExecutionRequest {
            instructions: vec![test_instruction()],
            payer: Keypair::new(),

            route: ExecutionRoute::SolanaRpc,
        };

        let result = engine.execute(request).await;

        assert!(result.is_ok());

        assert_eq!(rpc_calls.load(Ordering::Relaxed), 1);

        assert_eq!(jito_calls.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn jito_route_uses_jito_sender() {
        let rpc_calls = Arc::new(AtomicUsize::new(0));
        let jito_calls = Arc::new(AtomicUsize::new(0));

        let engine = ExecutionEngine::new(
            FakeBlockhashSource,
            FakeRpcSender {
                calls: Arc::clone(&rpc_calls),
            },
            FakeJitoSender {
                calls: Arc::clone(&jito_calls),
            },
            ConfirmationTracker::new(FakeStatusProvider),
            Duration::from_secs(1),
            RetryPolicy::new(3, Duration::from_millis(1), Duration::from_millis(10)).unwrap(),
            CancellationToken::new(),
            Duration::from_secs(30),
        );

        let request = ExecutionRequest {
            instructions: vec![test_instruction()],
            payer: Keypair::new(),

            route: ExecutionRoute::Jito,
        };

        let result = engine.execute(request).await;

        assert!(result.is_ok());

        assert_eq!(rpc_calls.load(Ordering::Relaxed), 0);

        assert_eq!(jito_calls.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn successful_submission_is_confirmed() {
        let rpc_calls = Arc::new(AtomicUsize::new(0));
        let jito_calls = Arc::new(AtomicUsize::new(0));

        let engine = ExecutionEngine::new(
            FakeBlockhashSource,
            FakeRpcSender {
                calls: Arc::clone(&rpc_calls),
            },
            FakeJitoSender {
                calls: Arc::clone(&jito_calls),
            },
            ConfirmationTracker::new(FakeStatusProvider),
            Duration::from_secs(1),
            RetryPolicy::new(3, Duration::from_millis(1), Duration::from_millis(10)).unwrap(),
            CancellationToken::new(),
            Duration::from_secs(30),
        );

        let request = ExecutionRequest {
            instructions: vec![test_instruction()],
            payer: Keypair::new(),
            route: ExecutionRoute::SolanaRpc,
        };

        let outcome = engine
            .execute(request)
            .await
            .expect("execution should succeed");

        assert!(matches!(
            outcome.confirmation,
            ConfirmationStatus::Confirmed
        ));
    }

    struct FakeFinalizedStatusProvider;

    impl SignatureStatusProvider for FakeFinalizedStatusProvider {
        async fn get_status(
            &self,
            _signature: &Signature,
        ) -> Result<ConfirmationStatus, ClientError> {
            Ok(ConfirmationStatus::Finalized)
        }
    }

    #[tokio::test]
    async fn finalized_satisfies_confirmed_target() {
        let rpc_calls = Arc::new(AtomicUsize::new(0));
        let jito_calls = Arc::new(AtomicUsize::new(0));

        let engine = ExecutionEngine::new(
            FakeBlockhashSource,
            FakeRpcSender {
                calls: Arc::clone(&rpc_calls),
            },
            FakeJitoSender {
                calls: Arc::clone(&jito_calls),
            },
            ConfirmationTracker::new(FakeFinalizedStatusProvider),
            Duration::from_secs(1),
            RetryPolicy::new(3, Duration::from_millis(1), Duration::from_millis(10)).unwrap(),
            CancellationToken::new(),
            Duration::from_secs(30),
        );

        let request = ExecutionRequest {
            instructions: vec![test_instruction()],
            payer: Keypair::new(),
            route: ExecutionRoute::SolanaRpc,
        };

        let outcome = engine
            .execute(request)
            .await
            .expect("finalized must satisfy confirmation");

        assert!(matches!(
            outcome.confirmation,
            ConfirmationStatus::Finalized
        ));
    }

    struct FailingRpcSender;

    impl RpcSender for FailingRpcSender {
        async fn send(
            &self,
            _transaction: &VersionedTransaction,
        ) -> Result<Signature, ClientError> {
            Err(solana_client::client_error::ClientError::from(
                solana_client::client_error::ClientErrorKind::Custom("fake rpc error".to_string()),
            ))
        }
    }

    struct CountingStatusProvider {
        calls: Arc<AtomicUsize>,
    }

    impl SignatureStatusProvider for CountingStatusProvider {
        async fn get_status(
            &self,
            _signature: &Signature,
        ) -> Result<ConfirmationStatus, ClientError> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            Ok(ConfirmationStatus::Confirmed)
        }
    }

    #[tokio::test]
    async fn submission_error_does_not_start_confirmation() {
        let confirmation_calls = Arc::new(AtomicUsize::new(0));

        let engine = ExecutionEngine::new(
            FakeBlockhashSource,
            FailingRpcSender,
            FakeJitoSender {
                calls: Arc::new(AtomicUsize::new(0)),
            },
            ConfirmationTracker::new(CountingStatusProvider {
                calls: Arc::clone(&confirmation_calls),
            }),
            Duration::from_secs(1),
            RetryPolicy::new(3, Duration::from_millis(1), Duration::from_millis(10)).unwrap(),
            CancellationToken::new(),
            Duration::from_secs(30),
        );

        let request = ExecutionRequest {
            instructions: vec![test_instruction()],
            payer: Keypair::new(),
            route: ExecutionRoute::SolanaRpc,
        };

        let result = engine.execute(request).await;

        assert!(
            matches!(result, Err(ExecutionError::SubmissionFailed)),
            "Ожидалась ошибка InvalidData, но получено: {:?}",
            result
        );

        assert_eq!(confirmation_calls.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn retry_waits_before_second_submission() {
        let rpc_calls = Arc::new(AtomicUsize::new(0));

        let engine = ExecutionEngine::new(
            FakeBlockhashSource,
            RetryOnceRpcSender {
                calls: Arc::clone(&rpc_calls),
            },
            FakeJitoSender {
                calls: Arc::new(AtomicUsize::new(0)),
            },
            ConfirmationTracker::new(FakeStatusProvider),
            Duration::from_secs(1),
            // специально заметная задержка
            RetryPolicy::new(3, Duration::from_millis(100), Duration::from_millis(100)).unwrap(),
            CancellationToken::new(),
            Duration::from_secs(30),
        );

        let request = ExecutionRequest {
            instructions: vec![test_instruction()],
            payer: Keypair::new(),
            route: ExecutionRoute::SolanaRpc,
        };

        let mut execution = Box::pin(engine.execute(request));

        // Через 20ms retry ещё НЕ должен успеть завершиться,
        // потому что policy требует ждать 100ms.
        let early = tokio::time::timeout(Duration::from_millis(20), &mut execution).await;

        assert!(
            early.is_err(),
            "execution retried immediately instead of waiting for retry delay"
        );

        // А дальше даём достаточно времени на sleep + второй успешный send.
        let result = tokio::time::timeout(Duration::from_millis(300), &mut execution)
            .await
            .expect("execution should finish after retry delay");

        assert!(result.is_ok());
        assert_eq!(rpc_calls.load(Ordering::Relaxed), 2);
    }

    #[tokio::test]
    async fn confirmation_timeout_does_not_blindly_resubmit() {
        let rpc_calls = Arc::new(AtomicUsize::new(0));

        let engine = ExecutionEngine::new(
            FakeBlockhashSource,
            FakeRpcSender {
                calls: Arc::clone(&rpc_calls),
            },
            FakeJitoSender {
                calls: Arc::new(AtomicUsize::new(0)),
            },
            ConfirmationTracker::new(NeverConfirmedProvider),
            Duration::from_millis(10),
            RetryPolicy::new(5, Duration::from_millis(1), Duration::from_millis(10)).unwrap(),
            CancellationToken::new(),
            Duration::from_secs(30),
        );

        let request = ExecutionRequest {
            instructions: vec![test_instruction()],
            payer: Keypair::new(),
            route: ExecutionRoute::SolanaRpc,
        };

        let result = engine.execute(request).await;

        assert!(matches!(result, Err(ExecutionError::ConfirmationTimeout)));

        assert_eq!(rpc_calls.load(Ordering::Relaxed), 1);
    }

    struct ExpiringBlockhashSource {
        latest_calls: Arc<AtomicUsize>,
        height_calls: Arc<AtomicUsize>,
    }

    impl BlockhashSource for ExpiringBlockhashSource {
        async fn latest(&self) -> Result<BlockhashInfo, ClientError> {
            let call = self.latest_calls.fetch_add(1, Ordering::Relaxed);

            if call == 0 {
                // blockhash A
                // жив до height 1000
                Ok(BlockhashInfo::new(Hash::new_unique(), 1_000))
            } else {
                // blockhash B
                // новый, ещё живой
                Ok(BlockhashInfo::new(Hash::new_unique(), 2_000))
            }
        }

        async fn current_block_height(&self) -> Result<u64, ClientError> {
            self.height_calls.fetch_add(1, Ordering::Relaxed);

            // Старый blockhash с last_valid = 1000 уже expired.
            Ok(1_001)
        }
    }

    #[tokio::test]
    async fn expired_blockhash_causes_rebuild() {
        let latest_calls = Arc::new(AtomicUsize::new(0));
        let height_calls = Arc::new(AtomicUsize::new(0));
        let rpc_calls = Arc::new(AtomicUsize::new(0));

        let engine = ExecutionEngine::new(
            ExpiringBlockhashSource {
                latest_calls: Arc::clone(&latest_calls),
                height_calls: Arc::clone(&height_calls),
            },
            RetryOnceRpcSender {
                calls: Arc::clone(&rpc_calls),
            },
            FakeJitoSender {
                calls: Arc::new(AtomicUsize::new(0)),
            },
            ConfirmationTracker::new(FakeStatusProvider),
            Duration::from_secs(1),
            RetryPolicy::new(3, Duration::from_millis(1), Duration::from_millis(10)).unwrap(),
            CancellationToken::new(),
            Duration::from_secs(30),
        );

        let request = ExecutionRequest {
            instructions: vec![test_instruction()],
            payer: Keypair::new(),
            route: ExecutionRoute::SolanaRpc,
        };

        let result = engine.execute(request).await;

        assert!(result.is_ok());

        // Первый send падает, второй проходит.
        assert_eq!(rpc_calls.load(Ordering::Relaxed), 2);

        // Перед retry проверили freshness.
        assert_eq!(height_calls.load(Ordering::Relaxed), 1);

        // A expired → запросили B.
        assert_eq!(latest_calls.load(Ordering::Relaxed), 2);
    }

    struct FailedStatusProvider;

    impl SignatureStatusProvider for FailedStatusProvider {
        async fn get_status(
            &self,
            _signature: &Signature,
        ) -> Result<ConfirmationStatus, ClientError> {
            Ok(ConfirmationStatus::Failed("failed".to_string()))
        }
    }

    #[tokio::test]
    async fn execution_failure_is_not_reported_as_transport_error() {
        let engine = ExecutionEngine::new(
            FakeBlockhashSource,
            FakeRpcSender {
                calls: Arc::new(AtomicUsize::new(0)),
            },
            FakeJitoSender {
                calls: Arc::new(AtomicUsize::new(0)),
            },
            ConfirmationTracker::new(FailedStatusProvider),
            Duration::from_secs(1),
            RetryPolicy::new(3, Duration::from_millis(1), Duration::from_millis(10)).unwrap(),
            CancellationToken::new(),
            Duration::from_secs(30),
        );

        let request = ExecutionRequest {
            instructions: vec![test_instruction()],
            payer: Keypair::new(),
            route: ExecutionRoute::SolanaRpc,
        };

        let result = engine.execute(request).await;

        assert!(
            matches!(result, Err(ExecutionError::ExecutionFailed)),
            "Ожидалась ошибка InvalidData, но получено: {:?}",
            result
        );
    }

    struct NeverConfirmedProvider;

    impl SignatureStatusProvider for NeverConfirmedProvider {
        async fn get_status(
            &self,
            _signature: &Signature,
        ) -> Result<ConfirmationStatus, ClientError> {
            Ok(ConfirmationStatus::NotFound)
        }
    }

    #[tokio::test]
    async fn confirmation_timeout_is_distinct() {
        let engine = ExecutionEngine::new(
            FakeBlockhashSource,
            FakeRpcSender {
                calls: Arc::new(AtomicUsize::new(0)),
            },
            FakeJitoSender {
                calls: Arc::new(AtomicUsize::new(0)),
            },
            ConfirmationTracker::new(NeverConfirmedProvider),
            // маленький timeout, чтобы тест не ждал долго
            Duration::from_millis(10),
            RetryPolicy::new(3, Duration::from_millis(1), Duration::from_millis(10)).unwrap(),
            CancellationToken::new(),
            Duration::from_secs(30),
        );

        let request = ExecutionRequest {
            instructions: vec![test_instruction()],
            payer: Keypair::new(),
            route: ExecutionRoute::SolanaRpc,
        };

        let result = engine.execute(request).await;

        assert!(
            matches!(result, Err(ExecutionError::ConfirmationTimeout)),
            "Ожидалась ошибка InvalidData, но получено: {:?}",
            result
        );
    }

    struct CountingBlockhashSource {
        calls: Arc<AtomicUsize>,
    }

    impl BlockhashSource for CountingBlockhashSource {
        async fn latest(&self) -> Result<BlockhashInfo, ClientError> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            Ok(BlockhashInfo::new(Hash::new_unique(), 1_000))
        }

        async fn current_block_height(&self) -> Result<u64, ClientError> {
            Ok(500)
        }
    }

    #[tokio::test]
    async fn fresh_blockhash_is_requested_for_execution() {
        let blockhash_calls = Arc::new(AtomicUsize::new(0));

        let engine = ExecutionEngine::new(
            CountingBlockhashSource {
                calls: Arc::clone(&blockhash_calls),
            },
            FakeRpcSender {
                calls: Arc::new(AtomicUsize::new(0)),
            },
            FakeJitoSender {
                calls: Arc::new(AtomicUsize::new(0)),
            },
            ConfirmationTracker::new(FakeStatusProvider),
            Duration::from_secs(1),
            RetryPolicy::new(3, Duration::from_millis(1), Duration::from_millis(10)).unwrap(),
            CancellationToken::new(),
            Duration::from_secs(30),
        );

        let request = ExecutionRequest {
            instructions: vec![test_instruction()],
            payer: Keypair::new(),
            route: ExecutionRoute::SolanaRpc,
        };

        let result = engine.execute(request).await;

        assert!(result.is_ok());

        assert_eq!(blockhash_calls.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn cancellation_before_submission_sends_nothing() {
        let rpc_calls = Arc::new(AtomicUsize::new(0));

        let cancellation_token = CancellationToken::new();
        cancellation_token.cancel();

        let engine = ExecutionEngine::new(
            FakeBlockhashSource,
            FakeRpcSender {
                calls: Arc::clone(&rpc_calls),
            },
            FakeJitoSender {
                calls: Arc::new(AtomicUsize::new(0)),
            },
            ConfirmationTracker::new(FakeStatusProvider),
            Duration::from_secs(1),
            RetryPolicy::new(3, Duration::from_millis(1), Duration::from_millis(10)).unwrap(),
            cancellation_token,
            Duration::from_secs(30),
        );

        let request = ExecutionRequest {
            instructions: vec![test_instruction()],
            payer: Keypair::new(),
            route: ExecutionRoute::SolanaRpc,
        };

        let result = engine.execute(request).await;

        assert!(matches!(result, Err(ExecutionError::Cancelled)));

        assert_eq!(rpc_calls.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn cancellation_stops_retry_loop() {
        let rpc_calls = Arc::new(AtomicUsize::new(0));

        let cancellation_token = CancellationToken::new();

        let token_clone = cancellation_token.clone();

        let engine = ExecutionEngine::new(
            FakeBlockhashSource,
            AlwaysRetryableRpcSender {
                calls: Arc::clone(&rpc_calls),
            },
            FakeJitoSender {
                calls: Arc::new(AtomicUsize::new(0)),
            },
            ConfirmationTracker::new(FakeStatusProvider),
            Duration::from_secs(1),
            RetryPolicy::new(100, Duration::from_secs(10), Duration::from_secs(10)).unwrap(),
            cancellation_token,
            Duration::from_secs(30),
        );

        let request = ExecutionRequest {
            instructions: vec![test_instruction()],
            payer: Keypair::new(),
            route: ExecutionRoute::SolanaRpc,
        };

        let result = tokio::spawn(async move { engine.execute(request).await });

        let rpc_clone = rpc_calls.clone();

        tokio::spawn(async move {
            print!(
                "second task check count = {}",
                rpc_clone.load(Ordering::Relaxed)
            );
            while rpc_clone.load(Ordering::Relaxed) == 0 {
                print!("second task == 0");
                tokio::task::yield_now().await;
            }

            print!("start cancel");
            token_clone.cancel();
        });

        let result = result.await.unwrap();

        assert!(matches!(result, Err(ExecutionError::Cancelled)));

        assert_eq!(rpc_calls.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn whole_operation_deadline_stops_retries() {
        let rpc_calls = Arc::new(AtomicUsize::new(0));

        let cancellation_token = CancellationToken::new();

        let engine = ExecutionEngine::new(
            FakeBlockhashSource,
            AlwaysRetryableRpcSender {
                calls: Arc::clone(&rpc_calls),
            },
            FakeJitoSender {
                calls: Arc::new(AtomicUsize::new(0)),
            },
            ConfirmationTracker::new(FakeStatusProvider),
            Duration::from_secs(1),
            RetryPolicy::new(100, Duration::from_millis(20), Duration::from_millis(50)).unwrap(),
            cancellation_token,
            Duration::from_millis(50),
        );

        let request = ExecutionRequest {
            instructions: vec![test_instruction()],
            payer: Keypair::new(),
            route: ExecutionRoute::SolanaRpc,
        };

        let result = engine.execute(request).await;

        match result {
            Err(ExecutionError::OutcomeUnknown { signature }) => {
                assert_ne!(signature, Signature::default());
            }

            other => panic!("expected OutcomeUnknown, got: {other:?}"),
        }

        assert_eq!(rpc_calls.load(Ordering::Relaxed), 2);
    }

    #[tokio::test]
    async fn whole_operation_deadline_stops_confirmation() {
        let rpc_calls = Arc::new(AtomicUsize::new(0));

        let cancellation_token = CancellationToken::new();

        let engine = ExecutionEngine::new(
            FakeBlockhashSource,
            FakeRpcSender {
                calls: Arc::clone(&rpc_calls),
            },
            FakeJitoSender {
                calls: Arc::new(AtomicUsize::new(0)),
            },
            ConfirmationTracker::new(NeverConfirmedProvider),
            Duration::from_secs(1),
            RetryPolicy::new(100, Duration::from_millis(20), Duration::from_millis(50)).unwrap(),
            cancellation_token,
            Duration::from_millis(50),
        );

        let request = ExecutionRequest {
            instructions: vec![test_instruction()],
            payer: Keypair::new(),
            route: ExecutionRoute::SolanaRpc,
        };

        let result = engine.execute(request).await;

        match result {
            Err(ExecutionError::OutcomeUnknown { signature }) => {
                assert_ne!(signature, Signature::default());
            }

            other => panic!("expected OutcomeUnknown, got: {other:?}"),
        }

        assert_eq!(rpc_calls.load(Ordering::Relaxed), 1);
    }

    struct HangingBlockhashSource;

    impl BlockhashSource for HangingBlockhashSource {
        async fn latest(&self) -> Result<BlockhashInfo, ClientError> {
            std::future::pending::<Result<BlockhashInfo, ClientError>>().await
        }

        async fn current_block_height(&self) -> Result<u64, ClientError> {
            panic!("current_block_height must not be called");
        }
    }

    #[tokio::test]
    async fn whole_operation_deadline_stops_blockhash_fetch() {
        let rpc_calls = Arc::new(AtomicUsize::new(0));

        let cancellation_token = CancellationToken::new();

        let engine = ExecutionEngine::new(
            HangingBlockhashSource,
            FakeRpcSender {
                calls: Arc::clone(&rpc_calls),
            },
            FakeJitoSender {
                calls: Arc::new(AtomicUsize::new(0)),
            },
            ConfirmationTracker::new(NeverConfirmedProvider),
            Duration::from_secs(1),
            RetryPolicy::new(100, Duration::from_millis(20), Duration::from_millis(50)).unwrap(),
            cancellation_token,
            Duration::from_millis(50),
        );

        let request = ExecutionRequest {
            instructions: vec![test_instruction()],
            payer: Keypair::new(),
            route: ExecutionRoute::SolanaRpc,
        };

        let result = engine.execute(request).await;

        assert!(matches!(result, Err(ExecutionError::DeadlineExceeded)));

        assert_eq!(rpc_calls.load(Ordering::Relaxed), 0);
    }
}
