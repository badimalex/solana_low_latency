// use super::ExecutionRequest;

// pub struct ExecutionEngine<B, T, R, J, C> {
//     blockhash_source: B,
//     transaction_builder: T,
//     rpc_sender: R,
//     jito_sender: J,
//     confirmation_tracker: C,
// }

// impl<B, T, R, J, C> ExecutionEngine<B, T, R, J, C> {
//     pub fn new(
//         blockhash_source: B,
//         transaction_builder: T,
//         rpc_sender: R,
//         jito_sender: J,
//         confirmation_tracker: C,
//     ) -> Self {
//         Self {
//             blockhash_source,
//             transaction_builder,
//             rpc_sender,
//             jito_sender,
//             confirmation_tracker,
//         }
//     }

//     pub fn execute(&self, request: ExecutionRequest) {
//         // TODO: ТВОЯ ЛОГИКА ЗДЕСЬ
//     }
// }

use std::time::Duration;

use crate::{
    blockhash::BlockhashSource,
    confirmation::{ConfirmationError, ConfirmationTracker, SignatureStatusProvider},
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
    ) -> Self {
        Self {
            blockhash_source,
            rpc_sender,
            jito_sender,
            confirmation_tracker,
            confirmation_timeout,
        }
    }

    pub async fn execute(
        &self,
        request: ExecutionRequest,
    ) -> Result<ExecutionOutcome, ExecutionError> {
        let blockhash_info = self
            .blockhash_source
            .latest()
            .await
            .map_err(|_| ExecutionError::PreparationFailed)?;

        let transaction = transaction_builder::build_v0_transaction(
            &request.payer,
            request.instructions,
            blockhash_info.blockhash,
            vec![],
        )
        .map_err(|_| ExecutionError::PreparationFailed)?;

        let signature = match request.route {
            ExecutionRoute::SolanaRpc => self
                .rpc_sender
                .send(&transaction)
                .await
                .map_err(|_| ExecutionError::SubmissionFailed)?,

            ExecutionRoute::Jito => self
                .jito_sender
                .send(&transaction)
                .await
                .map_err(|_| ExecutionError::SubmissionFailed)?,
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

    use crate::{
        blockhash::{BlockhashInfo, BlockhashSource},
        confirmation::{
            ConfirmationStatus::{self, Confirmed},
            ConfirmationTracker, SignatureStatusProvider,
        },
        jito::error::JitoError,
    };

    use super::*;
    use crate::execution::sender::{JitoSender, RpcSender};

    struct FakeBlockhashSource;

    impl BlockhashSource for FakeBlockhashSource {
        async fn latest(&self) -> Result<BlockhashInfo, ClientError> {
            Ok(BlockhashInfo::new(Hash::new_unique(), 1_000))
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
}
