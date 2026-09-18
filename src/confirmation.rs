#[cfg(test)]
use std::collections::VecDeque;
use std::time::Duration;

use solana_client::{client_error::ClientError, nonblocking::rpc_client::RpcClient};
use solana_keypair::Signature;
use tokio::time::sleep;

#[cfg(test)]
use tokio::sync::Mutex;

#[allow(async_fn_in_trait)]
pub trait SignatureStatusProvider: Send + Sync {
    async fn get_status(&self, _signature: &Signature) -> Result<ConfirmationStatus, ClientError>;
}

pub struct RpcStatusProvider {
    rpc: RpcClient,
}
#[allow(async_fn_in_trait)]
impl SignatureStatusProvider for RpcStatusProvider {
    async fn get_status(&self, signature: &Signature) -> Result<ConfirmationStatus, ClientError> {
        let statuses = self
            .rpc
            .get_signature_statuses_with_history(&[*signature])
            .await?;
        println!("stat : {:?}", statuses);

        let status = match &statuses.value[0] {
            None => return Ok(ConfirmationStatus::NotFound),
            Some(s) => s,
        };
        if let Some(error) = &status.err {
            return Ok(ConfirmationStatus::Failed(error.to_string()));
        }

        match status.confirmation_status() {
            solana_client::rpc_response::TransactionConfirmationStatus::Processed => {
                Ok(ConfirmationStatus::Processed)
            }
            solana_client::rpc_response::TransactionConfirmationStatus::Confirmed => {
                Ok(ConfirmationStatus::Confirmed)
            }
            solana_client::rpc_response::TransactionConfirmationStatus::Finalized => {
                Ok(ConfirmationStatus::Finalized)
            }
        }
    }
}

#[cfg(test)]
struct FakeStatusProvider {
    responses: Mutex<VecDeque<Result<ConfirmationStatus, ClientError>>>,
}

#[cfg(test)]
impl FakeStatusProvider {
    fn new(responses: Vec<Result<ConfirmationStatus, ClientError>>) -> Self {
        Self {
            responses: Mutex::new(responses.into()),
        }
    }
}

#[cfg(test)]
impl SignatureStatusProvider for FakeStatusProvider {
    async fn get_status(&self, _signature: &Signature) -> Result<ConfirmationStatus, ClientError> {
        let mut lock = self.responses.lock().await;
        lock.pop_front().expect("fake status queue is empty")
    }
}

#[derive(Debug)]
pub enum ConfirmationStatus {
    NotFound,
    Processed,
    Confirmed,
    Finalized,
    Failed(String),
}

#[derive(Debug)]
pub enum ConfirmationError {
    Rpc(ClientError),
    TransactionFailed(String),
    Timeout,
}

pub struct ConfirmationTracker<P: SignatureStatusProvider> {
    provider: P,
}

impl<P: SignatureStatusProvider> ConfirmationTracker<P> {
    pub fn new(provider: P) -> Self {
        Self { provider }
    }

    pub async fn wait_until_confirmed(
        &self,
        signature: &Signature,
        timeout: Duration,
    ) -> Result<ConfirmationStatus, ConfirmationError> {
        let retry_interval = Duration::from_millis(500);

        tokio::time::timeout(timeout, async {
            loop {
                match self.provider.get_status(signature).await {
                    Err(error) => {
                        return Err(ConfirmationError::Rpc(error));
                    }

                    Ok(status) => match status {
                        ConfirmationStatus::NotFound | ConfirmationStatus::Processed => {
                            sleep(retry_interval).await;
                        }

                        ConfirmationStatus::Confirmed => {
                            return Ok(ConfirmationStatus::Confirmed);
                        }
                        ConfirmationStatus::Finalized => {
                            return Ok(ConfirmationStatus::Finalized);
                        }
                        ConfirmationStatus::Failed(e) => {
                            return Err(ConfirmationError::TransactionFailed(e));
                        }
                    },
                }
            }
        })
        .await
        .map_err(|_| ConfirmationError::Timeout)?
    }
}

#[cfg(test)]
mod tests {
    // use solana_hash::Hash;

    // use crate::blockhash::BlockhashInfo;

    use std::time::Duration;

    use solana_client::client_error::{ClientError, ClientErrorKind};
    use solana_keypair::Signature;

    use crate::confirmation::{
        ConfirmationError,
        ConfirmationStatus::{self},
        ConfirmationTracker, FakeStatusProvider,
    };

    #[tokio::test]
    async fn waits_until_confirmed() {
        let fake = FakeStatusProvider::new(vec![
            Ok(ConfirmationStatus::NotFound),
            Ok(ConfirmationStatus::Processed),
            Ok(ConfirmationStatus::Confirmed),
        ]);

        let tracker = ConfirmationTracker::new(fake);
        let signature = Signature::default();

        // 3. вызвать:
        let res = tracker
            .wait_until_confirmed(&signature, Duration::from_secs(2))
            .await
            .unwrap();

        assert!(matches!(res, ConfirmationStatus::Confirmed));
    }

    #[tokio::test]
    async fn waits_until_finalized() {
        let fake = FakeStatusProvider::new(vec![
            Ok(ConfirmationStatus::NotFound),
            Ok(ConfirmationStatus::Finalized),
        ]);

        let tracker = ConfirmationTracker::new(fake);
        let signature = Signature::default();

        // 3. вызвать:
        let res = tracker
            .wait_until_confirmed(&signature, Duration::from_secs(2))
            .await
            .unwrap();

        assert!(matches!(res, ConfirmationStatus::Finalized));
    }

    #[tokio::test]
    async fn waits_until_failed() {
        let fake = FakeStatusProvider::new(vec![
            Ok(ConfirmationStatus::Processed),
            Ok(ConfirmationStatus::Failed("boom".to_string())),
        ]);

        let tracker = ConfirmationTracker::new(fake);
        let signature = Signature::default();

        // 3. вызвать:
        let result = tracker
            .wait_until_confirmed(&signature, Duration::from_secs(2))
            .await
            .unwrap_err();

        assert!(matches!(result, ConfirmationError::TransactionFailed(_)));
    }

    #[tokio::test]
    async fn waits_until_timeout() {
        let fake = FakeStatusProvider::new(vec![
            Ok(ConfirmationStatus::NotFound),
            Ok(ConfirmationStatus::NotFound),
            Ok(ConfirmationStatus::NotFound),
            Ok(ConfirmationStatus::NotFound),
        ]);

        let tracker = ConfirmationTracker::new(fake);
        let signature = Signature::default();

        // 3. вызвать:
        let result = tracker
            .wait_until_confirmed(&signature, Duration::from_secs(2))
            .await
            .unwrap_err();

        assert!(matches!(result, ConfirmationError::Timeout));
    }

    #[tokio::test]
    async fn waits_until_err() {
        let fake = FakeStatusProvider::new(vec![
            Ok(ConfirmationStatus::NotFound),
            Err(ClientError::from(ClientErrorKind::Custom(
                "fake rpc error".to_string(),
            ))),
        ]);

        let tracker = ConfirmationTracker::new(fake);
        let signature = Signature::default();

        // 3. вызвать:
        let result = tracker
            .wait_until_confirmed(&signature, Duration::from_secs(2))
            .await
            .unwrap_err();

        assert!(matches!(result, ConfirmationError::Rpc(_)));
    }
}
