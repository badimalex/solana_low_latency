use solana_client::client_error::ClientError;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_keypair::Signature;
use solana_transaction::versioned::VersionedTransaction;

use crate::jito::client::JitoClient;
use crate::jito::error::JitoError;

#[allow(async_fn_in_trait)]
pub trait RpcSender: Send + Sync {
    async fn send(&self, transaction: &VersionedTransaction) -> Result<Signature, ClientError>;
}

pub struct SolanaRpcSender {
    rpc: RpcClient,
}

impl SolanaRpcSender {
    pub fn new(rpc: RpcClient) -> Self {
        Self { rpc }
    }
}

impl RpcSender for SolanaRpcSender {
    async fn send(&self, transaction: &VersionedTransaction) -> Result<Signature, ClientError> {
        let signature = self.rpc.send_transaction(transaction).await?;
        Ok(signature)
    }
}

#[allow(async_fn_in_trait)]
pub trait JitoSender: Send + Sync {
    async fn send(&self, transaction: &VersionedTransaction) -> Result<Signature, JitoError>;
}
impl JitoSender for JitoClient {
    async fn send(&self, transaction: &VersionedTransaction) -> Result<Signature, JitoError> {
        let signature = self.send_transaction(transaction).await?;
        Ok(signature)
    }
}
