use solana_client::{client_error::ClientError, nonblocking::rpc_client::RpcClient};
use solana_commitment_config::CommitmentConfig;
use solana_hash::Hash;

#[derive(Debug, Clone)]
pub struct BlockhashInfo {
    pub blockhash: Hash,
    pub last_valid_block_height: u64,
}

impl BlockhashInfo {
    pub fn new(blockhash: Hash, last_valid_block_height: u64) -> Self {
        Self {
            blockhash,
            last_valid_block_height,
        }
    }

    pub fn is_expired(&self, current_block_height: u64) -> bool {
        current_block_height > self.last_valid_block_height
    }

    pub fn remaining_blocks(&self, current_block_height: u64) -> u64 {
        self.last_valid_block_height
            .saturating_sub(current_block_height)
    }
}

pub async fn fetch_latest(
    rpc_client: &RpcClient,
    commitment: CommitmentConfig,
) -> Result<BlockhashInfo, ClientError> {
    let (blockhash, last_valid_block_height) = rpc_client
        .get_latest_blockhash_with_commitment(commitment)
        .await?;

    Ok(BlockhashInfo::new(blockhash, last_valid_block_height))
}

#[cfg(test)]
mod tests {
    use solana_hash::Hash;

    use crate::blockhash::BlockhashInfo;

    #[test]
    fn test_not_expired_before_last_block() {
        let instance = BlockhashInfo::new(Hash::new_unique(), 150);
        assert!(!instance.is_expired(100));
        assert_eq!(instance.remaining_blocks(100), 50);
    }

    #[test]
    fn test_not_expired_at_last_block() {
        let instance = BlockhashInfo::new(Hash::new_unique(), 150);
        assert!(!instance.is_expired(150));
        assert_eq!(instance.remaining_blocks(150), 0);
    }

    #[test]
    fn test_expired_after_last_block() {
        let instance = BlockhashInfo::new(Hash::new_unique(), 150);
        assert!(instance.is_expired(151));
    }

    #[test]
    fn test_remaining_blocks_when_expired() {
        let instance = BlockhashInfo::new(Hash::new_unique(), 150);
        assert_eq!(instance.remaining_blocks(151), 0);
    }
}
