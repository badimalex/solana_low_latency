use serde::{Deserialize, Serialize};
use solana_keypair::Signature;

pub type BundleId = String;

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub enum InflightBundleStatus {
    Pending,
    Landed,
    Failed,
    Invalid,
}

#[derive(Debug, Clone)]
pub struct LandedBundleStatus {
    pub bundle_id: BundleId,
    pub transactions: Vec<Signature>,
    pub slot: u64,
    pub confirmation_status: String,
    pub err: Option<String>,
}
