use solana_keypair::Keypair;
use solana_message::Instruction;

use crate::confirmation::ConfirmationStatus;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ExecutionRoute {
    SolanaRpc,
    Jito,
}

pub struct ExecutionRequest {
    pub instructions: Vec<Instruction>,
    pub payer: Keypair,
    pub route: ExecutionRoute,
}

#[derive(Debug)]
pub struct ExecutionOutcome {
    pub signature: solana_keypair::Signature,
    pub confirmation: ConfirmationStatus,
}

#[derive(Debug)]
pub enum ExecutionError {
    PreparationFailed,
    SubmissionFailed,
    ExecutionFailed,
    ConfirmationFailed,
    ConfirmationTimeout,
    Cancelled,
    DeadlineExceeded,
    OutcomeUnknown {
        signature: solana_keypair::Signature,
    },
}
