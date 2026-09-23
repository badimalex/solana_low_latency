use solana_hash::Hash;
use solana_keypair::{Keypair, Signer};
use solana_message::{
    AddressLookupTableAccount, Instruction, VersionedMessage, v0::Message as MessageV0,
};
use solana_transaction::versioned::VersionedTransaction;

pub fn build_v0_transaction(
    payer: &Keypair,
    instructions: Vec<Instruction>,
    recent_blockhash: Hash,
    lookup_tables: Vec<AddressLookupTableAccount>,
) -> Result<VersionedTransaction, Box<dyn std::error::Error>> {
    let message_v0 = MessageV0::try_compile(
        &payer.pubkey(),
        &instructions,
        &lookup_tables,
        recent_blockhash,
    )?;

    let versioned_message = VersionedMessage::V0(message_v0);

    let transaction = VersionedTransaction::try_new(versioned_message, &[payer])?;

    Ok(transaction)
}

#[cfg(test)]
mod tests {
    use super::*;

    use solana_message::AccountMeta;
    use solana_pubkey::Pubkey;

    fn simple_instruction(program_id: Pubkey, account: Pubkey) -> Instruction {
        Instruction {
            program_id,
            accounts: vec![AccountMeta {
                pubkey: account,
                is_signer: false,
                is_writable: false,
            }],
            data: vec![1],
        }
    }

    fn get_v0_message(transaction: &VersionedTransaction) -> &MessageV0 {
        match &transaction.message {
            VersionedMessage::V0(message) => message,
            _ => panic!("expected v0 message"),
        }
    }

    #[test]
    fn builds_v0_transaction() {
        let payer = Keypair::new();
        let blockhash = Hash::new_unique();

        let instruction = simple_instruction(Pubkey::new_unique(), Pubkey::new_unique());

        let transaction =
            build_v0_transaction(&payer, vec![instruction], blockhash, vec![]).unwrap();

        assert!(matches!(transaction.message, VersionedMessage::V0(_)));

        assert!(
            transaction.version() == solana_transaction::versioned::TransactionVersion::Number(0)
        );
    }

    #[test]
    fn preserves_recent_blockhash() {
        let payer = Keypair::new();
        let blockhash = Hash::new_unique();

        let instruction = simple_instruction(Pubkey::new_unique(), Pubkey::new_unique());

        let transaction =
            build_v0_transaction(&payer, vec![instruction], blockhash, vec![]).unwrap();

        let message = get_v0_message(&transaction);

        assert_eq!(message.recent_blockhash, blockhash);
    }

    #[test]
    fn includes_expected_signer() {
        let payer = Keypair::new();
        let payer_pubkey = payer.pubkey();

        let instruction = simple_instruction(Pubkey::new_unique(), Pubkey::new_unique());

        let transaction =
            build_v0_transaction(&payer, vec![instruction], Hash::new_unique(), vec![]).unwrap();

        let message = get_v0_message(&transaction);

        assert!(message.account_keys.contains(&payer_pubkey));
    }

    #[test]
    fn compiles_multiple_instructions() {
        let payer = Keypair::new();

        let ix1 = simple_instruction(Pubkey::new_unique(), Pubkey::new_unique());

        let ix2 = simple_instruction(Pubkey::new_unique(), Pubkey::new_unique());

        let transaction =
            build_v0_transaction(&payer, vec![ix1, ix2], Hash::new_unique(), vec![]).unwrap();

        let message = get_v0_message(&transaction);

        assert_eq!(message.instructions.len(), 2);
    }

    #[test]
    fn builds_with_lookup_table() {
        let payer = Keypair::new();

        let account_a = Pubkey::new_unique();

        let instruction = simple_instruction(Pubkey::new_unique(), account_a);

        let lookup_table = AddressLookupTableAccount {
            key: Pubkey::new_unique(),
            addresses: vec![account_a],
        };

        let transaction = build_v0_transaction(
            &payer,
            vec![instruction],
            Hash::new_unique(),
            vec![lookup_table],
        )
        .unwrap();

        let message = get_v0_message(&transaction);

        println!("{:?}", message);

        assert!(!message.address_table_lookups.is_empty());
    }

    #[test]
    fn lookup_reduces_inline_account_keys() {
        let payer = Keypair::new();

        let account_a = Pubkey::new_unique();
        let program_id = Pubkey::new_unique();
        let blockhash = Hash::new_unique();

        let instruction = simple_instruction(program_id, account_a);

        let without_alt =
            build_v0_transaction(&payer, vec![instruction.clone()], blockhash, vec![]).unwrap();

        let lookup_table = AddressLookupTableAccount {
            key: Pubkey::new_unique(),
            addresses: vec![account_a],
        };

        let with_alt =
            build_v0_transaction(&payer, vec![instruction], blockhash, vec![lookup_table]).unwrap();

        let without_alt_message = get_v0_message(&without_alt);
        let with_alt_message = get_v0_message(&with_alt);

        println!("{:?}", without_alt_message);
        println!("{:?}", with_alt_message);
        assert!(with_alt_message.account_keys.len() < without_alt_message.account_keys.len());
    }
}
