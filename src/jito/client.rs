use base64::{Engine, prelude::BASE64_STANDARD};

use solana_hash::Hash;
use solana_keypair::{Keypair, Signer};
use solana_message::{
    AddressLookupTableAccount, Instruction, VersionedMessage, v0::Message as MessageV0,
};
use solana_pubkey::Pubkey;
use solana_transaction::versioned::VersionedTransaction;

pub struct JitoClient {
    client: reqwest::Client,
    endpoint: String,
}

impl JitoClient {
    pub fn new(endpoint: impl Into<String>) -> Self {
        Self {
            client: reqwest::Client::new(),
            endpoint: endpoint.into(),
        }
    }

    pub fn serialize_transaction(
        &self,
        transaction: &VersionedTransaction,
    ) -> Result<String, Box<dyn std::error::Error>> {
        let original_bytes = bincode::serialize(&transaction)?;

        let base64_str = BASE64_STANDARD.encode(&original_bytes);

        Ok(base64_str)
    }

    pub fn build_send_transaction_request(
        &self,
        transaction: &VersionedTransaction,
    ) -> Result<serde_json::Value, Box<dyn std::error::Error>> {
        let base64_transaction = self.serialize_transaction(transaction)?;

        let request = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "sendTransaction",
            "params": [
                base64_transaction,
                {
                    "encoding": "base64"
                }
            ]
        });

        Ok(request)
    }

    pub async fn get_tip_accounts_raw(
        &self,
    ) -> Result<serde_json::Value, Box<dyn std::error::Error>> {
        let url = format!("{}/api/v1/getTipAccounts", self.endpoint);

        let body = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getTipAccounts",
            "params": []
        });

        let response = self
            .client
            .post(&url)
            .header("Content-Type", "application/json")
            .json(&body)
            .send()
            .await?;

        let json_response: serde_json::Value = response.json().await?;

        Ok(json_response)
    }

    pub async fn get_tip_accounts(&self) -> Result<Vec<Pubkey>, Box<dyn std::error::Error>> {
        let raw_response = self.get_tip_accounts_raw().await?;

        let accounts_array = raw_response["result"]
            .as_array()
            .ok_or("Не удалось найти массив 'result' в ответе")?;

        let mut pubkeys = Vec::with_capacity(accounts_array.len());

        for value in accounts_array {
            let addr_str = value
                .as_str()
                .ok_or("Элемент массива не является строкой")?;

            let pubkey = addr_str.parse()?;
            pubkeys.push(pubkey);
        }

        Ok(pubkeys)
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::transaction_builder::build_v0_transaction;

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

    #[test]
    fn signed_transaction_can_be_serialized_to_base64_and_restored() {
        let payer = Keypair::new();
        let blockhash = Hash::new_unique();

        let instruction = simple_instruction(Pubkey::new_unique(), Pubkey::new_unique());

        let transaction =
            build_v0_transaction(&payer, vec![instruction], blockhash, vec![]).unwrap();

        let client = JitoClient::new("endpoint");

        let encoded = client.serialize_transaction(&transaction).unwrap();

        assert!(!encoded.is_empty(), "Ошибка: base64 строка пустая!");
        let actual_bytes = BASE64_STANDARD.decode(&encoded).unwrap();

        let expected_bytes = bincode::serialize(&transaction).unwrap();

        assert_eq!(expected_bytes, actual_bytes, "Ошибка: байты не совпадают!");
    }

    #[test]
    fn send_transaction_request_has_expected_structure() {
        // TODO: Собираем signed VersionedTransaction
        // let payer = Keypair::new();

        // // Создаем минимальное валидное сообщение для транзакции
        // let message = VersionedMessage::Legacy(Message::new(&[], Some(&payer.pubkey())));
        // let mut transaction = VersionedTransaction::try_new(message, &[]).unwrap();

        // Подписываем транзакцию
        // transaction.sign(&[&payer]).unwrap();

        let payer = Keypair::new();
        let blockhash = Hash::new_unique();

        let instruction = simple_instruction(Pubkey::new_unique(), Pubkey::new_unique());

        let transaction =
            build_v0_transaction(&payer, vec![instruction], blockhash, vec![]).unwrap();

        // TODO: Создаем JitoClient
        let client = JitoClient::new("endpoint");

        // TODO: Вызываем build_send_transaction_request
        let request = client
            .build_send_transaction_request(&transaction)
            .expect("Failed to build send transaction request");

        // Проверяем структуру JSON
        assert_eq!(request["jsonrpc"], "2.0");
        assert_eq!(request["id"], 1);
        assert_eq!(request["method"], "sendTransaction");

        // Проверяем параметры
        assert!(request["params"].is_array());
        let params = request["params"].as_array().unwrap();
        assert_eq!(params.len(), 2);

        let expected_base64 = client.serialize_transaction(&transaction).unwrap();

        assert_eq!(params[0], expected_base64);

        // request["params"][0] — непустая строка base64
        assert!(params[0].is_string());
        assert!(!params[0].as_str().unwrap().is_empty());

        // request["params"][1]["encoding"] == "base64"
        assert_eq!(params[1]["encoding"], "base64");
    }
}
