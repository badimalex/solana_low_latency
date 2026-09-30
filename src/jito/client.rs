use base64::{Engine, prelude::BASE64_STANDARD};

use solana_hash::Hash;
use solana_keypair::{Keypair, Signer};
use solana_message::{
    AddressLookupTableAccount, Instruction, VersionedMessage, v0::Message as MessageV0,
};
use solana_pubkey::Pubkey;
use solana_transaction::versioned::VersionedTransaction;

use crate::jito::{
    error::JitoError,
    types::{BundleStatusesResponse, InflightBundleStatus},
};

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

    pub async fn send_transaction(
        &self,
        transaction: &VersionedTransaction,
    ) -> Result<solana_keypair::Signature, JitoError> {
        let body = self
            .build_send_transaction_request(transaction)
            .map_err(|err| JitoError::LocalValidationError(err.to_string()))?;

        let url = format!("{}/api/v1/transactions", self.endpoint);

        println!("Jito URL: {}", url);

        let response = self
            .client
            .post(&url)
            .header("Content-Type", "application/json")
            .json(&body)
            .send()
            .await
            .map_err(|err| JitoError::HttpNetworkFailure(err.to_string()))?;

        let http_status = response.status();

        println!("HTTP status: {}", http_status);

        let raw_body = response
            .text()
            .await
            .map_err(|err| JitoError::HttpNetworkFailure(err.to_string()))?;

        if !http_status.is_success() {
            return Err(JitoError::HttpNetworkFailure(format!(
                "HTTP {}: {}",
                http_status, raw_body
            )));
        }

        let json_response: serde_json::Value = serde_json::from_str(&raw_body)
            .map_err(|err| JitoError::MalformedInvalidResponse(err.to_string()))?;

        if let Some(error_value) = json_response.get("error").filter(|value| !value.is_null()) {
            let code = error_value
                .get("code")
                .and_then(|c| c.as_i64())
                .unwrap_or(-1);
            let message = error_value
                .get("message")
                .and_then(|m| m.as_str())
                .unwrap_or("Unknown JSON-RPC error")
                .to_string();

            return Err(JitoError::JsonRpcRemoteError { code, message });
        }

        let result_value = json_response.get("result").ok_or_else(|| {
            JitoError::MalformedInvalidResponse("Missing result field in Jito response".to_string())
        })?;

        let signature_str = result_value.as_str().ok_or_else(|| {
            JitoError::MalformedInvalidResponse(
                "Invalid result field format (expected string)".to_string(),
            )
        })?;

        // 3. Signature не парсится
        let signature: solana_keypair::Signature = signature_str.parse().map_err(|e| {
            JitoError::MalformedInvalidResponse(format!(
                "Failed to parse transaction signature: {}",
                e
            ))
        })?;

        Ok(signature)
    }

    pub fn build_send_bundle_request(
        &self,
        transactions: &[VersionedTransaction],
    ) -> Result<serde_json::Value, Box<dyn std::error::Error>> {
        if transactions.len() > 5 {
            return Err("Jito bundles can contain a maximum of 5 transactions".into());
        }
        if transactions.is_empty() {
            return Err("Bundle must contain at least 1 transaction".into());
        }

        let mut encoded_txs = Vec::with_capacity(transactions.len());
        for tx in transactions {
            let encoded = self.serialize_transaction(tx)?;
            encoded_txs.push(serde_json::json!(encoded));
        }

        let request = serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "sendBundle",
            "params": [
                encoded_txs,
                {
                    "encoding": "base64"
                }
            ]
        });

        Ok(request)
    }

    pub async fn send_bundle(
        &self,
        transactions: &[VersionedTransaction],
    ) -> Result<String, JitoError> {
        let body = self
            .build_send_bundle_request(transactions)
            .map_err(|err| JitoError::LocalValidationError(err.to_string()))?;

        let url = format!("{}/api/v1/bundles", self.endpoint);
        let response = self
            .client
            .post(&url)
            .json(&body)
            .send()
            .await
            .map_err(|err| JitoError::HttpNetworkFailure(err.to_string()))?;
        let http_status = response.status();

        println!("HTTP status: {}", http_status);

        let raw_body = response
            .text()
            .await
            .map_err(|err| JitoError::HttpNetworkFailure(err.to_string()))?;

        if !http_status.is_success() {
            return Err(JitoError::HttpNetworkFailure(format!(
                "HTTP {}: {}",
                http_status, raw_body
            )));
        }

        let json_response: serde_json::Value = serde_json::from_str(&raw_body)
            .map_err(|err| JitoError::MalformedInvalidResponse(err.to_string()))?;

        if let Some(error_value) = json_response.get("error").filter(|value| !value.is_null()) {
            let code = error_value
                .get("code")
                .and_then(|c| c.as_i64())
                .unwrap_or(-1);
            let message = error_value
                .get("message")
                .and_then(|m| m.as_str())
                .unwrap_or("Unknown JSON-RPC error")
                .to_string();

            return Err(JitoError::JsonRpcRemoteError { code, message });
        }

        let bundle_id = json_response
            .get("result")
            .and_then(|v| v.as_str())
            .ok_or_else(|| {
                JitoError::MalformedInvalidResponse(
                    "Missing result field in Jito response".to_string(),
                )
            })?;

        Ok(bundle_id.to_string())
    }

    pub fn build_get_inflight_bundle_status_request(&self, bundle_id: &str) -> serde_json::Value {
        serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getInflightBundleStatuses",
            "params": [
                [
                    bundle_id
                ]
            ]
        })
    }

    pub async fn get_inflight_bundle_status(
        &self,
        bundle_id: &str,
    ) -> Result<InflightBundleStatus, JitoError> {
        let request_body = self.build_get_inflight_bundle_status_request(bundle_id);

        let url = format!("{}/api/v1/getInflightBundleStatuses", self.endpoint);
        let response = self
            .client
            .post(&url)
            .json(&request_body)
            .send()
            .await
            .map_err(|err| JitoError::HttpNetworkFailure(err.to_string()))?;

        let http_status = response.status();

        println!("HTTP status: {}", http_status);

        let raw_body = response
            .text()
            .await
            .map_err(|err| JitoError::HttpNetworkFailure(err.to_string()))?;

        if !http_status.is_success() {
            return Err(JitoError::HttpNetworkFailure(format!(
                "HTTP {}: {}",
                http_status, raw_body
            )));
        }

        let json_response: serde_json::Value = serde_json::from_str(&raw_body)
            .map_err(|err| JitoError::MalformedInvalidResponse(err.to_string()))?;

        if let Some(error_value) = json_response.get("error").filter(|value| !value.is_null()) {
            let code = error_value
                .get("code")
                .and_then(|c| c.as_i64())
                .unwrap_or(-1);
            let message = error_value
                .get("message")
                .and_then(|m| m.as_str())
                .unwrap_or("Unknown JSON-RPC error")
                .to_string();

            return Err(JitoError::JsonRpcRemoteError { code, message });
        }

        let status_value = json_response
            .get("result")
            .and_then(|r| r.get("value"))
            .and_then(|v| v.as_array())
            .ok_or_else(|| {
                JitoError::MalformedInvalidResponse(
                    "Missing or invalid result.value in Jito response".to_string(),
                )
            })?
            .first()
            .ok_or_else(|| {
                JitoError::MalformedInvalidResponse(
                    "Jito returned an empty result.value list".to_string(),
                )
            })?
            .get("status")
            .ok_or_else(|| {
                JitoError::MalformedInvalidResponse(
                    "Missing status field in Jito bundle response".to_string(),
                )
            })?;

        let status_str = status_value.as_str().ok_or_else(|| {
            JitoError::MalformedInvalidResponse("Missing result field in Jito response".to_string())
        })?;

        let status: InflightBundleStatus = serde_json::from_value(serde_json::Value::String(
            status_str.to_string(),
        ))
        .map_err(|_| {
            JitoError::MalformedInvalidResponse(format!(
                "Unknown status string from Jito: {}",
                status_str
            ))
        })?;

        Ok(status)
    }

    pub fn build_get_bundle_status_request(&self, bundle_id: &str) -> serde_json::Value {
        serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "method": "getBundleStatuses",
            "params": [
                [
                    bundle_id
                ]
            ]
        })
    }

    pub async fn get_bundle_status(
        &self,
        bundle_id: &str,
    ) -> Result<BundleStatusesResponse, JitoError> {
        let request_body = self.build_get_bundle_status_request(bundle_id);

        let url = format!("{}/api/v1/getBundleStatuses", self.endpoint);
        let response = self
            .client
            .post(&url)
            .json(&request_body)
            .send()
            .await
            .map_err(|err| JitoError::HttpNetworkFailure(err.to_string()))?;

        let http_status = response.status();

        println!("HTTP status: {}", http_status);

        let raw_body = response
            .text()
            .await
            .map_err(|err| JitoError::HttpNetworkFailure(err.to_string()))?;

        if !http_status.is_success() {
            return Err(JitoError::HttpNetworkFailure(format!(
                "HTTP {}: {}",
                http_status, raw_body
            )));
        }

        let json_response: serde_json::Value = serde_json::from_str(&raw_body)
            .map_err(|err| JitoError::MalformedInvalidResponse(err.to_string()))?;

        if let Some(error_value) = json_response.get("error").filter(|value| !value.is_null()) {
            let code = error_value
                .get("code")
                .and_then(|c| c.as_i64())
                .unwrap_or(-1);
            let message = error_value
                .get("message")
                .and_then(|m| m.as_str())
                .unwrap_or("Unknown JSON-RPC error")
                .to_string();

            return Err(JitoError::JsonRpcRemoteError { code, message });
        }

        let response: BundleStatusesResponse =
            serde_json::from_value(json_response).map_err(|err| {
                JitoError::MalformedInvalidResponse(format!(
                    "Failed to parse getBundleStatuses response: {}",
                    err
                ))
            })?;

        Ok(response)
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
        let payer = Keypair::new();
        let blockhash = Hash::new_unique();

        let instruction = simple_instruction(Pubkey::new_unique(), Pubkey::new_unique());

        let transaction =
            build_v0_transaction(&payer, vec![instruction], blockhash, vec![]).unwrap();

        let client = JitoClient::new("endpoint");

        let request = client
            .build_send_transaction_request(&transaction)
            .expect("Failed to build send transaction request");

        assert_eq!(request["jsonrpc"], "2.0");
        assert_eq!(request["id"], 1);
        assert_eq!(request["method"], "sendTransaction");

        assert!(request["params"].is_array());
        let params = request["params"].as_array().unwrap();
        assert_eq!(params.len(), 2);

        let expected_base64 = client.serialize_transaction(&transaction).unwrap();

        assert_eq!(params[0], expected_base64);

        assert!(params[0].is_string());
        assert!(!params[0].as_str().unwrap().is_empty());

        assert_eq!(params[1]["encoding"], "base64");
    }

    #[test]
    fn send_bundle_request_preserves_transaction_order() {
        let payer = Keypair::new();
        let blockhash = Hash::new_unique();

        let instruction_a = simple_instruction(Pubkey::new_unique(), Pubkey::new_unique());

        let instruction_b = simple_instruction(Pubkey::new_unique(), Pubkey::new_unique());

        let tx_a = build_v0_transaction(&payer, vec![instruction_a], blockhash, vec![]).unwrap();

        let tx_b = build_v0_transaction(&payer, vec![instruction_b], blockhash, vec![]).unwrap();

        let client = JitoClient::new("endpoint");

        let expected_tx_a_b64 = client.serialize_transaction(&tx_a).unwrap();
        let expected_tx_b_b64 = client.serialize_transaction(&tx_b).unwrap();

        let request = client.build_send_bundle_request(&[tx_a, tx_b]).unwrap();

        let tx_array = request["params"][0]
            .as_array()
            .expect("params[0] должен быть массивом");

        assert_eq!(
            tx_array.get(0).and_then(|v| v.as_str()),
            Some(expected_tx_a_b64.as_str()),
            "Первая транзакция не совпадает"
        );

        assert_eq!(
            tx_array.get(1).and_then(|v| v.as_str()),
            Some(expected_tx_b_b64.as_str()),
            "Вторая транзакция не совпадает"
        );
    }

    #[test]
    fn landed_string_is_parsed_into_inflight_bundle_status() {
        let json_value = serde_json::json!("Landed");
        let status: InflightBundleStatus = serde_json::from_value(json_value).unwrap();

        assert_eq!(status, InflightBundleStatus::Landed);
    }

    #[test]
    fn send_bundle_request_rejects_more_than_five_transactions() {
        let payer = Keypair::new();
        let blockhash = Hash::new_unique();
        let client = JitoClient::new("endpoint");

        let mut transactions = Vec::new();

        for _ in 0..6 {
            let instruction = simple_instruction(Pubkey::new_unique(), Pubkey::new_unique());

            let tx = build_v0_transaction(&payer, vec![instruction], blockhash, vec![]).unwrap();

            transactions.push(tx);
        }

        let result = client.build_send_bundle_request(&transactions);

        assert!(result.is_err())
    }
}
