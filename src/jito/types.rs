use serde::{Deserialize, Serialize};

pub type BundleId = String;

#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub enum InflightBundleStatus {
    Pending,
    Landed,
    Failed,
    Invalid,
}

#[derive(Debug, Deserialize)]
pub struct BundleStatusesResponse {
    pub result: BundleStatusesResult,
}

#[derive(Debug, Deserialize)]
pub struct BundleStatusesResult {
    pub value: Vec<LandedBundleStatus>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct LandedBundleStatus {
    pub bundle_id: BundleId,
    pub transactions: Vec<String>,
    pub slot: u64,
    pub confirmation_status: String,
    pub err: serde_json::Value,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_get_bundle_statuses_fixture() {
        let fixture = r#"
        {
          "jsonrpc": "2.0",
          "result": {
            "value": [
              {
                "bundle_id": "test-bundle-id",
                "transactions": [
                  "1111111111111111111111111111111111111111111111111111111111111111"
                ],
                "slot": 123456,
                "confirmation_status": "finalized",
                "err": {
                  "Ok": null
                }
              }
            ]
          },
          "id": 1
        }
        "#;

        let response: BundleStatusesResponse =
            serde_json::from_str(fixture).expect("fixture must parse");

        let bundle = response
            .result
            .value
            .first()
            .expect("fixture must contain one bundle");

        assert_eq!(bundle.bundle_id, "test-bundle-id");
        assert_eq!(bundle.slot, 123456);
        assert_eq!(bundle.confirmation_status, "finalized");

        assert_eq!(
            bundle.transactions.len(),
            1,
            "Должна быть ровно одна транзакция"
        );
        assert!(
            bundle.err.get("Ok").is_some(),
            "Поле err должно содержать ключ Ok"
        );
    }
}
