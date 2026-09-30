use serde::Serialize;
use std::fmt;

#[derive(Debug, Serialize)]
pub enum JitoError {
    HttpNetworkFailure(String),
    JsonRpcRemoteError { code: i64, message: String },
    MalformedInvalidResponse(String),
    LocalValidationError(String),
}

impl std::fmt::Display for JitoError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            JitoError::HttpNetworkFailure(err) => {
                write!(f, "HTTP/Network failure: {}", err)
            }
            JitoError::JsonRpcRemoteError { code, message } => {
                write!(f, "JSON-RPC remote error (code {}): {}", code, message)
            }
            JitoError::MalformedInvalidResponse(err) => {
                write!(f, "Malformed or invalid response: {}", err)
            }
            JitoError::LocalValidationError(err) => {
                write!(f, "Local validation error: {}", err)
            }
        }
    }
}

impl std::error::Error for JitoError {}
