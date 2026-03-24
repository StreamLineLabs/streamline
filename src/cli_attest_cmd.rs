//! `streamline-cli attest sign|verify` — wrap the broker's attestation
//! HTTP API for human/script use.

#![cfg(feature = "attestation")]

use base64::Engine;
use clap::Subcommand;
use serde::{Deserialize, Serialize};
use std::process::ExitCode;
use std::time::{SystemTime, UNIX_EPOCH};
use streamline::Result;
use streamline::StreamlineError;

use crate::cli_http;

#[derive(Subcommand, Debug)]
pub(crate) enum AttestCli {
    /// Sign an attestation envelope for a record. Prints the wire header
    /// value (base64 signature) on stdout.
    Sign {
        #[arg(long)]
        topic: String,
        #[arg(long, default_value_t = 0)]
        partition: i32,
        #[arg(long)]
        offset: i64,
        /// Record value (UTF-8). Use --value-b64 for binary.
        #[arg(long)]
        value: Option<String>,
        /// Base64-encoded record value
        #[arg(long = "value-b64")]
        value_b64: Option<String>,
        #[arg(long, default_value_t = 0)]
        schema_id: u32,
        #[arg(long)]
        timestamp_ms: Option<i64>,
        #[arg(long, default_value = "broker-0")]
        key_id: String,
    },

    /// Verify an attestation. Exits non-zero on failure.
    Verify {
        #[arg(long)]
        topic: String,
        #[arg(long, default_value_t = 0)]
        partition: i32,
        #[arg(long)]
        offset: i64,
        #[arg(long)]
        value: Option<String>,
        #[arg(long = "value-b64")]
        value_b64: Option<String>,
        #[arg(long, default_value_t = 0)]
        schema_id: u32,
        #[arg(long)]
        timestamp_ms: i64,
        #[arg(long, default_value = "broker-0")]
        key_id: String,
        #[arg(long, default_value = "ed25519")]
        algorithm: String,
        /// Signature to verify (base64)
        #[arg(long = "signature-b64")]
        signature_b64: String,
    },
}

#[derive(Debug, Serialize)]
struct SignBody<'a> {
    topic: &'a str,
    partition: i32,
    offset: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    value: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    value_b64: Option<String>,
    schema_id: u32,
    timestamp_ms: i64,
    key_id: &'a str,
}

#[derive(Debug, Serialize)]
struct VerifyBody<'a> {
    topic: &'a str,
    partition: i32,
    offset: i64,
    #[serde(skip_serializing_if = "Option::is_none")]
    value: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    value_b64: Option<String>,
    schema_id: u32,
    timestamp_ms: i64,
    key_id: &'a str,
    algorithm: &'a str,
    signature_b64: &'a str,
}

#[derive(Debug, Deserialize, Serialize)]
struct SignResponse {
    key_id: String,
    algorithm: String,
    timestamp_ms: i64,
    payload_sha256: String,
    signature_b64: String,
    header_name: String,
    header_value: String,
}

#[derive(Debug, Deserialize)]
struct VerifyResponse {
    valid: bool,
    #[serde(default)]
    key_id: String,
    #[serde(default)]
    algorithm: String,
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

fn validate_value(
    value: &Option<String>,
    value_b64: &Option<String>,
) -> Result<()> {
    match (value, value_b64) {
        (Some(_), None) | (None, Some(_)) => Ok(()),
        _ => Err(StreamlineError::Server(
            "provide exactly one of --value or --value-b64".into(),
        )),
    }
}

pub(crate) fn handle(cmd: AttestCli) -> Result<ExitCode> {
    let base = cli_http::default_url();
    match cmd {
        AttestCli::Sign {
            topic,
            partition,
            offset,
            value,
            value_b64,
            schema_id,
            timestamp_ms,
            key_id,
        } => {
            validate_value(&value, &value_b64)?;
            // Sanity-check b64
            if let Some(v) = &value_b64 {
                base64::engine::general_purpose::STANDARD
                    .decode(v.as_bytes())
                    .map_err(|e| StreamlineError::Server(format!("--value-b64 invalid: {e}")))?;
            }
            let body = SignBody {
                topic: &topic,
                partition,
                offset,
                value,
                value_b64,
                schema_id,
                timestamp_ms: timestamp_ms.unwrap_or_else(now_ms),
                key_id: &key_id,
            };
            let resp: SignResponse = cli_http::post_json(&base, "/api/v1/attest", &body)?;
            println!("{}", serde_json::to_string_pretty(&resp)?);
            Ok(ExitCode::SUCCESS)
        }
        AttestCli::Verify {
            topic,
            partition,
            offset,
            value,
            value_b64,
            schema_id,
            timestamp_ms,
            key_id,
            algorithm,
            signature_b64,
        } => {
            validate_value(&value, &value_b64)?;
            let body = VerifyBody {
                topic: &topic,
                partition,
                offset,
                value,
                value_b64,
                schema_id,
                timestamp_ms,
                key_id: &key_id,
                algorithm: &algorithm,
                signature_b64: &signature_b64,
            };
            let resp: VerifyResponse =
                cli_http::post_json(&base, "/api/v1/attest/verify", &body)?;
            if resp.valid {
                println!("valid (key_id={}, alg={})", resp.key_id, resp.algorithm);
                Ok(ExitCode::SUCCESS)
            } else {
                eprintln!("INVALID signature for key_id={} alg={}", resp.key_id, resp.algorithm);
                Ok(ExitCode::from(3))
            }
        }
    }
}
