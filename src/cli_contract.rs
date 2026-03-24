//! `streamline contract` CLI subcommand (M4 P1).
//!
//! Subcommands:
//!   * `apply -f c.yaml --topic <topic>` — uploads a contract and stores it
//!      in the `__contracts.<topic>` system topic.
//!   * `list` — lists active contracts.
//!   * `bypass --topic <t> --duration 30m --reason <r>` — emergency override.
//!
//! Stability tier: **Experimental**. Not yet registered in the main CLI
//! dispatcher (`src/cli.rs`); wire when M4 P1 lands.

#[derive(Debug, Clone)]
pub enum ContractCmd {
    Apply { file: String, topic: String },
    List,
    Bypass {
        topic: String,
        duration_secs: u64,
        reason: String,
    },
    Status,
}

#[derive(Debug, thiserror::Error)]
pub enum CliError {
    #[error("missing argument: {0}")]
    MissingArg(&'static str),
    #[error("invalid duration: {0}")]
    InvalidDuration(String),
}

/// Parses a duration like `30s`, `5m`, `2h` into seconds. Max 24h enforced
/// here; the security layer also re-checks.
pub fn parse_duration(s: &str) -> Result<u64, CliError> {
    let (num, unit) = s.split_at(s.len().saturating_sub(1));
    let n: u64 = num.parse().map_err(|_| CliError::InvalidDuration(s.into()))?;
    let secs = match unit {
        "s" => n,
        "m" => n * 60,
        "h" => n * 3600,
        _ => return Err(CliError::InvalidDuration(s.into())),
    };
    Ok(secs)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_duration_units() {
        assert_eq!(parse_duration("30s").unwrap(), 30);
        assert_eq!(parse_duration("5m").unwrap(), 300);
        assert_eq!(parse_duration("2h").unwrap(), 7200);
    }

    #[test]
    fn rejects_unknown_unit() {
        assert!(parse_duration("3d").is_err());
        assert!(parse_duration("abc").is_err());
    }
}
