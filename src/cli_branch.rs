//! `streamline branch` CLI subcommand (M5 P1).
//!
//! Subcommands:
//!   * `create <topic> <name> [--from-offset <n>|--from-now]`
//!   * `list [--topic <t>]`
//!   * `discard <topic> <name>`
//!   * `diff <topic> <name>` (P2)
//!
//! Stability tier: **Experimental**.

#[derive(Debug, Clone)]
pub enum BranchCmd {
    Create {
        topic: String,
        name: String,
        from_offset: Option<i64>,
    },
    List {
        topic: Option<String>,
    },
    Discard {
        topic: String,
        name: String,
    },
}

#[derive(Debug, thiserror::Error)]
pub enum BranchCliError {
    #[error("branch name must match [a-z0-9-]+: got `{0}`")]
    InvalidName(String),
    #[error("branch `{0}` already exists on topic `{1}`")]
    AlreadyExists(String, String),
}

/// Validate a branch name — used both by CLI parsing and broker admission.
pub fn validate_branch_name(name: &str) -> Result<(), BranchCliError> {
    if name.is_empty()
        || !name
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-')
    {
        return Err(BranchCliError::InvalidName(name.to_string()));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_valid_names() {
        assert!(validate_branch_name("exp-a").is_ok());
        assert!(validate_branch_name("v2").is_ok());
    }

    #[test]
    fn rejects_invalid() {
        assert!(validate_branch_name("").is_err());
        assert!(validate_branch_name("Exp_A").is_err());
        assert!(validate_branch_name("with spaces").is_err());
    }
}
