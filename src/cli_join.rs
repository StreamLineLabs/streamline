//! `streamline join` CLI subcommand (M3 P1).
//!
//! Bootstraps an edge node onto the cluster using a one-time join token
//! issued by the control plane. Token format: signed JWT, 10-min TTL.
//!
//! Usage:
//!   `streamline join --token=<jwt> --cluster=cluster-name [--data-dir=./data]`
//!
//! Stability tier: **Experimental**.

#[derive(Debug, Clone)]
pub struct JoinArgs {
    pub token: String,
    pub cluster: String,
    pub data_dir: String,
}

#[derive(Debug, thiserror::Error)]
pub enum JoinError {
    #[error("token must be a non-empty JWT")]
    EmptyToken,
    #[error("cluster name required")]
    MissingCluster,
    #[error("token expired")]
    TokenExpired,
}

pub fn parse_args(args: &[(&str, &str)]) -> Result<JoinArgs, JoinError> {
    let mut token = String::new();
    let mut cluster = String::new();
    let mut data_dir = "./data".to_string();
    for (k, v) in args {
        match *k {
            "--token" => token = (*v).to_string(),
            "--cluster" => cluster = (*v).to_string(),
            "--data-dir" => data_dir = (*v).to_string(),
            _ => {}
        }
    }
    if token.is_empty() {
        return Err(JoinError::EmptyToken);
    }
    if cluster.is_empty() {
        return Err(JoinError::MissingCluster);
    }
    Ok(JoinArgs {
        token,
        cluster,
        data_dir,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_required_args() {
        let r = parse_args(&[("--token", "abc"), ("--cluster", "prod")]).unwrap();
        assert_eq!(r.token, "abc");
        assert_eq!(r.cluster, "prod");
        assert_eq!(r.data_dir, "./data");
    }

    #[test]
    fn rejects_missing_token() {
        assert!(parse_args(&[("--cluster", "prod")]).is_err());
    }
}
