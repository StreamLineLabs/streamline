//! # Streamline Security — Pluggable Key Management
//!
//! **Status:** Phase-1 prototype scaffold (not yet wired into `lib.rs`).
//! See `docs/adr/0018-kms-plugin.md`.
//!
//! Provides a backend-agnostic [`KeyProvider`] trait used by:
//! - Agent Memory Fabric (M1) — per-agent envelope keys.
//! - Event Attestation (M4) — broker signing keys.
//!
//! The default [`local_age::LocalAgeProvider`] needs no external
//! infrastructure and is suitable for development. Production deployments
//! configure a Vault / KMS backend via the [`KeyProvider`] trait.
//!
//! Wiring guide:
//! 1. Add `pub mod security;` under the public-API section of `lib.rs`.
//! 2. Add a `kms` feature in `Cargo.toml` and gate optional crypto deps
//!    (`age`, `ed25519-dalek`, `vaultrs`) behind it.
//! 3. Add the trait to the prelude / re-exports as the API stabilizes.

pub mod audit;
#[cfg(feature = "attestation")]
pub mod attestation;
#[cfg(feature = "attestation")]
pub mod aws_kms;
pub mod error;
pub mod kms;
pub mod local_age;
#[cfg(feature = "attestation")]
pub mod vault;

pub use error::{KmsError, KmsResult};
pub use kms::{Algorithm, KeyId, KeyProvider, Signature};
