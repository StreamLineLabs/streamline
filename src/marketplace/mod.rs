//! Connectors Marketplace
//!
//! A registry and management system for pre-built stream connectors:
//! - Discover and install connectors via `streamline connector search/install`
//! - Connector SDK for building custom connectors
//! - Lifecycle management (install, configure, start, stop, uninstall)
//! - WASM-sandboxed execution for community connectors

pub mod catalog;
pub mod connector_sdk;
pub mod declarative;
pub mod registry;

pub use catalog::{
    ConnectorCatalogEntry, ConnectorCategory, ConnectorMetadata, MarketplaceCatalog,
};
pub use connector_sdk::{
    ConnectorContext, ConnectorLifecycle, ConnectorSdk, HealthCheck, HealthStatus,
    SinkConnectorTrait, SourceConnectorTrait,
};
pub use declarative::{
    ConnectorDirection, ConnectorManager as DeclarativeConnectorManager, ConnectorRuntimeState,
    ConnectorSpec, ManagedConnector,
};
pub use registry::{
    InstallationStatus, InstalledConnector, MarketplaceConfig, MarketplaceRegistry,
};
