# Generate API Documentation with cargo doc

## Priority: Quick Win

## Summary

Generate and publish API documentation from existing doc comments using `cargo doc`. The codebase has good doc comments that aren't being utilized.

## Current State

- Doc comments exist on public APIs
- `cargo doc` not integrated into build/release process
- No hosted documentation

## Implementation Tasks

1. **Verify docs build cleanly**
   ```bash
   cargo doc --no-deps --document-private-items
   ```

2. **Add to CI**
   ```yaml
   - name: Build docs
     run: cargo doc --no-deps
   - name: Upload docs
     uses: actions/upload-pages-artifact@v2
     with:
       path: target/doc
   ```

3. **Add doc tests** (optional)
   ```rust
   /// Creates a new topic with the specified configuration.
   ///
   /// # Examples
   ///
   /// ```
   /// use streamline::storage::TopicManager;
   ///
   /// let manager = TopicManager::new("./data").await?;
   /// manager.create_topic("my-topic", 3).await?;
   /// ```
   pub async fn create_topic(&self, name: &str, partitions: u32) -> Result<()>
   ```

4. **Add crate-level documentation** to `src/lib.rs`:
   ```rust
   //! # Streamline
   //!
   //! A Kafka protocol-compatible streaming solution.
   //!
   //! ## Quick Start
   //!
   //! ```no_run
   //! use streamline::server::Server;
   //! use streamline::config::ServerConfig;
   //!
   //! #[tokio::main]
   //! async fn main() {
   //!     let config = ServerConfig::default();
   //!     let server = Server::new(config);
   //!     server.run().await.unwrap();
   //! }
   //! ```
   ```

## Acceptance Criteria

- [ ] `cargo doc` builds without warnings
- [ ] Crate-level documentation added
- [ ] Key public APIs have examples
- [ ] Docs deployed to GitHub Pages (or similar)

## Effort

~1-2 hours

## Labels

`quick-win`, `documentation`
