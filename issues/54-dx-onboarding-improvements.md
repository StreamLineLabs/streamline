# DX Onboarding Improvements

## Summary

Reduce time-to-value for new developers by adding a zero-build quickstart path, a single demo command, Docker quickstart, Windows install support, server-backed seeding, default CLI profiles, and clearer onboarding docs.

## Current State

- README quick start and local dev flow rely on source builds (`README.md`, `docs/LOCAL_DEVELOPMENT.md`).
- `scripts/quickstart.sh` always builds from source and then seeds data via CLI quickstart.
- `streamline-cli quickstart` seeds data via `TopicManager` using the local data directory.
- `scripts/install.sh` supports macOS/Linux only.
- `docker-compose.yml` exists but no one-command docker quickstart.
- No single "Getting Started" doc; SDK docs live under `sdks/` without top-level links.

## Requirements

1. Quickstart can download + run binaries without requiring Rust.
2. One demo command starts server + seeds data after install.
3. Docker quickstart script/target brings up server and seeds demo data.
4. Windows install script for releases.
5. Quickstart prefers server APIs when a server is running.
6. Quickstart initializes a default CLI profile for future commands.
7. README includes a contributor fast path (`dev-setup.sh --minimal`).
8. Add `docs/GETTING_STARTED.md` and link to it.
9. README links to SDK quickstarts under `sdks/`.
10. Quickstart failure paths show actionable diagnostics.

## Implementation Tasks

1. **Zero-build quickstart**
   - Add `--download`/auto fallback to `scripts/quickstart.sh`.
   - Extend `scripts/install.sh` to support quickstart usage.
   - Update README Quick Start to include single copy-paste command.
2. **Demo command**
   - Add `streamline-cli demo` as a wrapper to quickstart (auto-start server, seed data).
   - Update CLI docs and README examples.
3. **Docker quickstart**
   - Add `scripts/quickstart-docker.sh` or `make docker-quickstart`.
   - Update `docs/DOCKER.md` and README with copy-paste steps.
4. **Windows install path**
   - Add `scripts/install.ps1`.
   - Document Windows install + PATH steps in README.
5. **Server-backed quickstart**
   - When server is running, create topics + seed data via HTTP/Kafka APIs.
   - Add/extend server API endpoints as needed in `src/server/api.rs`.
6. **Default profile initialization**
   - Write a `local` profile on quickstart (data dir + addresses).
   - Add `streamline-cli profile init` if needed.
7. **Contributor fast path**
   - Add "Contribute in 5 minutes" section in README.
8. **Getting started doc**
   - Create `docs/GETTING_STARTED.md` with 5/15/60 minute flows.
   - Link from README + DOCS.md.
9. **SDK quickstarts**
   - Add "Language Quickstarts" section in README linking to `sdks/*/README.md`.
10. **Actionable error hints**
   - On quickstart failure, print `streamline-cli doctor` and troubleshooting links.
   - Update `scripts/quickstart.sh` and CLI quickstart output.

## Acceptance Criteria

- [ ] Quickstart supports zero-build download path.
- [ ] `streamline-cli demo` starts server and seeds data successfully.
- [ ] Docker quickstart brings up server and seeds data with one command.
- [ ] Windows install script works for release downloads.
- [ ] Quickstart seeds data through server when running.
- [ ] Default `local` CLI profile is created for future commands.
- [ ] README includes contributor fast path.
- [ ] `docs/GETTING_STARTED.md` exists and is linked.
- [ ] README links to SDK quickstarts.
- [ ] Quickstart failures provide actionable diagnostics.

## Labels

`dx`, `onboarding`, `documentation`, `enhancement`
