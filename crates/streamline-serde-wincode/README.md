# streamline-serde-wincode

A fork of [`serde-wincode`](https://github.com/A-Manning/serde-wincode) 0.1.2 by
Ash Manning, republished under the same Apache-2.0 licence with one change: the
`wincode` dependency is pinned exactly, to `=0.4.9`.

## Why this fork exists

`streamline` persists data in the bincode 1 wire format. `bincode` itself is
unmaintained at every published version (RUSTSEC-2025-0141, `patched = []`), and
the advisory names `wincode` as the replacement; `serde-wincode` is the `serde`
bridge on top of it.

Upstream `serde-wincode` 0.1.2 declares:

```toml
wincode = { version = ">=0.4, <1", default-features = false }
```

That range spans `0.4.x`, `0.5.x` and `0.6.x`, which are **mutually
semver-incompatible** — for `0.x` versions Cargo treats the minor number as the
major. Cargo unifies requirements only *within* one compatibility range, so a
`wincode = "=0.4.9"` requirement elsewhere in the graph does not constrain this
edge at all. Both are resolved:

```text
streamline          -> wincode 0.4.9
serde-wincode 0.1.2 -> wincode 0.6.1     # rust-version 1.89.0
```

Two consequences, both of which reach consumers of the *published* crate:

1. **MSRV.** `wincode` 0.5.0 raised its `rust-version` to 1.89.0, above
   `streamline`'s MSRV of 1.88.
2. **ABI.** `streamline`'s `src/bincode_compat.rs` builds a
   `wincode::config::Configuration` from its own `wincode` and hands it to
   `SerdeCompat`. If `SerdeCompat` was compiled against a different `wincode`,
   the trait bounds do not line up and the build fails.

A `Cargo.lock` pin does not fix this: a lockfile binds this repository only, and
a downstream `cargo build` of a published `streamline` resolves afresh. Neither
does `[patch.crates-io]`, which is not propagated to consumers of a published
crate. The exact requirement has to live in a manifest that is *itself*
published — which is this one.

`tests/dependency_security_test.rs` in the parent repository proves the fix
against a real lockless consumer built from the packaged `.crate` files, rather
than by re-reading this repository's lockfile.

## Relationship to upstream

* `src/lib.rs`, `src/ser.rs` and `src/de.rs` are upstream 0.1.2 verbatim, except
  for a fork header on `lib.rs` and the replacement of the one test that
  depended on `bincode` (see below). The wire format is unchanged.
* The version number tracks the `streamline` workspace (0.4.0), which releases
  its crates together. It is **not** a claim about upstream's versioning.
* Upstream's `bincode` 1.3.3 dev-dependency is removed — `bincode` is banned
  from this workspace's dependency graph by the same advisory that motivated the
  move away from it. The byte-equality assertion it supported is kept, against
  hard-coded bincode 1.3.3 output.

Upstream fixes should be pulled in here; this fork exists for the dependency
edge, not to diverge.

## Licence

Apache-2.0, as upstream. See `LICENSE` for the licence text and `NOTICE` for
attribution.
