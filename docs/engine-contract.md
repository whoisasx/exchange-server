# Engine Stream Contract

The authoritative engine protocol contract lives in
[`../../engine/docs/engine-contract.md`](../../engine/docs/engine-contract.md).

Do not duplicate protocol semantics in exchange. Exchange keeps Rust protocol
types for consumers plus local JSON fallback fixtures for standalone test runs.
In this combined workspace, `cargo test -p protocol` reads
`../../engine/docs/examples` first and falls back to `docs/examples` only when
the sibling engine checkout is unavailable.

When changing engine protocol fields or examples:

1. Update `engine/docs/engine-contract.md` and `engine/docs/examples`.
2. Update exchange Rust protocol structs and consumers as needed.
3. Run `cargo test -p protocol` from `exchange` and engine's
   `protocol_fixture_tests`.
