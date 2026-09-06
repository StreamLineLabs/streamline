//! End-to-end test for the semantic-topics M2 P1 vertical slice.
//!
//! Spins up: `EmbedWorker` (embedding via `HashEmbedder`) → registry →
//! `handle_search`. Verifies recall@k on a synthetic mini-corpus.
//!
//! No broker is started — the slice under test is purely the in-process
//! pipeline. Broker integration lives in `tests/cluster_*.rs` once
//! M2 P2 wires the producer-side tap.

#![cfg(feature = "semantic-topics")]

use std::sync::Arc;
use std::time::{Duration, Instant};

use streamline::ai::semantic_topics::{registry, EmbedJob, EmbedWorker, HashEmbedder};
use streamline::server::search_api::{handle_search, SearchRequest};

fn submit_records(topic: &str, records: &[(i64, &str)]) {
    let worker = EmbedWorker::new(Arc::new(HashEmbedder::default()), 1024);
    let handle = worker.spawn();
    for (offset, body) in records {
        let job = EmbedJob {
            topic: topic.to_string(),
            partition: 0,
            offset: *offset,
            payload: body.as_bytes().to_vec(),
        };
        assert!(handle.try_submit(job), "queue should accept");
    }
    // Drop the handle so the worker thread observes channel close. Then
    // poll the registry until indexed count matches expected, with a
    // short bound so a stuck worker doesn't hang CI forever.
    drop(handle);
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        let len = registry::get(topic).map(|i| i.len()).unwrap_or(0);
        if len >= records.len() {
            break;
        }
        if Instant::now() > deadline {
            panic!(
                "embed worker did not index {} records within 2s (got {})",
                records.len(),
                len
            );
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}

#[test]
fn semantic_topic_e2e_top_match_is_correct() {
    let topic = "e2e-top-match";
    submit_records(
        topic,
        &[
            (1, "payment failed because card was declined"),
            (2, "user logged into the dashboard"),
            (3, "checkout completed successfully"),
            (4, "credit card payment authorization error"),
            (5, "newsletter subscription confirmed"),
        ],
    );

    let resp = handle_search(
        topic,
        SearchRequest {
            query: "payment authorization failure".into(),
            k: 2,
            filter: None,
        },
    );
    assert_eq!(resp.hits.len(), 2);
    let top_offsets: Vec<i64> = resp.hits.iter().map(|h| h.offset).collect();
    assert!(
        top_offsets.contains(&1) || top_offsets.contains(&4),
        "expected at least one of the two payment records in top-2; got {top_offsets:?}"
    );
}

#[test]
fn search_returns_empty_on_unindexed_topic() {
    let resp = handle_search(
        "definitely-never-indexed-topic",
        SearchRequest {
            query: "anything".into(),
            k: 10,
            filter: None,
        },
    );
    assert!(resp.hits.is_empty());
    // Latency should still be reported (handler runs the embedder + lookup).
    assert!(
        resp.took_ms < 100,
        "took {}ms, expected <100ms",
        resp.took_ms
    );
}
