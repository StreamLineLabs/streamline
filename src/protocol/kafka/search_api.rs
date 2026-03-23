//! Kafka wire protocol extension: API key 80 (`StreamlineSearch`).
//!
//! Reserved range: 80–119 (see `docs/protocol-extensions.md`).
//! Negotiated via `ApiVersions`; clients that don't know the key fall back
//! to the HTTP `/topics/{t}/search` endpoint.
//!
//! Stability tier: **Experimental**. Not yet registered in the protocol
//! dispatcher — wire up when M2 P1 lands.

/// API key reserved for semantic search RPC.
pub const API_KEY_STREAMLINE_SEARCH: i16 = 80;

/// Wire-format request frame (v0).
///
/// ```text
/// SearchRequest =>
///   topic            STRING
///   query            STRING        // text query; binary vector path is v1
///   k                INT32
///   filter_count     INT32
///   filter[i] =>
///     key            STRING
///     value          STRING
/// ```
#[derive(Debug, Clone)]
pub struct SearchRequestFrame {
    pub topic: String,
    pub query: String,
    pub k: i32,
    pub filter: Vec<(String, String)>,
}

/// Wire-format response frame (v0).
///
/// ```text
/// SearchResponse =>
///   error_code       INT16
///   hit_count        INT32
///   hits[i] =>
///     partition      INT32
///     offset         INT64
///     score          FLOAT32
/// ```
#[derive(Debug, Clone)]
pub struct SearchResponseFrame {
    pub error_code: i16,
    pub hits: Vec<SearchHitFrame>,
}

#[derive(Debug, Clone, Copy)]
pub struct SearchHitFrame {
    pub partition: i32,
    pub offset: i64,
    pub score: f32,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn api_key_is_in_reserved_range() {
        assert!((80..=119).contains(&API_KEY_STREAMLINE_SEARCH));
    }
}
