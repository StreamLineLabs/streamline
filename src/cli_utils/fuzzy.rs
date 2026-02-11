//! Fuzzy string matching for smart error recovery
//!
//! Provides "Did you mean?" suggestions when users mistype topic names,
//! command names, or other identifiers.

use colored::Colorize;

/// Calculate the Levenshtein distance between two strings
fn levenshtein_distance(s1: &str, s2: &str) -> usize {
    let s1_chars: Vec<char> = s1.chars().collect();
    let s2_chars: Vec<char> = s2.chars().collect();
    let len1 = s1_chars.len();
    let len2 = s2_chars.len();

    if len1 == 0 {
        return len2;
    }
    if len2 == 0 {
        return len1;
    }

    let mut matrix = vec![vec![0usize; len2 + 1]; len1 + 1];

    for (i, row) in matrix.iter_mut().enumerate().take(len1 + 1) {
        row[0] = i;
    }
    for (j, cell) in matrix[0].iter_mut().enumerate().take(len2 + 1) {
        *cell = j;
    }

    for i in 1..=len1 {
        for j in 1..=len2 {
            let cost = if s1_chars[i - 1] == s2_chars[j - 1] {
                0
            } else {
                1
            };

            matrix[i][j] = (matrix[i - 1][j] + 1) // deletion
                .min(matrix[i][j - 1] + 1) // insertion
                .min(matrix[i - 1][j - 1] + cost); // substitution
        }
    }

    matrix[len1][len2]
}

/// Calculate similarity score between two strings (0.0 to 1.0)
fn similarity_score(s1: &str, s2: &str) -> f64 {
    let s1_lower = s1.to_lowercase();
    let s2_lower = s2.to_lowercase();

    let max_len = s1_lower.len().max(s2_lower.len());
    if max_len == 0 {
        return 1.0;
    }

    let distance = levenshtein_distance(&s1_lower, &s2_lower);
    1.0 - (distance as f64 / max_len as f64)
}

/// Check if s1 is a prefix of s2 or vice versa
fn is_prefix_match(s1: &str, s2: &str) -> bool {
    let s1_lower = s1.to_lowercase();
    let s2_lower = s2.to_lowercase();
    s1_lower.starts_with(&s2_lower) || s2_lower.starts_with(&s1_lower)
}

/// Check if strings contain common substrings
fn has_common_substring(s1: &str, s2: &str, min_len: usize) -> bool {
    let s1_lower = s1.to_lowercase();
    let s2_lower = s2.to_lowercase();

    if s1_lower.len() < min_len || s2_lower.len() < min_len {
        return false;
    }

    for i in 0..=s1_lower.len().saturating_sub(min_len) {
        let substring = &s1_lower[i..i + min_len];
        if s2_lower.contains(substring) {
            return true;
        }
    }
    false
}

/// Match result with score
#[derive(Debug, Clone)]
pub struct FuzzyMatch {
    /// The matched candidate
    pub candidate: String,
    /// Similarity score (0.0 to 1.0)
    pub score: f64,
    /// Whether this is an exact match
    pub exact: bool,
}

/// Find similar strings from a list of candidates
///
/// Returns matches sorted by similarity score (highest first)
pub fn find_similar(input: &str, candidates: &[String], max_results: usize) -> Vec<FuzzyMatch> {
    if input.is_empty() || candidates.is_empty() {
        return vec![];
    }

    let mut matches: Vec<FuzzyMatch> = candidates
        .iter()
        .filter_map(|candidate| {
            // Check for exact match first
            if candidate.to_lowercase() == input.to_lowercase() {
                return Some(FuzzyMatch {
                    candidate: candidate.clone(),
                    score: 1.0,
                    exact: true,
                });
            }

            // Calculate base similarity
            let mut score = similarity_score(input, candidate);

            // Boost prefix matches
            if is_prefix_match(input, candidate) {
                score = (score + 0.2).min(0.99);
            }

            // Boost common substring matches
            if has_common_substring(input, candidate, 3) {
                score = (score + 0.1).min(0.99);
            }

            // Filter out low-scoring matches
            if score < 0.3 {
                return None;
            }

            Some(FuzzyMatch {
                candidate: candidate.clone(),
                score,
                exact: false,
            })
        })
        .collect();

    // Sort by score descending
    matches.sort_by(|a, b| b.score.partial_cmp(&a.score).unwrap_or(std::cmp::Ordering::Equal));

    // Take top results
    matches.truncate(max_results);

    matches
}

/// Generate a "Did you mean?" suggestion
pub fn suggest_correction(
    input: &str,
    candidates: &[String],
    resource_type: &str,
) -> Option<String> {
    let matches = find_similar(input, candidates, 3);

    if matches.is_empty() {
        return None;
    }

    let mut suggestion = String::new();

    if matches.len() == 1 {
        suggestion.push_str(&format!(
            "\n{} Did you mean '{}'?\n",
            "→".cyan(),
            matches[0].candidate.green()
        ));
    } else {
        suggestion.push_str(&format!("\n{} Similar {}s:\n", "→".cyan(), resource_type));
        for m in &matches {
            let score_display = if m.score > 0.8 {
                "(very similar)".green()
            } else if m.score > 0.6 {
                "(similar)".yellow()
            } else {
                "(partial match)".dimmed()
            };
            suggestion.push_str(&format!("    - {} {}\n", m.candidate.cyan(), score_display));
        }
    }

    Some(suggestion)
}

/// Suggest a quick fix command
pub fn suggest_quick_fix(
    input: &str,
    candidates: &[String],
    command_template: &str,
) -> Option<String> {
    let matches = find_similar(input, candidates, 1);

    matches.first().map(|m| {
        format!(
            "\n{} Quick fix: {}",
            "→".cyan(),
            command_template.replace("{}", &m.candidate).cyan()
        )
    })
}

/// Context for providing smart suggestions
pub struct SuggestionContext {
    /// Available topic names
    pub topics: Vec<String>,
    /// Available consumer group IDs
    pub groups: Vec<String>,
}

impl SuggestionContext {
    pub fn new() -> Self {
        Self {
            topics: vec![],
            groups: vec![],
        }
    }

    /// Load context from the data directory
    pub fn load_from_data_dir(data_dir: &std::path::Path) -> Self {
        use crate::{GroupCoordinator, TopicManager};

        let mut ctx = Self::new();

        if !data_dir.exists() {
            return ctx;
        }

        // Load topics
        let manager = match TopicManager::new(data_dir) {
            Ok(m) => {
                if let Ok(topics) = m.list_topics() {
                    ctx.topics = topics.into_iter().map(|t| t.name).collect();
                }
                std::sync::Arc::new(m)
            }
            Err(_) => return ctx,
        };

        // Load groups
        if let Ok(coordinator) = GroupCoordinator::new(data_dir, manager) {
            if let Ok(groups) = coordinator.list_groups() {
                ctx.groups = groups;
            }
        }

        ctx
    }

    /// Suggest a topic name correction
    pub fn suggest_topic(&self, input: &str) -> Option<String> {
        suggest_correction(input, &self.topics, "topic")
    }

    /// Suggest a group ID correction
    pub fn suggest_group(&self, input: &str) -> Option<String> {
        suggest_correction(input, &self.groups, "consumer group")
    }
}

impl Default for SuggestionContext {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_levenshtein_distance() {
        assert_eq!(levenshtein_distance("", ""), 0);
        assert_eq!(levenshtein_distance("abc", "abc"), 0);
        assert_eq!(levenshtein_distance("abc", ""), 3);
        assert_eq!(levenshtein_distance("", "abc"), 3);
        assert_eq!(levenshtein_distance("kitten", "sitting"), 3);
        assert_eq!(levenshtein_distance("orders", "ordres"), 2);
    }

    #[test]
    fn test_similarity_score() {
        assert!((similarity_score("abc", "abc") - 1.0).abs() < 0.001);
        assert!(similarity_score("orders", "ordres") > 0.6);
        assert!(similarity_score("abc", "xyz") < 0.5);
    }

    #[test]
    fn test_find_similar() {
        let candidates = vec![
            "orders".to_string(),
            "order-events".to_string(),
            "users".to_string(),
            "products".to_string(),
        ];

        let matches = find_similar("ordres", &candidates, 3);
        assert!(!matches.is_empty());
        assert_eq!(matches[0].candidate, "orders");

        let matches = find_similar("order", &candidates, 3);
        assert!(!matches.is_empty());
        // Should match both "orders" and "order-events"
        assert!(matches.iter().any(|m| m.candidate == "orders"));
        assert!(matches.iter().any(|m| m.candidate == "order-events"));
    }

    #[test]
    fn test_is_prefix_match() {
        assert!(is_prefix_match("ord", "orders"));
        assert!(is_prefix_match("orders", "ord"));
        assert!(!is_prefix_match("abc", "xyz"));
    }

    #[test]
    fn test_has_common_substring() {
        assert!(has_common_substring("order-events", "orders", 3));
        assert!(!has_common_substring("abc", "xyz", 2));
    }
}
