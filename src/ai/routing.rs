//! Semantic Routing
//!
//! AI-powered message routing based on content semantics.

use super::config::{RoutingConfig, RoutingRuleConfig};
use super::embedding::EmbeddingEngine;
use crate::error::Result;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::RwLock;

/// Semantic router for AI-powered message routing
pub struct SemanticRouter {
    /// Configuration
    config: RoutingConfig,
    /// Embedding engine
    embeddings: Arc<EmbeddingEngine>,
    /// Routing rules with pre-computed embeddings
    rules: Arc<RwLock<Vec<CompiledRule>>>,
}

impl SemanticRouter {
    /// Create a new semantic router
    pub fn new(config: RoutingConfig, embeddings: Arc<EmbeddingEngine>) -> Result<Self> {
        let router = Self {
            config: config.clone(),
            embeddings,
            rules: Arc::new(RwLock::new(Vec::new())),
        };

        // Don't block on rule compilation
        // Rules will be compiled when accessed
        Ok(router)
    }

    /// Initialize rules (compile embeddings)
    pub async fn initialize(&self) -> Result<()> {
        let mut compiled_rules = Vec::new();

        for rule_config in &self.config.rules {
            let embedding = if let Some(ref pre_computed) = rule_config.embedding {
                pre_computed.clone()
            } else if self.config.use_embeddings {
                // Generate embedding for rule description
                let result = self.embeddings.embed_text(&rule_config.description).await?;
                result.vector
            } else {
                Vec::new()
            };

            compiled_rules.push(CompiledRule {
                name: rule_config.name.clone(),
                description: rule_config.description.clone(),
                destination: rule_config.destination.clone(),
                priority: rule_config.priority,
                embedding,
                keywords: extract_keywords(&rule_config.description),
            });
        }

        // Sort by priority
        compiled_rules.sort_by(|a, b| b.priority.cmp(&a.priority));

        let mut rules = self.rules.write().await;
        *rules = compiled_rules;

        Ok(())
    }

    /// Add a routing rule
    pub async fn add_rule(&self, rule: RoutingRuleConfig) -> Result<()> {
        let embedding = if let Some(ref pre_computed) = rule.embedding {
            pre_computed.clone()
        } else if self.config.use_embeddings {
            let result = self.embeddings.embed_text(&rule.description).await?;
            result.vector
        } else {
            Vec::new()
        };

        let compiled = CompiledRule {
            name: rule.name.clone(),
            description: rule.description.clone(),
            destination: rule.destination.clone(),
            priority: rule.priority,
            embedding,
            keywords: extract_keywords(&rule.description),
        };

        let mut rules = self.rules.write().await;
        rules.push(compiled);
        rules.sort_by(|a, b| b.priority.cmp(&a.priority));

        Ok(())
    }

    /// Remove a routing rule by name
    pub async fn remove_rule(&self, name: &str) -> bool {
        let mut rules = self.rules.write().await;
        let len_before = rules.len();
        rules.retain(|r| r.name != name);
        rules.len() < len_before
    }

    /// Route a message
    pub async fn route(&self, _topic: &str, message: &str) -> Result<RoutingDecision> {
        if !self.config.enabled {
            return Ok(RoutingDecision {
                destination: self.config.default_destination.clone(),
                matched_rule: None,
                confidence: 1.0,
                alternatives: Vec::new(),
            });
        }

        let rules = self.rules.read().await;

        if rules.is_empty() {
            return Ok(RoutingDecision {
                destination: self.config.default_destination.clone(),
                matched_rule: None,
                confidence: 1.0,
                alternatives: Vec::new(),
            });
        }

        // Generate embedding for message
        let message_embedding = if self.config.use_embeddings {
            Some(self.embeddings.embed_text(message).await?.vector)
        } else {
            None
        };

        // Score each rule
        let mut scored_rules: Vec<(f32, &CompiledRule)> = Vec::new();

        for rule in rules.iter() {
            let score = if let Some(ref msg_emb) = message_embedding {
                if !rule.embedding.is_empty() {
                    EmbeddingEngine::cosine_similarity(msg_emb, &rule.embedding)
                } else {
                    keyword_match_score(message, &rule.keywords)
                }
            } else {
                keyword_match_score(message, &rule.keywords)
            };

            scored_rules.push((score, rule));
        }

        // Sort by score
        scored_rules.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal));

        // Check if best match exceeds threshold
        if let Some((score, rule)) = scored_rules.first() {
            if *score >= self.config.similarity_threshold {
                let alternatives: Vec<RoutingAlternative> = scored_rules
                    .iter()
                    .skip(1)
                    .take(3)
                    .map(|(s, r)| RoutingAlternative {
                        destination: r.destination.clone(),
                        rule_name: r.name.clone(),
                        score: *s,
                    })
                    .collect();

                return Ok(RoutingDecision {
                    destination: rule.destination.clone(),
                    matched_rule: Some(rule.name.clone()),
                    confidence: *score,
                    alternatives,
                });
            }
        }

        // No match above threshold, use default
        Ok(RoutingDecision {
            destination: self.config.default_destination.clone(),
            matched_rule: None,
            confidence: 0.0,
            alternatives: scored_rules
                .iter()
                .take(3)
                .map(|(s, r)| RoutingAlternative {
                    destination: r.destination.clone(),
                    rule_name: r.name.clone(),
                    score: *s,
                })
                .collect(),
        })
    }

    /// Route with custom rules (for testing or one-off routing)
    pub async fn route_with_rules(
        &self,
        message: &str,
        custom_rules: &[RoutingRule],
    ) -> Result<RoutingDecision> {
        if custom_rules.is_empty() {
            return Ok(RoutingDecision {
                destination: self.config.default_destination.clone(),
                matched_rule: None,
                confidence: 1.0,
                alternatives: Vec::new(),
            });
        }

        let message_embedding = self.embeddings.embed_text(message).await?.vector;

        let mut scored_rules: Vec<(f32, &RoutingRule)> = Vec::new();

        for rule in custom_rules {
            let rule_embedding = self.embeddings.embed_text(&rule.description).await?.vector;
            let score = EmbeddingEngine::cosine_similarity(&message_embedding, &rule_embedding);
            scored_rules.push((score, rule));
        }

        scored_rules.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal));

        if let Some((score, rule)) = scored_rules.first() {
            if *score >= self.config.similarity_threshold {
                return Ok(RoutingDecision {
                    destination: rule.destination.clone(),
                    matched_rule: Some(rule.name.clone()),
                    confidence: *score,
                    alternatives: Vec::new(),
                });
            }
        }

        Ok(RoutingDecision {
            destination: self.config.default_destination.clone(),
            matched_rule: None,
            confidence: 0.0,
            alternatives: Vec::new(),
        })
    }

    /// Get all rules
    pub async fn list_rules(&self) -> Vec<RoutingRuleInfo> {
        let rules = self.rules.read().await;
        rules
            .iter()
            .map(|r| RoutingRuleInfo {
                name: r.name.clone(),
                description: r.description.clone(),
                destination: r.destination.clone(),
                priority: r.priority,
                has_embedding: !r.embedding.is_empty(),
            })
            .collect()
    }

    /// Get router statistics
    pub async fn stats(&self) -> RouterStats {
        let rules = self.rules.read().await;
        RouterStats {
            total_rules: rules.len(),
            rules_with_embeddings: rules.iter().filter(|r| !r.embedding.is_empty()).count(),
            default_destination: self.config.default_destination.clone(),
            similarity_threshold: self.config.similarity_threshold,
        }
    }
}

/// Compiled routing rule with pre-computed embedding
#[derive(Debug, Clone)]
struct CompiledRule {
    /// Rule name
    name: String,
    /// Description/example text
    description: String,
    /// Target destination
    destination: String,
    /// Priority
    priority: i32,
    /// Pre-computed embedding
    embedding: Vec<f32>,
    /// Extracted keywords for fallback matching
    keywords: Vec<String>,
}

/// Routing rule definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RoutingRule {
    /// Rule name
    pub name: String,
    /// Description or example text for semantic matching
    pub description: String,
    /// Target destination (topic, partition, endpoint)
    pub destination: String,
    /// Priority (higher = checked first)
    pub priority: i32,
}

impl RoutingRule {
    /// Create a new routing rule
    pub fn new(name: &str, description: &str, destination: &str) -> Self {
        Self {
            name: name.to_string(),
            description: description.to_string(),
            destination: destination.to_string(),
            priority: 0,
        }
    }

    /// Set priority
    pub fn with_priority(mut self, priority: i32) -> Self {
        self.priority = priority;
        self
    }
}

/// Routing decision
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RoutingDecision {
    /// Selected destination
    pub destination: String,
    /// Matched rule name (if any)
    pub matched_rule: Option<String>,
    /// Confidence score (0.0 - 1.0)
    pub confidence: f32,
    /// Alternative destinations
    pub alternatives: Vec<RoutingAlternative>,
}

impl RoutingDecision {
    /// Check if a rule was matched
    pub fn is_matched(&self) -> bool {
        self.matched_rule.is_some()
    }

    /// Get the best alternative if no match
    pub fn best_alternative(&self) -> Option<&RoutingAlternative> {
        self.alternatives.first()
    }
}

/// Alternative routing option
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RoutingAlternative {
    /// Destination
    pub destination: String,
    /// Rule name
    pub rule_name: String,
    /// Match score
    pub score: f32,
}

/// Information about a routing rule
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RoutingRuleInfo {
    /// Rule name
    pub name: String,
    /// Description
    pub description: String,
    /// Destination
    pub destination: String,
    /// Priority
    pub priority: i32,
    /// Whether embedding is computed
    pub has_embedding: bool,
}

/// Router statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RouterStats {
    /// Total number of rules
    pub total_rules: usize,
    /// Rules with pre-computed embeddings
    pub rules_with_embeddings: usize,
    /// Default destination
    pub default_destination: String,
    /// Similarity threshold
    pub similarity_threshold: f32,
}

/// Multi-topic router for routing to multiple destinations
pub struct MultiRouter {
    /// Primary router
    router: Arc<SemanticRouter>,
    /// Topic-specific routers
    topic_routers: Arc<RwLock<std::collections::HashMap<String, Arc<SemanticRouter>>>>,
}

impl MultiRouter {
    /// Create a new multi-router
    pub fn new(router: Arc<SemanticRouter>) -> Self {
        Self {
            router,
            topic_routers: Arc::new(RwLock::new(std::collections::HashMap::new())),
        }
    }

    /// Add a topic-specific router
    pub async fn add_topic_router(&self, topic: String, router: Arc<SemanticRouter>) {
        let mut routers = self.topic_routers.write().await;
        routers.insert(topic, router);
    }

    /// Route message with topic-specific routing
    pub async fn route(&self, topic: &str, message: &str) -> Result<RoutingDecision> {
        let routers = self.topic_routers.read().await;

        if let Some(topic_router) = routers.get(topic) {
            topic_router.route(topic, message).await
        } else {
            self.router.route(topic, message).await
        }
    }
}

/// Extract keywords from text for fallback matching
fn extract_keywords(text: &str) -> Vec<String> {
    text.to_lowercase()
        .split(|c: char| !c.is_alphanumeric())
        .filter(|s| !s.is_empty() && s.len() > 2)
        .map(|s| s.to_string())
        .collect()
}

/// Calculate keyword match score
fn keyword_match_score(message: &str, keywords: &[String]) -> f32 {
    if keywords.is_empty() {
        return 0.0;
    }

    let message_lower = message.to_lowercase();
    let matches = keywords
        .iter()
        .filter(|k| message_lower.contains(k.as_str()))
        .count();

    matches as f32 / keywords.len() as f32
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ai::config::EmbeddingConfig;

    #[test]
    fn test_routing_rule() {
        let rule = RoutingRule::new("errors", "Error messages and exceptions", "error-topic")
            .with_priority(10);

        assert_eq!(rule.name, "errors");
        assert_eq!(rule.destination, "error-topic");
        assert_eq!(rule.priority, 10);
    }

    #[test]
    fn test_routing_decision() {
        let decision = RoutingDecision {
            destination: "topic-a".to_string(),
            matched_rule: Some("rule-1".to_string()),
            confidence: 0.9,
            alternatives: Vec::new(),
        };

        assert!(decision.is_matched());
        assert_eq!(decision.matched_rule, Some("rule-1".to_string()));
    }

    #[test]
    fn test_extract_keywords() {
        let keywords = extract_keywords("Error handling and exception processing");
        assert!(keywords.contains(&"error".to_string()));
        assert!(keywords.contains(&"handling".to_string()));
        assert!(keywords.contains(&"exception".to_string()));
    }

    #[test]
    fn test_keyword_match_score() {
        let keywords = vec!["error".to_string(), "exception".to_string()];

        let score1 = keyword_match_score("This is an error message", &keywords);
        assert!(score1 > 0.0);

        let score2 = keyword_match_score("This has error and exception", &keywords);
        assert!(score2 > score1);

        let score3 = keyword_match_score("Normal message", &keywords);
        assert_eq!(score3, 0.0);
    }

    #[tokio::test]
    async fn test_semantic_router() {
        let embedding_config = EmbeddingConfig::default();
        let routing_config = RoutingConfig::default();
        let embeddings = Arc::new(EmbeddingEngine::new(embedding_config).unwrap());

        let router = SemanticRouter::new(routing_config, embeddings).unwrap();

        // Route without rules should return default
        let decision = router.route("topic", "test message").await.unwrap();
        assert_eq!(decision.destination, "default");
        assert!(decision.matched_rule.is_none());
    }

    #[tokio::test]
    async fn test_router_with_rules() {
        let embedding_config = EmbeddingConfig::default();
        let routing_config = RoutingConfig {
            enabled: true,
            similarity_threshold: 0.1, // Low threshold for testing
            ..Default::default()
        };

        let embeddings = Arc::new(EmbeddingEngine::new(embedding_config).unwrap());
        let router = SemanticRouter::new(routing_config, embeddings).unwrap();

        // Add a rule
        let rule = RoutingRuleConfig {
            name: "errors".to_string(),
            description: "Error messages and exceptions".to_string(),
            destination: "error-topic".to_string(),
            priority: 10,
            embedding: None,
        };

        router.add_rule(rule).await.unwrap();

        // Check stats
        let stats = router.stats().await;
        assert_eq!(stats.total_rules, 1);
    }
}
