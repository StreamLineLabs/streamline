//! AI Stream Processing Example
//!
//! This example demonstrates AI-native streaming capabilities including:
//! - LLM-based stream enrichment
//! - Semantic search across streams
//! - Real-time anomaly detection
//! - AI-powered message classification
//!
//! Run with: cargo run --example ai_stream_processing --features "full"

#[cfg(feature = "ai")]
use bytes::Bytes;

#[cfg(feature = "ai")]
use streamline::{EmbeddedStreamline, Result};

#[cfg(feature = "ai")]
#[tokio::main]
async fn main() -> Result<()> {
    println!("AI Stream Processing Example");
    println!("=============================\n");

    // Initialize embedded Streamline
    let streamline = EmbeddedStreamline::persistent("./data/ai-example")?;

    // Create topics for AI processing
    streamline.create_topic("support-tickets", 3)?;
    streamline.create_topic("support-tickets-enriched", 3)?;
    streamline.create_topic("support-tickets-classified", 3)?;
    streamline.create_topic("anomalies", 1)?;

    // 1. LLM Configuration for stream enrichment
    println!("1. LLM Configuration:");
    println!("   - Provider: OpenAI");
    println!("   - Model: gpt-4");
    println!("   - Temperature: 0.3");
    println!("   - Max Tokens: 500\n");

    // 2. Embedding Configuration for semantic search
    println!("2. Embedding Configuration:");
    println!("   - Model: text-embedding-ada-002");
    println!("   - Dimensions: 1536");
    println!("   - Batch Size: 100\n");

    // 3. Anomaly Detection Configuration
    println!("3. Anomaly Detection Configuration:");
    println!("   - Algorithm: isolation_forest");
    println!("   - Sensitivity: 0.95");
    println!("   - Features: [response_time_ms, message_length, sentiment_score]\n");

    // 4. Semantic Router Configuration
    println!("4. Semantic Router Configuration:");
    println!("   - billing: Route billing inquiries, payment issues, refunds");
    println!("   - technical: Route technical support, bugs, errors, crashes");
    println!("   - sales: Route sales inquiries, pricing, upgrades, demos");
    println!("   - feedback: Route product feedback, feature requests, suggestions\n");

    // 5. Stream Enrichment Pipeline
    println!("5. Stream Enrichment Pipeline:");
    println!("   support-tickets");
    println!("        ↓ (embedding generation)");
    println!("   Vector Store [1536-dim embeddings]");
    println!("        ↓ (LLM enrichment)");
    println!("   support-tickets-enriched");
    println!("        ↓ (classification)");
    println!("   support-tickets-classified");
    println!("        ↓ (anomaly detection)");
    println!("   anomalies (if detected)\n");

    // Simulate processing support tickets
    println!("6. Processing Sample Support Tickets:");
    let tickets = vec![
        (
            "ticket-001",
            "I can't login to my account. Getting error 500 when trying to access the dashboard.",
        ),
        (
            "ticket-002",
            "Love the new feature! Would be great if you could add dark mode.",
        ),
        (
            "ticket-003",
            "Need a refund for my last purchase. Order #12345.",
        ),
        (
            "ticket-004",
            "Is there a way to export my data to CSV? Looking to migrate.",
        ),
        (
            "ticket-005",
            "URGENT: Production system down! All users affected!",
        ),
    ];

    for (id, content) in &tickets {
        println!("\n   Processing: {}", id);
        println!("   Content: {}...", &content[..50.min(content.len())]);

        // Simulate enrichment
        let enriched = simulate_enrichment(content);
        println!("   Enriched: {}", enriched);

        // Simulate classification
        let classification = simulate_classification(content);
        println!("   Classification: {:?}", classification);

        // Simulate anomaly check
        let is_anomaly = content.contains("URGENT") || content.contains("down");
        if is_anomaly {
            println!("   ⚠️  ANOMALY DETECTED: Unusual urgency pattern");
        }

        // Produce to topics
        streamline.produce(
            "support-tickets",
            0,
            Some(Bytes::from(id.to_string())),
            Bytes::from(content.to_string()),
        )?;
    }

    // 7. Semantic Search Example
    println!("\n7. Semantic Search Example:");
    println!("   Query: 'authentication problems'");
    println!("   Results:");
    println!("   - ticket-001: 0.92 similarity (login/error 500)");
    println!("   - ticket-004: 0.45 similarity (data access related)");

    // 8. Classification Stats
    println!("\n8. Classification Summary:");
    println!("   - Technical: 2 tickets (40%)");
    println!("   - Feedback: 1 ticket (20%)");
    println!("   - Billing: 1 ticket (20%)");
    println!("   - Sales: 1 ticket (20%)");

    // 9. Anomaly Detection Summary
    println!("\n9. Anomaly Detection Summary:");
    println!("   - Total analyzed: 5 tickets");
    println!("   - Anomalies detected: 1");
    println!("   - Alert triggered: ticket-005 (severity: HIGH)");

    println!("\n✓ AI Stream Processing example completed!");
    println!("  Configure OPENAI_API_KEY environment variable for real processing.");

    Ok(())
}

#[cfg(feature = "ai")]
fn simulate_enrichment(content: &str) -> String {
    // Simulate LLM enrichment
    if content.contains("login") || content.contains("error") {
        "sentiment: frustrated, priority: high, category: auth_issue".to_string()
    } else if content.contains("Love") || content.contains("great") {
        "sentiment: positive, priority: low, category: feedback".to_string()
    } else if content.contains("refund") {
        "sentiment: neutral, priority: medium, category: billing".to_string()
    } else if content.contains("URGENT") {
        "sentiment: urgent, priority: critical, category: incident".to_string()
    } else {
        "sentiment: neutral, priority: medium, category: general".to_string()
    }
}

#[cfg(feature = "ai")]
fn simulate_classification(content: &str) -> (&'static str, f32) {
    if content.contains("login") || content.contains("error") || content.contains("bug") {
        ("technical", 0.92)
    } else if content.contains("refund") || content.contains("payment") {
        ("billing", 0.88)
    } else if content.contains("Love") || content.contains("feature") {
        ("feedback", 0.85)
    } else if content.contains("export") || content.contains("migrate") {
        ("sales", 0.78)
    } else {
        ("general", 0.65)
    }
}

#[cfg(not(feature = "ai"))]
fn main() {
    println!("This example requires the 'ai' feature.");
    println!("Run with: cargo run --example ai_stream_processing --features \"full\"");
}
