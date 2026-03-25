//! M1 P0 — Decay & Summarization Simulation.
//!
//! Replaces the planned `decay.ipynb` with a self-contained Rust binary
//! so it runs in CI without a Python toolchain. Outputs CSV-formatted
//! summary statistics on stdout and a brief recommendation for the
//! `episodic`-tier half-life default.
//!
//! Run:
//! ```bash
//! cargo run --bin decay_sim --manifest-path streamline/Cargo.toml
//! ```
//!
//! NOTE: this binary is intentionally not declared in `Cargo.toml`
//! `[[bin]]` yet — it is a Phase-0 spike artifact. To execute, copy this
//! file under `src/bin/` or add a `[[bin]]` entry temporarily.
//!
//! The simulation:
//! - Generates 90 days of synthetic agent events (varying density/day).
//! - Applies an exponential decay with configurable half-life.
//! - Reports % retained vs. half-life across {3,7,14,30,60} days.
//! - Reports % "important" facts retained — proxied by access_count > 3.

use std::collections::HashMap;

const DAYS: usize = 90;
const HALF_LIVES_DAYS: &[f64] = &[3.0, 7.0, 14.0, 30.0, 60.0];
const IMPORTANCE_THRESHOLD: f64 = 0.2;

struct Memory {
    day_added: u32,
    base_importance: f64,
    access_count: u32,
}

fn main() {
    let mut rng_state: u64 = 0xc0ffee;
    let mut next_u64 = || -> u64 {
        rng_state = rng_state
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        rng_state
    };
    let mut next_f = || -> f64 { (next_u64() >> 11) as f64 / (1u64 << 53) as f64 };

    // Generate events.
    let mut memories: Vec<Memory> = Vec::new();
    for day in 0..DAYS as u32 {
        // Avg 12 events/day with weekly seasonality.
        let lambda = 12.0 + (((day % 7) as f64) - 3.0).abs() * 2.0;
        let n = (lambda + (next_f() * 6.0 - 3.0)).max(1.0) as u32;
        for _ in 0..n {
            // Importance ~ skewed Beta-ish: most events boring, few interesting.
            let r = next_f();
            let base = if r < 0.85 { r * 0.3 } else { 0.5 + r * 0.5 };
            // Access count: 70% never accessed; 25% accessed 1-3x; 5% hot.
            let acc_r = next_f();
            let access = if acc_r < 0.70 {
                0
            } else if acc_r < 0.95 {
                (next_f() * 3.0) as u32 + 1
            } else {
                (next_f() * 20.0) as u32 + 4
            };
            memories.push(Memory {
                day_added: day,
                base_importance: base,
                access_count: access,
            });
        }
    }

    println!("# M1 P0 Decay Simulation");
    println!("# Synthetic 90-day agent log: {} events", memories.len());
    println!();
    println!("half_life_days,retained_pct,important_retained_pct,heavy_hit_retained_pct");

    let mut best_balance = (0.0_f64, f64::INFINITY);

    for &half_life in HALF_LIVES_DAYS {
        let lambda = (2f64.ln()) / half_life;
        let mut total = 0u64;
        let mut retained = 0u64;
        let mut important_total = 0u64;
        let mut important_retained = 0u64;
        let mut hot_total = 0u64;
        let mut hot_retained = 0u64;

        for m in &memories {
            let age = (DAYS as u32 - 1 - m.day_added) as f64;
            let recency = (-lambda * age).exp();
            let access_boost = (m.access_count as f64 + 1.0).ln() * 0.15;
            let score = m.base_importance * 0.6 + recency * 0.3 + access_boost.min(0.4) * 0.4;
            let kept = score >= IMPORTANCE_THRESHOLD;
            total += 1;
            if kept {
                retained += 1;
            }
            if m.base_importance > 0.5 {
                important_total += 1;
                if kept {
                    important_retained += 1;
                }
            }
            if m.access_count >= 4 {
                hot_total += 1;
                if kept {
                    hot_retained += 1;
                }
            }
        }
        let pct = retained as f64 * 100.0 / total as f64;
        let imp_pct = if important_total > 0 {
            important_retained as f64 * 100.0 / important_total as f64
        } else {
            0.0
        };
        let hot_pct = if hot_total > 0 {
            hot_retained as f64 * 100.0 / hot_total as f64
        } else {
            0.0
        };
        println!("{half_life:.0},{pct:.1},{imp_pct:.1},{hot_pct:.1}");

        // Heuristic: minimize |retained - 30%| while important_retained > 90%.
        let drift = (pct - 30.0).abs();
        if imp_pct >= 90.0 && drift < best_balance.1 {
            best_balance = (half_life, drift);
        }
    }

    println!();
    println!("# Recommended episodic half-life: {} days", best_balance.0 as u32);
    println!("#   (target: ~30% total retained, important retention ≥ 90%)");

    // Tier-distribution histogram.
    let mut tier_counts: HashMap<&str, u32> = HashMap::new();
    for m in &memories {
        let tier = if m.base_importance > 0.6 || m.access_count >= 4 {
            "semantic"
        } else if m.access_count >= 1 {
            "episodic-warm"
        } else {
            "episodic-cold"
        };
        *tier_counts.entry(tier).or_insert(0) += 1;
    }
    println!();
    println!("# Tier distribution at end of 90 days:");
    for (k, v) in &tier_counts {
        println!("#   {k}: {v}");
    }
}
