//! Optimization algorithms for auto-tuning
//!
//! This module provides various optimization algorithms for automatically tuning
//! Streamline's performance parameters. Each algorithm has different characteristics:
//!
//! - **Gradient Descent**: Fast convergence for smooth, convex objectives. Best for
//!   fine-tuning when near optimal values. Uses momentum for faster convergence.
//!
//! - **Genetic Algorithm**: Global optimization using evolutionary principles. Good for
//!   exploring large parameter spaces with multiple local optima.
//!
//! - **Bayesian Optimization**: Sample-efficient optimization using surrogate models.
//!   Best when evaluations are expensive (each configuration requires time to measure).
//!
//! - **Simulated Annealing**: Probabilistic global optimization that can escape local
//!   optima. Good balance between exploration and exploitation.
//!
//! # Example
//!
//! ```
//! use streamline::autotuning::optimizer::{GeneticOptimizer, OptimizerConfig};
//!
//! // Define parameter bounds: (min, max) for each parameter
//! let bounds = vec![
//!     (1.0, 100.0),   // batch_size
//!     (1.0, 32.0),    // buffer_count
//!     (0.0, 1.0),     // compression_ratio
//! ];
//!
//! let config = OptimizerConfig {
//!     population_size: 20,
//!     mutation_rate: 0.1,
//!     crossover_rate: 0.7,
//!     ..Default::default()
//! };
//!
//! let mut optimizer = GeneticOptimizer::new(config, bounds);
//!
//! // Fitness function: higher is better
//! let fitness_fn = |params: &[f64]| {
//!     // Evaluate configuration performance
//!     params[0] * params[1] - params[2] * 10.0
//! };
//!
//! // Run for 10 generations
//! for _ in 0..10 {
//!     optimizer.evolve(fitness_fn);
//! }
//!
//! if let Some(best) = optimizer.best() {
//!     println!("Best parameters: {:?}", best.genes);
//! }
//! ```

use rand::Rng;
use serde::{Deserialize, Serialize};

/// Optimization algorithm type.
///
/// Different algorithms have different trade-offs between exploration (searching
/// new areas of the parameter space) and exploitation (refining known good solutions).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum OptimizerType {
    /// Gradient descent with momentum.
    /// Best for: Fine-tuning near optimal values, smooth objective functions.
    GradientDescent,
    /// Genetic algorithm with elitism, tournament selection, crossover, and mutation.
    /// Best for: Large parameter spaces, multiple local optima.
    Genetic,
    /// Bayesian optimization using acquisition functions.
    /// Best for: Expensive evaluations, sample-efficient optimization.
    Bayesian,
    /// Simulated annealing with exponential cooling schedule.
    /// Best for: Escaping local optima, balanced exploration/exploitation.
    SimulatedAnnealing,
    /// Pure random search.
    /// Best for: Baseline comparison, very high-dimensional spaces.
    RandomSearch,
}

/// Configuration for optimization algorithms.
///
/// Different fields are used by different optimizer types:
/// - Gradient descent uses: `learning_rate`
/// - Genetic algorithm uses: `population_size`, `mutation_rate`, `crossover_rate`
/// - Simulated annealing uses: `initial_temperature`, `cooling_rate`
/// - Bayesian optimization uses: `num_samples`
#[derive(Debug, Clone)]
pub struct OptimizerConfig {
    /// Which optimization algorithm to use
    pub optimizer_type: OptimizerType,

    /// Learning rate for gradient descent (typically 0.001 to 0.1).
    /// Smaller values = more stable but slower convergence.
    pub learning_rate: f64,

    /// Number of individuals in genetic algorithm population.
    /// Larger populations = better exploration but more evaluations per generation.
    pub population_size: usize,

    /// Probability of mutating each gene (0.0 to 1.0).
    /// Higher values = more exploration, lower = more exploitation.
    pub mutation_rate: f64,

    /// Probability of gene crossover between parents (0.0 to 1.0).
    /// Higher values = more mixing of parent traits.
    pub crossover_rate: f64,

    /// Starting temperature for simulated annealing.
    /// Higher values = more initial exploration.
    pub initial_temperature: f64,

    /// Temperature decay factor per iteration (0.0 to 1.0).
    /// Closer to 1.0 = slower cooling, more exploration.
    pub cooling_rate: f64,

    /// Number of initial random samples for Bayesian optimization.
    /// More samples = better surrogate model but slower start.
    pub num_samples: usize,
}

impl Default for OptimizerConfig {
    fn default() -> Self {
        Self {
            optimizer_type: OptimizerType::GradientDescent,
            learning_rate: 0.01,
            population_size: 20,
            mutation_rate: 0.1,
            crossover_rate: 0.7,
            initial_temperature: 100.0,
            cooling_rate: 0.95,
            num_samples: 10,
        }
    }
}

/// An individual solution in the genetic algorithm population.
///
/// Each individual represents a candidate solution (set of parameter values)
/// with an associated fitness score indicating how well it performs.
#[derive(Debug, Clone)]
pub struct Individual {
    /// Parameter values (genes) representing a candidate solution.
    /// Each element corresponds to one tunable parameter.
    pub genes: Vec<f64>,

    /// Fitness score indicating solution quality (higher is better).
    /// Set to 0.0 initially and updated during evaluation.
    pub fitness: f64,
}

impl Individual {
    /// Create a new individual
    pub fn new(genes: Vec<f64>) -> Self {
        Self {
            genes,
            fitness: 0.0,
        }
    }

    /// Create a random individual within bounds
    pub fn random(bounds: &[(f64, f64)]) -> Self {
        let mut rng = rand::thread_rng();
        let genes: Vec<f64> = bounds
            .iter()
            .map(|(min, max)| rng.gen_range(*min..*max))
            .collect();
        Self::new(genes)
    }

    /// Crossover with another individual
    pub fn crossover(&self, other: &Individual, rate: f64) -> (Individual, Individual) {
        let mut rng = rand::thread_rng();
        let mut child1 = self.genes.clone();
        let mut child2 = other.genes.clone();

        for i in 0..self.genes.len() {
            if rng.gen::<f64>() < rate {
                std::mem::swap(&mut child1[i], &mut child2[i]);
            }
        }

        (Individual::new(child1), Individual::new(child2))
    }

    /// Mutate the individual
    pub fn mutate(&mut self, rate: f64, bounds: &[(f64, f64)]) {
        let mut rng = rand::thread_rng();
        for (i, &(min, max)) in bounds.iter().enumerate().take(self.genes.len()) {
            if rng.gen::<f64>() < rate {
                let range = max - min;
                let delta = rng.gen_range(-range * 0.1..range * 0.1);
                self.genes[i] = (self.genes[i] + delta).clamp(min, max);
            }
        }
    }
}

/// Genetic algorithm optimizer for global parameter optimization.
///
/// Uses evolutionary principles to search the parameter space:
/// 1. Initialize random population
/// 2. Evaluate fitness of each individual
/// 3. Select parents using tournament selection
/// 4. Create offspring via crossover and mutation
/// 5. Replace population with offspring (elitism preserves best)
/// 6. Repeat until convergence or budget exhausted
///
/// # Characteristics
/// - Good at escaping local optima
/// - Parallel evaluation possible (all individuals independent)
/// - Requires many fitness evaluations
/// - Works well with discontinuous or noisy objectives
pub struct GeneticOptimizer {
    /// Algorithm configuration
    config: OptimizerConfig,
    /// Current population of candidate solutions
    population: Vec<Individual>,
    /// Parameter bounds: (min, max) for each dimension
    bounds: Vec<(f64, f64)>,
    /// Current generation number
    generation: usize,
    /// Best individual found across all generations
    best_ever: Option<Individual>,
}

impl GeneticOptimizer {
    /// Create a new genetic optimizer
    pub fn new(config: OptimizerConfig, bounds: Vec<(f64, f64)>) -> Self {
        let population: Vec<Individual> = (0..config.population_size)
            .map(|_| Individual::random(&bounds))
            .collect();

        Self {
            config,
            population,
            bounds,
            generation: 0,
            best_ever: None,
        }
    }

    /// Evolve one generation
    pub fn evolve<F>(&mut self, fitness_fn: F)
    where
        F: Fn(&[f64]) -> f64,
    {
        // Evaluate fitness
        for ind in &mut self.population {
            ind.fitness = fitness_fn(&ind.genes);
        }

        // Sort by fitness (descending)
        self.population
            .sort_by(|a, b| b.fitness.partial_cmp(&a.fitness).unwrap_or(std::cmp::Ordering::Equal));

        // Update best ever
        if let Some(best) = self.population.first() {
            if self.best_ever.as_ref().map_or(true, |current| best.fitness > current.fitness) {
                self.best_ever = Some(best.clone());
            }
        }

        // Selection (tournament)
        let mut new_population = Vec::new();

        // Elitism: keep best individual
        if let Some(best) = self.population.first() {
            new_population.push(best.clone());
        }

        // Create new individuals
        let mut rng = rand::thread_rng();
        while new_population.len() < self.config.population_size {
            // Tournament selection
            let parent1 = self.tournament_select(&mut rng);
            let parent2 = self.tournament_select(&mut rng);

            // Crossover
            let (mut child1, mut child2) = parent1.crossover(&parent2, self.config.crossover_rate);

            // Mutation
            child1.mutate(self.config.mutation_rate, &self.bounds);
            child2.mutate(self.config.mutation_rate, &self.bounds);

            new_population.push(child1);
            if new_population.len() < self.config.population_size {
                new_population.push(child2);
            }
        }

        self.population = new_population;
        self.generation += 1;
    }

    /// Tournament selection
    fn tournament_select(&self, rng: &mut impl Rng) -> Individual {
        let tournament_size = 3;
        let mut best: Option<&Individual> = None;

        for _ in 0..tournament_size {
            let idx = rng.gen_range(0..self.population.len());
            let candidate = &self.population[idx];
            if best.map_or(true, |b| candidate.fitness > b.fitness) {
                best = Some(candidate);
            }
        }

        best.cloned().unwrap_or_else(|| Individual::random(&self.bounds))
    }

    /// Get best individual
    pub fn best(&self) -> Option<&Individual> {
        self.best_ever.as_ref()
    }

    /// Get current generation
    pub fn generation(&self) -> usize {
        self.generation
    }
}

/// Gradient descent optimizer with momentum for local optimization.
///
/// Iteratively improves parameters by following the gradient of the objective
/// function. Uses momentum to accelerate convergence and escape shallow local
/// minima.
///
/// # Characteristics
/// - Fast convergence near optima
/// - Requires gradient computation (numerical approximation)
/// - May get stuck in local optima
/// - Best for fine-tuning after rough optimization
///
/// # Algorithm
/// ```text
/// momentum[i] = β * momentum[i] + α * gradient[i]
/// params[i] = params[i] + momentum[i]
/// ```
/// where α is the learning rate and β = 0.9 is the momentum factor.
pub struct GradientDescentOptimizer {
    /// Algorithm configuration
    config: OptimizerConfig,
    /// Current parameter values
    current: Vec<f64>,
    /// Parameter bounds: (min, max) for each dimension
    bounds: Vec<(f64, f64)>,
    /// Accumulated momentum for each parameter
    momentum: Vec<f64>,
    /// Current iteration count
    iteration: usize,
}

impl GradientDescentOptimizer {
    /// Create a new gradient descent optimizer
    pub fn new(config: OptimizerConfig, initial: Vec<f64>, bounds: Vec<(f64, f64)>) -> Self {
        let momentum = vec![0.0; initial.len()];
        Self {
            config,
            current: initial,
            bounds,
            momentum,
            iteration: 0,
        }
    }

    /// Perform one step of gradient descent
    pub fn step<F>(&mut self, gradient_fn: F) -> Vec<f64>
    where
        F: Fn(&[f64]) -> Vec<f64>,
    {
        let gradient = gradient_fn(&self.current);

        // Apply gradient with momentum
        let momentum_factor = 0.9;
        for (i, &grad) in gradient.iter().enumerate().take(self.current.len()) {
            self.momentum[i] =
                momentum_factor * self.momentum[i] + self.config.learning_rate * grad;
            self.current[i] += self.momentum[i];
            self.current[i] = self.current[i].clamp(self.bounds[i].0, self.bounds[i].1);
        }

        self.iteration += 1;
        self.current.clone()
    }

    /// Get current parameters
    pub fn current(&self) -> &[f64] {
        &self.current
    }

    /// Get iteration count
    pub fn iteration(&self) -> usize {
        self.iteration
    }
}

/// Simulated annealing optimizer for global optimization.
///
/// Probabilistic optimization inspired by metallurgical annealing. At high
/// temperatures, accepts worse solutions to escape local optima. As temperature
/// decreases, becomes more greedy and refines the best solution.
///
/// # Characteristics
/// - Good at escaping local optima
/// - Single-point search (one evaluation per iteration)
/// - Temperature schedule controls exploration/exploitation
/// - Simple to implement and tune
///
/// # Algorithm
/// ```text
/// 1. Generate neighbor by perturbing current solution
/// 2. If neighbor is better, accept it
/// 3. If worse, accept with probability exp((f_new - f_old) / T)
/// 4. Cool temperature: T = T * cooling_rate
/// ```
pub struct SimulatedAnnealingOptimizer {
    /// Algorithm configuration
    config: OptimizerConfig,
    /// Current parameter values
    current: Vec<f64>,
    /// Fitness of current solution
    current_fitness: f64,
    /// Best parameters found so far
    best: Vec<f64>,
    /// Fitness of best solution
    best_fitness: f64,
    /// Current temperature (controls acceptance probability)
    temperature: f64,
    /// Parameter bounds: (min, max) for each dimension
    bounds: Vec<(f64, f64)>,
    /// Current iteration count
    iteration: usize,
}

impl SimulatedAnnealingOptimizer {
    /// Create a new simulated annealing optimizer
    pub fn new(config: OptimizerConfig, initial: Vec<f64>, bounds: Vec<(f64, f64)>) -> Self {
        Self {
            temperature: config.initial_temperature,
            current: initial.clone(),
            current_fitness: f64::NEG_INFINITY,
            best: initial,
            best_fitness: f64::NEG_INFINITY,
            config,
            bounds,
            iteration: 0,
        }
    }

    /// Perform one step
    pub fn step<F>(&mut self, fitness_fn: F) -> Vec<f64>
    where
        F: Fn(&[f64]) -> f64,
    {
        let mut rng = rand::thread_rng();

        // Generate neighbor solution
        let mut neighbor = self.current.clone();
        for (i, val) in neighbor.iter_mut().enumerate() {
            let (min, max) = self.bounds[i];
            let range = (max - min) * 0.1 * (self.temperature / self.config.initial_temperature);
            let delta = rng.gen_range(-range..range);
            *val = (*val + delta).clamp(min, max);
        }

        // Evaluate
        let neighbor_fitness = fitness_fn(&neighbor);

        // Accept or reject
        let accept = if neighbor_fitness > self.current_fitness {
            true
        } else {
            let delta = neighbor_fitness - self.current_fitness;
            let probability = (delta / self.temperature).exp();
            rng.gen::<f64>() < probability
        };

        if accept {
            self.current = neighbor;
            self.current_fitness = neighbor_fitness;

            if neighbor_fitness > self.best_fitness {
                self.best = self.current.clone();
                self.best_fitness = neighbor_fitness;
            }
        }

        // Cool down
        self.temperature *= self.config.cooling_rate;
        self.iteration += 1;

        self.current.clone()
    }

    /// Get best solution
    pub fn best(&self) -> (&[f64], f64) {
        (&self.best, self.best_fitness)
    }

    /// Get current temperature
    pub fn temperature(&self) -> f64 {
        self.temperature
    }
}

/// Bayesian optimizer for sample-efficient global optimization.
///
/// Uses past observations to build a model of the objective function and
/// intelligently select the next point to evaluate. Balances exploration
/// (trying uncertain regions) with exploitation (refining known good regions).
///
/// # Characteristics
/// - Very sample-efficient (minimizes evaluations)
/// - Best when evaluations are expensive (e.g., benchmarking)
/// - Builds a surrogate model of the objective
/// - Uses acquisition function to select next point
///
/// # Algorithm
/// ```text
/// 1. If not enough samples, explore randomly
/// 2. Otherwise, generate candidate points
/// 3. Score each candidate using acquisition function (UCB)
/// 4. Evaluate best candidate and add to observations
/// ```
///
/// This implementation uses a simplified Upper Confidence Bound (UCB)
/// acquisition function based on distance to observed points.
pub struct BayesianOptimizer {
    /// Algorithm configuration
    config: OptimizerConfig,
    /// History of (parameters, fitness) observations
    observations: Vec<(Vec<f64>, f64)>,
    /// Parameter bounds: (min, max) for each dimension
    bounds: Vec<(f64, f64)>,
    /// Best observation: (parameters, fitness)
    best: Option<(Vec<f64>, f64)>,
}

impl BayesianOptimizer {
    /// Create a new Bayesian optimizer
    pub fn new(config: OptimizerConfig, bounds: Vec<(f64, f64)>) -> Self {
        Self {
            config,
            observations: Vec::new(),
            bounds,
            best: None,
        }
    }

    /// Add an observation
    pub fn observe(&mut self, params: Vec<f64>, fitness: f64) {
        self.observations.push((params.clone(), fitness));

        if self.best.as_ref().map_or(true, |(_, best_fitness)| fitness > *best_fitness) {
            self.best = Some((params, fitness));
        }
    }

    /// Suggest next point to evaluate
    pub fn suggest(&self) -> Vec<f64> {
        let mut rng = rand::thread_rng();

        if self.observations.len() < self.config.num_samples {
            // Initial random exploration
            return self
                .bounds
                .iter()
                .map(|(min, max)| rng.gen_range(*min..*max))
                .collect();
        }

        // Simple acquisition function: expected improvement approximation
        // Generate candidate points and pick the one with highest acquisition value
        let num_candidates = 100;
        let mut best_candidate: Option<(Vec<f64>, f64)> = None;

        for _ in 0..num_candidates {
            let candidate: Vec<f64> = self
                .bounds
                .iter()
                .map(|(min, max)| rng.gen_range(*min..*max))
                .collect();

            let acquisition_value = self.acquisition_function(&candidate);

            if best_candidate.as_ref().map_or(true, |(_, best_val)| acquisition_value > *best_val) {
                best_candidate = Some((candidate, acquisition_value));
            }
        }

        best_candidate.map(|(c, _)| c).unwrap_or_else(|| {
            self.bounds
                .iter()
                .map(|(min, max)| (min + max) / 2.0)
                .collect()
        })
    }

    /// Simple acquisition function (Upper Confidence Bound approximation)
    fn acquisition_function(&self, point: &[f64]) -> f64 {
        if self.observations.is_empty() {
            return 0.0;
        }

        // Simple distance-based acquisition
        // Prefer points that are far from already observed points (exploration)
        // and in regions with high fitness (exploitation)
        let mut min_distance = f64::MAX;
        let mut nearby_fitness = 0.0;
        let mut nearby_count = 0;

        for (obs_point, obs_fitness) in &self.observations {
            let distance: f64 = point
                .iter()
                .zip(obs_point.iter())
                .zip(self.bounds.iter())
                .map(|((a, b), (min, max))| {
                    let normalized_diff = (a - b) / (max - min);
                    normalized_diff * normalized_diff
                })
                .sum::<f64>()
                .sqrt();

            if distance < min_distance {
                min_distance = distance;
            }

            if distance < 0.2 {
                nearby_fitness += obs_fitness;
                nearby_count += 1;
            }
        }

        let exploration_bonus = min_distance;
        let exploitation_score = if nearby_count > 0 {
            nearby_fitness / nearby_count as f64
        } else {
            0.0
        };

        exploration_bonus * 10.0 + exploitation_score
    }

    /// Get best observation
    pub fn best(&self) -> Option<(&[f64], f64)> {
        self.best.as_ref().map(|(p, f)| (p.as_slice(), *f))
    }

    /// Get number of observations
    pub fn num_observations(&self) -> usize {
        self.observations.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_individual_random() {
        let bounds = vec![(0.0, 10.0), (0.0, 100.0)];
        let ind = Individual::random(&bounds);

        assert!(ind.genes[0] >= 0.0 && ind.genes[0] <= 10.0);
        assert!(ind.genes[1] >= 0.0 && ind.genes[1] <= 100.0);
    }

    #[test]
    fn test_individual_crossover() {
        let ind1 = Individual::new(vec![1.0, 2.0, 3.0]);
        let ind2 = Individual::new(vec![4.0, 5.0, 6.0]);

        let (_child1, _child2) = ind1.crossover(&ind2, 1.0);
        // With 100% crossover rate, genes should be swapped
    }

    #[test]
    fn test_genetic_optimizer() {
        let config = OptimizerConfig {
            population_size: 10,
            mutation_rate: 0.1,
            crossover_rate: 0.7,
            ..Default::default()
        };

        let bounds = vec![(0.0, 10.0), (0.0, 10.0)];
        let mut optimizer = GeneticOptimizer::new(config, bounds);

        // Fitness function: maximize sum of parameters
        let fitness_fn = |genes: &[f64]| genes.iter().sum();

        for _ in 0..10 {
            optimizer.evolve(fitness_fn);
        }

        assert!(optimizer.best().is_some());
        assert_eq!(optimizer.generation(), 10);
    }

    #[test]
    fn test_simulated_annealing() {
        let config = OptimizerConfig {
            initial_temperature: 100.0,
            cooling_rate: 0.95,
            ..Default::default()
        };

        let bounds = vec![(0.0, 10.0), (0.0, 10.0)];
        let initial = vec![5.0, 5.0];
        let mut optimizer = SimulatedAnnealingOptimizer::new(config, initial, bounds);

        // Fitness function: maximize sum
        let fitness_fn = |genes: &[f64]| genes.iter().sum();

        for _ in 0..100 {
            optimizer.step(fitness_fn);
        }

        assert!(optimizer.temperature() < 100.0);
    }

    #[test]
    fn test_bayesian_optimizer() {
        let config = OptimizerConfig {
            num_samples: 5,
            ..Default::default()
        };

        let bounds = vec![(0.0, 10.0), (0.0, 10.0)];
        let mut optimizer = BayesianOptimizer::new(config, bounds);

        // Add some observations
        optimizer.observe(vec![1.0, 1.0], 2.0);
        optimizer.observe(vec![5.0, 5.0], 10.0);
        optimizer.observe(vec![9.0, 9.0], 18.0);

        let suggestion = optimizer.suggest();
        assert_eq!(suggestion.len(), 2);
    }
}
