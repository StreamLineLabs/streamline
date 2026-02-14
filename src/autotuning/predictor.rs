//! Performance prediction models for auto-tuning
//!
//! This module provides machine learning models for predicting system performance
//! based on configuration parameters and historical metrics. Used by the auto-tuner
//! to estimate the impact of parameter changes before applying them.
//!
//! # Models Available
//!
//! - **Linear Regression**: Simple, fast, interpretable. Good for linear relationships
//!   between parameters and performance.
//!
//! - **Polynomial Regression**: Captures non-linear relationships by transforming
//!   features to polynomial basis.
//!
//! - **Time Series Forecaster**: Predicts future throughput/latency using Holt's
//!   linear method with anomaly detection.
//!
//! - **Neural Network**: Multi-layer perceptron for complex, non-linear patterns.
//!   Xavier initialization, ReLU activation.
//!
//! - **Performance Predictor**: Combines all models for comprehensive prediction
//!   of performance scores, trends, and anomalies.
//!
//! # Example
//!
//! ```
//! use streamline::autotuning::predictor::{LinearRegression, TimeSeriesForecaster};
//!
//! // Train a linear regression model
//! let mut model = LinearRegression::new(2); // 2 features
//! let x = vec![vec![1.0, 2.0], vec![2.0, 3.0], vec![3.0, 4.0]];
//! let y = vec![5.0, 8.0, 11.0]; // y = 2*x1 + 1.5*x2
//! model.fit(&x, &y, 0.01, 1000);
//!
//! let prediction = model.predict(&[2.5, 3.5]);
//!
//! // Use time series forecasting
//! let mut forecaster = TimeSeriesForecaster::new(100, 0.3, 0.1);
//! for value in [100.0, 110.0, 120.0, 130.0] {
//!     forecaster.observe(value);
//! }
//! let future_value = forecaster.forecast(5); // Predict 5 steps ahead
//! ```

use serde::{Deserialize, Serialize};
use std::collections::VecDeque;

/// Prediction model type selection.
///
/// Choose based on your data characteristics:
/// - Linear/Polynomial: When you know the relationship form
/// - MovingAverage/ExponentialSmoothing: For time series smoothing
/// - NeuralNetwork: For complex, unknown relationships
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ModelType {
    /// Linear regression: y = w1*x1 + w2*x2 + ... + b
    Linear,
    /// Polynomial regression with feature transformation
    Polynomial,
    /// Simple moving average for smoothing
    MovingAverage,
    /// Exponential smoothing with trend (Holt's method)
    ExponentialSmoothing,
    /// Feedforward neural network with ReLU activation
    NeuralNetwork,
}

/// Linear regression model using gradient descent.
///
/// Fits a linear model y = w1*x1 + w2*x2 + ... + wn*xn + b
/// using batch gradient descent optimization.
///
/// # Training
/// Uses gradient descent with configurable learning rate and epochs.
/// Coefficients are stored with the bias term as the last element.
///
/// # Performance
/// - Training: O(epochs * samples * features)
/// - Prediction: O(features)
#[derive(Debug, Clone, Default)]
pub struct LinearRegression {
    /// Model coefficients [w1, w2, ..., wn, bias]
    coefficients: Vec<f64>,
    /// Number of input features
    num_features: usize,
    /// Number of samples used in training
    samples: usize,
    /// Mean squared error from training
    mse: f64,
}

impl LinearRegression {
    /// Create a new linear regression model
    pub fn new(num_features: usize) -> Self {
        Self {
            coefficients: vec![0.0; num_features + 1], // +1 for bias
            num_features,
            samples: 0,
            mse: 0.0,
        }
    }

    /// Fit the model using gradient descent
    pub fn fit(&mut self, x: &[Vec<f64>], y: &[f64], learning_rate: f64, epochs: usize) {
        if x.is_empty() || y.is_empty() || x.len() != y.len() {
            return;
        }

        self.samples = x.len();
        self.coefficients = vec![0.0; self.num_features + 1];

        for _ in 0..epochs {
            let mut gradients = vec![0.0; self.num_features + 1];
            let mut total_error = 0.0;

            for (features, &target) in x.iter().zip(y.iter()) {
                let prediction = self.predict_internal(features);
                let error = prediction - target;
                total_error += error * error;

                // Update gradients
                for (j, grad) in gradients.iter_mut().enumerate().take(self.num_features) {
                    *grad += error * features.get(j).copied().unwrap_or(0.0);
                }
                gradients[self.num_features] += error; // Bias gradient
            }

            // Apply gradients
            for (j, coef) in self.coefficients.iter_mut().enumerate() {
                *coef -= learning_rate * gradients[j] / self.samples as f64;
            }

            self.mse = total_error / self.samples as f64;
        }
    }

    /// Predict for a single sample
    pub fn predict(&self, features: &[f64]) -> f64 {
        self.predict_internal(features)
    }

    fn predict_internal(&self, features: &[f64]) -> f64 {
        let mut result = self.coefficients.last().copied().unwrap_or(0.0); // Bias
        for (i, &coef) in self.coefficients.iter().take(self.num_features).enumerate() {
            result += coef * features.get(i).copied().unwrap_or(0.0);
        }
        result
    }

    /// Get mean squared error
    pub fn mse(&self) -> f64 {
        self.mse
    }

    /// Get coefficients
    pub fn coefficients(&self) -> &[f64] {
        &self.coefficients
    }
}

/// Polynomial regression model
#[derive(Debug, Clone)]
pub struct PolynomialRegression {
    /// Degree of polynomial
    degree: usize,
    /// Underlying linear model
    linear: LinearRegression,
}

impl PolynomialRegression {
    /// Create a new polynomial regression model
    pub fn new(num_features: usize, degree: usize) -> Self {
        // Number of polynomial features
        let poly_features = num_features * degree;
        Self {
            degree,
            linear: LinearRegression::new(poly_features),
        }
    }

    /// Transform features to polynomial features
    fn transform_features(&self, features: &[f64]) -> Vec<f64> {
        let mut poly = Vec::new();
        for &f in features {
            for d in 1..=self.degree {
                poly.push(f.powi(d as i32));
            }
        }
        poly
    }

    /// Fit the model
    pub fn fit(&mut self, x: &[Vec<f64>], y: &[f64], learning_rate: f64, epochs: usize) {
        let poly_x: Vec<Vec<f64>> = x.iter().map(|f| self.transform_features(f)).collect();
        self.linear.fit(&poly_x, y, learning_rate, epochs);
    }

    /// Predict for a single sample
    pub fn predict(&self, features: &[f64]) -> f64 {
        let poly_features = self.transform_features(features);
        self.linear.predict(&poly_features)
    }
}

/// Time series forecaster using Holt's linear exponential smoothing.
///
/// Tracks both level and trend components of a time series to make
/// forecasts and detect anomalies. Suitable for data with trends
/// but without seasonality.
///
/// # Algorithm (Holt's Method)
/// ```text
/// level[t] = α * y[t] + (1 - α) * (level[t-1] + trend[t-1])
/// trend[t] = β * (level[t] - level[t-1]) + (1 - β) * trend[t-1]
/// forecast[t+h] = level[t] + h * trend[t]
/// ```
///
/// # Parameters
/// - `alpha`: Level smoothing (0-1). Higher = more responsive to recent values.
/// - `beta`: Trend smoothing (0-1). Higher = faster trend adaptation.
///
/// # Features
/// - Forecasting: Predict values h steps ahead
/// - Moving average: Smooth over configurable window
/// - Anomaly detection: Z-score based outlier detection
#[derive(Debug, Clone)]
pub struct TimeSeriesForecaster {
    /// Rolling history of observed values
    history: VecDeque<f64>,
    /// Maximum number of values to retain
    max_history: usize,
    /// Level smoothing factor α (0 to 1)
    alpha: f64,
    /// Trend smoothing factor β (0 to 1)
    beta: f64,
    /// Current estimated level (intercept)
    level: f64,
    /// Current estimated trend (slope)
    trend: f64,
}

impl TimeSeriesForecaster {
    /// Create a new time series forecaster
    pub fn new(max_history: usize, alpha: f64, beta: f64) -> Self {
        Self {
            history: VecDeque::new(),
            max_history,
            alpha: alpha.clamp(0.0, 1.0),
            beta: beta.clamp(0.0, 1.0),
            level: 0.0,
            trend: 0.0,
        }
    }

    /// Add a new observation
    pub fn observe(&mut self, value: f64) {
        if self.history.is_empty() {
            self.level = value;
            self.trend = 0.0;
        } else {
            // Holt's linear method
            let prev_level = self.level;
            self.level = self.alpha * value + (1.0 - self.alpha) * (self.level + self.trend);
            self.trend = self.beta * (self.level - prev_level) + (1.0 - self.beta) * self.trend;
        }

        self.history.push_back(value);
        if self.history.len() > self.max_history {
            self.history.pop_front();
        }
    }

    /// Forecast n steps ahead
    pub fn forecast(&self, steps: usize) -> f64 {
        self.level + self.trend * steps as f64
    }

    /// Get moving average
    pub fn moving_average(&self, window: usize) -> f64 {
        if self.history.is_empty() {
            return 0.0;
        }

        let window = window.min(self.history.len());
        let sum: f64 = self.history.iter().rev().take(window).sum();
        sum / window as f64
    }

    /// Detect anomaly using z-score
    pub fn is_anomaly(&self, value: f64, threshold: f64) -> bool {
        if self.history.len() < 10 {
            return false;
        }

        let mean = self.moving_average(self.history.len());
        let variance: f64 = self.history.iter().map(|v| (v - mean).powi(2)).sum::<f64>()
            / self.history.len() as f64;
        let std_dev = variance.sqrt();

        if std_dev < 1e-10 {
            return false;
        }

        let z_score = (value - mean).abs() / std_dev;
        z_score > threshold
    }

    /// Get current trend
    pub fn trend(&self) -> f64 {
        self.trend
    }

    /// Get history length
    pub fn history_len(&self) -> usize {
        self.history.len()
    }
}

/// Dense (fully connected) neural network layer.
///
/// Implements a single layer of a feedforward neural network with
/// Xavier weight initialization for stable training.
///
/// # Forward Pass
/// ```text
/// output[i] = Σ(weights[i][j] * input[j]) + bias[i]
/// ```
///
/// Supports optional ReLU activation: `max(0, x)`
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct DenseLayer {
    /// Weight matrix [output_size x input_size]
    weights: Vec<Vec<f64>>,
    /// Bias vector [output_size]
    biases: Vec<f64>,
    /// Number of input features
    input_size: usize,
    /// Number of output features
    output_size: usize,
}

impl DenseLayer {
    /// Create a new dense layer with random initialization
    pub fn new(input_size: usize, output_size: usize) -> Self {
        use rand::Rng;
        let mut rng = rand::thread_rng();

        // Xavier initialization
        let scale = (2.0 / (input_size + output_size) as f64).sqrt();

        let weights: Vec<Vec<f64>> = (0..output_size)
            .map(|_| {
                (0..input_size)
                    .map(|_| rng.gen_range(-scale..scale))
                    .collect()
            })
            .collect();

        let biases = vec![0.0; output_size];

        Self {
            weights,
            biases,
            input_size,
            output_size,
        }
    }

    /// Forward pass
    pub fn forward(&self, input: &[f64]) -> Vec<f64> {
        self.weights
            .iter()
            .zip(&self.biases)
            .map(|(w, &b)| {
                let sum: f64 = w.iter().zip(input).map(|(wi, xi)| wi * xi).sum();
                sum + b
            })
            .collect()
    }

    /// Forward pass with ReLU activation
    pub fn forward_relu(&self, input: &[f64]) -> Vec<f64> {
        self.forward(input)
            .into_iter()
            .map(|x| x.max(0.0))
            .collect()
    }
}

/// Feedforward neural network for performance prediction.
///
/// Multi-layer perceptron (MLP) with configurable hidden layers.
/// Uses ReLU activation for hidden layers and linear output.
///
/// # Architecture
/// ```text
/// Input → [Dense + ReLU] × N → Dense → Output
/// ```
///
/// # Initialization
/// Uses Xavier initialization for stable gradient flow.
#[derive(Debug, Clone)]
pub struct NeuralNetworkPredictor {
    /// Hidden layers with ReLU activation
    layers: Vec<DenseLayer>,
    /// Output layer (linear activation)
    output_layer: DenseLayer,
}

impl NeuralNetworkPredictor {
    /// Create a new neural network
    pub fn new(input_size: usize, hidden_sizes: &[usize], output_size: usize) -> Self {
        let mut layers = Vec::new();
        let mut prev_size = input_size;

        for &size in hidden_sizes {
            layers.push(DenseLayer::new(prev_size, size));
            prev_size = size;
        }

        let output_layer = DenseLayer::new(prev_size, output_size);

        Self {
            layers,
            output_layer,
        }
    }

    /// Forward pass
    pub fn predict(&self, input: &[f64]) -> Vec<f64> {
        let mut current = input.to_vec();

        for layer in &self.layers {
            current = layer.forward_relu(&current);
        }

        self.output_layer.forward(&current)
    }
}

/// Combined performance predictor for the auto-tuning system.
///
/// Integrates multiple prediction models to provide comprehensive
/// performance forecasting and anomaly detection:
///
/// - **Parameter Model**: Linear regression mapping configuration
///   parameters to a performance score.
///
/// - **Throughput Forecaster**: Time series prediction for message
///   throughput with trend detection.
///
/// - **Latency Forecaster**: Time series prediction for request
///   latency with anomaly detection.
///
/// # Usage
/// 1. Call `observe()` to record parameter configurations and their
///    resulting throughput/latency measurements.
/// 2. The model automatically retrains after 10+ observations.
/// 3. Use `predict_score()` to estimate performance of new configurations.
/// 4. Use `forecast_*()` and `is_*_anomaly()` for time series analysis.
pub struct PerformancePredictor {
    /// Model mapping parameters to performance scores
    param_model: LinearRegression,
    /// Forecaster for throughput time series
    throughput_forecaster: TimeSeriesForecaster,
    /// Forecaster for latency time series
    latency_forecaster: TimeSeriesForecaster,
    /// Training history: (parameters, score)
    training_data: Vec<(Vec<f64>, f64)>,
    /// Maximum training samples to retain
    max_samples: usize,
}

impl PerformancePredictor {
    /// Create a new performance predictor
    pub fn new(num_params: usize, max_samples: usize) -> Self {
        Self {
            param_model: LinearRegression::new(num_params),
            throughput_forecaster: TimeSeriesForecaster::new(1000, 0.3, 0.1),
            latency_forecaster: TimeSeriesForecaster::new(1000, 0.3, 0.1),
            training_data: Vec::new(),
            max_samples,
        }
    }

    /// Add observation
    pub fn observe(&mut self, params: Vec<f64>, throughput: f64, latency: f64) {
        // Simple score combining throughput and latency
        let score = throughput.log10().max(0.0) - latency.log10().max(0.0);

        self.training_data.push((params, score));
        if self.training_data.len() > self.max_samples {
            self.training_data.remove(0);
        }

        self.throughput_forecaster.observe(throughput);
        self.latency_forecaster.observe(latency);

        // Retrain if enough samples
        if self.training_data.len() >= 10 {
            self.train();
        }
    }

    /// Train the parameter model
    fn train(&mut self) {
        let x: Vec<Vec<f64>> = self.training_data.iter().map(|(p, _)| p.clone()).collect();
        let y: Vec<f64> = self.training_data.iter().map(|(_, s)| *s).collect();
        self.param_model.fit(&x, &y, 0.01, 100);
    }

    /// Predict performance score for given parameters
    pub fn predict_score(&self, params: &[f64]) -> f64 {
        self.param_model.predict(params)
    }

    /// Forecast throughput
    pub fn forecast_throughput(&self, steps: usize) -> f64 {
        self.throughput_forecaster.forecast(steps)
    }

    /// Forecast latency
    pub fn forecast_latency(&self, steps: usize) -> f64 {
        self.latency_forecaster.forecast(steps)
    }

    /// Check if throughput is anomalous
    pub fn is_throughput_anomaly(&self, value: f64) -> bool {
        self.throughput_forecaster.is_anomaly(value, 3.0)
    }

    /// Check if latency is anomalous
    pub fn is_latency_anomaly(&self, value: f64) -> bool {
        self.latency_forecaster.is_anomaly(value, 3.0)
    }

    /// Get throughput trend
    pub fn throughput_trend(&self) -> f64 {
        self.throughput_forecaster.trend()
    }

    /// Get latency trend
    pub fn latency_trend(&self) -> f64 {
        self.latency_forecaster.trend()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_linear_regression() {
        let mut model = LinearRegression::new(2);

        // Simple linear relationship: y = 2*x1 + 3*x2 + 1
        let x = vec![
            vec![1.0, 1.0],
            vec![2.0, 1.0],
            vec![1.0, 2.0],
            vec![2.0, 2.0],
        ];
        let y = vec![6.0, 8.0, 9.0, 11.0];

        model.fit(&x, &y, 0.01, 1000);

        let prediction = model.predict(&[1.5, 1.5]);
        assert!((prediction - 8.5).abs() < 0.5);
    }

    #[test]
    fn test_time_series_forecaster() {
        let mut forecaster = TimeSeriesForecaster::new(100, 0.3, 0.1);

        // Add increasing trend
        for i in 0..20 {
            forecaster.observe(i as f64 * 10.0 + 100.0);
        }

        let forecast = forecaster.forecast(5);
        assert!(forecast > 290.0); // Should be above last observation

        let trend = forecaster.trend();
        assert!(trend > 0.0); // Positive trend
    }

    #[test]
    fn test_anomaly_detection() {
        let mut forecaster = TimeSeriesForecaster::new(100, 0.3, 0.1);

        // Add values with small variation around 100
        for i in 0..50 {
            // Values between 98 and 102
            let value = 100.0 + (i % 5) as f64 - 2.0;
            forecaster.observe(value);
        }

        // Normal value should not be an anomaly
        assert!(!forecaster.is_anomaly(100.0, 3.0));
        // Value 50 standard deviations away should be an anomaly
        assert!(forecaster.is_anomaly(200.0, 3.0));
    }

    #[test]
    fn test_neural_network() {
        let nn = NeuralNetworkPredictor::new(3, &[4, 4], 1);

        let input = vec![1.0, 2.0, 3.0];
        let output = nn.predict(&input);

        assert_eq!(output.len(), 1);
    }

    #[test]
    fn test_performance_predictor() {
        let mut predictor = PerformancePredictor::new(2, 100);

        // Add observations
        for i in 0..20 {
            let params = vec![i as f64, (20 - i) as f64];
            let throughput = 10000.0 + i as f64 * 1000.0;
            let latency = 1000.0 - i as f64 * 10.0;
            predictor.observe(params, throughput, latency);
        }

        let score = predictor.predict_score(&[10.0, 10.0]);
        assert!(score.is_finite());
    }
}
