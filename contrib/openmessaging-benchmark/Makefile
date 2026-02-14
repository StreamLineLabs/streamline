# OpenMessaging Benchmark Makefile
#
# Common operations for running and managing benchmarks.
#
# Usage:
#   make help          - Show available targets
#   make up            - Start all systems
#   make benchmark     - Run default benchmark
#   make report        - Generate report from latest results
#   make clean         - Stop and remove containers
#

.PHONY: help up up-monitoring down clean benchmark benchmark-quick benchmark-all \
        report report-html logs status shell-streamline shell-kafka \
        install-deps validate

# Configuration
COMPOSE_FILE := docker-compose.benchmarks.yml
RESULTS_DIR := results
SCRIPTS_DIR := scripts
WORKLOAD ?= streamline-comparison
SYSTEMS ?= streamline,kafka

# Colors
CYAN := \033[0;36m
GREEN := \033[0;32m
YELLOW := \033[0;33m
NC := \033[0m

#------------------------------------------------------------------------------
# Help
#------------------------------------------------------------------------------

help: ## Show this help message
	@echo ""
	@echo "OpenMessaging Benchmark - Makefile Targets"
	@echo ""
	@echo "$(CYAN)Container Management:$(NC)"
	@grep -E '^(up|down|clean|status|logs).*:.*##' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*## "}; {printf "  $(GREEN)%-20s$(NC) %s\n", $$1, $$2}'
	@echo ""
	@echo "$(CYAN)Benchmarking:$(NC)"
	@grep -E '^benchmark.*:.*##' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*## "}; {printf "  $(GREEN)%-20s$(NC) %s\n", $$1, $$2}'
	@echo ""
	@echo "$(CYAN)Reporting:$(NC)"
	@grep -E '^report.*:.*##' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*## "}; {printf "  $(GREEN)%-20s$(NC) %s\n", $$1, $$2}'
	@echo ""
	@echo "$(CYAN)Utilities:$(NC)"
	@grep -E '^(shell|install|validate).*:.*##' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*## "}; {printf "  $(GREEN)%-20s$(NC) %s\n", $$1, $$2}'
	@echo ""
	@echo "$(CYAN)Variables:$(NC)"
	@echo "  $(GREEN)WORKLOAD$(NC)    Workload to run (default: streamline-comparison)"
	@echo "  $(GREEN)SYSTEMS$(NC)     Systems to benchmark (default: streamline,kafka)"
	@echo "  $(GREEN)OMB_HOME$(NC)    Path to OMB installation (required for benchmarks)"
	@echo ""
	@echo "$(CYAN)Examples:$(NC)"
	@echo "  make up                           # Start Streamline and Kafka"
	@echo "  make benchmark SYSTEMS=streamline # Benchmark only Streamline"
	@echo "  make benchmark-quick              # Quick validation benchmark"
	@echo "  make benchmark-all                # Benchmark all systems"
	@echo "  make report                       # Generate comparison report"
	@echo ""

#------------------------------------------------------------------------------
# Container Management
#------------------------------------------------------------------------------

up: ## Start benchmark containers (Streamline + Kafka by default)
	@echo "Starting containers..."
	docker compose -f $(COMPOSE_FILE) up -d streamline kafka
	@echo "Waiting for health checks..."
	@sleep 10
	@$(MAKE) status

up-all: ## Start all messaging system containers
	@echo "Starting all containers..."
	docker compose -f $(COMPOSE_FILE) up -d streamline kafka redpanda pulsar nats rabbitmq
	@echo "Waiting for health checks..."
	@sleep 30
	@$(MAKE) status

up-monitoring: ## Start containers with monitoring (Prometheus + Grafana)
	@echo "Starting containers with monitoring..."
	docker compose -f $(COMPOSE_FILE) --profile monitoring up -d
	@echo "Waiting for health checks..."
	@sleep 15
	@echo ""
	@echo "$(GREEN)Grafana available at: http://localhost:3000 (admin/admin)$(NC)"
	@echo "$(GREEN)Prometheus available at: http://localhost:9090$(NC)"
	@$(MAKE) status

down: ## Stop all containers (keep volumes)
	docker compose -f $(COMPOSE_FILE) --profile monitoring down

clean: ## Stop containers and remove volumes
	docker compose -f $(COMPOSE_FILE) --profile monitoring down -v
	@echo "Containers and volumes removed"

status: ## Show container status
	@echo ""
	@echo "Container Status:"
	@docker compose -f $(COMPOSE_FILE) ps --format "table {{.Name}}\t{{.Status}}\t{{.Ports}}" 2>/dev/null || \
		docker compose -f $(COMPOSE_FILE) ps
	@echo ""

logs: ## Show container logs (use CONTAINER=name for specific container)
ifdef CONTAINER
	docker compose -f $(COMPOSE_FILE) logs -f $(CONTAINER)
else
	docker compose -f $(COMPOSE_FILE) logs -f --tail=100
endif

#------------------------------------------------------------------------------
# Benchmarking
#------------------------------------------------------------------------------

benchmark: ## Run benchmark with current SYSTEMS and WORKLOAD
	@if [ -z "$(OMB_HOME)" ]; then \
		echo "$(YELLOW)Error: OMB_HOME not set$(NC)"; \
		echo "Set it with: export OMB_HOME=/path/to/openmessaging-benchmark"; \
		exit 1; \
	fi
	@mkdir -p $(RESULTS_DIR)
	./$(SCRIPTS_DIR)/run-benchmarks.sh \
		--systems $(SYSTEMS) \
		--workload $(WORKLOAD) \
		--skip-startup

benchmark-quick: ## Run quick validation benchmark (~1 min)
	@$(MAKE) benchmark WORKLOAD=streamline-quick

benchmark-comparison: ## Run full comparison benchmark (~10 min)
	@$(MAKE) benchmark WORKLOAD=streamline-comparison

benchmark-throughput: ## Run throughput-focused benchmark (~30 min)
	@$(MAKE) benchmark WORKLOAD=streamline-throughput

benchmark-latency: ## Run latency-focused benchmark (~10 min)
	@$(MAKE) benchmark WORKLOAD=streamline-latency

benchmark-ecommerce: ## Run e-commerce simulation benchmark
	@$(MAKE) benchmark WORKLOAD=workload-ecommerce

benchmark-iot: ## Run IoT telemetry benchmark
	@$(MAKE) benchmark WORKLOAD=workload-iot

benchmark-logging: ## Run log aggregation benchmark
	@$(MAKE) benchmark WORKLOAD=workload-logging

benchmark-all: ## Benchmark all systems with comparison workload
	@$(MAKE) benchmark SYSTEMS=streamline,kafka,redpanda,pulsar,nats,rabbitmq

#------------------------------------------------------------------------------
# Reporting
#------------------------------------------------------------------------------

report: ## Generate markdown report from latest results
	@if [ -z "$$(ls -A $(RESULTS_DIR)/*.json 2>/dev/null)" ]; then \
		echo "$(YELLOW)No result files found in $(RESULTS_DIR)$(NC)"; \
		exit 1; \
	fi
	@python3 $(SCRIPTS_DIR)/process_results.py \
		$(RESULTS_DIR)/*.json \
		--format markdown \
		--output $(RESULTS_DIR)/report.md
	@echo "Report generated: $(RESULTS_DIR)/report.md"
	@cat $(RESULTS_DIR)/report.md

report-html: ## Generate HTML report from latest results
	@if [ -z "$$(ls -A $(RESULTS_DIR)/*.json 2>/dev/null)" ]; then \
		echo "$(YELLOW)No result files found in $(RESULTS_DIR)$(NC)"; \
		exit 1; \
	fi
	@python3 $(SCRIPTS_DIR)/process_results.py \
		$(RESULTS_DIR)/*.json \
		--format html \
		--output $(RESULTS_DIR)/report.html
	@echo "Report generated: $(RESULTS_DIR)/report.html"
	@echo "Open with: open $(RESULTS_DIR)/report.html"

report-csv: ## Generate CSV report from latest results
	@if [ -z "$$(ls -A $(RESULTS_DIR)/*.json 2>/dev/null)" ]; then \
		echo "$(YELLOW)No result files found in $(RESULTS_DIR)$(NC)"; \
		exit 1; \
	fi
	@python3 $(SCRIPTS_DIR)/process_results.py \
		$(RESULTS_DIR)/*.json \
		--format csv \
		--output $(RESULTS_DIR)/results.csv
	@echo "CSV generated: $(RESULTS_DIR)/results.csv"

report-charts: ## Generate comparison charts (requires matplotlib)
	@if [ -z "$$(ls -A $(RESULTS_DIR)/*.json 2>/dev/null)" ]; then \
		echo "$(YELLOW)No result files found in $(RESULTS_DIR)$(NC)"; \
		exit 1; \
	fi
	@python3 $(SCRIPTS_DIR)/process_results.py \
		$(RESULTS_DIR)/*.json \
		--chart $(RESULTS_DIR)/charts
	@echo "Charts generated in: $(RESULTS_DIR)/"

report-all: report report-html report-csv report-charts ## Generate all report formats

clean-results: ## Remove all result files
	rm -rf $(RESULTS_DIR)/*
	@echo "Results cleaned"

#------------------------------------------------------------------------------
# Utilities
#------------------------------------------------------------------------------

shell-streamline: ## Open shell in Streamline container
	docker exec -it benchmark-streamline /bin/sh

shell-kafka: ## Open shell in Kafka container
	docker exec -it benchmark-kafka /bin/bash

install-deps: ## Install Python dependencies for reporting
	pip install -r $(SCRIPTS_DIR)/requirements.txt

validate: ## Validate configuration files
	@echo "Validating Docker Compose..."
	@docker compose -f $(COMPOSE_FILE) config --quiet && echo "$(GREEN)Docker Compose: OK$(NC)"
	@echo ""
	@echo "Validating workloads..."
	@for f in workloads/*.yaml; do \
		if [ -f "$$f" ]; then \
			python3 -c "import yaml; yaml.safe_load(open('$$f'))" 2>/dev/null && \
				echo "$(GREEN)$$f: OK$(NC)" || \
				echo "$(YELLOW)$$f: INVALID$(NC)"; \
		fi; \
	done
	@echo ""
	@echo "Validating driver configs..."
	@for f in driver-streamline/*.yaml; do \
		if [ -f "$$f" ]; then \
			python3 -c "import yaml; yaml.safe_load(open('$$f'))" 2>/dev/null && \
				echo "$(GREEN)$$f: OK$(NC)" || \
				echo "$(YELLOW)$$f: INVALID$(NC)"; \
		fi; \
	done

#------------------------------------------------------------------------------
# Development
#------------------------------------------------------------------------------

.PHONY: dev-up dev-benchmark

dev-up: ## Start only Streamline for development
	docker compose -f $(COMPOSE_FILE) up -d streamline
	@sleep 5
	@$(MAKE) status

dev-benchmark: ## Quick benchmark for development iteration
	@$(MAKE) benchmark SYSTEMS=streamline WORKLOAD=streamline-quick
