# Makefile for eventflow development
# Usage: make <target>

.PHONY: help install install-dev format lint type-check security test test-cov quality clean pre-commit update ci

# Default target
.DEFAULT_GOAL := help

#==============================================================================
# HELP
#==============================================================================

help: ## Show this help message
	@echo "eventflow Development Commands"
	@echo ""
	@echo "Setup:"
	@grep -E '^(install|install-dev|pre-commit|update):.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-15s %s\n", $$1, $$2}'
	@echo ""
	@echo "Quality:"
	@grep -E '^(format|lint|type-check|security|quality):.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-15s %s\n", $$1, $$2}'
	@echo ""
	@echo "Testing:"
	@grep -E '^(test|test-cov|test-fast):.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-15s %s\n", $$1, $$2}'
	@echo ""
	@echo "Maintenance:"
	@grep -E '^(clean|ci):.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-15s %s\n", $$1, $$2}'

#==============================================================================
# SETUP
#==============================================================================

install: ## Install production dependencies
	poetry install --only main

install-dev: ## Install all dependencies and pre-commit hooks
	poetry install
	poetry run pre-commit install

pre-commit: ## Install pre-commit hooks
	poetry run pre-commit install
	poetry run pre-commit install --hook-type commit-msg

update: ## Update all dependencies
	poetry update
	poetry run pre-commit autoupdate

#==============================================================================
# CODE QUALITY
#==============================================================================

format: ## Auto-format code with Ruff and Black
	poetry run ruff check --fix eventflow tests
	poetry run black eventflow tests
	@echo "Code formatted successfully"

lint: ## Run Ruff linter
	poetry run ruff check eventflow tests
	@echo "Linting passed"

type-check: ## Run MyPy type checker
	poetry run mypy eventflow
	@echo "Type checking passed"

security: ## Run Bandit security scanner
	poetry run bandit -c pyproject.toml -r eventflow
	@echo "Security scan passed"

quality: format lint type-check security ## Run all quality checks (formats first)
	@echo "All quality checks passed"

#==============================================================================
# TESTING
#==============================================================================

test: ## Run tests
	poetry run pytest

test-cov: ## Run tests with coverage report
	poetry run pytest --cov=eventflow --cov-report=term-missing --cov-report=html
	@echo "Coverage report generated in htmlcov/"

test-fast: ## Run tests without slow markers
	poetry run pytest -m "not slow"

#==============================================================================
# MAINTENANCE
#==============================================================================

clean: ## Remove build artifacts and caches
	rm -rf build/
	rm -rf dist/
	rm -rf *.egg-info/
	rm -rf .pytest_cache/
	rm -rf .mypy_cache/
	rm -rf .ruff_cache/
	rm -rf htmlcov/
	rm -rf .coverage
	rm -rf coverage.xml
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	@echo "Cleaned build artifacts"

ci: quality test-cov ## Simulate CI pipeline locally
	@echo "CI simulation passed"
