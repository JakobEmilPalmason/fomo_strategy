.PHONY: help install setup clean test lint format run-split-adjust run-coalesce run-validation

help: ## Show this help message
	@echo "Available commands:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

install: ## Install dependencies
	pip install -r requirements.txt

setup: ## Initial setup
	python -m venv .venv
	@echo "Virtual environment created. Activate it with:"
	@echo "source .venv/bin/activate  # On Unix/Mac"
	@echo ".venv\\Scripts\\activate     # On Windows"

clean: ## Clean up temporary files
	find . -type f -name "*.pyc" -delete
	find . -type d -name "__pycache__" -delete
	find . -type d -name "*.egg-info" -exec rm -rf {} +
	rm -rf build/ dist/ .pytest_cache/ .coverage htmlcov/

test: ## Run tests
	python -m pytest tests/ -v

lint: ## Run linting
	flake8 scripts/ --max-line-length=100 --ignore=E203,W503
	black --check scripts/

format: ## Format code
	black scripts/ --line-length=100
	isort scripts/

run-split-adjust: ## Run split adjustment
	python scripts/split_adjust_minutes.py --base .

run-coalesce: ## Run data coalescing
	python scripts/coalesce_unadjusted_minutes.py --base .

run-validation: ## Run data validation
	python scripts/validation.py --base .

run-aggregate-daily: ## Run daily aggregation from minutes
	python scripts/aggregate_daily_from_minutes.py --base .

create-dirs: ## Create necessary directories
	mkdir -p 2_unadjusted_parquet/minute_data
	mkdir -p 3_adjusted_data/minute_sa
	mkdir -p logs
	mkdir -p audit
	mkdir -p universe

check-env: ## Check environment setup
	@echo "Checking environment..."
	@python -c "import pandas, pyarrow, numpy; print('✅ Dependencies installed')"
	@if [ -f .env ]; then echo "✅ .env file exists"; else echo "⚠️  .env file missing - copy from env_template.txt"; fi
	@if [ -f "1_raw_files/reference/splits.parquet" ]; then echo "✅ splits.parquet exists"; else echo "⚠️  splits.parquet missing"; fi 