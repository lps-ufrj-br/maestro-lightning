SHELL := /bin/bash

# Configuration
ENV_NAME := maestro_lightning-env
ACTIVATE := source activate.sh

.PHONY: install clean env-rm

# Install environment and package in editable mode
install:
	@echo "🛠️  Creating/Updating virtual environment and installing package..."
	@$(ACTIVATE) && pip install -e .
	@echo "✅ Installation complete."

# Delete the virtual environment
env-rm:
	@echo "🔥 Removing virtual environment: $(ENV_NAME)..."
	rm -rf $(ENV_NAME)
	@echo "✨ Environment removed."

# Clean target to remove all build and cache files, and the environment
clean: env-rm
	@echo "🧹 Cleaning up build artifacts and cache files..."
	rm -rf build/
	rm -rf dist/
	rm -rf *.egg-info
	find . -type d -name "__pycache__" -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete
	find . -type f -name "*.pyo" -delete
	find . -type f -name "*.pyd" -delete
	find . -type f -name ".DS_Store" -delete
	@echo "✨ Done!"
