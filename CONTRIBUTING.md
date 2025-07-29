# Contributing to FOMO Strategy

Thank you for your interest in contributing to the FOMO Strategy project! This document provides guidelines for contributing.

## Getting Started

1. Fork the repository
2. Clone your fork locally
3. Create a virtual environment and install dependencies:
   ```bash
   python -m venv .venv
   source .venv/bin/activate  # On Windows: .venv\Scripts\activate
   pip install -r requirements.txt
   ```

## Development Workflow

1. Create a feature branch from `main`:
   ```bash
   git checkout -b feature/your-feature-name
   ```

2. Make your changes and test them:
   ```bash
   make test
   make lint
   ```

3. Commit your changes with a descriptive message:
   ```bash
   git commit -m "Add feature: description of changes"
   ```

4. Push to your fork and create a pull request

## Code Style

- Follow PEP 8 guidelines
- Use type hints where appropriate
- Keep functions focused and well-documented
- Write docstrings for all public functions

## Testing

- Add tests for new functionality
- Ensure all tests pass before submitting
- Run the test suite:
  ```bash
  make test
  ```

## Code Quality

- Run linting before submitting:
  ```bash
  make lint
  ```
- Format your code:
  ```bash
  make format
  ```

## Pull Request Guidelines

1. Provide a clear description of the changes
2. Include any relevant issue numbers
3. Ensure all tests pass
4. Update documentation if needed
5. Keep PRs focused and reasonably sized

## Reporting Issues

When reporting issues, please include:
- A clear description of the problem
- Steps to reproduce
- Expected vs actual behavior
- Environment details (OS, Python version, etc.)

## Questions?

Feel free to open an issue for questions or discussions about the project. 