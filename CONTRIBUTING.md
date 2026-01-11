# Contributing to Graph Analytics AI

Thank you for your interest in contributing to Graph Analytics AI! This document provides guidelines and instructions for contributing.

## Getting Started

1. Fork the repository
2. Clone your fork: `git clone https://github.com/yourusername/graph-analytics-orchestrator.git`
3. Create a branch: `git checkout -b feature/your-feature-name`
4. Install in development mode: `pip install -e .[dev]`

## Development Setup

```bash
# Install dependencies
pip install -r requirements.txt
pip install -e .[dev]

# Run tests
pytest

# Run linting
black graph_analytics_orchestrator/
flake8 graph_analytics_orchestrator/
mypy graph_analytics_orchestrator/
```

## Code Style

- Follow PEP 8 style guidelines
- Use `black` for code formatting
- Use type hints where possible
- Write docstrings for all public functions and classes

## Testing

- Write tests for new features
- Ensure all tests pass: `pytest`
- Aim for high test coverage

## Submitting Changes

1. Ensure your code passes all tests and linting
2. Update documentation if needed
3. Commit your changes with clear commit messages
4. Push to your fork
5. Open a pull request with a clear description

## Pull Request Guidelines

- Provide a clear description of changes
- Reference any related issues
- Ensure all tests pass
- Update documentation as needed
- Follow the existing code style

## Areas for Contribution

- Additional graph algorithms
- Performance improvements
- Documentation improvements
- Bug fixes
- Test coverage improvements

## Questions?

Open an issue or contact the maintainers.

