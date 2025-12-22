# Contributing to eventflow

Thank you for your interest in contributing to eventflow! This document provides guidelines and instructions for contributing.

## Table of Contents

- [Development Setup](#development-setup)
- [Code Quality Standards](#code-quality-standards)
- [Development Workflow](#development-workflow)
- [Testing](#testing)
- [Commit Guidelines](#commit-guidelines)
- [Pull Request Process](#pull-request-process)

## Development Setup

### Prerequisites

- Python 3.10, 3.11, or 3.12
- [Poetry](https://python-poetry.org/docs/#installation) for dependency management

### Initial Setup

1. **Clone the repository:**
   ```bash
   git clone https://github.com/parhamdavari/eventflow.git
   cd eventflow
   ```

2. **Install dependencies and pre-commit hooks:**
   ```bash
   make install-dev
   ```

   This installs all dependencies and sets up pre-commit hooks that will automatically format and check your code before each commit.

3. **Verify installation:**
   ```bash
   make test
   ```

## Code Quality Standards

We maintain high code quality through automated tooling. All code must pass:

| Tool | Purpose | Command |
|------|---------|---------|
| **Black** | Code formatting | `make format` |
| **Ruff** | Linting + import sorting | `make lint` |
| **MyPy** | Type checking | `make type-check` |
| **Bandit** | Security scanning | `make security` |

### Quick Commands

```bash
# Auto-format your code
make format

# Run all quality checks
make quality

# Run tests with coverage
make test-cov

# Simulate full CI pipeline locally
make ci
```

### IDE Integration (Recommended)

Configure your IDE to format on save:

**VS Code** (`.vscode/settings.json`):
```json
{
    "editor.formatOnSave": true,
    "python.formatting.provider": "black",
    "editor.codeActionsOnSave": {
        "source.organizeImports": true
    },
    "[python]": {
        "editor.defaultFormatter": "ms-python.black-formatter"
    }
}
```

## Development Workflow

### 1. Create a Branch

```bash
git checkout -b feature/your-feature-name
# or
git checkout -b fix/issue-description
```

### 2. Make Changes

Write your code following these guidelines:

- **Type hints:** All functions must have type annotations
- **Docstrings:** Use Google-style docstrings for public APIs
- **Tests:** Add tests for new functionality

### 3. Validate Locally

Before committing, run the full quality suite:

```bash
make ci
```

This simulates the CI pipeline and catches issues before they reach GitHub.

### 4. Commit

Pre-commit hooks will automatically:
- Format code with Black
- Sort imports with Ruff
- Check for common issues

If hooks make changes, simply stage and commit again:

```bash
git add .
git commit -m "feat: add new feature"
```

## Testing

### Running Tests

```bash
# All tests
make test

# With coverage report
make test-cov

# Specific test file
poetry run pytest tests/unit/test_events.py -v

# Specific test
poetry run pytest tests/unit/test_events.py::test_event_creation -v
```

### Writing Tests

- Place tests in `tests/unit/` or `tests/integration/`
- Use descriptive test names: `test_event_serialization_handles_nested_objects`
- Use pytest fixtures for setup (see `tests/conftest.py`)

### Coverage Requirements

- Minimum coverage: **80%**
- New code should have comprehensive test coverage
- Coverage report: `htmlcov/index.html` after `make test-cov`

## Commit Guidelines

We follow [Conventional Commits](https://www.conventionalcommits.org/):

```
<type>(<scope>): <description>

[optional body]

[optional footer]
```

### Types

| Type | Description |
|------|-------------|
| `feat` | New feature |
| `fix` | Bug fix |
| `docs` | Documentation only |
| `style` | Formatting, no code change |
| `refactor` | Code change that neither fixes nor adds |
| `perf` | Performance improvement |
| `test` | Adding tests |
| `chore` | Maintenance tasks |

### Examples

```
feat(inbox): add batch processing support
fix(outbox): handle connection timeout gracefully
docs: update API reference for EventConsumer
test(inbox): add integration tests for PostgreSQL
```

## Pull Request Process

1. **Ensure all checks pass:**
   ```bash
   make ci
   ```

2. **Push your branch:**
   ```bash
   git push -u origin feature/your-feature-name
   ```

3. **Create a Pull Request:**
   - Use a clear, descriptive title
   - Reference any related issues
   - Describe what changes you made and why

4. **Address Review Feedback:**
   - Respond to all comments
   - Push additional commits to address feedback
   - Re-request review when ready

### PR Checklist

- [ ] Tests pass locally (`make test`)
- [ ] Quality checks pass (`make quality`)
- [ ] New code has test coverage
- [ ] Documentation updated if needed
- [ ] Commit messages follow conventions

## Questions?

- Open an issue for bugs or feature requests
- Start a discussion for questions or ideas

Thank you for contributing!
