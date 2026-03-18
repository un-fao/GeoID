# Contributing to DynaStore

Thank you for your interest in contributing to DynaStore! This document provides guidelines for contributing to the project.

## Code of Conduct

Be respectful, inclusive, and professional in all interactions.

## Getting Started

### Development Setup

1. **Fork and clone the repository**
   ```bash
   git clone https://github.com/your-username/dynastore.git
   cd dynastore
   ```

2. **Create a virtual environment**
   ```bash
   python -m venv .venv
   source .venv/bin/activate  # On Windows: .venv\Scripts\activate
   ```

3. **Install dependencies**
   ```bash
   pip install -e '.[all]'
   ```

4. **Start the development environment**
   ```bash
   ./manage.sh dev
   ```

## Development Workflow

### Creating a Feature Branch

```bash
git checkout -b feature/your-feature-name
```

### Making Changes

1. Write your code following the project's coding standards
2. Add tests for new functionality
3. Update documentation as needed
4. Ensure all tests pass

### Running Tests

```bash
# Run all tests
pytest tests/

# Run specific test file
pytest tests/path/to/test_file.py

# Run with coverage
pytest tests/ --cov=dynastore --cov-report=html
```

### Code Quality

We use the following tools for code quality:

```bash
# Format code (if using black)
black src/ tests/

# Lint code (if using ruff)
ruff check src/ tests/

# Type checking (if using mypy)
mypy src/
```

## Pull Request Process

1. **Update your fork**
   ```bash
   git fetch upstream
   git rebase upstream/main
   ```

2. **Push your changes**
   ```bash
   git push origin feature/your-feature-name
   ```

3. **Create a Pull Request**
   - Go to the GitHub repository
   - Click "New Pull Request"
   - Select your branch
   - Fill in the PR template with:
     - Description of changes
     - Related issues
     - Testing performed
     - Screenshots (if UI changes)

4. **PR Requirements**
   - All tests must pass
   - Code coverage should not decrease
   - Documentation updated if needed
   - Commit messages are clear and descriptive

## Coding Standards

### Python Style

- Follow PEP 8 guidelines
- Use type hints for function signatures
- Write docstrings for all public APIs
- Keep functions focused and concise

### Async/Await

- All I/O operations must be async
- Use `async with` for resource management
- Avoid blocking operations in async code

### Database Operations

- Always use `DBResource` for database connections
- Use parameterized queries to prevent SQL injection
- Close connections properly using context managers

### Logging

- Use DynaStore's logger: `from dynastore.core.logger import get_logger`
- Log at appropriate levels (DEBUG, INFO, WARNING, ERROR)
- Include context in log messages

### Error Handling

- Use specific exception types
- Provide meaningful error messages
- Log errors with stack traces when appropriate

## Plugin Naming Convention

DynaStore uses a consistent **underscore-everywhere** convention for naming
modules, extensions, tasks, and extras.  This ensures that the name a developer
writes in `pyproject.toml` matches the folder on disk and the entry-point key —
no mental translation required.

### Rules

| Artifact | Naming | Example |
|----------|--------|---------|
| Python folder | `_` (language requirement) | `src/dynastore/extensions/catalog_events/` |
| Entry-point key | `_` (matches folder) | `catalog_events = "…:CatalogEventHandler"` |
| Optional-dependency key | `_` (matches entry-point) | `extension_catalog_events = []` |
| `SCOPE` env var value | `_` | `SCOPE=api_core,catalog_api` |

### Why not hyphens?

Python module names **must** use underscores (`import catalog_events`).
Pip normalizes extras names to hyphens internally (PEP 685), but the
**source** `pyproject.toml` can use either character — pip treats them as
equivalent.  By writing underscores everywhere we get one convention from
folder to entry-point to extras to SCOPE, with zero `.replace()` ambiguity.

### Prefixes

Optional-dependency keys use a prefix to indicate the plugin type:

| Prefix | Plugin type | Example |
|--------|-------------|---------|
| `module_` | Module | `module_catalog = [...]` |
| `extension_` | Extension | `extension_web = [...]` |
| `task_` | Task | `task_gdal = [...]` |

The discovery system strips these prefixes automatically when matching against
entry-point names (e.g. `extension_catalog_events` matches entry-point `catalog_events`).

### Downstream projects

External projects that register dynastore entry-points should follow the same
convention.  Example `pyproject.toml`:

```toml
[project.optional-dependencies]
module_spanner = []
extension_permissions = []
extension_catalog_events = []

my_app = [
    "my-project[module_spanner]",
    "my-project[extension_permissions]",
    "my-project[extension_catalog_events]",
]

[project.entry-points."dynastore.modules"]
spanner = "my_project.modules.spanner:SpannerModule"

[project.entry-points."dynastore.extensions"]
permissions    = "my_project.extensions.permissions:PermissionsExtension"
catalog_events = "my_project.extensions.catalog_events:CatalogEventHandler"
```

## Creating External Modules

See the [example project template](../examples/my-project/) for a ready-to-use
starting point when creating modules, extensions, or tasks in a separate repository.

## Documentation

- Update README.md for user-facing changes
- Update docstrings for API changes
- Add examples for new features
- Keep documentation concise and clear

## Commit Messages

Use clear, descriptive commit messages:

```
feat: Add support for external modules
fix: Resolve database connection leak
docs: Update installation instructions
test: Add integration tests for catalog API
refactor: Simplify event handling logic
```

## Questions or Issues?

- Check existing issues and discussions
- Create a new issue with:
  - Clear description
  - Steps to reproduce (for bugs)
  - Expected vs actual behavior
  - Environment details

## License

By contributing, you agree that your contributions will be licensed under the Apache 2.0 License.
