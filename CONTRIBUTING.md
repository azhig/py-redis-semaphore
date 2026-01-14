# Contributing

Thank you for your interest in contributing to py-redis-semaphore!

## Development Setup

```bash
# Clone the repository
git clone https://github.com/azhig/py-redis-semaphore.git
cd py-redis-semaphore

# Install dependencies
uv sync

# Install pre-commit hooks
make install-hooks

# Start Redis
make redis-up
```

## Development Workflow

### Running Tests

```bash
# Run all tests
make test

# Run specific test file
uv run pytest tests/test_semaphore.py -v

# Run with coverage
uv run pytest --cov=src/redis_semaphore
```

### Code Quality

```bash
# Run all checks (lint + typecheck + test)
make check

# Format code
make format

# Fix linting issues
make ruff-fix

# Type checking
make typecheck
```

### Pre-commit Hooks

Pre-commit hooks run automatically on `git commit`. To run manually:

```bash
make pre-commit
```

## Pull Request Process

1. **Fork** the repository
2. **Create a branch** from `main` for your feature/fix
3. **Make your changes** with tests
4. **Run checks**: `make check`
5. **Commit** with a clear message
6. **Push** and open a Pull Request

### Commit Messages

Use clear, descriptive commit messages:

- `fix: resolve deadlock in async acquire`
- `feat: add Redis Cluster support`
- `docs: improve Sentinel configuration example`
- `test: add edge case for lock timeout`

### Code Style

- Follow existing code patterns
- Add type hints to all public functions
- Write docstrings for public APIs
- Keep functions focused and small

## Testing Guidelines

- Write tests for new features
- Test both sync and async APIs
- Include edge cases (timeouts, errors, race conditions)
- Use descriptive test names

### Test Structure

```
tests/
├── test_semaphore.py      # Core semaphore tests
├── test_async.py          # Async-specific tests
├── test_sentinel.py       # Sentinel integration tests
├── test_heartbeat.py      # Heartbeat mechanism tests
└── ...
```

## Architecture Notes

Key files to understand:

- `src/redis_semaphore/semaphore.py` - Main Semaphore/Mutex classes
- `src/redis_semaphore/lua_scripts.py` - Atomic Lua scripts
- `src/redis_semaphore/heartbeat.py` - TTL refresh mechanism

See [CLAUDE.md](CLAUDE.md) for detailed architecture overview.

## Questions?

Open an issue or discussion for questions about contributing.
