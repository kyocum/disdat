# Contributing to Disdat

## Development environment

Disdat uses [`uv`](https://docs.astral.sh/uv/) for environment and dependency
management, and supports **Python 3.10–3.14**.

Create a virtual environment and install the package with its development
extras (editable):

```bash
uv venv .venv --python 3.13          # any of 3.10–3.14
uv pip install -e ".[dev]"
```

This installs the runtime dependencies plus the dev tools (pytest, moto,
pyarrow, s3fs, grpcio-tools, etc.) declared under
`[project.optional-dependencies].dev` in `pyproject.toml`.

## Running the tests

```bash
.venv/bin/python -m pytest tests -q
```

Useful subsets:

```bash
.venv/bin/python -m pytest tests/functional -q       # functional tests
.venv/bin/python -m pytest tests/bundles -q          # bundle / hyperframe tests
.venv/bin/python -m pytest tests/functional/test_remote.py::test_push -x   # one test
```

### Test contexts

The functional tests write to disdat's local context store
(`~/.disdat/context/`). Tests clean up their own contexts on success, and the
suite's teardown (`tests/conftest.py`) removes any leftover `__test*` contexts
when a run *fails* — so a later run starts clean. Your real contexts are never
touched.

You only need to clean up by hand if a run is hard-killed before teardown
runs (e.g. an interrupted process). In that case:

```bash
.venv/bin/python -c "import disdat.api as api; \
[api.delete_context(context_name=c) for c in list(api.ls_contexts()) \
 if c.startswith('_') and c.lstrip('_').startswith('test')]"
```

## Testing across all supported Python versions

`tox.ini` is configured for `py310`–`py314` (and writes an HTML coverage
report). It runs whichever interpreters you have installed
(`skip_missing_interpreters=true`):

```bash
uv tool run tox          # or: pipx run tox
```

For day-to-day work a single-version run is fastest; run `tox` before pushing
to confirm the full 3.10–3.14 matrix.

## Building

```bash
uv build                 # builds the sdist + wheel into dist/
```

The version is derived from git tags via `setuptools_scm`; there is no version
string to bump by hand.

## Pull requests

- Branch from `master`; one focused change per PR.
- Make sure `pytest tests` passes (ideally `tox` across versions) before
  opening the PR.
