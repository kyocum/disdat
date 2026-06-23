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
export MP_CONTEXT_TYPE=fork          # required: disdat uses multiprocessing
.venv/bin/python -m pytest tests -q
```

`MP_CONTEXT_TYPE=fork` is required on macOS — disdat relies on forked
multiprocessing, and the default `spawn` start method causes the
multiprocessing tests to hang or fail.

Useful subsets:

```bash
.venv/bin/python -m pytest tests/functional -q       # functional tests
.venv/bin/python -m pytest tests/bundles -q          # bundle / hyperframe tests
.venv/bin/python -m pytest tests/functional/test_remote.py::test_push -x   # one test
```

### Heads-up: leftover test contexts

The functional tests write to disdat's local context store
(`~/.disdat/context/`) and only clean up on success. If a run fails partway, it
can leave `__test*` / `___test*` contexts behind, and a subsequent run may then
see stale bundles (spurious count-mismatch failures). Reset just the test
contexts — your real contexts are left untouched:

```bash
.venv/bin/python -c "import disdat.api as api; \
[api.delete_context(context_name=c) for c in list(api.ls_contexts()) \
 if c.startswith('__test') or c.startswith('___test')]"
```

## Testing across all supported Python versions

`tox.ini` is configured for `py310`–`py314` (it sets `MP_CONTEXT_TYPE=fork` and
writes an HTML coverage report). It runs whichever interpreters you have
installed (`skip_missing_interpreters=true`):

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
