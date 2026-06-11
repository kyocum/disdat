# Plan: Python 3.10–3.14 Compatibility

**Plan id:** `py314`
**Base branch:** `master`
**Goal:** Make `disdat` install, import, and pass its test suite on CPython
3.10, 3.11, 3.12, 3.13, and 3.14 — by modernizing the dependency pins,
migrating packaging to `pyproject.toml` managed with `uv`, fixing the code
that the modern deps break, and verifying across all five versions in CI.

## Context / current state

- Packaging is `setup.py` + `setuptools_scm` (no `pyproject.toml`).
- `setup.py` already declares `python_requires=">=3.9, <3.14"` and classifiers
  through 3.13; the real blocker to 3.13/3.14 is the upper-bound dependency
  pins, which have no wheels for recent Python.
- Tests live under `tests/functional/` and `tests/bundles/`, run via `tox`
  (envlist py39–313).
- SQLAlchemy usage is **Core only** (Table/MetaData/`text()`/`execute()`),
  ~9 `execute()` sites, all in `disdat/hyperframe.py`.
- The protobuf module `disdat/hyperframe_pb2.py` is pre-3.20 generated style
  with a Python-2 shim; `.proto` sources exist under
  `infrastructure/Dockerfiles/hyperframe_def/`, so it is regenerable.

## Tooling rules (per project conventions)

- Add tools to the **project first** (e.g. `grpcio-tools` as a dev dependency
  for `protoc` via `python -m grpc_tools.protoc`); use `pipx` only for things
  that must be system-wide; never system-wide `pip`.
- Use `uv` for env + lockfile management.

## Phases

Each phase is one branch (`plan/py314/phase-<n>`) and one PR targeting
`master`, logged in `design_docs/plans/PLAN_LOG.md`.

### Phase 0 — Plan scaffolding (this branch)
- Add this plan document and seed `PLAN_LOG.md`.
- No code or dependency changes.

### Phase 1 — Packaging migration to pyproject.toml + uv
- Write PEP 621 `pyproject.toml`: build backend `setuptools` + `setuptools_scm`,
  `requires-python = ">=3.10,<3.15"`, classifiers 3.10–3.14, entry point
  `dsdt = disdat.entrypoints.cli_ep:main`, `[tool.setuptools_scm]` writing
  `disdat/version.py`, package-data (config/disdat, VERSION, *.json).
- Dependency groups for dev/rel/test managed via `uv`; generate `uv.lock`.
- Remove/shrink `setup.py`; reconcile `MANIFEST.in`.
- Add `grpcio-tools` as a dev dependency (vendored `protoc`).

### Phase 2 — Modernize dependency pins
- `numpy>=1.24` (allow 2.x), `pandas>=2.0,<3.0`, `sqlalchemy>=2.0,<3.0`,
  `protobuf>=5,<7`, `boto3>=1.34,<2.0`; drop ancient `s3fs<=0.4.2`.

### Phase 3 — Code changes to match new deps
- numpy aliases: `np.unicode_`→`np.str_`, `np.string_`→`np.bytes_`,
  `np.float_`→`np.float64` (`disdat/hyperframe.py`, `tests/bundles/test_hframe.py`).
- Regenerate `disdat/hyperframe_pb2.py` (and any `bundle_pb2`) from `.proto`
  via `grpc_tools.protoc`, matched to the chosen protobuf pin.
- SQLAlchemy 1.3 → 2.0 migration in `disdat/hyperframe.py` (~9 execute sites:
  connection/transaction semantics, `text()`, result-row access).
- `disdat/resource.py`: replace `pkgutil.get_loader()` + `loader.load_module()`
  (removed in 3.12) with `importlib`.

### Phase 4 — Test & verify across versions
- Update `tox.ini` envlist → py310–py314; align dev deps with pyproject.
- Run functional + bundle suites locally via `uv run` on available interpreters;
  fix fallout (likely pandas 2.x / numpy 2.x dtype + SQLAlchemy rows).

### Phase 5 — CI
- Add `.github/workflows/test.yml`: matrix python 3.10–3.14, `uv sync` +
  `uv run pytest`, on push/PR (free on public repo, no secrets).
- Update publish workflow: replace `python setup.py sdist` with `uv build`
  (or `python -m build`); bump `actions/*` major versions.

## Risks

- **SQLAlchemy 2.0** is the highest-uncertainty step (autocommit/connection
  semantics). Contained to `hyperframe.py`, covered by `test_hframe.py`.
- **protobuf regen** must use a `protoc`/runtime pair matching the chosen pin.
- **pandas 2.x / numpy 2.x** may surface dtype/serialization changes in the
  hyperframe round-trip tests; caught by the existing suite.
