# Plan: Remove the SQLAlchemy dependency

**Plan id:** `rm-sqla`
**Base branch:** `master`
**Goal:** Drop `sqlalchemy` from disdat's runtime dependencies, replacing it with
Python's stdlib `sqlite3`, while preserving all current behavior and passing the
existing test suite unchanged in intent.

## Context / why

Disdat's only database is a **per-context local SQLite index** at
`~/.disdat/context/<name>/ctxt.db`. It is a rebuildable cache — the protobuf
files on disk are the source of truth. SQLAlchemy is used **Core-only** (no ORM,
no migrations): `Table`/`Column`/`MetaData` DDL, `create_engine`, and
`conn.execute(text(<raw SQL string>))`. All queries are already hand-built raw
SQL strings, so SQLAlchemy is doing very little for us beyond engine/connection
management and result-row mapping.

The recent py3.10–3.14 migration (`design_docs/plans/py-3.10-3.14-migration.plan.md`)
flagged the SQLAlchemy 1.3→2.0 step as the highest-uncertainty change, precisely
because the library's semantics shift under us. Removing the dependency entirely
eliminates that class of churn, shrinks the dependency surface, and removes a
`<3.0` upper-bound pin that constrains future Python compatibility. The usage is
narrow and well-contained, which makes removal tractable and low-risk.

## Current state (verified)

- `sqlalchemy` is declared once: `pyproject.toml` (`"sqlalchemy>=2.0,<3.0"`).
- Imported in exactly two package modules:
  - `disdat/hyperframe.py` — `BLOB, Column, DateTime, Enum, MetaData,
    String, Table, Text, UniqueConstraint`, `IntegrityError`, `sql.text`. Also a
    function-local `from sqlalchemy.sql import text` inside `r_pb_db`.
  - `disdat/data_context.py` — only `create_engine`.
- `api.py` and `fs.py` never touch SQLAlchemy or the engine — they go through
  `DataContext`. `DataContext` is the sole engine holder (`self.local_engine`)
  and the only live caller of the `hyperframe.py` DB functions.
- Backend is **SQLite only**. Every `create_engine` uses a `sqlite:///` URL
  (`ctxt.db` file, or `:memory:`). The `remote_engine`/postgres/DynamoDB paths
  are dead code (guarded by `and False`).
- The DB functions in `hyperframe.py` take an `engine_g` argument and use the
  `with engine.begin()` (write/txn) and `with engine.connect()` (read) context
  managers. The tests (`tests/bundles/test_hframe.py`) construct the engine
  via `create_engine("sqlite:///:memory:")` and pass it straight into these
  functions — so the engine's interface is part of the contract both prod and
  tests depend on.

### Behavioral seams to preserve exactly
1. **`RecordState` enum round-trip — on-disk-compatible.** Verified:
   `Enum(RecordState)` stores the enum **by name** (the string `"valid"`,
   `"deleted"`, …), and the where/update clauses already interpolate
   `state.name`. `_write_row` writes `self.state`; `from_row` reads it back and
   callers compare it as an enum
   (`data_context.py`: `hfr_from_db.state == hyperframe.RecordState.valid`).
   The replacement stores `state.name` and reconstructs `RecordState[name]` —
   which is **byte-for-byte identical to the current on-disk `state` column**, so
   existing `ctxt.db` files remain readable without a rebuild.
2. **String-key row access.** `from_row` uses `row._mapping` and returns it for
   non-pb (grouped) queries; the caller does `row["human_name"]`
   (`data_context.py`). `sqlite3.Row` supports exactly this (`row["col"]`), so it
   is a clean substitute.
3. **BLOB handling.** `pb` is stored as `bytes` and read back; `from_row`
   already normalizes `str`/`memoryview`. sqlite3 returns `bytes` for BLOB —
   the existing normalization still works.
4. **`IntegrityError` on insert conflict** — maps to `sqlite3.IntegrityError`.
   UPSERT policy is `"FAIL"` (insert raises on conflict, caught and logged).
5. **`creation_date`** is written as a `datetime` (`datetime.utcfromtimestamp`)
   and compared as `"%Y-%m-%d %X"` strings in the where clause. Store it as that
   same string format so ordering and `<=`/`>=` comparisons stay identical.

## Design

Introduce a **thin `sqlite3`-backed engine wrapper** inside
`disdat/hyperframe.py` that mimics the small slice of the SQLAlchemy interface
the code already uses, so the ~9 DB function bodies and call sites change as
little as possible.

### 1. `_SqliteEngine` + connection wrapper (`disdat/hyperframe.py`)
A minimal class replacing the SQLAlchemy engine:
- `__init__(self, url, echo=False)` — accept the existing `"sqlite:///..."`
  URLs (parse out `:memory:` vs a file path). Accept-and-ignore `echo` (or route
  it to a debug log) so both prod (`echo=False`) and test (`echo=True`) call
  sites pass it unchanged. Hold connection config.
- `connect()` — return a context manager yielding a lightweight connection
  wrapper for **reads** (no implicit commit).
- `begin()` — return a context manager yielding a connection wrapper that
  **commits on clean exit, rolls back on exception** (matches SQLAlchemy 2.0
  `begin()` semantics the code comments already document).
- `dispose()` — close the underlying connection.
- The connection wrapper exposes `.execute(stmt, params=None)` returning a
  result object that supports iteration and `.fetchone()` (used by
  `bundle_count`). For raw-SQL statements it accepts a plain SQL string (drop the
  `text()` wrapper) or a small internal statement object.

Key decision on connection lifetime for `:memory:`: an in-memory SQLite DB lives
only as long as its connection. The current test/`in_memory` flow relies on a
single engine holding the data across `create_table` → `w_pb_db` → `r_pb_db`
calls. So the wrapper must keep **one persistent connection** for a `:memory:`
URL (open once in `__init__`, reused by `connect()`/`begin()`, closed by
`dispose()`), while for a file URL it may open per-operation connections. Use
`check_same_thread=False` guardedly only if needed; default single-threaded use
matches current behavior.

### 2. Replace the DDL (`hyperframe.py` `_create_table` methods + `create_table`)
The `Table(...)`/`Column(...)` definitions become `CREATE TABLE IF NOT EXISTS`
SQL. Keep the schema identical:
- `hframes(uuid TEXT PRIMARY KEY, owner TEXT, human_name TEXT,
  processing_name TEXT, creation_date TEXT, state TEXT, pb BLOB)`
- `hframes_tags(key TEXT, uuid TEXT, value TEXT, UNIQUE(key, uuid))`
- Same pattern for the test-only tables (`lineage`, `frames`, `linkauth`,
  `links`) so `tests/bundles/test_hframe.py` still exercises them.

`PBObject.create_table(cls, db_engine)` runs the `CREATE TABLE` statements the
class declares, in one transaction, replacing `MetaData.create_all`.

`write_row`/`_write_row` keep returning dict-of-rows keyed by table name; the
insert helper (`_sql_write_tbl_rows`) builds a parameterized
`INSERT INTO <tbl> (cols...) VALUES (?...)` from the row dict instead of
`table.insert()`. **Store `state` as `state.name`.**

### 3. Rewrite the ~9 DB functions to drop `text()`
`w_pb_db`, `r_pb_db`, `bundle_count`, `select_hfr_db`, `update_hfr_db`,
`delete_hfr_db`, `delete_fr_db`. These already build raw SQL strings; the change
is: remove `text(...)`, call `conn.execute(sql)` on the wrapper, catch
`sqlite3.IntegrityError`, and keep the `with engine.begin()/connect()`
structure. `_where_clause`/`_translate`/`_tag_query`/`_groupby_clause` are pure
string builders — unchanged.

### 4. `from_row`
Replace `row._mapping` with `sqlite3.Row` key access (set
`connection.row_factory = sqlite3.Row` in the wrapper). Reconstruct
`state = RecordState[row["state"]]` for the pb branch; return the `sqlite3.Row`
directly for the non-pb branch (supports `row["human_name"]`). Keep the
`str`/`memoryview`→`bytes` normalization for `pb`.

### 5. `data_context.py`
- Remove `from sqlalchemy import create_engine`.
- Replace the two `create_engine("sqlite:///...")` calls in `init_local_db`
  with `hyperframe._SqliteEngine("sqlite:///...")` (or a small factory
  `hyperframe.make_engine(url)`). The `echo=False` kwarg is
  accepted-and-ignored by the wrapper (see §1) so call sites change minimally.
- `self.local_engine.dispose()` — wrapper provides `dispose()`.
- **Rebuild-on-error safety net.** In `init_local_db`, wrap the on-disk
  `ctxt.db` open/first-read so a corrupt or unreadable file
  (`sqlite3.DatabaseError`) is deleted and rebuilt from the on-disk `.pb` files
  via the existing `rebuild_db()`. This is belt-and-suspenders — the schema is
  unchanged (§ seam 1), so old DBs open fine; this only guards genuine
  corruption. Log a warning when it triggers so silent rebuilds are visible.

### 5a. Remove dead remote-engine scaffolding (`data_context.py`)
While in this file, delete the dead DynamoDB/postgres index scaffolding — it is
guarded by `and False` and never constructs an engine at runtime:
- `self.remote_engine = None` and the `init_remote_db()` call in `__init__`.
- The whole `init_remote_db()` method, including the
  `boto3.resource("dynamodb", ...)` branch.
- The now-unused `import boto3` — verified its **only** use in this file is the
  dead dynamo path.
- The stale `postgresql://...` docstring example and the "may be indexed by a
  postgres database" line.
- **Keep** `remote_ctxt_url` and all S3 remote-binding logic — that is live and
  unrelated to the dead DB-index scaffolding.

### 6. `pyproject.toml`
Remove `"sqlalchemy>=2.0,<3.0"` and regenerate `uv.lock` via `uv lock`. Update
the nearby comment block that references the SQLAlchemy 2.0 migration.

### 7. Tests (`tests/bundles/test_hframe.py`)
Replace `from sqlalchemy import create_engine` and
`create_engine("sqlite:///:memory:", echo=True)` with the wrapper factory
(`echo=True` is passed through and ignored). The round-trip assertions (hash
equality) stay unchanged — they are the regression guard for the whole change.

## Files to modify
- `disdat/hyperframe.py` — new `_SqliteEngine`/connection wrapper; rewrite DDL,
  the ~9 DB functions, `create_table`, `write_row`/`_sql_write_tbl_rows`,
  `from_row`; drop all `sqlalchemy` imports.
- `disdat/data_context.py` — drop `create_engine` import; use the wrapper; add
  the rebuild-on-error safety net; remove the dead remote-engine/DynamoDB/postgres
  scaffolding and the now-unused `import boto3`.
- `pyproject.toml` — remove the dep + comment; `uv.lock` regenerated.
- `tests/bundles/test_hframe.py` — construct the wrapper instead of
  `create_engine`.
- `design_docs/plans/PLAN_LOG.md` + this design doc — per repo convention.

## Delivery (plan branches + PLAN_LOG)
Follow the repo convention used by the py314 migration — one branch
`plan/rm-sqla/phase-<n>` and one PR per phase, targeting `master`, tracked in
`design_docs/plans/PLAN_LOG.md` (use the `maintain-plan-log` and `plan-branches`
skills). Never commit to `master` directly.

- **Phase 0 — scaffolding.** Add `design_docs/plans/remove-sqlalchemy.plan.md`
  and seed `PLAN_LOG.md`. No code changes.
- **Phase 1 — sqlite3 layer.** Implement `_SqliteEngine`/connection wrapper and
  rewrite `hyperframe.py` (DDL + all DB functions + `from_row`); update
  `data_context.py` (wrapper + rebuild-on-error safety net + remove the dead
  remote-engine/DynamoDB/postgres scaffolding and `import boto3`); update
  `tests/bundles/test_hframe.py`. Keep the `sqlalchemy` dep still installed so
  the branch is testable at each step.
- **Phase 2 — drop the dependency.** Remove `sqlalchemy` from `pyproject.toml`,
  regenerate `uv.lock`, update the comment block, and confirm nothing imports
  `sqlalchemy` (`grep -rn sqlalchemy disdat/ tests/` returns nothing).

## Verification
1. **Unit round-trip** (primary guard): `uv run pytest tests/bundles/test_hframe.py`
   — `test_hframe_rw_db`, `test_linkauth_rw_db`, `test_link_rw_db` must pass
   (protobuf-hash equality after write→read).
2. **Full suite:** `uv run pytest` (functional tests drive the API →
   `DataContext` → sqlite index end-to-end: `test_context.py`, `test_add.py`,
   `test_cat.py`, `test_remote.py`, etc.). `test_context.py` specifically pins
   the grouped-name / non-pb `from_row` path — must still pass.
3. **Live smoke test** via the API (in-memory + on-disk context):
   create a context, `api.add()` a bundle, list names (`get_hframe_names`),
   `api.cat()` it, then `api.rm()` — exercising insert, grouped select, and
   delete against a real `ctxt.db`. Also verify `rebuild_db`/`dbck` re-index
   from disk `.pb` files.
4. **Dependency gone:** `grep -rn "sqlalchemy" disdat/ tests/ pyproject.toml`
   returns nothing; `uv run python -c "import disdat.api"` succeeds without
   sqlalchemy installed. Also `grep -n "boto3\|remote_engine\|init_remote_db"
   disdat/data_context.py` returns nothing (dead scaffolding removed), while
   `remote_ctxt_url` / S3 binding remain intact.
5. **Rebuild-on-error path:** corrupt a `ctxt.db` (write garbage bytes), open
   the context, and confirm it is transparently rebuilt from disk `.pb` files
   with a logged warning, then reads succeed.
6. **CI matrix** (Phase 2 PR): the existing py3.10–3.14 GitHub Actions matrix
   (`uv run pytest`) is green.
