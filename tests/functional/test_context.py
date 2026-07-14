#
# Copyright 2017 Human Longevity, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
import os
import sqlite3

import pytest

import disdat.api as api
from disdat.data_context import DataContext
from disdat.fs import DisdatFS


def test_create_context():
    context_name = "__test__"
    assert context_name not in api.ls_contexts(), "Context exists"

    api.context(context_name)
    assert context_name in api.ls_contexts(), "Test context does exists"
    api.delete_context(context_name=context_name)
    assert context_name not in api.ls_contexts(), "Test context exists"


def test_independent_context():
    context_1_name = "__test_context_1__"
    context_2_name = "__test_context_2__"

    api.context(context_1_name)
    api.context(context_2_name)

    _ = api.Bundle(context_1_name, name="context_test", data=2)

    assert (
        len(api.search(context_1_name)) == 1
    ), "Only one bundle should be in context one"
    assert len(api.search(context_2_name)) == 0, "Context two should be empty"

    api.delete_context(context_name=context_1_name)
    api.delete_context(context_name=context_2_name)

    assert context_1_name not in api.ls_contexts(), "Contexts should be removed"
    assert context_2_name not in api.ls_contexts(), "Contexts should be removed"


def test_get_hframe_names():
    """Regression: grouped name listing under SQLAlchemy 2.0.

    get_hframe_names() runs a groupby query whose rows have no ``pb`` column,
    so hyperframe.from_row returns the non-pb branch. Under SQLAlchemy 2.0 a
    raw Row has no string-key access, so the result must be a RowMapping for
    ``row["human_name"]`` to work.
    """
    context_name = "__test_names__"
    api.context(context_name)
    try:
        api.Bundle(context_name, name="alpha", data=1)
        api.Bundle(context_name, name="beta", data=2)

        data_context = DisdatFS().get_context(context_name)
        assert sorted(data_context.get_hframe_names()) == ["alpha", "beta"]
    finally:
        api.delete_context(context_name=context_name)


def test_file_db_corrupt_detects_bad_format(tmp_path):
    """A file that is not a sqlite database is classified as corrupt."""
    db_file = os.path.join(str(tmp_path), "ctxt.db")
    with open(db_file, "wb") as f:
        f.write(b"this is definitely not a sqlite database" * 4)

    assert DataContext._file_db_corrupt(db_file) is True


def test_file_db_corrupt_false_for_valid_db(tmp_path):
    """A valid (even empty) sqlite database is not classified as corrupt."""
    db_file = os.path.join(str(tmp_path), "ctxt.db")
    conn = sqlite3.connect(db_file)
    conn.execute("CREATE TABLE t (a TEXT)")
    conn.commit()
    conn.close()

    assert DataContext._file_db_corrupt(db_file) is False


def test_file_db_corrupt_reraises_transient_lock(tmp_path, monkeypatch):
    """A transient error (e.g. 'database is locked') must NOT be reported as
    corruption -- it is re-raised so the caller does not delete a valid,
    possibly in-use database.
    """
    db_file = os.path.join(str(tmp_path), "ctxt.db")
    conn = sqlite3.connect(db_file)
    conn.execute("CREATE TABLE t (a TEXT)")
    conn.commit()
    conn.close()

    def _raise_locked(*_args, **_kwargs):
        raise sqlite3.OperationalError("database is locked")

    # Simulate the lock surfacing on connect.
    monkeypatch.setattr(sqlite3, "connect", _raise_locked)

    with pytest.raises(sqlite3.OperationalError):
        DataContext._file_db_corrupt(db_file)


if __name__ == "__main__":
    pytest.main([__file__])
