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

from luigi.target import Target
import pyodbc
import logging
import contextlib
import pandas as pd

_logger = logging.getLogger(__name__)

#
# Your database may not support schema creation to logically separate
# tables in different data contexts.   This should be a configuration option.
#

ENABLE_DISDAT_SCHEMAS = False


@contextlib.contextmanager
def get_connection(dsn):
    conn = pyodbc.connect(dsn=dsn)
    yield conn
    conn.close()


@contextlib.contextmanager
def get_cursor(dsn):
    conn = pyodbc.connect(dsn=dsn)
    cursor = conn.cursor()
    yield cursor
    cursor.commit()
    cursor.close()
    conn.close()


def single_query(dsn, query, cursor=None):
    """

    Args:
        dsn (unicode) : The database configuration dsn
        query (unicode) : Raw query text
        cursor (`pyodbc.cursor`: An valid PyODBC Cursor object

    Returns:
        df (pd.DataFrame) or desc (str)

    """

    #print ("query: Running query:{}".format(query))

    get_rows = True
    if "DROP" in query or "CREATE" in query or "ALTER" in query:
        get_rows = False

    def _query(cursor, query):
        cursor.execute(query)
        desc = cursor.description
        df = None
        if get_rows:
            rows = cursor.fetchall()
            col_names = [x[0] for x in cursor.description]
            df = pd.DataFrame.from_records(rows, columns=col_names)
        return desc, df

    if cursor is None:
        with get_cursor(dsn) as cursor:
            desc, df = _query(cursor, query)
    else:
        desc, df = _query(cursor, query)

    if get_rows:
        return df
    else:
        return desc


def multi_tx_query(dsn, queries):
    """
    Issue multiple queries in a single transaction.  We depend
    on our get_cursor() function to call cursor.commit().

    Args:
        dsn (unicode) : The database configuration dsn
        queries (list:unicode) : Query texts

    Returns:
        list:df (pd.DataFrame) or list:desc (str)

    """
    results = []
    with get_cursor(dsn) as cursor:
        for q in queries:
            results.append(single_query(dsn, q, cursor))

    return results


def table_exists_vertica(dsn, schema, table):
    """
    Determine whether a table exists in a Vertica DataBase

    Args:
        dsn (unicode) : The db configuration to use
        schema (unicode) : The schema holding table
        table (unicode) : The table name

    Returns:
        (bool) : True if table is present in the db
    """

    query = u"""
    SELECT DISTINCT table_name, table_type FROM all_tables
    WHERE table_name ILIKE '{}' and
    schema_name ilike '{}';
    """.format(table, schema)

    with get_connection(dsn) as conn:
        result = pd.read_sql(query, conn)

    if len(result) <= 0:
        return False
    else:
        return True


def schema_exists_vertica(dsn, schema):
    """
    Determine whether a schema exists in a Vertica DataBase

    Args:
        dsn (unicode) : The db configuration to use
        schema (unicode) : The schema holding table

    Returns:
        (bool) : True if schema is present in the db
    """

    query = u"""
    SELECT DISTINCT schema_name FROM schemata
    WHERE schema_name ILIKE '{}'
    """.format(schema)

    with get_connection(dsn) as conn:
        result = pd.read_sql(query, conn)

    if len(result) <= 0:
        return False
    else:
        return True


def drop_table_vertica(dsn, table, run=True):
    """

    Args:
        dsn (unicode): The db configuration to use
        table (unicode): The name of the table to drop, i.e., <schema.table>
        run (bool): Execute the query (default), else if False, just return the query text

    Returns:
        (unicode): The unicode string containing the query

    """
    query = u'DROP TABLE if exists {};'.format(table)

    if run:
        result = single_query(dsn, query)
    else:
        result = query

    return result


def drop_view_vertica(dsn, table, run=True):
    """

    Args:
        dsn (unicode): The db configuration to use
        table (unicode): The name of the table to drop, i.e., <schema.table>
        run (bool): Execute the query (default), else if False, just return the query text

    Returns:
        (unicode): The unicode string containing the query

    """
    query = u'DROP VIEW if exists {};'.format(table)

    if run:
        result = single_query(dsn, query)
    else:
        result = query

    return result


def database_name_vertica(dsn):
    """
    SQL for getting the current database name

    Args:
        dsn (unicode): The db configuration to use

    Returns:
        (unicode): The name of the database

    """
    query = u'SELECT CURRENT_DATABASE();'

    result = single_query(dsn, query)

    return result[result.columns[0]].iloc[0]


class DBTarget(Target):
    """

    A database table target.  Users create a target object.
    They use the target object to create a table called `table_name`.

    Unlike Luigi's File_Target, users don't use this object to issue
    any calls to the database.  It is only to parameterize their sql
    query strings with the destination table name.

    The DBTarget.exists() function can be used to verify the table
    exists.  Note that this only checks for table existence.

    We assume that the user has configured their system to reach their DB
    through the use of a Data Source Name or DSN, and that a file, like
    ~/.odbc.ini, defines the attributes of the DSN, including the database,
    login, password, servername, port, and driver.


    """

    disdat_prefix = "DISDAT"

    def __init__(self, pipe, dsn, table_name, schema_name, servername='unknown', database='unknown', context=None, uuid=None, port=-1):
        """
        User creates a db_target within a Disdat Pipe when they want to work on a database table.

        The virt_name is the virtual table name (schema.'DISDAT_<context>_<table_name>')
        The phys_name is the physical table name (schema.'DISDAT_<context>_<table_name>_<uuid>')

        Note that at the moment we are using the bundle uuid to append to the end of the virt_name.
        We probably should use the link_uuid.   But we would need to have the db index table to then
        figure out which bundles contained which tables.

        Args:
            pipe (`disdat.pipe.PipeTask`): The pipe that is requesting to create a table
            dsn (unicode): The DSN of the database.  Assumes access via odbc.  If None, will create a
                DBTarget but will not be 'connected.'
            table_name (unicode): The name of the table the user wants to create.
            schema_name (unicode): Pass in schema name.  Currently do not auto-generate schema
            servername (unicode): If no dsn, use servername.  Default is 'unknown'
            database (unicode): If no dsn, use database.  Default is 'unknown'
            context (`disdat.data_context.DataContext`): Optional, only used if pipe argument is None.
            uuid (unicode): Optional, only used if pipe argument is None.
        """
        global ENABLE_DISDAT_SCHEMAS

        if schema_name is not None:
            ENABLE_DISDAT_SCHEMAS = False
        else:
            ENABLE_DISDAT_SCHEMAS = True
            _logger.error("Disdat generated schemas not yet implemented")
            raise NotImplementedError

        self.table_name = table_name
        self.servername = servername
        self.database = database
        self.port     = port

        self.dsn = dsn
        try:
            with get_connection(dsn) as c:
                self.servername = c.getinfo(pyodbc.SQL_SERVER_NAME)
                self.database = c.getinfo(pyodbc.SQL_DATABASE_NAME)
                if self.database == '':
                    self.database = database_name_vertica(self.dsn)
                _logger.debug("Found database[{}] on server[{}]".format(self.database, self.servername))
            self.connected = True
        except Exception as e:
            print ("Failed to get connection using dsn[{}] while creating db target:{}".format(self.dsn, e))
            print ("Using servername[{}] and database[{}].".format(self.servername,self.database))
            self.connected = False

        self.pipe = pipe
        if self.pipe is not None:
            self.context = pipe.pfs.get_curr_context()
        else:
            self.context = context

        self.phys_name_url = None
        self.virt_name = None
        self.phys_name = None
        self.schema = schema_name
        self.committed = False

        self.uuid = uuid
        if self.uuid is None:
            pce = self.pipe.pfs.get_path_cache(self.pipe)
            assert(pce is not None)
            self.uuid = pce.uuid
        self.sql_name_uuid = self.uuid.replace('-', '')

        self.init()

        # If this is a user, they must add the pipe argument, and we
        # keep track of the different db_targets they create.
        if self.pipe is not None: self.pipe.add_db_target(self)

    def init(self):
        """
        Create schema name, ensure it exists.
        Create physical table name.

        Sometimes we create a DBTarget outside of running a particular pipe or task.  This happens on
        commit, for example.  In that case, we assume a bundle uuid as an argument.

        Returns:
            None
        """

        if ENABLE_DISDAT_SCHEMAS:
            """
            Here the schema contains <disdat_prefix>_<context>
            """
            self.schema = u"{}_{}".format(DBTarget.disdat_prefix, self.context.get_local_name())

            if not schema_exists_vertica(self.schema):
                query = u"""
                CREATE SCHEMA IF NOT EXISTS {}
                """.format(self.schema)
                single_query(self.dsn, query)

            self.phys_name = u"{}.{}_{}".format(self.schema, self.table_name, self.sql_name_uuid)
        else:
            """
            Here the name is prefixed by <disdat_prefix>_<context>
            """
            assert self.schema
            self.phys_name = u"{}.{}_{}_{}_{}".format(self.schema,
                                                      self.disdat_prefix,
                                                      self.context.get_local_name(),
                                                      self.table_name,
                                                      self.sql_name_uuid)

        """ The fully qualified physical name is the text representation of the link stored in the bundle """
        self.phys_name_url = self.url()

        self.virt_name = DBTarget.phys_to_virt(self.phys_name)

    def commit(self):
        """
        Commit a database table link by creating (maybe replacing) a view for the table, using the physical name.

        Returns:
            None

        """
        virtual_view = """
        CREATE VIEW {} as
        SELECT * from {}
        """.format(self.virt_name, self.phys_name)

        try:
            queries = []
            queries.append(drop_view_vertica(self.dsn, self.virt_name, run=False))
            queries.append(virtual_view)
            multi_tx_query(self.dsn, queries)
        except Exception as e:
            raise Exception("DBTarget:commit failed: {}".format(e))

    def rm(self, drop_view=False):
        """
        Remove a database table link.  There may or may not be a view.  Remove both.

        Note: The policy for whether to remove the corresponding view is currently the
        responsibility of the caller (e.g., data_context.py).

        Args:
            drop_view (bool): If true, drop the view as well as the phys table

        Returns:
            None

        """
        try:
            queries = []
            if drop_view:
                queries.append(drop_view_vertica(self.dsn, self.virt_name, run=False))
            queries.append(drop_table_vertica(self.dsn, self.phys_name, run=False))
            multi_tx_query(self.dsn, queries)
        except Exception as e:
            raise Exception("DBTarget:rm failed: {}".format(e))

    def is_latest_committed(self):
        """
        Check to see if there is a view for this virtual table
        that uses this physical table as its basis.  If yes, then
        this is the latest committed version on the db.

        TODO: Needs to be used in a transaction to remove the table, else possible
        someone commits a newer version and we remove the latest view.

        Note: This works for Vertica.  Other DBs?  Investigate SqlAlchemy or modularize.

        Returns:
            bool
        """

        schema, table_name = self.virt_name.split('.')

        query = """
        SELECT view_definition FROM views
        WHERE table_name ilike '{}'
        AND table_schema ilike '{}'
        AND view_definition ilike '%{}%';
        """.format(table_name, schema, self.sql_name_uuid)

        try:
            result = single_query(self.dsn, query)
            if len(result) > 0:
                return True
            else:
                return False
        except Exception as e:
            raise Exception("DBTarget:is_latest_committed failed: {}".format(e))

    def url(self, remove_context=False):
        """
        The phys_name_url contains the servername, database, schema, and table name.
        It is the string that may be used in lieu of a db_target.

        Returns:
            (unicode): <database>.<schema>.<table>@<servername>

        """
        if remove_context:
            return "db://{}.{}@{}".format(self.database, self.phys_name.replace("{}_".format(self.context.get_local_name()), ''), self.servername)
        else:
            return "db://{}.{}@{}".format(self.database, self.phys_name, self.servername)

    @staticmethod
    def remove_context_from_url(url, context):
        """
        Given URL, remove the context.  Assumes that context follows the disdat_prefix.

        Args:
            url:
            context:

        Returns:
            (unicode): <database>.<schema>.<table>@<servername>

        """
        return url.replace("{}_{}".format(DBTarget.disdat_prefix, context), "{}".format(DBTarget.disdat_prefix))

    def drop_table(self):
        """
        Remove this db_target from the database.

        Only remove the phys table for now.

        TODO: We need the dir table to remove the logical table.

        Returns:
            None
        """

        return drop_table_vertica(self.dsn, self.phys_name)

    @property
    def pn(self):
        """
        Retrieve the physical table name.   The virtual table name will only be available
        after the task runs and only if the user then commits the bundle.

        if ENABLE_DISDAT_SCHEMAS:
            phys = {}_{}.{}_{}.format(schema(disdat prefix, context), name, uuid)
        else:
            phys = {}.{}_{}_{}_{}.format(user schema, disdat_prefix, context, name, uuid)


        Returns:
             name (str)
        """

        return self.phys_name

    @property
    def tn(self):
        """
        Return table name (without disdat_prefix and context)

        Returns:
            (unicode): table_name

        """
        return self.table_name

    @staticmethod
    def phys_to_virt(phys_name):
        """
        Convert physical to virtual name, i.e., strip uuid

        if ENABLE_DISDAT_SCHEMAS:
        phys = {}_{}.{}_{}.format(schema(disdat prefix, context), name, uuid)
        else:
             = {}.{}_{}_{}_{}.format(user schema, disdat_prefix, context, name, uuid)

        virt = {}.{}_{}_{}    schema, disdat_prefix, context

        Returns:
            (unicode): schema.name

        """
        schema, name = phys_name.split('.')

        name = '_'.join(name.split('_')[:-1])

        return u"{}.{}".format(schema, name)


    @staticmethod
    def schema_from_phys(phys_name):
        """
        Return schema from phys name

        if ENABLE_DISDAT_SCHEMAS:
        phys = {}_{}.{}_{}.format(schema(disdat prefix, context), name, uuid)
        else:
             = {}.{}_{}_{}_{}.format(user schema, disdat_prefix, context, name, uuid)

        virt = {}.{}    schema, name

        Returns:
            (unicode): schema.name

        """
        schema, name = phys_name.split('.')

        return unicode(schema)

    @staticmethod
    def schema_from_url(url):
        """

        Args:
            url:

        Returns:

        """
        return url.replace('db://', '').split('@')[0].split('.')[1]

    @staticmethod
    def table_from_url(url):
        """
        Returns table name

        Args:
            url:

        Returns:

        """
        return url.replace('db://', '').split('@')[0].split('.')[2]

    @staticmethod
    def servername_from_url(url):
        """
        Extract servername from URL

        Returns:
            (unicode): servername

        """
        servername = url.replace('db://', '').split('@')[-1]
        return unicode(servername)

    @staticmethod
    def database_from_url(url):
        """
        Extract database from URL

        Returns:
            (unicode): database

        """
        database = url.replace('db://', '').split('.')[0]
        return unicode(database)

    def exists(self):
        """
        Returns ``True`` if the :py:class:`Target` exists and ``False`` otherwise.
        """

        phys_exists = table_exists_vertica(self.dsn, unicode(self.pn.split('.')[0]), unicode(self.pn.split('.')[1]))

        return phys_exists

