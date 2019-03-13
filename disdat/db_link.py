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
from __future__ import print_function


class DBLink(object):
    """

    A database table target.  Often we need a task to create a table in a database.
    This object simply records of the db, the schema, the table name,
    that were used to make a table.

    Versioning and naming tables, though, is challenging.

    The pattern Disdat currently supports is to:

    1.) Upstream tasks create data you want in or as a table and output parquet file.
    This is your "versioned" database table.

    2.) A subsequent downstream task uses upstream task as input.   The sole purpose of this task is to create / insert
    data into a database table.   The users determines the naming scheme.

    A.) if you want versions, you have the upstream bundle.
    B.) if you want to "commit" run the downstream task, else, for testing run just the upstream
    C.) The lastest is whatever the user named it.  No more versions in the DB.

    The DBTarget, then is a marker output bundle.  If it exists, we assume the user has already made the table --
    they synchronously waited to get confirmation of the transaction.  And since each bundle knows its inputs,
    we have the data used to create the table.

    We assume that the user has configured their system to reach their DB
    through the use of a Data Source Name or DSN, and that a file, like
    ~/.odbc.ini, defines the attributes of the DSN, including the database,
    login, password, servername, port, and driver.

    Optional features:
    0.) The user can ask for a "physical table name" from the DBTarget.  This will prepend "DISDAT_" and append "_UUID".
    This is the bundle UUID -- the user must ensure their names do not collide with multiple tables in a bundle.
    1.) User over-rides rm():  Called when the bundle is removed from a context.  Argument tells you whether bundle
    was committed.
    1.) User over-rides commit():  Disdat calls this when the bundle is committed.  For example the user may write
    code to create a view from the latest table.

    """

    disdat_prefix = "DISDAT"

    def __init__(self, pipe_task, dsn, table_name, schema_name, servername='unknown',
                 database='unknown', uuid=None, port=-1):
        """
        User creates a db_target within a Disdat Pipe when they want to work on a database table.

        The virt_name is the virtual table name (schema.'DISDAT_<table_name>')
        The phys_name is the physical table name (schema.'DISDAT_<table_name>_<uuid>')

        Note that at the moment we are using the bundle uuid to append to the end of the virt_name.

        Args:
            pipe_task (`disdat.pipe.PipeTask`): The pipe that is requesting to create a table.  May be None
            dsn (unicode): The DSN of the database.  Assumes access via odbc.  If None, will create a
                DBTarget but will not be 'connected.'
            table_name (unicode): The name of the table the user wants to create.
            schema_name (unicode): Pass in schema name.  Currently do not auto-generate schema
            servername (unicode): If no dsn, use servername.  Default is 'unknown'
            database (unicode): If no dsn, use database.  Default is 'unknown'
            uuid (unicode): Optional, only used if pipe argument is None.
        """

        self.table_name = table_name
        self.servername = servername
        self.database = database
        self.port     = port
        self.user_name = 'unknown'
        self.dsn = dsn
        self.pipe_task = pipe_task

        self.phys_name_url = None
        self.virt_name = None
        self.phys_name = None
        self.schema = schema_name
        self.committed = False

        self.uuid = uuid
        if self.uuid is None:
            pce = self.pipe_task.pfs.get_path_cache(self.pipe_task)
            assert(pce is not None)
            self.uuid = pce.uuid
        self.sql_name_uuid = self.uuid.replace('-', '')

        self.init()

        # If this is a user, they must add the pipe argument, and we
        # keep track of the different db_targets they create.
        if self.pipe_task is not None:
            self.pipe_task.add_db_target(self)

    def init(self):
        """
        Create schema name, ensure it exists.
        Create physical table name.

        Sometimes we create a DBTarget outside of running a particular pipe or task.  This happens on
        commit, for example.  In that case, we assume a bundle uuid as an argument.

        Returns:
            None
        """

        """
        Here the name is prefixed by <disdat_prefix>
        We don't prefix with the context because bundles are independent of context
        """
        assert self.schema
        self.phys_name = "{}.{}_{}_{}".format(self.schema,
                                              self.disdat_prefix,
                                              self.table_name,
                                              self.sql_name_uuid)

        """ The fully qualified physical name is the text representation of the link stored in the bundle """
        self.phys_name_url = self.url()

        self.virt_name = DBLink.phys_to_virt(self.phys_name)

    def commit(self):
        """

        Commit a database table link by creating (maybe replacing) a view for the table, using the physical name.

        Returns:
            None

        """
        pass

    def rm(self, commit_tag=False):
        """
        User-supplied code for removing a database table link.

        Args:
            commit_tag (bool): Indicate whether the bundle has been committed.

        Returns:
            bool: Whether the remove was successful.

        """
        return True

    def url(self):
        """
        The phys_name_url contains the servername, database, schema, and table name.
        It is the string that may be used in lieu of a db_target.

        Returns:
            (unicode): <database>.<schema>.<table>@<servername>

        """
        return "db://{}.{}@{}".format(self.database, self.phys_name, self.servername)

    @property
    def pn(self):
        """
        Retrieve the physical table name.   Use this name if you need
        to generate a unique table each time you run the same task.

        phys = {}.{}_{}_{}.format(user schema, disdat_prefix, name, uuid)


        Returns:
             name (str)
        """

        return self.phys_name

    @property
    def vn(self):
        """
        Return virtual table name (schema.disdat_prefix_name)

        Use this name if you want the name to reflect that Disdat writes this table.

        Returns:
            (unicode): table_name

        """
        return self.virt_name

    @property
    def tn(self):
        """
        Return table name (without disdat_prefix or UUID of the pn).

        Returns:
            (unicode): table_name

        """
        return self.table_name

    @staticmethod
    def phys_to_virt(phys_name):
        """
        Convert physical to virtual name, i.e., strip uuid

        phys = {}.{}_{}_{}.format(user schema, disdat_prefix, name, uuid)

        virt = {}.{}_{}    schema, disdat_prefix, name

        Returns:
            (str): schema.name

        """
        schema, name = phys_name.split('.')

        name = '_'.join(name.split('_')[:-1])

        return "{}.{}".format(schema, name)

    @staticmethod
    def schema_from_phys(phys_name):
        """
        Return schema from phys name

        phys = {}.{}_{}_{}.format(user schema, disdat_prefix, name, uuid)

        Returns:
            (str): schema.name

        """
        schema, name = phys_name.split('.')

        return str(schema)

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
        return str(servername)

    @staticmethod
    def database_from_url(url):
        """
        Extract database from URL

        Returns:
            (unicode): database

        """
        database = url.replace('db://', '').split('.')[0]
        return str(database)
