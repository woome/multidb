"""
PostgreSQL multi-database backend for Django.

Requires psycopg 2: http://initd.org/projects/psycopg2

See the documentation for multidb.replication.mapper.DatabaseMapper for
most details.
"""

import logging
import re

from django.conf import settings
from django.db.backends.signals import connection_created
from django.db.backends import *
from django.db.backends.postgresql_psycopg2.base import (
        DatabaseClient, DatabaseCreation, DatabaseIntrospection
    )
from django.db.backends.postgresql_psycopg2.base import DatabaseFeatures as Psycopg2DatabaseFeatures
from django.db.backends.postgresql_psycopg2.base import DatabaseOperations as Psycopg2DatabaseOperations
from multidb.replication.mapper import DatabaseMapper
from django.utils.safestring import SafeUnicode, SafeString
from django.utils.functional import curry as partial
try:
    import psycopg2 as Database
    import psycopg2.extensions
except ImportError, e:
    from django.core.exceptions import ImproperlyConfigured
    raise ImproperlyConfigured("Error loading psycopg2 module: %s" % e)

DatabaseError = Database.DatabaseError
IntegrityError = Database.IntegrityError

psycopg2.extensions.register_type(psycopg2.extensions.UNICODE)
psycopg2.extensions.register_adapter(SafeString, psycopg2.extensions.QuotedString)
psycopg2.extensions.register_adapter(SafeUnicode, psycopg2.extensions.QuotedString)

logger = logging.getLogger("multidb.backend")


class DatabaseFeatures(Psycopg2DatabaseFeatures):
    uses_custom_query_class = True
    uses_savepoints = False


class DatabaseOperations(Psycopg2DatabaseOperations):
    def query_class(self, base_class):
        class MultiDBQuery(base_class):
            """A custom Query class for query direction with multidb"""
            def execute_sql(self, *args, **kwargs):
                self.connection._query = self
                try:
                    return super(MultiDBQuery, self).execute_sql(*args, **kwargs)
                finally:
                    self.connection._query = None
                    # _tables was set during the query, we do need this here
                    self.connection._tables = None
        return MultiDBQuery

    def lookup_cast(self, lookup_type):
        lookup = '%s'

        # Cast text lookups to text to allow things like filter(x__contains=4)
        if lookup_type in ('iexact', 'contains', 'icontains', 'startswith',
                           'istartswith', 'endswith', 'iendswith'):
            lookup = "%s::text"

        # Use LOWER(x) for case-insensitive lookups; it's faster.
        if lookup_type in ('iexact', 'icontains', 'istartswith', 'iendswith'):
            lookup = 'LOWER(%s)' % lookup

        return lookup


def _get_tables(query):
    from_tables = set()
    for alias in query.tables:
        try:
            if query.alias_refcount[alias] and alias in query.alias_map:
                name = query.alias_map[alias][0]
                from_tables.add(name)
        except (AttributeError, KeyError):
            continue
    for name in query.extra_tables:
        from_tables.add(name)
    return list(from_tables) or [query.model._meta.db_table]


# Classes for defining the mapping interface
class DatabaseSettings(dict):
    """Dict that prints database settings in a consistent manner"""
    def __str__(self):
        return "%(database)s %(user)s %(host)s %(port)s" % self


class DatabaseWrapper(BaseDatabaseWrapper):
    operators = {
        'exact': '= %s',
        'iexact': "= LOWER(%s)",
        'contains': 'LIKE %s',
        'icontains': 'LIKE LOWER(%s)',
        'regex': '~ %s',
        'iregex': '~* %s',
        'gt': '> %s',
        'gte': '>= %s',
        'lt': '< %s',
        'lte': '<= %s',
        'startswith': 'LIKE %s',
        'endswith': 'LIKE %s',
        'istartswith': 'LIKE LOWER(%s)',
        'iendswith': 'LIKE LOWER(%s)',
    }

    def __init__(self, *args, **kwargs):
        super(DatabaseWrapper, self).__init__(*args, **kwargs)
        self.features = DatabaseFeatures()
        autocommit = self.settings_dict['DATABASE_OPTIONS'].get('autocommit', False)
        self.features.uses_autocommit = autocommit
        self.isolation_level = int(not autocommit)
        self.features.can_return_id_from_insert = autocommit
        self.ops = DatabaseOperations()
        self.client = DatabaseClient(self)
        self.creation = DatabaseCreation(self)
        self.introspection = DatabaseIntrospection(self)
        self.validation = BaseDatabaseValidation()
        self.mapper = DatabaseMapper(self)

        self.connections = {}

        self._tables = None
        self._query = None
        self._needs_master = False

    def connect(self, params):
        """Get a connection to a database as specified by params"""
        if str(params) in self.connections:
            return self.connections[str(params)]
        else:
            try:
                connection = Database.connect(**params)
                self.connections[str(params)] = connection
                connection.set_client_encoding('UTF8')
                connection.set_isolation_level(self.isolation_level)
                connection_created.send(sender=self.__class__)
                cursor = connection.cursor()
                cursor.execute("SET TIME ZONE %s", [settings.TIME_ZONE])
                cursor.close()
            except:
                logger.exception("Error connecting to %s" % params['database'])
                raise
            return connection

    def _get_default_params(self):
        """Get a dict of default values suitable for use with Database.connect"""
        settings_dict = self.settings_dict
        if settings_dict['DATABASE_NAME'] == '':
            from django.core.exceptions import ImproperlyConfigured
            raise ImproperlyConfigured("You need to specify DATABASE_NAME in your Django settings file.")
        conn_params = DatabaseSettings(database=settings_dict['DATABASE_NAME'])
        conn_params.update(settings_dict['DATABASE_OPTIONS'])
        if 'autocommit' in conn_params:
            del conn_params['autocommit']
        if settings_dict['DATABASE_USER']:
            conn_params['user'] = settings_dict['DATABASE_USER']
        if settings_dict['DATABASE_PASSWORD']:
            conn_params['password'] = settings_dict['DATABASE_PASSWORD']
        if settings_dict['DATABASE_HOST']:
            conn_params['host'] = settings_dict['DATABASE_HOST']
        if settings_dict['DATABASE_PORT']:
            conn_params['port'] = settings_dict['DATABASE_PORT']
        return conn_params

    def _get_params_for_query(self):
        """Get the db params for the current query"""
        from django.db.models.sql import (InsertQuery, UpdateQuery, DeleteQuery)
        tables = self._tables
        needs_master = self._needs_master \
                    or isinstance(self._query, InsertQuery) \
                    or isinstance(self._query, UpdateQuery) \
                    or isinstance(self._query, DeleteQuery)

        return self.mapper.get_connection_description(
            tables,
            needs_master)

    def _cursor(self, *args, **kwargs):
        """Return a cursor.

        We return a ProxyCursor so that we can still perform database
        direction for future queries against that cursor. If we are forced to
        return a cursor for the default database (because the query is not
        from the ORM and therefore we don't have enough information yet) then
        the query can still be redirected when execute is called.

        """
        params = self._get_default_params()
        if self._query is not None:
            self._tables = _get_tables(self._query)
        if self._tables is not None:
            params.update(self._get_params_for_query())

        connection = self.connect(params)
        cursor = connection.cursor(*args, **kwargs)
        cursor.tzinfo_factory = None

        return ProxyCursor(cursor, params, self, *args, **kwargs)

    def connection_apply(self, method, *args, **kwargs):
        """Map a method call to all the current connections"""
        for connection in self.connections.values():
            if callable(method):
                method(connection, *args, **kwargs)
            else:
                try:
                    getattr(connection, method)(*args, **kwargs)
                except Database.OperationalError, e:
                    raise Database.OperationalError("%s - %s" % (str(e), connection))

    def _enter_transaction_management(self, managed):
        """
        Switch the isolation level when needing transaction support, so that
        the same transaction is visible across all the queries.
        """
        if self.features.uses_autocommit and managed and not self.isolation_level:
            self._set_isolation_level(1)

    def _leave_transaction_management(self, managed):
        """
        If the normal operating mode is "autocommit", switch back to that when
        leaving transaction management.
        """
        if self.features.uses_autocommit and not managed and self.isolation_level:
            self._set_isolation_level(0)

    def _set_isolation_level(self, level):
        """
        Do all the related feature configurations for changing isolation
        levels. This doesn't touch the uses_autocommit feature, since that
        controls the movement *between* isolation levels.
        """
        assert level in (0, 1)
        try:
            for connection in self.connections.values():
                connection.set_isolation_level(level)
        finally:
            self.isolation_level = level

    # Transactions currently are applied across the entire set of connections
    _commit = partial(connection_apply, method='commit')
    _rollback = partial(connection_apply, method='rollback')

    def close(self):
        self.connection_apply('close')
        self.connections.clear()


class ProxyCursor(object):
    """Proxies an existing DBAPI2 cursor object.

    This allows us to handle query direction for simple non-orm queries. This
    is intended to handle the few queries that sneak in through django contrib
    apps. It probably won't work with a large application that doesn't use the
    ORM for almost all of its queries.

    """

    def __init__(self, cursor, params, db_wrapper, *args, **kwargs):
        self.__dict__["db"] = db_wrapper
        self.__dict__["params"] = params
        self.__dict__["_cursor"] = cursor
        self.__dict__["_args"] = args
        self.__dict__["_kwargs"] = kwargs

    def execute(self, operation, *parameters):
        """Check if this is the correct cursor and then execute"""
        if self.db._tables is None or (not self.db._needs_master and self.db._query is None):
            # Make a rough guess of where this should go
            pattern = """\s*(INSERT INTO|UPDATE|DELETE FROM|SELECT .* FROM) "?(.*?)"?(\s|;|$)"""
            m = re.match(pattern, operation, re.DOTALL | re.IGNORECASE)
            if not m:
                # Unable to determine anything, use default connection
                logger.warning("Unhandled non-orm query: %s" % operation)
            else:
                tables = self.db._tables or [m.group(2)]
                if re.match("SELECT.*", m.group(1)):
                    needs_master = self.db._needs_master
                else:
                    needs_master = True

                desc = self.db.mapper.get_connection_description(tables, needs_master)
                params = self.db._get_default_params()
                params.update(desc)
                if params != self.params:
                    connection = self.db.connect(params)
                    cursor = connection.cursor(*self._args, **self._kwargs)
                    cursor.tzinfo_factory = None
                    self.__dict__["_cursor"] = cursor
                    self.params.update(params)

        if settings.DEBUG_SQL:
            operation += ' -- %s' % self.params['database']

        try:
            return self._cursor.execute(operation, *parameters)
        except DatabaseError, err:
            try:
                raise
            finally:
                try:
                    # Try to always log an error with as much information as
                    # possible while avoiding interference from any secondary
                    # errors
                    query = operation
                    pgcode = ''
                    try:
                        query = self._cursor.mogrify(operation.encode('UTF8'), *parameters)
                    except:
                        pass
                    try:
                        pgcode = err.pgcode
                    except:
                        pass
                    logger.error(
                        "Query error on database:%s\n pg code:%s\n query: \"%s\"",
                        self.params['database'], pgcode, query,
                        exc_info=True
                        )
                except:
                    pass

    def __getattr__(self, attr):
        return getattr(self._cursor, attr)

    def __setattr__(self, attr, val):
        return setattr(self._cursor, attr, val)
