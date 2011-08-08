import random
import logging
from datetime import datetime, timedelta
from django.utils.datastructures import MergeDict
from django.conf import settings

logger = logging.getLogger("multidb.replication.mapper")


class ReplicationQueryError(Exception):
    pass


class TableDoesNotExist(ReplicationQueryError):
    def __init__(self, table_name):
        self.message = "TableDoesNotExist: %s" % table_name

    def __str__(self):
        return self.message


class UnsatisfiableQuery(ReplicationQueryError):
    """The query is unsatisfiable in the current database environment."""
    pass


class DatabaseMapper(object):
    """Provides a system to map ORM database queries in a multidb environment.

    The expected environment is single-master (per replication set) with
    asynchronous replication. A replication set is a set of tables that are
    mastered on the same database and are replicated to the same set of
    databases. That is, each table can only be written to on a single database
    but can be read from multiple databases that may have slightly out of date
    data.  It is not required that all django models are mastered on the same
    database, but you need to ensure that there is enough overlap to satisfy
    all your application's queries.

    The DatabaseMapper tracks which tables have been written to recently, and
    for a period of time afterwards will direct all reads against those tables
    to the appropriate master database. The timeout is specified in
    milliseconds by MULTIDB_AFFINITY_TIMEOUT in the Django settings file. To
    retain this between requests use the MultiDBMiddleware middleware and
    Django's SessionMiddleware.

    An application's replication environment is given by a module specified in
    settings.TABLE_MAPPER. This module should have a dicts (or objects
    implementing the dict protocol) named 'tables'. The 'tables' dict maps
    schema-qualified table names (for now you must use 'public') to
    replication sets. Replication sets are specified as dicts mapping the
    replication set name to a list of database names. The first entry in each
    list must be the master database for that replication set.

    Example:

    mapper = {
        'public.auth_user': {'auth_repset': ['db1', 'db2', 'db3']},
        'public.app_person': {'auth_repset': ['db1', 'db2', 'db3']},
        'public.ledger_entry': {'books_repset': ['db4', 'db2', 'db3']},
    }

    The database driver expects to be able to connect to all of these using
    the standard database settings in the Django configuration, varying only
    the database name. A connection pooler such as pgbouncer is recommended
    for this.

    """
    def __init__(self, connection):
        if not hasattr(settings, 'MULTIDB_AFFINITY_TIMEOUT'):
            from django.core.exceptions import ImproperlyConfigured
            raise ImproperlyConfigured(
                "Requires MULTIDB_AFFINITY_TIMEOUT in Django settings.")
        self.connection = connection
        self._table_module = __import__(settings.TABLE_MAPPER, {}, {}, [''])
        self.tables = self._table_module.mapper
        if hasattr(settings, 'TABLE_MAPPER_OVERLAY'):
            overlay_mod = __import__(settings.TABLE_MAPPER_OVERLAY,
                                     {}, {}, [''])
            self.tables = MergeDict(overlay_mod.mapper, self.tables)
        self._manual_affinity = {}
        self._affinity = {}
        self._affinity_modified = False

    def _refresh_tables(self):
        """Attempt to reload the module with the replication environment"""
        reload(self._table_module)
        self.tables = self._table_module.mapper

    def _has_affinity(self, table):
        """Is master access required for this table?"""
        if self._manual_affinity.get(table):
            logger.debug("Affinity explicitly set for %s" % table)
            return True

        timeout = self._affinity.get(table)
        if timeout:
            if datetime.now() < timeout:
                logger.debug("Affinity is set for %s" % table)
                return True
            else:
                logger.debug("Releasing affinity for %s" % table)
                del self._affinity[table]
                return False
        else:
            return False

    def _set_affinity(self, table):
        """Turn affinity on for a table"""
        self._affinity[str(table)] = datetime.now()\
                + timedelta(seconds=settings.MULTIDB_AFFINITY_TIMEOUT)
        if hasattr(table, 'base_table'):
            # On a partitioned table we need to set affinity for the base table
            # because that's where selects will go
            self._affinity[table.base_table()] = datetime.now()\
                    + timedelta(seconds=settings.MULTIDB_AFFINITY_TIMEOUT)
            logger.debug("Setting affinity for %s" % table.base_table())
        self._affinity_modified = True
        logger.debug("Setting affinity for %s" % table)

    def _clean_affinity(self):
        """Remove expired entries from the affinity table"""
        now = datetime.now()
        for table, timeout in self._affinity.copy().iteritems():
            if now >= timeout:
                del self._affinity[table]

    def _hosts_for_table(self, table):
        """Get the hosts a table can be found on"""
        try:
            repset_record = self.tables["public.%s" % table]
        except KeyError:
            logger.debug('Table cache miss for %s, refreshing cache.' % table)
            self._refresh_tables()
            try:
                repset_record = self.tables["public.%s" % table]
            except KeyError:
                raise TableDoesNotExist(table)

        repset = repset_record.keys()[0]
        replica_hosts = repset_record[repset]

        exclusion_settings = getattr(settings, 'EXCLUDED_DATABASES', {})
        exclusions = set(exclusion_settings.get(repset, []))
        if replica_hosts[0] in exclusions:
            raise ReplicationQueryError(
                'Cannot exclude provider %s for repset %s'
                % (replica_hosts[0], repset))
        replica_hosts = [h for h in replica_hosts if h not in exclusions]

        return replica_hosts

    def get_connection_description(self, tables, needs_master):
        """Get connection parameters for a query.

        This is given a list of tables involved in a query and whether the
        query needs the master database (set by the multidb backend for write
        queries) and returns a dict specifying connection parameters for the
        database to use. The first table in the list is always the primary
        table in the query (as Django sees each query as associated with a
        model).

        """
        if needs_master:
            # Set affinity for a write query
            self._set_affinity(tables[0])

        # resolve any lazy table names
        tables = [str(t).strip('"') for t in tables]
        # Now get the list of acceptable dbs for this query
        repsets = [(t, self._hosts_for_table(t)) for t in tables]

        # Narrow the list by taking the intersection of repsets
        if needs_master:
            # We must use the provider
            host = repsets[0][1][0]
            # Ensure that the other tables are present
            for table, dbs in repsets[1:]:
                # Check if the table requires a master
                if self._has_affinity(table):
                    if host != dbs[0]:
                        logger.info("Unable to satisfy affinity for %s - %s"\
                                % (t, repsets))
                if host not in dbs:
                    logger.error("No set of databases for query. %s" % repsets)
                    raise UnsatisfiableQuery
            hosts = [host]
        else:
            hosts = set(repsets[0][1])
            aff_tables = []
            for table, dbs in repsets:
                # Check if the table requires a master
                if self._has_affinity(table):
                    aff_tables.append(table)
                    hosts.intersection_update(dbs[0:1])
                else:
                    hosts.intersection_update(dbs)
            if aff_tables and not hosts:
                # We ended up with no possible hosts to use
                # Try again without affinity
                logger.info("Unable to satisfy affinity for %s - %s"\
                        % (aff_tables, repsets))
                hosts = set(repsets[0][1])
                for _, dbs in repsets:
                    hosts.intersection_update(dbs)
            if not hosts:
                logger.error("No set of databases for query. %s" % repsets)
                raise UnsatisfiableQuery

        hosts = list(hosts)

        # If we have a choice of hosts then choose one.
        if len(hosts) > 1:
            dbname = _read_picker(hosts)
        else:
            dbname = hosts[0]

        if logger.isEnabledFor(logging.DEBUG):
            logger.debug("serving connection for %s from %s", tables, dbname)

        return {'database': dbname}


def _read_picker(hosts_list):
    """Picks a read server from the list.

    The current method checks the list of globally preferred databases, using
    the first one possible. If there is no preferred database that satisfies
    the query, or if none are specified, it calls _weighted_picker.
    """
    preferred_hosts = getattr(settings, 'MULTIDB_PREFER_DATABASE', [])
    if isinstance(preferred_hosts, basestring):
        preferred_hosts = [preferred_hosts]

    for host in preferred_hosts:
        if host in hosts_list:
            return host

    return _weighted_picker(hosts_list)


def _weighted_picker(hosts):
    """Weighted random picker.

    Define settings.MULTIDB_WEIGHTS as a dict of database names to integers.
    The default weight of each server is 10.
    """
    base_weight = getattr(settings, 'MULTIDB_WEIGHTS', {})
    if base_weight == {}:
        return random.choice(hosts)

    weight = dict()
    for h in hosts:
        weight[h] = base_weight.get(h, 10)
    reverse_weight = []
    for h, w in weight.iteritems():
        reverse_weight.extend([h] * w)

    if reverse_weight == []:
        return random.choice(hosts)

    return random.choice(reverse_weight)

# End
