from __future__ import with_statement
import os
import os.path
import re
import imp
import logging
from datetime import datetime, timedelta

from django.db import models, transaction, connection, DatabaseError
from django.core.management.color import no_style

from django.conf import settings

if settings.USE_MULTIDB:
    from multidb.replication import replquery
    from multidb.replication.mapper import TableDoesNotExist

from multidb.feature.postgres import (sql_for_partition,
                                      sql_to_get_indexes,
                                      sql_to_get_partitions,
                                      sql_to_get_sequences)
from multidb.feature.db_alter import ConditionNotMet
from multidb.feature import signals

"""Feature Management.

A feature is a piece of functionality linked to a database (or other
important config) change.

A feature script is named for the feature (following a naming
convention) and specifies what is to be done to the database (or other
config) in python terms. The feature system records that a particular
feature has been exerted on the database (or other config) and it will
not be done again.

You can cause any feature to be reapplied by removing that feature
record from the database's list of features.

As long as this feature app is included in Django's INSTALLED_APPS
setting then features are automatically executed whenever syncdb is called.

Features can be limited to particular apps (in which case the running
of the feature script is limited to a syncdb that includes that app)
or be generic (run when any app is syncdb'd).

A feature is named like this:

  feature_[app]_<featurename>

for example:

  feature_woome_ipctrl_add_geo_column

shows a feature limited to running only when the woome.ipctrl app is
updated. A generic feature name would look like this:

  feature__add_person_column

Note that app names have the "." replaced with "_"
"""

PARENT_MODULE_DIR = settings.FEATURE_DIR
FEATURE_NAME_PATTERN = re.compile(r'^(%s/)*feature_(?P<featurename>(.*)).py'
                                  % PARENT_MODULE_DIR, re.IGNORECASE)

PGBOUNCER_PORT = 54320
CONNECT = "dbname='%%s' user='%%s' host='%%s' port=%d" % PGBOUNCER_PORT

TABLE_PRIVS = set(['SELECT', 'INSERT', 'UPDATE', 'DELETE', 'RULE',
                   'REFERENCES', 'TRIGGER', 'ALL'])


class FeatureManager(models.Manager):
    def __init__(self):
        super(FeatureManager, self).__init__()
        self.logger = logging.getLogger("multidb.feature.FeatureManager")

    def __get_features(self, src_appname=''):
        """
        Returns a list of features available.
        optional src_appname to show only features available in an app.
        """

        appname = src_appname
        if src_appname.startswith("django_"):
            self.logger.debug("skipping django modules for now")
            return

        if not src_appname.startswith("woome_"):
            appname = "woome_" + appname

        entries = [os.path.splitext(entry)[0] for entry in
                   os.listdir(PARENT_MODULE_DIR) if entry.endswith('.py')]
        self.logger.debug("filesystem entries: %s", entries)

        mname_re = "feature_((%s*)_.+)" % appname
        feature_names = filter(lambda e: re.match(mname_re, e), entries)
        self.logger.debug("feature names: %s", feature_names)
        features = [re.match(mname_re, feature_mname).group(1)
                    for feature_mname in feature_names]

        return features

    @transaction.commit_manually
    def _apply_feature(self, feature, dry_run, mark_only):
        """Apply a single feature."""
        self.logger.debug("Applying %s ...", feature)
        try:
            if not mark_only:
                imp.load_source(
                    "feature_" + feature,
                    os.path.join(PARENT_MODULE_DIR,
                                 "feature_" + feature + ".py")
                    )
                self.logger.debug("Applied.")
        except ConditionNotMet, e:
            self.logger.info("Skipping %s: %s", feature, e)
            self.logger.debug("Failed due to:", exc_info=True)
        except Exception, e:
            self.logger.exception("Failure running %s", feature)
        else:
            try:
                installed_feature = Feature(name=feature)
                installed_feature.save()
            except Exception, e:
                self.logger.error("Failure saving %s: %s", feature, e)
                self.logger.debug("Failed due to:", exc_info=True)
            else:
                if not dry_run:
                    transaction.commit()
                if mark_only:
                    self.logger.info("Marked %s as applied.", feature)
                else:
                    self.logger.info("Applied %s", feature)
        finally:
            transaction.rollback()

    def apply_features(self, appname='', dry_run=False, mark_only=False):
        """
        Apply features that are missing from the database.
        'appname' specifies the app that they should be applied for
        if '', all pending features are applied.
        """

        self.logger.debug("called with appname: %s", appname)

        features = self.__get_features(appname)
        if not appname.startswith("woome_"):
            appname = "woome_" + appname

        installed_features = Feature.objects.filter(name__istartswith=appname)
        not_installed = [nf for nf in
                set(features).difference([f.name for f in installed_features])]
        not_installed.sort(_feature_compare_alg)

        for feature_to_run in not_installed:
            self._apply_feature(feature_to_run, dry_run, mark_only)

    def apply_feature(self, featurename, dry_run=False, mark_only=False):
        if not featurename:
            self.logger.error("a filename is required for this function")
            raise Exception("invalid feature filename")

        if featurename.find("/") > -1 or featurename.endswith('.py'):
            match = re.match(FEATURE_NAME_PATTERN, featurename)
            if not match:
                self.logger.error("invalid feature filename: %s", featurename)
                raise Exception("invalid feature filename")

            featurename = match.groupdict()['featurename']

        # check if already installed and run it otherwise.
        try:
            Feature.objects.get(name=featurename)
        except Feature.DoesNotExist:
            self._apply_feature(featurename, dry_run, mark_only)
        else:
            self.logger.warn("feature %s is already installed", featurename)

    def print_feature(self, featurename):
        if not featurename:
            self.logger.error("a feature name is required for this function")
            raise Exception("no feature to print")

        if featurename.find('/') > -1 or featurename.endswith('.py'):
            match = re.match(FEATURE_NAME_PATTERN, featurename)
            if not match:
                self.logger.error("invalid feature name.. %s", featurename)
                raise Exception("invalid feature name")
            featurename = match.groupdict()['featurename']

        featurefile = PARENT_MODULE_DIR + '/feature_' + featurename + '.py'

        try:
            fd = open(featurefile, 'r')
            print "".join(fd.readlines())
        except IOError:
            self.logger.error("feature does not exist ... %s", featurefile)
            raise Exception("no feature file found")

    def list_features(self, appname, installed=''):
        """
        Show all available features from [appname] and where installed
        status = [installed]
        """
        show_all = False
        if type(installed) == str:
            show_all = True

        features = self.__get_features(appname)
        if not features:
            print "No features in feature/ directory"
            return

        if show_all:
            features.sort(_feature_compare_alg)
            print "%s FEATURES AVAILABLE:\n\n%s\n" % \
                            (len(features), "\n".join(features))
        else:
            if not appname.startswith("woome_"):
                appname = "woome_" + appname
            installed_features = Feature.objects.filter(
                name__istartswith=appname).order_by('name')
            if installed:
                print "%s INSTALLED FEATURES:\n\n%s\n" % \
                    (installed_features.count(),
                     "\n".join([f.name for f in installed_features]))
            else:
                not_installed = [nf for nf in
                                 set(features).difference(
                                     [f.name for f in installed_features])]
                not_installed.sort(_feature_compare_alg)
                print "%s PENDING FEATURES:\n\n%s\n" % \
                        (len(not_installed), "\n".join(not_installed))


class TableMapper(object):
    def __new__(cls, *args, **kwargs):
        if settings.USE_MULTIDB:
            return ReplicatedTableMapper()
        else:
            return SimpleTableMapper()


class SimpleTableMapper(object):
    def __init__(self):
        super(SimpleTableMapper, self).__init__()
        self._db = settings.DATABASE_NAME
        self._connection = connection

    def dbs_for_table(self, table):
        return [self._db]

    def dbs_for_repset(self, queue):
        return [self._db]

    def connection_for_table(self, table):
        return self._connection

    def get_connection(self, db):
        return self._connection

    def get_queue(self, db):
        raise NotImplementedError

    def installed_tables(self):
        return self._connection.introspection.table_names()


class ReplicatedTableMapper(object):
    def __init__(self):
        super(ReplicatedTableMapper, self).__init__()
        self._replquery = replquery.ReplQuery()
        self._connections = {}

    def dbs_for_table(self, table):
        """Returns the database hosts for a given table, provider first"""
        if hasattr(table, 'base_table'):
            table = table.base_table()
        try:
            repset_record = connection.mapper.tables["public.%s" % table]
        except KeyError:
            raise TableDoesNotExist(table)

        repset = repset_record.keys()[0]
        return repset_record[repset]

    def dbs_for_repset(self, queue):
        def get_repset(db, queue):
            conn = self.get_connection(db)
            cursor = conn.cursor()
            sql = """select queue_name, consumer_name
                     from pgq.get_consumer_info() where
                     queue_name like '%s';""" % queue.upper()
            cursor.execute(sql)
            queues = cursor.fetchall()
            return queues

        dbs = replquery.ReplQuery()._get_databases()
        repset = filter(lambda x: x[1],
                        [(db, get_repset(db, queue)) for db in dbs])[0]
        hosts = [repset[0]]
        for queue_name, host in repset[1]:
            host_match = re.match("^(.*)_([a-zA-Z]+[0-9]{2,3})$", host)
            if host_match:
                hosts.append(host_match.group(2))

        return hosts

    def get_connection(self, db):
        conn = self._connections.get(db, None)
        if conn is None:
            conn = self._replquery._get_db_con(db)
            conn.set_isolation_level(0)
            self._connections[db] = conn
        return conn

    def queue_for_table(self, table):
        """Get the londiste queue for a table"""
        # This should be part of replquery
        if hasattr(table, 'base_table'):
            table = table.base_table()
        repsets = {}
        dbs = self._replquery._get_databases()
        for db in dbs:
            for queue, slave_host in self._replquery._get_replication_sets(db):
                slave_host_match = re.match("^(.*)_([a-zA-Z]+[0-9]{2,3})$",
                                            slave_host)
                if slave_host_match:
                    repset = slave_host_match.group(1)
                    repsets[repset] = queue

        try:
            repset_record = connection.mapper.tables["public.%s" % table]
        except KeyError:
            raise TableDoesNotExist(table)
        r = repset_record.keys()[0]
        return repsets[r]

    def installed_tables(self):
        return [t.replace('public.', '')
                for t in connection.mapper.tables.keys()]


class DbFeatureManager(models.Manager):
    def __init__(self, mapper):
        super(DbFeatureManager, self).__init__()
        self.logger = logging.getLogger("FeatureManager")
        self.mapper = mapper

    def alter_table(self, sql):
        """Alters a table associated with a given model"""
        table_name = self.model._meta.db_table
        sql = (("alter table %s " % table_name)
              + (sql % {'table_name': table_name}))
        self._run_sql(sql)
        signals.altered_table.send(sender=self.model)

    def apply_grants(self, table=None, queue=None):
        if not getattr(settings, 'FEATURE_APPLY_GRANTS', True):
            return
        grants = self._sql_for_grant(self.feature.provider_grants, table=table)
        for grant in grants:
            try:
                self._run_sql(grant, queue=queue, provider_only=True)
            except DatabaseError, err:
                self.logger.error('Grant failed with "%s" \nstatement: %s',
                                  err, grant)
        grants = self._sql_for_grant(self.feature.subscriber_grants,
                                     table=table)
        for grant in grants:
            try:
                self._run_sql(grant, queue=queue, subscriber_only=True)
            except DatabaseError, err:
                self.logger.error('Grant failed with "%s" \nstatement: %s',
                                  err, grant)

    def create_table(self, queue):
        """Create a table on a given replication set"""
        sql, _ = connection.creation.sql_create_model(self.model,
                                                      no_style(), set())
        refs = self._sql_for_constraints()

        self._run_sql("\n".join(sql + refs), queue=queue,
                      provider_only=True)
        self._run_sql("\n".join(sql), queue=queue, subscriber_only=True)
        self.apply_grants(queue=queue)

        signals.created_table.send(sender=self.model, queue=queue)

    def create_m2m_table(self, queue, fields=[]):
        """Create M2M table attached to a specific model."""
        sql = []
        if not fields:
            sql = connection.creation.sql_for_many_to_many(self.model,
                                                           no_style())
        else:
            model_fields = dict((f.name, f) for f in
                                 self.model._meta.local_many_to_many)
            for field in fields:
                if not(field in model_fields.keys()):
                    raise Exception(
                        "field %s is not a valid M2M field for this model %s" %
                        (field, self.model))
                output = connection.creation.sql_for_many_to_many_field(
                                self.model, model_fields[field], no_style())
                sql.extend(output)

        self._run_sql("\n".join(sql), queue=queue)
        self.apply_grants(queue=queue)

        signals.created_table.send(sender=self.model, queue=queue)

    def create_partition(self, n=1):
        """Create a partition for the current time.

        If n > 1 this will also create future partitions."""
        db_table = self.model._meta.db_table

        # Get the partitions that already exist
        partitions = self.partitions()

        # Start with the current partition
        timestamp = datetime.now()
        for m in range(n):
            with db_table.partition(None, timestamp=timestamp) as [start, end]:
                # Update timestamp for the next iteration
                timestamp = end + timedelta(seconds=1)
                if str(db_table) in partitions and not self.feature.force:
                    # This partition already exists in the database
                    print "Skipping %s..." % db_table
                    continue

                # Get the sql to create and set up the table
                create = sql_for_partition(self.model, start, end)
                constraints = self._sql_for_constraints()

                indexes = self._sql_for_indexes()

                try:
                    self._run_sql(create, table=db_table)
                except Exception, err:
                    self.logger.error(
                        "could not create partition %s with %s as %s",
                        db_table, err, create)
                else:
                    for stmt in constraints:
                        try:
                            self._run_sql(stmt, table=db_table,
                                          provider_only=True)
                        except Exception, err:
                            self.logger.error(
                                "failed adding constraint on %s with %s as %s",
                                db_table, err, stmt)

                    self.apply_grants()

                    for stmt in indexes:
                        try:
                            self._run_sql(stmt, table=db_table)
                        except Exception, err:
                            self.logger.error(
                                "failed making index for %s with %s as %s",
                                db_table, err, stmt)

                    signals.created_table.send(sender=self.model)

    def partitions(self):
        """Get the existing partitions for this model."""
        db_table = self.model._meta.db_table
        base_table = db_table.base_table()
        hosts = self.mapper.dbs_for_table(table=base_table)
        conn = self.mapper.get_connection(hosts[0])
        cursor = conn.cursor()
        cursor.execute(sql_to_get_partitions(base_table))
        child_tables = cursor.fetchall()
        # reverse map and filter
        parts = {}
        for (t,) in child_tables:
            try:
                start = db_table.parse_partition(t)
            except ValueError:
                # Table didn't match format string, so skip it.
                pass
            else:
                parts[t] = start
        return parts

    def _sql_for_indexes(self):
        """Get sql for indexes on a partition.

        Indexes are copied from the base table.

        """
        # This does not support having different indexes on different hosts
        indexes = []
        table = self.model._meta.db_table
        base_table = table.base_table()
        hosts = self.mapper.dbs_for_table(table=base_table)
        conn = self.mapper.get_connection(hosts[0])
        cursor = conn.cursor()
        cursor.execute(sql_to_get_indexes(base_table))
        result = cursor.fetchall()
        for line in result:
            # Replace the table in the index name
            ind = re.sub(base_table, str(table), line[0])
            indexes.append(ind + ';')
        return indexes

    def _sql_for_constraints(self, only_new=False):
        """Generate foreign key constraints for a partition."""
        _, references = connection.creation.sql_create_model(
            self.model, no_style(), set())
        table = self.model._meta.db_table
        existing = {}
        attr_index = {}
        keys = []
        if references and only_new:
            intr = connection.introspection
            hosts = self.mapper.dbs_for_table(table)
            conn = self.mapper.get_connection(hosts[0])
            cursor = conn.cursor()
            existing = intr.get_relations(cursor, str(table))
            # This mess resolves the attribute indices given by
            # get_relations into field names that we can actually use
            # e.g.  {1: (0, u'geonames_geoname')}
            #    => {'geoname_id': ('id', u'geonames_geoname')}
            to_describe = [table] + [t for _, t in existing.values()]
            for t in to_describe:
                attr_index[t] = [attr[0] for attr in
                                 intr.get_table_description(cursor, str(t))]
            # It would probably be better to just rewrite get_relations
            # and join this data in the database
            existing = dict((attr_index[table][i], (attr_index[t][j], t))
                            for i, (j, t) in existing.iteritems())
        template = "ALTER TABLE %s ADD FOREIGN KEY (%s) REFERENCES %s (%s)%s;"
        for model, rec in references.items():
            for _, field in rec:
                if only_new:
                    if existing.get(field.column) == (
                       model._meta.get_field(field.rel.field_name).column,
                       model._meta.db_table):
                        continue
                keys.append(template % (
                        self.model._meta.db_table,
                        field.column,
                        model._meta.db_table,
                        model._meta.get_field(field.rel.field_name).column,
                        connection.creation.sql_for_deferrable(field)
                      ))
        return keys

    def add_index(self, index, options=None, unique=False, concurrently=False,
            where=None):
        table = self.model._meta.db_table
        if unique:
            create = "CREATE UNIQUE INDEX "
        else:
            create = "CREATE INDEX "

        if concurrently:
            conc = "CONCURRENTLY "
        else:
            conc = ""

        if hasattr(index, '__iter__'):
            indexname = '%s_%s_idx' % (table, '_'.join(index))
            index = ', '.join(index)
        else:
            indexname = '%s_%s_idx' % (table, index)

        if options:
            idx = "%(name)s on %(table)s (%(index)s, %(options)s)" \
                    % {'name': indexname, 'table': table,
                       'index': index, 'options': options}
        else:
            idx = "%(name)s on %(table)s (%(index)s)" \
                    % {'name': indexname, 'table': table, 'index': index}

        if where:
            if not where.startswith('('):
                where = '(' + where + ')'
            idx += " WHERE %s" % where

        self._run_sql(create + conc + idx)

    def drop_index(self, index):
        table = self.model._meta.db_table

        sql = "DROP INDEX %(table)s_%(index)s_idx" % {'table': table,
                                                      'index': index}
        self._run_sql(sql)

    def _sql_for_grant(self, permissions, table=None):
        if table is None:
            table = self.model._meta.db_table
        hosts = self.mapper.dbs_for_table(table=table)
        conn = self.mapper.get_connection(hosts[0])
        cursor = conn.cursor()
        cursor.execute(sql_to_get_sequences, [str(table)])
        sequences = [rec[0] for rec in cursor.fetchall()]
        sql = []
        for user, perm in permissions.items():
            assert(perm.upper() in TABLE_PRIVS)
            sql.append('GRANT %s ON "%s" TO "%s";'
                        % (perm.upper(), table, user))
            # A bit of a hack, just grant all on sequences.
            for seq in sequences:
                sql.append('GRANT ALL ON %s TO "%s";' % (seq, user))

        return sql

    def grant(self, **perms):
        sql = '\n'.join(self._sql_for_grant(perms))
        self._run_sql(sql)

    def set_not_null(self, column):
        self.alter_table("ALTER COLUMN % SET NOT NULL;" % column)

    def drop_not_null(self, column):
        self.alter_table("ALTER COLUMN % DROP NOT NULL;" % column)

    def add_constraint(self, name, constraint):
        self.alter_table("ADD CONSTRAINT %s %s" % (name, constraint))

    def add_to_queue(self, table=None, queue=None, expect_sync=False):
        """Add a table to a londiste queue"""
        table = table or self.model._meta.db_table
        if queue is None:
            queue = self.mapper.queue_for_table(table)
        table = str(table)
        if not '.' in table:
            table = 'public.' + table
        sql = "SELECT londiste.provider_add_table('%s', '%s')" % (queue, table)
        self._run_sql(sql, queue=queue, provider_only=True)
        sql = "SELECT londiste.subscriber_add_table('%s', '%s')" % (queue,
                                                                    table)
        if expect_sync:
            sql = ("BEGIN; %s; SELECT londiste.subscriber_set_table_state"
                   "('%s', '%s', null, 'ok'); END;"
                   % (sql, queue, table))
        self._run_sql(sql, queue=queue, subscriber_only=True)

    def refresh_trigger(self, table=None, queue=None):
        """Refresh londiste triggers on a table

        This should be called when a column is added or removed from a table.
        """
        table = table or self.model._meta.db_table
        if queue is None:
            queue = self.mapper.queue_for_table(table)
        table = str(table)
        if not '.' in table:
            table = 'public.' + table
        sql = ("SELECT londiste.provider_refresh_trigger('%s', '%s')"
               % (queue, table))
        self._run_sql(sql, queue=queue, provider_only=True)

    def _run_sql(self, sql, params=None, table=None, queue=None,
                 subscriber_first=False,
                 provider_only=False,
                 subscriber_only=False):
        """Execute a sql statement on all relevant databases"""
        if table is None:
            table = str(self.model._meta.db_table)
        if queue:
            # Override the default host lookup
            hosts = self.mapper.dbs_for_repset(queue)
        else:
            hosts = self.mapper.dbs_for_table(self.model._meta.db_table)

        provider = hosts[0]
        subscribers = hosts[1:]

        # Perform the alteration on the hosts
        def apply_subscribers():
            for host in subscribers:
                try:
                    # Connect to the appropriate db host
                    conn = self.mapper.get_connection(host)
                    cursor = conn.cursor()
                    cursor.execute("SET TIME ZONE %s", [settings.TIME_ZONE])
                    # Avoid running twice in a loopback setup.
                    if host != provider:
                        cursor.execute(sql, params)
                        self.logger.debug(host + ": " + cursor.query)
                except Exception, e:
                    if self.feature.force:
                        self.logger.warning("Ignoring error %s", e)
                        pass
                    else:
                        raise

        def apply_provider():
            try:
                conn = self.mapper.get_connection(provider)
                cursor = conn.cursor()
                cursor.execute("SET TIME ZONE %s", [settings.TIME_ZONE])
                cursor.execute(sql, params)
                self.logger.debug(provider + ": " + cursor.query)
            except Exception, e:
                if self.feature.force:
                    self.logger.warning("Ignoring error %s", e)
                    pass
                else:
                    raise

        if(subscriber_first):
            if not provider_only:
                apply_subscribers()
            if not subscriber_only:
                apply_provider()
        else:
            if not subscriber_only:
                apply_provider()
            if not provider_only:
                apply_subscribers()


class Feature(models.Model):
    """
    This is designed to help manage DB version control.

    Particular changes to the database can be named as a 'feature' and
    insert and remove themselves from this table when they are
    installed/removed.

    Removal has to be a manual event right now... but maybe we'll find
    a way to record such changes in such a way that we can always roll
    them back, even if it involves some data loss.
    """

    name = models.CharField(max_length=200)

    objects = models.Manager()
    ctrl = FeatureManager()

    class Admin:
        pass

    def __unicode__(self):
        return self.name


class DatabaseFeature(object):
    """
    Subclass this and override appropriate methods to implement a feature.
    """

    models = []
    grants = {}

    def __init__(self):
        self.force = False
        self._mapper = TableMapper()
        for m in self.models:
            self.init_model(m)

    def init_model(self, m):
        m.add_to_class('features', DbFeatureManager(mapper=self._mapper))
        m.features.feature = self

    # Override these methods
    def alter_schema(self):
        """Override this with code to alter the database structure."""
        pass

    def update_data(self):
        """Override this with code that alters data in the database."""
        pass

    def setup(self):
        """Override to run things just before the feature is applied."""
        pass

    def teardown(self):
        """Override to run things just after the feature is applied."""
        pass

    # Implementation
    def apply(self, force=False):
        self.force = force
        self.setup()
        self.alter_schema()
        self.update_data()
        self.teardown()


# Signal handlers for londiste
def create_table_handler(sender, table=None, queue=None, **kwargs):
    sender.features.add_to_queue(table=table, queue=queue)
    newtables = replquery.ReplQuery().discover()
    from django.conf import settings
    table_module = __import__(settings.TABLE_MAPPER, {}, {}, [''])
    tables = table_module.mapper
    tables._base = newtables


def alter_table_handler(sender, table=None, queue=None, **kwargs):
    sender.features.refresh_trigger(table=table, queue=queue)


class WoomeFeature(DatabaseFeature):
    provider_grants = {
            'woome': 'ALL',
            'mdb': 'ALL',
            }

    subscriber_grants = {
            'woome': 'SELECT',
            'mdb': 'SELECT',
            }

    def setup(self):
        if settings.USE_MULTIDB:
            # Connect signals for londiste
            signals.created_table.connect(create_table_handler)
            signals.altered_table.connect(alter_table_handler)

    def teardown(self):
        if settings.USE_MULTIDB:
            signals.created_table.disconnect(create_table_handler)
            signals.altered_table.disconnect(alter_table_handler)

RE_CREATED = re.compile(r'_(\d+)$')


def _feature_compare_alg(f1, f2):
    """
    a comparable
    accepts two feature names f1 and f2 and determines which was created first
    returns
        -1: f1 older,
         1: f1 newer,
         0: No date info in both feature names,

    For backward compatibility - run all features without dates in their
    names first.

    """
    f1_date = re.search(RE_CREATED, f1)
    f2_date = re.search(RE_CREATED, f2)
    if f1_date:
        if f2_date:
            return (int(f1_date.groups()[0]) > int(f2_date.groups()[0])
                    and 1 or -1)
        return 1  # implies feature1 is newer than feature2
    elif f2_date:
        return -1
    else:
        # both are old style feature names
        return f1 > f2 and 1 or -1

# End
