from __future__ import with_statement

import re
import logging

import psycopg2
import psycopg2.extensions
psycopg2.extensions.register_type(psycopg2.extensions.UNICODE)

from django.conf import settings


class MutipleReplicationSetError(Exception):
    """Raised when an environment is found with tables attached to multiple replication sets"""
    def __init__(self, table_name, repsets):
        self.message = "MutipleReplicationSetError: %s -> %s" % (table_name, str(repsets))

    def __str__(self):
        return self.message


class ReplQuery(object):

    def _get_db_con(self, dbname):
        """Facilitate connection to pgbouncer"""
        CONNECT = "dbname='%%s' user='%s' host='%s' port=%d" % (
            settings.DATABASE_USER,
            settings.DATABASE_HOST,
            settings.DATABASE_PORT
            )
        conn = psycopg2.connect(CONNECT % dbname)
        # This is a psycopg2 thing
        conn.set_isolation_level(0)
        conn.set_client_encoding('UNICODE')

        return conn

    def _get_databases(self):
        """Talk to pgbouncer to get a list of database servers

        Pgbouncer uses null terminated strings so we have to convert the
        resulting data."""

        logger = logging.getLogger("replication.replquery._get_databases")
        conn = None
        try:
            conn = self._get_db_con("pgbouncer")
            c = conn.cursor()
            c.execute("show databases")
        except:
            raise
        else:
            l = c.fetchall()
            db_names = filter(lambda name: re.match("^[a-zA-Z0-9]+[0-9]{2,3}$",name),
                              [d[0].rstrip('\0') for d in l])
            logger.debug("databases -> %s" % " ".join(db_names))
            return db_names
        finally:
            if conn:
                conn.close()

    def _get_replication_sets(self, db_name):
        """Get the replication sets consuming tables from  a particular db

        queue_name is replication set short name
        consumer_name is the replication set long name + the hosting db."""

        logger = logging.getLogger("replication.replquery._get_replication_sets")
        conn = None
        try:
            conn = self._get_db_con(db_name)
            c = conn.cursor()
            c.execute("select queue_name, consumer_name"
                      " from pgq.get_consumer_info()"
                      " where queue_name like '%_RS';")
        except Exception, e:
            logger.error("error %s while getting repsets for %s", e, db_name)
        else:
            queues = c.fetchall()
            logger.debug("queues on %s -> %s", db_name, queues)
            return queues
        finally:
            if conn:
                conn.close()

    def _get_tables(self, db_name, repset_stub):

        repset = "%s_RS" % repset_stub.upper()
        logger = logging.getLogger("replication.replquery._get_tables")
        logger.debug("asking for tables for: %s", repset)

        conn = None
        try:
            conn = self._get_db_con(db_name)
            c = conn.cursor()
            c.execute("select table_name from londiste.provider_get_table_list(%s)",
                      [repset])
            tables = [t for (t,) in c.fetchall()]
            logger.debug("tables for repset %s: %s", repset, tables)
            return tables
        finally:
            if conn:
                conn.close()

    def discover(self):
        """Find out about the db environment

        Returns a dictionary of tables with the values being the hosts
        that carry the table.

        The master for the table is placed first in the list.
        """

        tables = {}
        dbs = self._get_databases()

        # Make a dictionary of each database master and the repsets it masters
        # This works because only a master hosts the queue for a replica
        repsets = dict(filter(lambda x: x[1],
                              [(db, self._get_replication_sets(db)) for db in dbs]))
        for master_db, slave_instances in repsets.iteritems():
            for repset_name, slave_host in slave_instances:
                slave_host_match = re.match("^(.*)_([a-zA-Z0-9]+[0-9]{2,3})$", slave_host)
                if slave_host_match:
                    repset = slave_host_match.group(1)
                    repset_match = re.match("^(.*)_replica$", repset)
                    repset_stub = repset_match.group(1)
                    slave_hostname = slave_host_match.group(2)
                    table_list = self._get_tables(master_db, repset_stub)
                    for table in table_list:
                        this_tables_repsets = tables.get(table, {}).keys()
                        if this_tables_repsets == [] \
                                or this_tables_repsets == [repset]:
                            table_details = tables.get(table, { repset: [master_db] })
                            table_details = { repset: table_details[repset] + [slave_hostname] }
                            tables[table] = table_details
                        else:
                            raise MutipleReplicationSetError(table, this_tables_repsets + [repset])

        return tables


# End
