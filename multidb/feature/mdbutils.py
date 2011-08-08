import re
from multidb import replication


# Londiste hook methods
def get_queue(table_name):
    repsets = {}
    rq = replication.replquery.ReplQuery()
    dbs = rq._get_databases()
    for db in dbs:
        for queue, slave_host in rq._get_replication_sets(db):
            slave_host_match = re.match("^(.*)_([a-zA-Z]+[0-9]{3})$",
                                        slave_host)
            if slave_host_match:
                repset = slave_host_match.group(1)
                repsets[repset] = queue

    repset_record = replication._get_tables()["public.%s" % table_name]
    r = repset_record.keys()[0]
    return repsets[r]


def londiste_alter_table_master(conn, manager=None, **kwargs):
    c = conn.cursor()
    table = manager.model._meta.db_table
    queue = get_queue(manager)
    queue = queue.upper()
    if not queue.endswith('_RS'):
        queue = queue + '_RS'
    sql = "select londiste.provider_refresh_trigger(%s, %s)"
    c.execute(sql, [queue, table])


def londiste_create_table_provider(conn, table=None, manager=None,
                                   queue=None, **kwargs):
    c = conn.cursor()
    if not table:
        if not manager:
            raise Exception("Must define either table or manager")
        table = manager.model._meta.db_table
    if not table.startswith('public.'):
        table = 'public.' + table
    if not queue:
        queue = get_queue(manager)
    queue = queue.upper()
    if not queue.endswith('_RS'):
        queue = queue + '_RS'
    sql = "select londiste.provider_add_table(%s, %s)"
    c.execute(sql, [queue, table])


def londiste_create_table_subscriber(conn, table=None, manager=None,
                                     queue=None, **kwargs):
    c = conn.cursor()
    if not table:
        if not manager:
            raise Exception("Must define either table or manager")
        table = manager.model._meta.db_table
    if not table.startswith('public.'):
        table = 'public.' + table
    if not queue:
        queue = get_queue(manager)
    queue = queue.upper()
    if not queue.endswith('_RS'):
        queue = queue + '_RS'
    sql = "select londiste.subscriber_add_table(%s, %s)"
    c.execute(sql, [queue, table])
