"""Postgres sql helpers for features."""


def sql_for_partition(model, start, end):
    sql = """CREATE TABLE "%(table_name)s" (
    CHECK ( "%(column)s" >= '%(start)s'::timestamptz
        AND "%(column)s" < '%(end)s'::timestamptz)
    ) INHERITS ("%(base_table)s");
    ALTER TABLE "%(table_name)s" ADD PRIMARY KEY ("%(pk)s");"""

    table = model._meta.db_table
    return sql % {
        'table_name': str(table),
        'column': model._meta.get_field(table.partitioned_field()).column,
        'start': start,
        'end': end,
        'base_table': table.base_table(),
        'pk': model._meta.pk.column
        }


def sql_to_get_indexes(table):
    sql = """SELECT pg_get_indexdef(i.oid)
  FROM pg_index x
  JOIN pg_class c ON c.oid = x.indrelid
  JOIN pg_class i ON i.oid = x.indexrelid
WHERE c.relkind = 'r'::"char"
AND i.relkind = 'i'::"char"
AND x.indisprimary = 'false'
AND c.relname = '%s';""" % table
    return sql


def sql_to_get_partitions(table):
    sql = """select cc.relname
    from pg_inherits i
    join pg_class cp on cp.oid = i.inhparent
    join pg_class cc on cc.oid = i.inhrelid
    where cp.relname = '%s';"""
    return sql % table

sql_to_get_sequences = """\
select pg_get_serial_sequence(c.relname, a.attname)
from pg_class c
join pg_attribute a on c.oid = a.attrelid
where c.relname = %s
and not pg_get_serial_sequence(c.relname, a.attname) is null;"""
