"""Python/django to make doing SQL ALTER statements (and other DCL)
nice and easy"""
from __future__ import with_statement
import bz2
import contextlib
import gzip
import socket
import logging
from django.conf import settings
from django.db import connection
from multidb.feature.utils import open_data_from_cache
from multidb.feature.utils import pipe_to_db_shell


def alter(statement, *statements, **kwargs):
    stmts = [statement]
    stmts.extend(statements)

    collector = kwargs.get('collector', None)

    cursor = connection.cursor()
    for s in stmts:
        try:
            cursor.execute(s)
            try:
                row = cursor.fetchone()
                if collector:
                    collector(s, row)
            except Exception, e:
                pass
        except Exception, e:
            print e


class ConditionNotMet(Exception):
    pass


def requires(**conditions):
    """
    checks specified conditions are met before calling the function
    """

    def cont(fn):
        def wrapper(*args, **kwargs):
            logger = logging.getLogger("feature.db_alter")
            # First test whether we can run this or not
            for condition, values in conditions.iteritems():
                logger.info("Found condition %s" % condition)
                if condition == 'hosts':
                    if socket.gethostname() not in values:
                        raise ConditionNotMet("hostname '%s' not in %s"
                                            % (socket.gethostname(), values))
                elif condition in ('dbtype', 'db_type'):
                    if settings.DATABASE_TYPE not in values:
                        raise ConditionNotMet("db type '%s' not in %s"
                                            % (settings.DATABASE_TYPE, values))

            # Now run the feature if it passed those tests
            return fn(*args, **kwargs)
        return wrapper
    return cont


def alter_from_datafile(
    filename,
    compression=None,
    pipe_to_shell=False,
    pipe_to_shell_stream=None,
    uses_copy=False):
    if compression is None and filename.endswith('.bz2'):
        compression = 'bz2'
    elif compression is None and filename.endswith('.gz'):
        compression = 'gzip'

    opener = {
        'gz': gzip.GzipFile,
        'gzip': gzip.GzipFile,
        'bz2': bz2.BZ2File,
        'bzip2': bz2.BZ2File,
        None: open,
    }[compression]
    with contextlib.closing(open_data_from_cache(filename, opener=opener)) as f:
        if pipe_to_shell or uses_copy:
            pipe_to_db_shell(f, stream=pipe_to_shell_stream)
        else:
            alter(f.read())

# End
