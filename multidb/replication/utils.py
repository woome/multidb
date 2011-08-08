from __future__ import with_statement
from contextlib import contextmanager
from functools import wraps


@contextmanager
def master(model, *models):
    """Context manager to require master db for queries on the given models.

    Example:

    with master(Model):
        m = Model.get(value=v)

    This will ensure that the select done by Model.get is against the master
    database for Model.

    """
    from django.db import connection
    if not hasattr(connection, 'mapper'):
        yield
    else:
        affinity = connection.mapper._manual_affinity
        # Keep track of which tables weren't already set so nested calls work
        changed = []
        for m in [model] + list(models):
            table = m._meta.db_table
            if not affinity.get(table):
                affinity[table] = True
                changed.append(table)
        try:
            yield
        finally:
            for table in changed:
                del affinity[table]


def with_master(model, *models):
    """Returns a decorator to use the master database in a function.

    Example:

    @with_master(Model1 [, Model2, ...])
    function(arg):
        ...

    This will cause any access on Model1 (and Model2 etc) to be made using the
    master database for that model.

    """
    def master_dec(func):
        @wraps(func)
        def wrapper(*args, **kwds):
            with master(model, *models):
                return func(*args, **kwds)
        return wrapper
    return master_dec

# End
