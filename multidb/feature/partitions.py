"""Classes for partitioning using postgres"""

import threading
from datetime import datetime, timedelta


class LazyString(object):
    def __init__(self, stringpattern, function=lambda: ""):
        self.stringpattern = stringpattern
        self.function = function

    def startswith(self, *args):
        self.__str__().startswith(*args)

    def __str__(self):
        return self.stringpattern % self.function()


class DatePartitionTableName(LazyString):
    """Create partition names for tables lazily.

    Partitioned tables needed partition specific names applied
    on insert. Using this class to specify Django's Meta.db_table
    let's you delay the computation of the table name and supply
    the relevant datetime object later.

    For example:

       class Meta:
           db_table = MonthlyPartitionTableName(
                 base_table="appname_thismodel",
                 partitioned_field="created"
                 )

       def save(self, **kwargs):
           with self._meta.db_table.partition(self) as partition:
              super(ThisModel, self).save(**kwargs)

    This will cause ThisModel to be saved into a partition like:

       appname__thismodel_y2010m01

    The 'partition' guard variable is a list of start and end times
    for the partition relevancy. These can be used to construct check
    constraints for a partition table.
    """

    def __init__(self, base_table, partitioned_field):

        # Define a helper function for the lazy string
        def helper():
            tablename = ""
            try:
                if self._threadlocal.datetime:
                    tablename = self._start_time(
                        self._threadlocal.datetime).strftime(
                            self._time_pattern())
            except:
                pass

            return tablename

        # Make this a lazystring computing the string with the
        # threadlocal
        super(DatePartitionTableName, self).__init__(
            base_table + '%s',
            lambda: helper()
            )

        # Store the base table
        self._base_table = base_table
        # Store the column name
        self._partitioned_field = partitioned_field
        # Define the threadlocal
        self._threadlocal = threading.local()

    def _start_time(self, dt):
        """This is designed to be overridden"""
        return dt

    def _end_time(self, dt):
        """This is designed to be overridden"""
        return dt

    def _time_pattern(self):
        """This is designed to be overridden"""
        return ""

    def partitioned_field(self):
        return self._partitioned_field

    def base_table(self):
        return self._base_table

    def parse_partition(self, table):
        """Get the starting datetime from a table name"""
        pattern = self._base_table + self._time_pattern()
        dt = datetime.strptime(table, pattern)
        return dt

    def partition(self, instance, timestamp=None):
        """Return a context guard for setting the table as a partition.

        The datetime for the partition is taken from the instance
        unless the timestamp is specified in the arguments.
        """

        db_table_self = self
        ts = timestamp

        class ctxguard:
            def __enter__(self):
                timestamp = ts
                if not timestamp:
                    timestamp = instance.__dict__[
                        db_table_self.partitioned_field()]
                db_table_self._threadlocal.datetime = timestamp
                # Not sure what to return here
                return [
                    db_table_self._start_time(timestamp),
                    db_table_self._end_time(timestamp)
                    ]

            def __exit__(self, type, values, traceback):
                db_table_self._threadlocal.datetime = ""

        return ctxguard()


class DailyPartitionTableName(DatePartitionTableName):
    def _time_pattern(self):
        return "_y%Yd%j"

    def _start_time(self, dt):
        return datetime(dt.year, dt.month, dt.day, tzinfo=dt.tzinfo)

    def _end_time(self, dt):
        return self._start_time(dt) + timedelta(days=1)


class WeeklyPartitionTableName(DatePartitionTableName):
    def _time_pattern(self):
        return "_y%Yw%W"

    def _start_time(self, dt):
        starttime = dt - timedelta(days=dt.weekday())
        return starttime.replace(hour=0, minute=0,
                                 second=0, microsecond=0)

    def _end_time(self, dt):
        return (self._start_time(dt).replace(
                    hour=23, minute=59, second=59, microsecond=999999) +
                timedelta(weeks=1) - timedelta(days=1))


class MonthlyPartitionTableName(DatePartitionTableName):
    def _time_pattern(self):
        return "_y%Ym%m"

    def _start_time(self, dt):
        return datetime(dt.year, dt.month, 1, tzinfo=dt.tzinfo)

    def _end_time(self, dt):
        endtime = (datetime(dt.year + 1, 1, 1, tzinfo=dt.tzinfo)
                   if dt.month == 12
                   else datetime(dt.year, dt.month + 1,
                                 day=1, tzinfo=dt.tzinfo))
        return endtime

# End
