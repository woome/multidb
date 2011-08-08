from __future__ import absolute_import
import logging
from django.db import models
from django.core.management.base import BaseCommand
from multidb.feature.partitions import (DailyPartitionTableName,
                                        WeeklyPartitionTableName,
                                        MonthlyPartitionTableName)
from multidb.feature.models import WoomeFeature


# How many partitions should we make for each model
PARTITION_BUFFER = {
    DailyPartitionTableName: 10,
    WeeklyPartitionTableName: 4,
    MonthlyPartitionTableName: 3,
}


def find_partition_models(cls):
    """Returns a list of the models using cls as a db_table."""
    return [m for m in models.get_models() if isinstance(m._meta.db_table, cls)]


def make_partitions_for_model(model, n=1):
    """Attempt to make partition names for all the partition models we have.

    Only models found by find_partition_models() are used, that is
    they must declare their Meta.db_table as one of the
    DatePartitionedTableName types.

    The code here is completly postgres specific. But other databases
    can do partitions and it should be possible to write versions for
    them.
    """

    logger = logging.getLogger("feature.management.commands.partition.make_partitions")
    logger.info("Creating partitions for %s" % model)

    class PartitionFeature(WoomeFeature):
        def __init__(self, model, *args, **kwargs):
            self.models.append(model)
            super(PartitionFeature, self).__init__(*args, **kwargs)

        def alter_schema(self):
            model.features.create_partition(n=n)

    PartitionFeature(model).apply()


def make_partitions():
    logger = logging.getLogger("feature.management.commands.partition")
    for model, number in PARTITION_BUFFER.items():
        for m in find_partition_models(model):
            try:
                make_partitions_for_model(m, number)
            except Exception, err:
                logger.error("Failure partitioning for %s: %s", m, err)


class Command(BaseCommand):
    help = """Create partition tables for all partitioned models."""
    can_import_settings = True

    def handle(self, *args, **options):
        logging.getLogger("feature.management.commands.partition").setLevel(logging.DEBUG)
        logging.getLogger("FeatureManager").setLevel(logging.DEBUG)
        make_partitions()
