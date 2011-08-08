import logging
import django.db
from django.core.management.base import BaseCommand
from multidb.feature.models import DatabaseFeature


class Command(BaseCommand):
    help = """Create missing foreign key constraints for all models."""
    can_import_settings = True

    def handle(self, *args, **options):
        logger = logging.getLogger('multidb.feature.makefkeys')

        class ForeignKeyFeature(DatabaseFeature):
            models = django.db.models.get_models()

            def alter_schema(self):
                for model in self.models:
                    logger.info("Generating constraints for %s",
                                model._meta.db_table)
                    try:
                        refs = model.features._sql_for_constraints(
                                only_new=True)
                        if refs:
                            model.features._run_sql('\n'.join(refs),
                                                    provider_only=True)
                    except django.db.DatabaseError, err:
                        logger.warning(
                                "Failed creating constraints for %s: %s",
                                model._meta.db_table, err)
                    except Exception, err:
                        logger.warning(
                                "Not creating constraints for %s",
                                model._meta.db_table, exc_info=True)

        feature = ForeignKeyFeature()
        feature.apply()
