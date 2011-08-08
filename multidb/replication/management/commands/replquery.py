import os
import pprint
import logging
import tempfile
import simplejson
from optparse import make_option
from django.core.management.base import BaseCommand


class Command(BaseCommand):
    help = """Manage the replication environment."""
    can_import_settings = False
    requires_model_validation = False

    option_list = BaseCommand.option_list + (
        make_option('--live', '-l', action='store_true', dest='live',
                    help='use the live replication environment as source'),
        make_option('--config', '-c', action='store', dest='config',
                    help='use the configuration for the named environment'),
        make_option('--regen', '-r', action='store_true', dest='regen',
                    help='regenerate the cache from the live system'),
        make_option('--noop', '-n', action='store_true', dest='noop',
                    help='print new environment without writing any changes '
                         '(used with -r)'),
        )

    def handle(self, *args, **options):
        from django.conf import settings
        from multidb.replication.replquery import ReplQuery

        logger = logging.getLogger('replication.management.commands.replquery')
        config = options.get('config') or settings.TABLE_MAPPER
        parts = config.split('.')
        config_dir = os.path.join(settings.APP_ROOT, *parts[0:-1])
        config_file = os.path.join(config_dir, parts[-1] + '.py')
        if options.get('regen', False):
            # Renegerate config
            description = ReplQuery().discover()
            printer = pprint.PrettyPrinter()
            # cache = "mapper = " + printer.pformat(description)
            cache = ("from utils.datatypes.overridedict import OverrideDict"
                     "mapper = OverrideDict(%s)"
                     % printer.pformat(description))
            if options.get('noop', False):
                print cache
                logger.info("Cache not saved. Run without -n to write.")
            else:
                (tmpfd, tmpfname) = tempfile.mkstemp(dir=config_dir)
                try:
                    os.write(tmpfd, cache)
                finally:
                    os.close(tmpfd)
                os.rename(tmpfname, config_file)
                logger.info("New replication environment written to %s",
                            config_file)
        else:
            if options.get('live', False):
                description = ReplQuery().discover()
            else:
                try:
                    mod = __import__(config, {}, {}, [''])
                except ImportError, e:
                    logger.error("Unable to import mapper %s" % config)
                    raise
                else:
                    description = mod.mapper
            for unit in description.iteritems():
                print simplejson.dumps(unit)
