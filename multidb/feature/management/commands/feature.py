import os
import sys
import pwd
import itertools
from re import sub
from datetime import datetime
from optparse import make_option

from django.core.management.base import BaseCommand, CommandError
from django.core.exceptions import ImproperlyConfigured
from django.db import models, connection

from multidb.feature.management.commands import featurehelp
from multidb.feature.models import Feature

introspection = connection.introspection


class Command(BaseCommand):
    help = """used to control operations for the feature application

Commands:
   run        runs the feature(s)
   new        create a new feature
   make       creates a new feature by running 'python manage.py syncdb'
   list       list all available features
   cat        outputs the content of a feature to screen
   help       displays a help manual
"""
    requires_model_validation = True
    output_transaction = True
    can_import_settings = True

    args = "command [appname]"

    option_list = BaseCommand.option_list + (
        make_option('--test', '-t',
                    action='store_true', dest='dry_run', default=False,
                    help="executes feature(s) with transaction turned off. used with 'run' command"),
        make_option('--debug', '-d',
                    action='store_true', help='run command in debug mode'),
        make_option('--mark', '-m', dest='mark_only', default=False,
                    action='store_true', help="mark feature(s) as applied only"),
        make_option('--?', '-?',
                    action='help', help='prints list of available commands for feature manipulation'),
        make_option('--installed', '-i',
                    action='store_true', dest='installed', default='',
                    help="prints only feature(s) already installed when used with 'list' command",),
        make_option('--pending', '-p',
                    action='store_false', dest='installed', default='',
                    help="prints only feature(s) waiting to be installed when used with 'list' command",),
    )

    def handle(self, command='', *args, **options):
        valid_commands = ('new', 'list', 'run', 'make', 'help', 'cat')
        if not command:
            # do it for all apps
            raise CommandError("Usage is \nfeature command %s" % self.args)
        else:
            if command not in valid_commands:
                raise CommandError("Valid commands for feature: %s" % str(valid_commands))
            if len(args) > 1 and command != 'help':
                raise CommandError("\nUsage is \nfeature %s %s" % (command, self.args))

        from django.conf import settings
        try:
            self._dir = settings.FEATURE_DIR
        except AttributeError:
            raise ImproperlyConfigured("You must set FEATURE_DIR for features to work")

        self.__fetch_handler(command)(*args, **options)

    def __fetch_handler(self, command):
        return getattr(self, 'handle_%sfeature' % command)

    def __get_app_names(self):
        from django.conf import settings
        return map(lambda n: sub('woome.', '', n), filter(lambda n: not n.startswith('django.'), settings.INSTALLED_APPS))

    def __get_model_names(self, app):
        models.get_models()

    def handle_helpfeature(self, *args, **options):
        """
        prints a help document for the features app or for each command
        """

        if len(args) == 0:
            print featurehelp.ALL
        elif len(args) == 1:
            print getattr(featurehelp, args[0].upper())
        else:
            raise CommandError("\nUsage: \n\tpython manage.py feature help [command]")

    def handle_newfeature(self, *args, **options):
        """
        creates a new feature file by asking the user a few keys questions
        and generating a template feature file to be completed
        """

        # get feature app(s)
        accepted_appnames = self.__get_app_names()
        warning = "Which app are you creating this feature for? Leave blank or choose one of\n%s : " % accepted_appnames
        print warning
        appname = sys.stdin.readline().strip()
        # validate
        while 42:
            if appname and appname not in accepted_appnames:
                print warning
                appname = sys.stdin.readline().strip()
                continue
            break

        # get feature model, can be none
        warning = "Which model(s) are you creating this feature for? Leave blank to choose None\ne.g \nPerson\n\tOR\nPerson, Mugshot : "
        print warning
        modelname = sys.stdin.readline().strip()
        modelname = modelname.lower().replace(',', '_').replace(' ', '')

        # get feature name
        warning = "Now enter a short name for your feature. This is required : "
        print warning
        feature_name = sys.stdin.readline().strip()
        while not feature_name:
            print warning
            feature_name = sys.stdin.readline().strip()
        feature_name = feature_name.lower().replace(', ', '_').replace(',', '_').replace(' ', '_')

        # generate feature filename
        timenow = datetime.now()
        filename = "feature_woome_" + "_".join([appname, modelname, feature_name, timenow.strftime("%Y%m%d%H%M%S")]) + ".py"

        # save feature template to file
        FEATURE_TEMPLATE = """\
# %(date)s

# App: %(appname)s
# Author: %(author)s
# Models: %(modelname)s
# Description: %(feature_name)s

from multidb.feature.models import WoomeFeature
from %(appname)s.models import %(modelname)s

class %(modelname)s_Feature(WoomeFeature):
    models = [%(modelname)s]
    def alter_schema(self):
        %(modelname)s.features.alter_table("--do your alteration in here--")


feature = %(modelname)s_Feature()
feature.apply()

# End


""" % {
'date': timenow.strftime('%B %d, %Y'),
'appname': appname,
'author': pwd.getpwuid(os.getuid())[0],
'modelname': "".join(map(lambda s: s.capitalize(), modelname.split('_'))),
'feature_name': feature_name}

        filepath = os.path.join(self._dir, filename)
        fd = open(filepath, 'w')
        fd.write(FEATURE_TEMPLATE)
        fd.close()

        # open file for editing
        def run_editor(path):
            os.system("$EDITOR %s" % path)

        run_editor(filepath)
        print "\ncreated new feature file: " + filepath

    def handle_runfeature(self, *args, **options):
        """
        Run features specific to the app args[0] or run all features if no args
        """
        if options.get("debug"):
            import pdb
            pdb.set_trace()

        dry_run = options.get("dry_run", False)
        mark_only = options.get("mark_only", False)
        try:
            # resolve appname or feature name from arg
            if args:
                try:
                    appname = args[0].endswith('/') and  args[0][:-1] or args[0]
                    appname = models.get_app(appname) and appname
                    Feature.ctrl.apply_features(appname, dry_run=dry_run, mark_only=mark_only)  # run all features in app 'appname'
                except ImproperlyConfigured:
                    Feature.ctrl.apply_feature(args[0], dry_run=dry_run, mark_only=mark_only)  # run single feature 'args[0]'
            else:
                Feature.ctrl.apply_features(dry_run=dry_run, mark_only=mark_only)  # run all features
        finally:
            if options.get('dry_run'):
                print "\nThe 'run' command was run with dry-run enabled.\n" +\
                      "The features run, haven't been applied to your schema\n" +\
                      "Rerun this command without the '-t' option when you are ready to save your schema changes."

    def handle_listfeature(self, *args, **options):
        """
        list all available features
        """
        appname = ''
        if args:
            appname = args[0]
        Feature.ctrl.list_features(appname=appname, installed=options.get('installed', ''),)

    def handle_makefeature(self, *args, **options):
        """
        generates a feature file
        which contains sql commands to create tables for any new installed models
        essentially what 'python manage.py syncdb' would have done - but this allows
        us to know when it was carried out
        """
        verbosity = options.get('verbosity')

        class no_style:
            def __getattr__(self, attr):
                return lambda x: x

        # Do nothing if this is true
        noaction = options.get("dry_run")

        style = no_style()

        table_name_converter = (lambda x: x) \
            if hasattr(connection.features, "uses_case_insensitive_names") \
            and getattr(connection.features, "uses_case_insensitive_names") \
            else lambda x: x.upper()
        tables = map(table_name_converter, introspection.table_names())

        all_models = set(itertools.chain(*map(models.get_models, models.get_apps())))  # all available models
        uninstalled_tables = set(map(lambda m: m._meta.db_table, all_models)).difference(tables)  # all uninstalled tables
        uninstalled_models = filter(lambda model: model._meta.db_table in uninstalled_tables, all_models)
        installed_models = set(filter(lambda model: model._meta.db_table in tables, all_models))

        SQL = []

        created_models = set()
        pending_references = {}

        # Create the tables for each model
        for model in uninstalled_models:
            sql, references = introspection.sql_create_model(model, style, installed_models)
            installed_models.add(model)
            created_models.add(model)
            for refto, refs in references.items():
                pending_references.setdefault(refto, []).extend(refs)
                if refto in installed_models:
                    sql.extend(introspection.sql_for_pending_references(refto, style, pending_references))
            sql.extend(introspection.sql_for_pending_references(model, style, pending_references))
            if verbosity == 2:
                print "Generating sql for table %s" % model._meta.db_table
            for statement in sql:
                SQL.append(statement)
            tables.append(table_name_converter(model._meta.db_table))

        # Create the m2m tables. This must be done after all tables have been created
        # to ensure that all referred tables will exist.
        for model in uninstalled_models:
            if model in created_models:
                sql = introspection.sql_for_many_to_many(model, style)
                if sql:
                    if verbosity == 2:
                        print "Generating sql for many-to-many tables for %s.%s model" % (model._meta.app_label, model._meta.object_name)
                    for statement in sql:
                        SQL.append(statement)

        # Install SQL indicies for all newly created models
        for model in uninstalled_models:
            if model in created_models:
                index_sql = introspection.sql_indexes_for_model(model, style)
                if index_sql:
                    if verbosity == 2:
                        print "Generating sql for index for %s.%s model" % (model._meta.app_label, model._meta.object_name)

                    for sql in index_sql:
                        SQL.append(sql)

        if not SQL:
            print "nothing to do here.."
            return

        # Otherwise, here's the SQL
        SQL = '"""\n' + '\n'.join([sql for sql in SQL]) + '\n"""'
        print SQL

        if not noaction:
            # write it out to file
            timenow = datetime.now()
            filename = "feature_woome_syncdb_install_new_model_%s.py" % datetime.now().strftime("%Y%m%d%H%M%S")
            filepath = os.path.join(self._dir, filename)
            try:
                fd = open(filepath, 'w')
                fd.write("""\
# %(date)s
# %(filename)s
# Author: %(author)s

from multidb.feature.db_alter import alter\n
from multidb.feature.models import WoomeFeature\n

SQL = %(SQL)s

def dofeature():
    alter(SQL)

dofeature()
""" % {'date': timenow.strftime('%B %d, %Y'),
       'filename': filename,
       'author': pwd.getpwuid(os.getuid())[0],
       'SQL': SQL,})
            finally:
                if fd:
                    try:
                        fd.close()
                    except:
                        print >> sys.stderr, "an error while trying to write the filter file"
                    else:
                        print "\ncreated new feature file: " + filepath
                        print "====================================================="

    def handle_catfeature(self, *args, **options):
        try:
            assert(len(args) > 0)
            Feature.ctrl.print_feature(args[0])  # run single feature 'args[0]'
        except AssertionError:
            raise CommandError("Usage is \nfeature cat featurename")
        except Exception:
            raise CommandError("failed")

# End
