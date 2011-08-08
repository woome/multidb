NEW = """
new
\tcreates a new feature file and opens it for editing based on 'appname.modelname'
"""

MAKE = """
make
\tgenerates a new feature for newly added models.
\tcommand options
\t-v\t\tverbose - prints out sql statements for new model
"""

LIST = """
list
\tdisplays a list of all features available
\t[command options]
\t-p\t\tpending features [not yet installed]
\t-i\t\tinstalled features
"""

RUN = """
run
\tinstalls the model changes in pending features
\t[command options]
\t-t\t\trun in test mode
\t-d\t\trun in debug mode. [pdb]
"""

CAT = """
cat
\toutputs the content of a feature to screen
"""

ALL = """
%(new)s
%(list)s
%(run)s
%(make)s
%(cat)s
""" % {'new': NEW, 'list': LIST, 'run': RUN, 'make': MAKE, 'cat': CAT}
