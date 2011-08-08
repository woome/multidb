from setuptools import setup

packages = ['multidb.' + p for p in
            ['replication', 'replication.management',
             'replication.management.commands',
             'feature', 'feature.management', 'feature.management.commands',
             'backend',
             ]] + ['multidb']

setup(
    name='multidb',
    version=0.01,
    description='Multidb backend for django',
    long_description="""Multidb backend for django""",
    author='Woome',
    author_email='patrick@woome.com',
    license="GNU GPL v3",
    url="http://github.com/woome/multidb",
    download_url="http://github.com/woome/multidb/tarball/master",
    packages=packages,
    install_requires=['psycopg2', 'django'],
    platforms=['any'],
    #keywords=['asynchronous', 'queue', 'task queue'],
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: GNU General Public License (GPL)',
        'Operating System :: POSIX',
        'Programming Language :: Python',
        ],
)
