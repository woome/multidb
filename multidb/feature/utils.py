import fcntl
import getpass
import logging
import os
import subprocess
import sys
import tempfile
import urllib2

from django.conf import settings


def _set_cloexec(fileno):
    fcntl.fcntl(fileno, fcntl.F_SETFD,
        fcntl.fcntl(fileno, fcntl.F_GETFD) | fcntl.FD_CLOEXEC)


def _set_nonblocking(fileno):
    fcntl.fcntl(fileno, fcntl.F_SETFL,
        fcntl.fcntl(fileno, fcntl.F_GETFL) | os.O_NONBLOCK)


def _fork_callable(callable):
    r, w = os.pipe()
    _set_cloexec(w)
    _set_nonblocking(w)
    pid = os.fork()
    if not pid:
        os.dup2(r, 0)
        os.close(w)
        callable()
        sys._exit(0)
    os.close(r)
    return pid, w


def shellargs():
    args = ['psql']
    if settings.DATABASE_USER:
        args += ["-U", settings.DATABASE_USER]
    if settings.DATABASE_HOST:
        args.extend(["-h", settings.DATABASE_HOST])
    if settings.DATABASE_PORT:
        args.extend(["-p", str(settings.DATABASE_PORT)])
    args += [settings.DATABASE_NAME]
    return args


def runshell():
    args = shellargs()
    os.execvp('psql', args)


def pipe_to_db_shell(f_in, bufsize=4096, stream=None, errstream=None):
    """Pipe the contents of the fd f_in to  a spawned psql shell.

    'stream' is where to write the output from psql if any
    'errstream' is where to write the errors from psql if any
    """
    args = shellargs()
    sp = subprocess.Popen(
        args,
        bufsize=bufsize,
        stdin=f_in,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        )

    output, errs = sp.communicate()

    if hasattr(stream, "write"):
        stream.write(output)

    if hasattr(errstream, "write"):
        errstream.write(errs)

    if sp.returncode > 0:
        raise Exception("Non Zero Exit Status: %s" % sp.returncode)


class _LazyPasswordManager(object):
    def __init__(self):
        self._cache = {}

    def find_user_password(self, realm, authuri):
        try:
            return self._cache[(realm, authuri)]
        except KeyError:
            un = raw_input('Username for %s (Realm "%s"): ' % (authuri, realm))
            gotpw = False
            while not gotpw:
                pw = getpass.getpass('Password: ')
                pwc = getpass.getpass('Confirm Password: ')
                if pw != pwc:
                    print 'Passwords Do Not Match!'
                else:
                    gotpw = True
            self._cache[(realm, authuri)] = un, pw
            return un, pw

    def add_password(self, *args, **kwargs):
        pass


_lazypasswordmanager = _LazyPasswordManager()


def urlopen(url):
    auth_handler = urllib2.HTTPBasicAuthHandler(_lazypasswordmanager)
    opener = urllib2.build_opener(auth_handler)
    return opener.open(url)


def urlretrieve(url):
    f = urlopen(url)
    temp = tempfile.TemporaryFile()
    while True:
        data = f.read(4096)
        if not data:
            break
        temp.write(data)
    f.close()
    temp.seek(0)
    return temp


def clone_repo(src, dst):
    # could do this with the mercurial api but there's a
    # bit of faffing involved to load config.
    logger = logging.getLogger('feature.utils.clone_repo')
    cmd = ['hg', 'clone', src,
              os.path.abspath(dst)]
    p = subprocess.Popen(cmd,
                          stdout=subprocess.PIPE,
                          stderr=subprocess.STDOUT)
    stdout, stderr = p.communicate()
    if p.returncode != 0:
        logger.critical('hg command to clone repo "%s" to "%s" failed with '
            'returncode %s\noutput was...\n%s' %
            (src, dst, p.returncode, stdout))
        raise Exception('hg clone failed')


def update_repo(repo, remotesrc=None):
    # could do this with the mercurial api but there's a
    # bit of faffing involved to load config.
    logger = logging.getLogger('feature.utils.update_repo')
    cmd = ['hg', 'pull', '-u']
    if remotesrc:
        cmd.append(remotesrc)
    p = subprocess.Popen(cmd,
                         cwd=os.path.abspath(repo),
                         stdout=subprocess.PIPE,
                         stderr=subprocess.STDOUT)
    stdout, stderr = p.communicate()
    if p.returncode != 0:
        logger.critical('hg command to update repo "%s" from "%s" failed '
            'with returncode %s\noutput was...\n%s' %
            (repo, remotesrc, p.returncode, stdout))
        raise Exception('hg pull -u failed')


def open_data_from_cache(filename, opener=open):
    logger = logging.getLogger('feature.utils.open_data_from_cache')
    cacherepo = getattr(settings, 'FEATURE_DATA_DIR',
                        os.path.expanduser('~/featuredata'))
    remoterepo = settings.FEATURE_DATA_REPO_REMOTE
    expectedfn = os.path.join(os.path.abspath(cacherepo), filename)
    if not os.path.exists(cacherepo):
        clone_repo(remoterepo, cacherepo)
    if not os.path.exists(expectedfn):
        update_repo(cacherepo)
        if not os.path.exists(expectedfn):
            logger.critical('could not find file "%s" in working copy of '
                'repo "%s"' % (expectedfn, cacherepo))
            raise Exception('could not find data file for feature')

    return opener(expectedfn)
