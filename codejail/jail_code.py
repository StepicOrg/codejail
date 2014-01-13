"""Run code in a jail."""
import logging
import os
import shutil
import subprocess
import sys
import threading
import time

from . import util

log = logging.getLogger(__name__)

# TODO: limit too much stdout data?

DEFAULT_CONFIG = {
    # time in seconds, None for unlimited
    "TIME": 2,
    # memory in bytes, None for unlimited
    "MEMORY": 128 * 1024 * 1024,
    # allowed size of written files in bytes, None for unlimited
    "FILE_SIZE": 0,
    "CAN_FORK": False,
}


class Limits(object):
    UNLIMITED_CONFIG = {
        "TIME": None,
        "MEMORY": None,
        "FILE_SIZE": None,
        "CAN_FORK": True,
    }

    def __init__(self, conf, partial=False):
        if not partial:
            assert len(conf) == 4, "wrong codejail config"
            self.time = conf["TIME"]
            self.memory = conf["MEMORY"]
            self.can_fork = conf["CAN_FORK"]
            self.file_size = conf["FILE_SIZE"]
        else:
            options = {"TIME", "MEMORY", "CAN_FORK", "FILE_SIZE"}
            assert set(conf.keys()).issubset(options), "wrong codejail config"
            for opt in options:
                setattr(self, opt.lower(), conf.get(opt, self.UNLIMITED_CONFIG[opt]))

    def __and__(self, other):
        if other is None:
            return self

        if not isinstance(other, Limits):
            raise TypeError

        def maybe_min(attr):
            values = [getattr(x, attr) for x in [self, other] if getattr(x, attr) is not None]
            return min(values) if values else None

        return Limits({
            "TIME": maybe_min("time"),
            "MEMORY": maybe_min("memory"),
            "CAN_FORK": maybe_min("can_fork"),
            "FILE_SIZE": maybe_min("file_size")
        })

    def __repr__(self):
        return "<Limits time:{} mem:{} fsize:{} fork:{}>".format(self.time, self.memory,
                                                                 self.file_size, self.can_fork)

    def enforce(self):
        import resource  # available only on Unix

        def set_limit(limit, value):
            resource.setrlimit(limit, (value, value))

        if not self.can_fork:
            set_limit(resource.RLIMIT_NPROC, 0)

        if self.file_size is not None:
            set_limit(resource.RLIMIT_FSIZE, self.file_size)

        if self.time is not None:
            # time limit is enforced by killer thread so
            # increase it here to be killed with nice message
            set_limit(resource.RLIMIT_CPU, self.time + 1)

        if self.memory is not None:
            set_limit(resource.RLIMIT_AS, self.memory)


class Command(object):
    def __init__(self, name, argv, limits, env):
        self.name = name
        self.argv = argv
        self.limits = limits
        self.env = env

    def __repr__(self):
        return "<Command {}: {}>".format(self.name, ' '.join(self.argv))

# Configure the commands

# COMMANDS is a map from an abstract command name to a list of command-line
# pieces, such as subprocess.Popen wants.
COMMANDS = {}


def configure(command, bin_path, limits_conf, user=None, extra_args=None, env=None):
    """
    Configure a command for `jail_code` to use.

    `command` is the abstract command you're configuring, such as "python" or
    "node".  `bin_path` is the path to the binary.  `user`, if provided, is
    the user name to run the command under.

    """
    extra_args = extra_args or []
    limits = Limits(limits_conf)
    if "python" in command:
        # -E means ignore the environment variables PYTHON*
        # -B means don't try to write .pyc files.
        extra_args.extend(['-E', '-B'])

    cmd_argv = []
    if user:
        # Run as the specified user
        cmd_argv.extend(['sudo', '-u', user])

    # Run the command!
    cmd_argv.append(bin_path)
    cmd_argv.extend(extra_args)

    COMMANDS[command] = Command(command, cmd_argv, limits, env)


def is_configured(command):
    """
    Has `jail_code` been configured for `command`?

    Returns true if the abstract command `command` has been configured for use
    in the `jail_code` function.

    """
    return command in COMMANDS


def auto_configure():
    # By default, look where our current Python is, and maybe there's a
    # python-sandbox alongside.  Only do this if running in a virtualenv.
    if hasattr(sys, 'real_prefix'):
        if os.path.isdir(sys.prefix + "-sandbox"):
            configure("python", sys.prefix + "-sandbox/bin/python", DEFAULT_CONFIG, "sandbox")


class JailResult(object):
    """
    A passive object for us to return from jail_code.
    """

    def __init__(self):
        self.stdout = self.stderr = self.status = None
        self.time_limit_exceeded = False


class Jail(object):
    def __init__(self, tmp_root=None):
        self.tmpdir = None
        self.tmpdir_context_manager = util.temp_directory(tmp_root=tmp_root)

    def __enter__(self):
        self.tmpdir = self.tmpdir_context_manager.__enter__()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.tmpdir_context_manager.__exit__(exc_type, exc_val, exc_tb)

    def run_code(self, command, code=None, files=None, command_argv=None, argv=None, stdin=None,
                 slug=None, limits=None):
        """
        Run code in a jailed subprocess.

        `command` is an abstract command ("python", "node", ...) that must have
        been configured using `configure`.

        `code` is a string containing the code to run.  If no code is supplied,
        then the code to run must be in one of the `files` copied, and must be
        named in the `argv` list.

        `files` is a list of file paths, they are all copied to the jailed
        directory.  Note that no check is made here that the files don't contain
        sensitive information.  The caller must somehow determine whether to allow
        the code access to the files.  Symlinks will be copied as symlinks.  If the
        linked-to file is not accessible to the sandbox, the symlink will be
        unreadable as well.

        `argv` is the command-line arguments to supply.

        `stdin` is a string, the data to provide as the stdin for the process.

        `slug` is an arbitrary string, a description that's meaningful to the
        caller, that will be used in log messages.

        Return an object with:

            .stdout: stdout of the program, a string
            .stderr: stderr of the program, a string
            .status: exit status of the process: an int, 0 for success

        """
        files = files or []
        command_argv = command_argv or []
        argv = argv or []
        if isinstance(stdin, str):
            stdin = stdin.encode()

        if not is_configured(command):
            raise Exception("jail_code needs to be configured for %r" % command)
        command = COMMANDS[command]
        limits = Limits(limits, partial=True) if limits else None
        limits = command.limits & limits

        if slug:
            log.warning("Executing jailed code %s in %s", slug, self.tmpdir)

        # Create the main file.
        if code:
            files.append((code, "jailed_code"))
            command_argv = command_argv + ["jailed_code"]

        # All the supporting files are copied into our directory.
        self.write_files(files)

        cmd = command.argv + command_argv + argv
        popen_kwargs = dict(cwd=self.tmpdir,
                            env=command.env,
                            stdin=subprocess.PIPE,
                            stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE,
                            start_new_session=True,
                            preexec_fn=limits.enforce)
        if sys.platform == "win32":
            popen_kwargs.update(preexec_fn=None,
                                start_new_session=False)
        result = self.do_popen(cmd, stdin, limits.time, popen_kwargs)

        return result

    @staticmethod
    def do_popen(cmd, stdin, time_limit, popen_kwargs):
        subproc = subprocess.Popen(cmd, **popen_kwargs)
        # Start the time killer thread.
        killer = ProcessKillerThread(subproc, limit=time_limit)
        if time_limit is not None:
            killer.start()
        result = JailResult()
        result.stdout, result.stderr = subproc.communicate(stdin)
        result.status = subproc.returncode
        if killer.killed:
            result.status = -1
            result.time_limit_exceeded = True

        return result

    def write_files(self, files):
        for element in files:
            if isinstance(element, str):
                filename = element
                dest = os.path.join(self.tmpdir, os.path.basename(filename))
                if os.path.islink(filename):
                    os.symlink(os.readlink(filename), dest)
                elif os.path.isfile(filename):
                    shutil.copy(filename, self.tmpdir)
                else:
                    shutil.copytree(filename, dest, symlinks=True)
            else:
                file_content, filename = element
                dest = os.path.join(self.tmpdir, os.path.basename(filename))
                with open(dest, 'w', encoding="utf-8") as f:
                    f.write(file_content)


def jail_code(command, code=None, files=None, command_argv=None, argv=None, stdin=None,
              slug=None, limits=None):
    with Jail() as jail:
        return jail.run_code(command, code, files, command_argv, argv, stdin,
                             slug, limits)


class ProcessKillerThread(threading.Thread):
    """
    A thread to kill a process after a given time limit.
    """

    def __init__(self, subproc, limit):
        super(ProcessKillerThread, self).__init__()
        self.subproc = subproc
        self.limit = limit
        self.killed = False

    def run(self):
        start = time.time()
        while (time.time() - start) < self.limit:
            time.sleep(.25)
            if self.subproc.poll() is not None:
                # Process ended, no need for us any more.
                return

        if self.subproc.poll() is None:
            # Can't use subproc.kill because we launched the subproc with sudo.
            pgid = os.getpgid(self.subproc.pid)
            log.warning(
                "Killing process %r (group %r), ran too long: %.1fs",
                self.subproc.pid, pgid, time.time() - start
            )
            self.killed = True
            subprocess.call(["sudo", "pkill", "-9", "-g", str(pgid)])
