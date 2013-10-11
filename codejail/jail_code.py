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

DEFAULT_LIMITS = {
    # CPU seconds, defaulting to 1.
    "CPU": 2,
    # Real time, defaulting to 1 second.
    "REALTIME": 2,
    # Total process virutal memory, in bytes, defaulting to unlimited.
    "VMEM": 0,
    "FORK": 0,
    "FSIZE": 0
}


def unite_limits(limit1, limit2):
    def extract(val):
        return [l[val] for l in [limit1, limit2] if val in l]
    cpus = extract('CPU')
    realtimes = extract('REALTIME')
    vmems = extract('VMEM')
    forks = extract('FORK')
    fsizes = extract('FSIZE')
    new_limits = {
        'CPU': min(cpus),
        'REALTIME': min(realtimes),
        'VMEM': min(vmems) if 0 not in vmems else max(vmems),
        'FORK': min(forks),
        'FSIZE': min(fsizes)
    }
    return new_limits


class Command(object):
    def __init__(self, name, argv, limits):
        self.name = name
        self.argv = argv
        self.limits = limits

# Configure the commands

# COMMANDS is a map from an abstract command name to a list of command-line
# pieces, such as subprocess.Popen wants.
COMMANDS = {}


def configure(command, bin_path, user=None, extra_args=None, limits=None):
    """
    Configure a command for `jail_code` to use.

    `command` is the abstract command you're configuring, such as "python" or
    "node".  `bin_path` is the path to the binary.  `user`, if provided, is
    the user name to run the command under.

    """
    extra_args = extra_args or []
    limits = limits or {}
    for k, v in DEFAULT_LIMITS.items():
        if k not in limits:
            limits[k] = v
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

    COMMANDS[command] = Command(command, cmd_argv, limits)


def is_configured(command):
    """
    Has `jail_code` been configured for `command`?

    Returns true if the abstract command `command` has been configured for use
    in the `jail_code` function.

    """
    return command in COMMANDS

# By default, look where our current Python is, and maybe there's a
# python-sandbox alongside.  Only do this if running in a virtualenv.
if hasattr(sys, 'real_prefix'):
    if os.path.isdir(sys.prefix + "-sandbox"):
        configure("python", sys.prefix + "-sandbox/bin/python", "sandbox")


class JailResult(object):
    """
    A passive object for us to return from jail_code.
    """

    def __init__(self):
        self.stdout = self.stderr = self.status = None


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
                 slug=None, env=None, limits=None):
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
        env = env or {}
        if isinstance(stdin, str):
            stdin = stdin.encode()

        if not is_configured(command):
            raise Exception("jail_code needs to be configured for %r" % command)
        command = COMMANDS[command]
        limits = unite_limits(command.limits, limits or {})

        def set_process_limits():
            import resource  # available only on Unix
            if not limits["FORK"]:
                resource.setrlimit(resource.RLIMIT_NPROC, (0, 0))
            fsize = limits["FSIZE"]
            resource.setrlimit(resource.RLIMIT_FSIZE, (fsize, fsize))
            # CPU seconds, not wall clock time.
            cpu = limits["CPU"]
            if cpu:
                resource.setrlimit(resource.RLIMIT_CPU, (cpu, cpu))
            # Total process virtual memory.
            vmem = limits["VMEM"]
            if vmem:
                resource.setrlimit(resource.RLIMIT_AS, (vmem, vmem))

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
                            env=env,
                            stdin=subprocess.PIPE,
                            stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE,
                            start_new_session=True,
                            preexec_fn=set_process_limits)
        if sys.platform == "win32":
            popen_kwargs.update(preexec_fn=None,
                                start_new_session=False)

        result = self.do_popen(cmd, stdin, limits["REALTIME"], popen_kwargs)
        return result

    @staticmethod
    def do_popen(cmd, stdin, time_limit, popen_kwargs):
        subproc = subprocess.Popen(cmd, **popen_kwargs)
        # Start the time killer thread.
        if time_limit:
            killer = ProcessKillerThread(subproc, limit=time_limit)
            killer.start()
        result = JailResult()
        result.stdout, result.stderr = subproc.communicate(stdin)
        result.status = subproc.returncode

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
                with open(dest, 'w') as f:
                    f.write(file_content)


def jail_code(command, code=None, files=None, command_argv=None, argv=None, stdin=None,
              slug=None, env=None, limits=None):
    with Jail() as jail:
        return jail.run_code(command, code, files, command_argv, argv, stdin,
                             slug, env, limits)


class ProcessKillerThread(threading.Thread):
    """
    A thread to kill a process after a given time limit.
    """

    def __init__(self, subproc, limit):
        super(ProcessKillerThread, self).__init__()
        self.subproc = subproc
        self.limit = limit

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
            subprocess.call(["sudo", "pkill", "-9", "-g", str(pgid)])
