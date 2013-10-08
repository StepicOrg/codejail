"""Run code in a jail."""
import logging
import os
import resource
import shutil
import subprocess
import sys
import threading
import time

from .util import temp_directory

log = logging.getLogger(__name__)

# TODO: limit too much stdout data?

DEFAULT_LIMITS = {
    # CPU seconds, defaulting to 1.
    "CPU": 2,
    # Real time, defaulting to 1 second.
    "REALTIME": 2,
    # Total process virutal memory, in bytes, defaulting to unlimited.
    "VMEM": 0,
}


def unite_limits(limit1, limit2):
    def extract(val):
        return [l[val] for l in [limit1, limit2] if val in l]
    cpus = extract('CPU')
    realtimes = extract('REALTIME')
    vmems = extract('VMEM')
    new_limits = {
        'CPU': min(cpus),
        'REALTIME': min(realtimes),
        'VMEM': min(vmems) if 0 not in vmems else max(vmems)
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
    limits = limits or dict(DEFAULT_LIMITS)
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


def jail_code(command, code=None, files=None, command_argv=None, argv=None, stdin=None,
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
    if not is_configured(command):
        raise Exception("jail_code needs to be configured for %r" % command)
    command = COMMANDS[command]
    limits = unite_limits(command.limits, limits or {})

    def set_process_limits():
        # No subprocesses or files.
        resource.setrlimit(resource.RLIMIT_NPROC, (0, 0))
        resource.setrlimit(resource.RLIMIT_FSIZE, (0, 0))

        # CPU seconds, not wall clock time.
        cpu = limits["CPU"]
        if cpu:
            resource.setrlimit(resource.RLIMIT_CPU, (cpu, cpu))

        # Total process virtual memory.
        vmem = limits["VMEM"]
        if vmem:
            resource.setrlimit(resource.RLIMIT_AS, (vmem, vmem))

    with temp_directory() as tmpdir:

        if slug:
            log.warning("Executing jailed code %s in %s", slug, tmpdir)

        # All the supporting files are copied into our directory.
        for element in files:
            if isinstance(element, str):
                filename = element
                dest = os.path.join(tmpdir, os.path.basename(filename))
                if os.path.islink(filename):
                    os.symlink(os.readlink(filename), dest)
                elif os.path.isfile(filename):
                    shutil.copy(filename, tmpdir)
                else:
                    shutil.copytree(filename, dest, symlinks=True)
            else:
                file_content, filename = element
                dest = os.path.join(tmpdir, os.path.basename(filename))
                with open(dest, 'w') as f:
                    f.write(file_content)

        # Create the main file.
        if code:
            with open(os.path.join(tmpdir, "jailed_code"), "w") as jailed:
                jailed.write(code)

            command_argv = command_argv + ["jailed_code"]

        cmd = command.argv + command_argv + argv

        popen_kwargs = dict(cwd=tmpdir,
                            env=env,
                            stdin=subprocess.PIPE,
                            stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE,
                            start_new_session=True,
                            preexec_fn=set_process_limits)

        if sys.platform == "win32":
            popen_kwargs.update(preexec_fn=None,
                                start_new_session=False)

        subproc = subprocess.Popen(cmd, **popen_kwargs)

        # Start the time killer thread.
        realtime = limits["REALTIME"]
        if realtime:
            killer = ProcessKillerThread(subproc, limit=realtime)
            killer.start()

        result = JailResult()
        if isinstance(stdin, str):
            encoded_stdin = stdin.encode()
        else:
            encoded_stdin = stdin

        result.stdout, result.stderr = subproc.communicate(encoded_stdin)

        result.status = subproc.returncode

    return result


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
