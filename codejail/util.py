"""Helpers for codejail."""

import contextlib
import logging
import os
import select
import selectors
import shutil
import subprocess
import tempfile


logger = logging.getLogger(__name__)


@contextlib.contextmanager
def temp_directory(tmp_root=None, cleanup_executable=None):
    """
    A context manager to make and use a temp directory.
    The directory will be removed when done.
    """
    temp_dir = tempfile.mkdtemp(prefix="codejail-", dir=tmp_root)
    # Make directory readable and writable by other users ('sandbox' user
    # needs to be able to read and write it).
    os.chmod(temp_dir, 0o777)
    try:
        yield temp_dir
    finally:
        if cleanup_executable:
            # Cleaning up all files in the sandbox directory. They may be owned
            # by sandbox user and have arbitrary permissions, so use sudo with
            # a special cleanup script.
            p = subprocess.Popen(['sudo', cleanup_executable, temp_dir],
                                 stdout=subprocess.PIPE,
                                 stderr=subprocess.STDOUT)
            cleanup_stdout, _ =  p.communicate(timeout=60)
            if p.returncode:
                logger.error("Failed to cleanup the working directory: %s",
                             cleanup_stdout.decode(errors='replace'))
        # if this errors, something is genuinely wrong, so don't ignore errors.
        shutil.rmtree(temp_dir)


@contextlib.contextmanager
def change_directory(new_dir):
    """
    A context manager to change the directory, and then change it back.
    """
    old_dir = os.getcwd()
    os.chdir(new_dir)
    try:
        yield new_dir
    finally:
        os.chdir(old_dir)


def subprocess_communicate(proc, input=None, output_maxsize=None):
    """A modified version of Popen._communicate method from stdlib."""

    _PIPE_BUF = getattr(select, 'PIPE_BUF', 512)

    if proc.stdin and not proc._communication_started:
        # Flush stdio buffer.  This might block, if the user has
        # been writing to .stdin in an uncontrolled fashion.
        proc.stdin.flush()
        if not input:
            proc.stdin.close()

    stdout = None
    stderr = None

    # Only create this mapping if we haven't already.
    if not proc._communication_started:
        proc._fileobj2output = {}
        proc._fileobj2output_size = {}
        proc.output_truncated = False
        if proc.stdout:
            proc._fileobj2output[proc.stdout] = []
            proc._fileobj2output_size[proc.stdout] = 0
        if proc.stderr:
            proc._fileobj2output[proc.stderr] = []
            proc._fileobj2output_size[proc.stderr] = 0

    if proc.stdout:
        stdout = proc._fileobj2output[proc.stdout]
    if proc.stderr:
        stderr = proc._fileobj2output[proc.stderr]

    proc._save_input(input)

    if proc._input:
        input_view = memoryview(proc._input)

    with subprocess._PopenSelector() as selector:
        if proc.stdin and input:
            selector.register(proc.stdin, selectors.EVENT_WRITE)
        if proc.stdout:
            selector.register(proc.stdout, selectors.EVENT_READ)
        if proc.stderr:
            selector.register(proc.stderr, selectors.EVENT_READ)

        while selector.get_map():
            ready = selector.select()

            # XXX Rewrite these to use non-blocking I/O on the file
            # objects; they are no longer using C stdio!

            for key, events in ready:
                if key.fileobj is proc.stdin:
                    chunk = input_view[proc._input_offset :
                                       proc._input_offset + _PIPE_BUF]
                    try:
                        proc._input_offset += os.write(key.fd, chunk)
                    except BrokenPipeError:
                        selector.unregister(key.fileobj)
                        key.fileobj.close()
                    else:
                        if proc._input_offset >= len(proc._input):
                            selector.unregister(key.fileobj)
                            key.fileobj.close()
                elif key.fileobj in (proc.stdout, proc.stderr):
                    data = os.read(key.fd, 32768)
                    if not data:
                        selector.unregister(key.fileobj)
                        key.fileobj.close()
                    proc._fileobj2output_size[key.fileobj] += len(data)
                    if proc._fileobj2output_size[key.fileobj] > output_maxsize:
                        proc.output_truncated = True
                    else:
                        proc._fileobj2output[key.fileobj].append(data)

    proc.wait()

    # All data exchanged.  Translate lists into strings.
    if stdout is not None:
        stdout = b''.join(stdout)
    if stderr is not None:
        stderr = b''.join(stderr)

    # Translate newlines, if requested.
    # This also turns bytes into strings.
    if proc.universal_newlines:
        if stdout is not None:
            stdout = proc._translate_newlines(stdout,
                                              proc.stdout.encoding)
        if stderr is not None:
            stderr = proc._translate_newlines(stderr,
                                              proc.stderr.encoding)

    return (stdout, stderr)
