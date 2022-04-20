"""
Utilities for the workflow
"""
from typing import List, Optional, Tuple, Union

from contextlib import contextmanager
from email.message import EmailMessage
from io import BytesIO
import logging
from matplotlib.figure import Figure
import mimetypes
import os
from pathlib import Path
from rechunker import rechunk as _rechunk
import shutil as sh
import signal
import smtplib
import sys
import time
from traceback import format_exception
import warnings
import xarray as xr
import zarr

from .config import parse_config
logger = logging.getLogger(__name__)


@parse_config
def rechunk(
    path_in: Union[os.PathLike, xr.Dataset],
    path_out: os.PathLike,
    *,
    chunks: dict,
    worker_mem: str,
    temp_store: os.PathLike = None,
):
    """
    Rechunk a dataset into a new zarr.

    Parameters
    ----------
    path_in : path or xr.Dataset
      Input to rechunk.
    path_out : path
      Path to the target zarr.
    chunks: dict
      Mapping from variables to mappings from dimension name to size.
    worker_mem : str
      The maximal memory usage of each task.
      When using a distributed Client, this an approximate memory _per thread_.
      Each worker of the client should have access to 10-20% more memory than this times the number of threads.
    temp_store: path, optional
      A path to a zarr where to store intermediate results.
    """
    if isinstance(path_in, os.PathLike):
        path_in = Path(path_in)

        if path_in.suffix == ".zarr":
            ds = zarr.open(str(path_in))
        else:
            ds = xr.open_dataset(path_in)
    else:
        ds = path_in

    plan = _rechunk(
        ds, chunks, worker_mem, str(path_out), temp_store=str(temp_store)
    )

    plan.execute()

    if temp_store is not None:
        sh.rmtree(temp_store)


@parse_config
def send_mail(
    *,
    to: str,
    subject: str,
    msg: str,
    server: str = "127.0.0.1",
    port: int = 25,
    attachments: Optional[List[Union[Tuple[str, Union[Figure, os.PathLike]], Figure, os.PathLike]]] = None
):
    """Send email.

    Send an email to a single address through a login-less SMTP server.
    The default values of server and port should work out-of-the-box on Ouranos's systems.

    Parameters
    ----------
    to: str
      Email address to which send the email.
    subject: str
      Subject line.
    msg: str
      Main content of the email. Can be UTF-8 and multi-line.
    server : str
      SMTP server url. Defaults to 127.0.0.1, the local host. This function does not try to log-in.
    port: int
      Port of the SMTP service on the server. Defaults to 25, which is usually the default port on unix-like systems.
    attachments : list of paths or matplotlib figures or tuples of a string and a path or figure, optional
      List of files to attach to the email.
      Elements of the list can be paths, the mimetypes of those is guessed and the files are read and sent.
      Elements can also be matplotlib Figures which are send as png image (savefig) with names like "Figure00.png".
      Finally, elements can be tuples of a filename to use in the email and the attachment, handled as above.
    """
    # Inspired by https://betterprogramming.pub/how-to-send-emails-with-attachments-using-python-dd37c4b6a7fd
    email = EmailMessage()
    email["Subject"] = subject
    email["From"] = f"{os.getlogin()}@{os.uname().nodename}"
    email["To"] = to
    email.set_content(msg)

    for i, att in enumerate((attachments or [])):
        fname = None
        if isinstance(att, tuple):
            fname, att = att
        if isinstance(att, Figure):
            data = BytesIO()
            att.savefig(data, format='png')
            data.seek(0)
            email.add_attachment(
                data.read(), maintype='image', subtype='png', filename=fname or f"Figure{i:02d}.png"
            )
        else:  # a path
            attpath = Path(att)
            ctype, encoding = mimetypes.guess_type(attpath)
            if ctype is None or encoding is not None:
                ctype = 'application/octet-stream'
            maintype, subtype = ctype.split('/', 1)

            with attpath.open('rb') as fp:
                email.add_attachment(
                    fp.read(), maintype=maintype, subtype=subtype, filename=fname or attpath.name
                )

    with smtplib.SMTP(host=server, port=port) as SMTP:
        SMTP.send_message(email)


class TimeoutException(Exception):
    """An exception raised with a timeout occurs."""

    def __init__(self, seconds, task="", **kwargs):
        self.msg = f'Task {task} timed out after {seconds} seconds'
        super().__init__(self.msg, **kwargs)


@contextmanager
def timeout(seconds, task=""):
    """Timeout context manager. Only one can be used at a time."""

    def _timeout_handler(signum, frame):
        raise TimeoutException(seconds, task)

    old_handler = signal.signal(signal.SIGALRM, _timeout_handler)
    signal.alarm(seconds)
    try:
        yield
    finally:
        signal.alarm(0)
        signal.signal(signal.SIGALRM, old_handler)


class ExitOverride:
    def __init__(self):
        self.exit_code = None
        self.exc_info = None
        self.hooked = False

    def hook(self):
        if not self.hooked:
            self._orig_exit = sys.exit
            self._orig_excepthook = sys.excepthook
            sys.exit = self.exit
            sys.excepthook = self.exc_handler
            self.hooked = True
        else:
            warnings.warn("Exit hooks have already been overrided.")

    def unhook(self):
        if self.hooked:
            sys.exit = self._orig_exit
            sys.excepthook = self._orig_excepthook
        else:
            raise ValueError("Exit hooks were not overriden, can't unhook.")

    def exit(self, code=0):
        self.exit_code = code
        self._orig_exit(code)

    def exc_handler(self, *exc_info):
        self.exc_info = exc_info
        self._orig_excepthook(*exc_info)


exit_override = ExitOverride()
exit_override.hook()


@parse_config
def send_mail_on_exit(
    *,
    subject: Optional[str] = None,
    msg_ok: Optional[str] = None,
    msg_err: Optional[str] = None,
    on_error_only: bool = False,
    skip_ctrlc: bool = True,
    **mail_kwargs
):
    subject = subject or "Workflow"
    msg_err = msg_err or 'Workflow exited with some errors.'
    if (
        not on_error_only
        and exit_override.exc_info is None
        and exit_override.exit_code in [None, 0]
    ):
        send_mail(
            subject=subject + " - Success",
            msg=msg_ok or "Workflow exited successfully.",
            **mail_kwargs
        )
    elif exit_override.exc_info is None and (exit_override.exit_code or 0) > 0:
        send_mail(
            subject=subject + " - Success",
            msg=f"{msg_err}\nSystem exited with code {exit_override.exit_code}.",
            **mail_kwargs
        )
    elif (
        exit_override.exc_info is not None
        and (exit_override.exc_info[0] is not KeyboardInterrupt or not skip_ctrlc)
    ):
        tb = ''.join(format_exception(*exit_override.exc_info))
        msg_err = f"{msg_err}\n\n{tb}"
        send_mail(subject=subject + " - Failure", msg=msg_err, **mail_kwargs)


def get_task_checker(tasks, start=0, stop=-1, exclude=None):
    exclude = exclude or []
    if isinstance(start, int):
        start = tasks[start]
    if isinstance(stop, int):
        stop = tasks[stop]

    # Transform into a dictionary where keys are task names and values their rank.
    tasks = dict(map(reversed, enumerate(tasks)))

    def _task_checker(task):
        return tasks[start] <= tasks[task] <= tasks[stop] and task not in exclude

    return _task_checker


@parse_config
class measure_time:
    """Context for timing a code block.

    Parameters
    ----------
    name : str, optiona
      A name to give to the block being timed, for meaningful logging.
    cpu : boolean
      If True, the CPU time is also measured and logged.
    logger : logging.Logger, optional
      The logger object to use when sending Info messages with the measured time.
      Defaults to a logger from this module.
    """

    def __init__(
        self,
        name: Optional[str] = None,
        cpu: bool = False,
        logger: logging.Logger = logger
    ):
        self.name = name or ""
        self.cpu = cpu
        self.logger = logger

    def __enter__(self):
        self.start = time.perf_counter()
        self.start_cpu = time.process_time()
        self.logger.info(f"Started process {self.name}.")
        return

    def __exit__(self, *args, **kwargs):
        elapsed = time.perf_counter() - self.start
        elapsed_cpu = time.process_time() - self.start_cpu
        occ = elapsed_cpu / elapsed
        s = f"Process {self.name} done in {elapsed:.02f} s"
        if self.cpu:
            s += f" and used {elapsed_cpu:.02f} of cpu time ({occ:.1%} % occupancy)."

        self.logger.info(s)
