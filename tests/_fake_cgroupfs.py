"""A model of the cgroup v2 kernel interface for the limits tests.

:class:`FakeKernel` is a :class:`cronstable._cgroups.CgroupFS` over a real
directory tree under ``tmp_path``, so the backend's reads (and
``_CgroupV2Reader``'s) hit ordinary files.  Each mutating call applies the
kernel rule the backend depends on before it touches them:

* No internal processes: enabling a domain controller on a populated
  non-root cgroup is EBUSY, and so is migrating into a cgroup whose
  ``cgroup.subtree_control`` holds one.  A cgroup-namespace root is not
  exempt; only the real root is.
* Enabling only threaded controllers on a populated cgroup succeeds and
  turns it into a threaded domain, the trap a one-at-a-time enable falls
  into.
* Disabling a controller that a child cgroup still enables for its own
  children is EBUSY.  A controller's interface files (``cpu.max``,
  ``cpu.weight.nice``, ...) exist in a cgroup while its parent enables it.
* Migration moves a pid between ``cgroup.procs`` files and rewrites
  ``/proc/<pid>/cgroup``; an unknown pid is ESRCH.  A pid in ``exiting``
  cannot move, and its write succeeds anyway, as for a task in its exit
  path.
* ``rmdir`` is EBUSY while the cgroup or a child cgroup has processes,
  and while child cgroups remain.
* A read-only mount is EROFS and reports nothing writable; a missing
  interface file is ENOENT.

``faults`` injects errnos by operation and path suffix:
``{("write", "pids.max"): errno.EINVAL}`` fails every matching call;
a list value fails the next matching calls in turn, and a ``0`` in it lets
that call through.  ``refuse`` maps a controller to the errno that
enabling it anywhere fails with, as a kernel with RT group scheduling
refuses ``+cpu`` with EINVAL.

``sleep`` records each pause in ``sleeps`` and then calls ``on_sleep``
with their count, if set; nothing waits.  ``nice`` reads ``nice_value``.

Every file operation holds one lock, as the kernel's ``cgroup_mutex``
serializes cgroupfs, so threaded tests see whole operations.
"""

import errno
import functools
import os
import threading

from cronstable._cgroups import CgroupFS

ENODATA = getattr(errno, "ENODATA", 61)

DOMAIN = frozenset({"memory", "io"})

# cpu.stat lines that only the cpu controller adds
_CPU_STAT_EXTRA = "nr_periods 0\nnr_throttled 0\nthrottled_usec 0\n"


def _put(path, text):
    with open(path, "w", encoding="ascii", newline="\n") as fh:
        fh.write(text)


def _get(path):
    with open(path, encoding="ascii") as fh:
        return fh.read()


def _serialized(method):
    """``method`` under the model's lock."""

    @functools.wraps(method)
    def locked(self, *args, **kwargs):
        with self.mutex:
            return method(self, *args, **kwargs)

    return locked


class FakeKernel(CgroupFS):
    """A cgroupfs and procfs model; see the module docstring."""

    read = _serialized(CgroupFS.read)
    exists = _serialized(CgroupFS.exists)
    isdir = _serialized(CgroupFS.isdir)
    subdirs = _serialized(CgroupFS.subdirs)

    def __init__(
        self,
        tmp_path,
        *,
        own="user.slice/app.scope",
        controllers=("cpu", "memory", "pids"),
        daemon_pid=100,
        others=(),
        euid=1000,
        ns_root=False,
        swap=True,
        kill_file=True,
        page=4096,
    ):
        root = tmp_path / "cgroup"
        proc = tmp_path / "proc"
        super().__init__(str(root), str(proc))
        self.mutex = threading.RLock()
        self.controllers = tuple(controllers)
        self.daemon_pid = daemon_pid
        self.euid = euid
        self.ns_root = ns_root
        self.swap = swap
        self.kill_file = kill_file
        self.page = page
        self.readonly = False
        self.unwritable = set()
        self.xattrs = {}
        self.faults = {}
        self.refuse = {}  # controller -> errno of every enable of it
        # pids that land in a cgroup right before its next enable
        self.arrivals = []
        self.exiting = set()  # pids that stay put when moved
        self.sleeps = []
        self.on_sleep = None
        self.nice_value = 0
        self.members = {}  # pid -> cgroup relative path ("" is the root)
        self.writes = []  # (cgroup, file, data) for every write attempt
        self.killed = []  # (pid, signal or "cgroup.kill")
        self.rmdirs = []  # every rmdir attempt
        self.opened = []
        os.makedirs(os.path.join(self.proc, "self"))
        _put(os.path.join(self.proc, "self", "oom_score_adj"), "0\n")
        os.mkdir(self.root)
        self._populate("", self.controllers, top=True)
        rel = ""
        for part in [p for p in own.split("/") if p]:
            self._set_subtree(rel, set(self.controllers))
            rel = part if not rel else rel + "/" + part
            os.mkdir(self.path(rel))
            self._populate(rel, self.controllers)
        self.own = rel
        for pid in (daemon_pid, *others):
            self.spawn(pid, rel)

    # --- paths --------------------------------------------------------------

    def path(self, rel, name=None):
        parts = [p for p in rel.split("/") if p]
        out = os.path.join(self.root, *parts) if parts else self.root
        return os.path.join(out, name) if name else out

    def rel_of(self, path):
        rel = os.path.relpath(path, self.root)
        return "" if rel == "." else rel.replace(os.sep, "/")

    def file(self, rel, name):
        return _get(self.path(rel, name))

    def set_file(self, rel, name, text):
        _put(self.path(rel, name), text)

    def subtree(self, rel):
        return set(self.file(rel, "cgroup.subtree_control").split())

    # --- processes ----------------------------------------------------------

    @_serialized
    def spawn(self, pid, rel):
        """Start ``pid`` in cgroup ``rel``."""
        old = self.members.get(pid)
        self.members[pid] = rel
        self._sync(pid, old)

    @_serialized
    def exit(self, pid):
        """``pid`` exits and is reaped: it leaves procfs too."""
        old = self.members.pop(pid)
        self._sync_procs(old)
        os.unlink(os.path.join(self.proc, str(pid), "cgroup"))

    @_serialized
    def join(self, pid, path):
        """What the child's ``write(fd, "0")`` does: move it to ``path``."""
        self._migrate(self.rel_of(path), str(pid))

    def cgroup_of(self, pid):
        return self.members.get(pid)

    def _sync(self, pid, old):
        if old is not None:
            self._sync_procs(old)
        rel = self.members[pid]
        self._sync_procs(rel)
        line = "0::/{}\n".format(rel)
        piddir = os.path.join(self.proc, str(pid))
        os.makedirs(piddir, exist_ok=True)
        _put(os.path.join(piddir, "cgroup"), line)
        if pid == self.daemon_pid:
            _put(os.path.join(self.proc, "self", "cgroup"), line)

    def _sync_procs(self, rel):
        if not os.path.isdir(self.path(rel)):
            return
        pids = sorted(p for p, r in self.members.items() if r == rel)
        self.set_file(rel, "cgroup.procs", "".join("%d\n" % p for p in pids))

    def _populated(self, rel, *, subtree=False):
        prefix = rel + "/" if rel else ""
        return any(
            r == rel or (subtree and r.startswith(prefix))
            for r in self.members.values()
        )

    # --- interface files ----------------------------------------------------

    def _populate(self, rel, available, top=False):
        files = {
            "cgroup.procs": "",
            "cgroup.controllers": " ".join(sorted(available)) + "\n",
            "cgroup.subtree_control": "\n",
            "cpu.stat": "usage_usec 0\nuser_usec 0\nsystem_usec 0\n",
        }
        if not top or self.ns_root:
            files["cgroup.type"] = "domain\n"
        if not top and self.kill_file:
            files["cgroup.kill"] = ""
        for name, text in files.items():
            self.set_file(rel, name, text)
        if not top:
            self._controller_files(rel, available)

    def _controller_files(self, rel, controllers):
        files = {}
        if "cpu" in controllers:
            files["cpu.max"] = "max 100000\n"
            files["cpu.weight"] = "100\n"
            files["cpu.weight.nice"] = "0\n"
            stat = self.file(rel, "cpu.stat")
            if "throttled_usec" not in stat:
                files["cpu.stat"] = stat + _CPU_STAT_EXTRA
        if "memory" in controllers:
            files["memory.max"] = "max\n"
            files["memory.current"] = "0\n"
            files["memory.peak"] = "0\n"
            files["memory.oom.group"] = "0\n"
            files["memory.events"] = (
                "low 0\nhigh 0\nmax 0\noom 0\noom_kill 0\noom_group_kill 0\n"
            )
            if self.swap:
                files["memory.swap.max"] = "max\n"
        if "pids" in controllers:
            files["pids.max"] = "max\n"
            files["pids.events"] = "max 0\n"
        for name, text in files.items():
            if name == "cpu.stat" or not os.path.exists(self.path(rel, name)):
                self.set_file(rel, name, text)

    def _set_subtree(self, rel, controllers):
        self.set_file(
            rel, "cgroup.subtree_control", " ".join(sorted(controllers)) + "\n"
        )
        for child in self._children(rel):
            self.set_file(
                child,
                "cgroup.controllers",
                " ".join(sorted(controllers)) + "\n",
            )
            self._controller_files(child, controllers)
            if "cpu" not in controllers:
                self._drop_cpu_files(child)

    def _children(self, rel):
        for name in os.listdir(self.path(rel)):
            child = name if not rel else rel + "/" + name
            if os.path.isdir(self.path(child)):
                yield child

    def _drop_cpu_files(self, rel):
        for name in ("cpu.max", "cpu.weight", "cpu.weight.nice"):
            if os.path.exists(self.path(rel, name)):
                os.unlink(self.path(rel, name))
        stat = self.file(rel, "cpu.stat")
        self.set_file(rel, "cpu.stat", stat.replace(_CPU_STAT_EXTRA, ""))

    # --- faults -------------------------------------------------------------

    def _fault(self, op, path):
        norm = path.replace(os.sep, "/")
        for (fop, suffix), value in self.faults.items():
            if fop != op or not norm.endswith(suffix):
                continue
            if isinstance(value, list):
                if not value:
                    continue
                code = value.pop(0)
                if not code:
                    return
            else:
                code = value
            raise OSError(code, os.strerror(code), path)

    def _mutable(self, op, path):
        self._fault(op, path)
        if self.readonly:
            raise OSError(errno.EROFS, os.strerror(errno.EROFS), path)

    # --- CgroupFS -----------------------------------------------------------

    @_serialized
    def write(self, path, data):
        self._mutable("write", path)
        if not os.path.isfile(path):
            raise OSError(errno.ENOENT, os.strerror(errno.ENOENT), path)
        rel = self.rel_of(os.path.dirname(path))
        name = os.path.basename(path)
        self.writes.append((rel, name, data))
        if name == "cgroup.procs":
            self._migrate(rel, data)
        elif name == "cgroup.subtree_control":
            self._enable(rel, data)
        elif name == "cgroup.kill":
            self._kill_tree(rel)
        elif name == "cpu.max":
            quota = data.split()[0]
            if quota != "max" and int(quota) < 1000:
                raise OSError(errno.EINVAL, os.strerror(errno.EINVAL), path)
            _put(path, data + "\n")
        elif name == "cpu.weight.nice" and not -20 <= int(data) <= 19:
            raise OSError(errno.ERANGE, os.strerror(errno.ERANGE), path)
        else:
            _put(path, data + "\n")

    def _migrate(self, dest, data):
        pid = int(data.strip()) or self.daemon_pid
        if pid not in self.members:
            raise OSError(errno.ESRCH, os.strerror(errno.ESRCH))
        if dest and self.subtree(dest) & DOMAIN:
            raise OSError(errno.EBUSY, os.strerror(errno.EBUSY))
        if pid not in self.exiting:
            self.spawn(pid, dest)

    def _enable(self, rel, data):
        tokens = data.split()
        add = {t[1:] for t in tokens if t.startswith("+")}
        remove = {t[1:] for t in tokens if t.startswith("-")}
        if add - set(self.file(rel, "cgroup.controllers").split()):
            raise OSError(errno.ENOENT, os.strerror(errno.ENOENT))
        for controller in sorted(add):
            if controller in self.refuse:
                code = self.refuse[controller]
                raise OSError(code, os.strerror(code))
        if any(self.subtree(child) & remove for child in self._children(rel)):
            raise OSError(errno.EBUSY, os.strerror(errno.EBUSY))
        if self.arrivals:
            self.spawn(self.arrivals.pop(0), rel)
        exempt = rel == "" and not self.ns_root
        if add and not exempt and self._populated(rel):
            if add & DOMAIN:
                raise OSError(errno.EBUSY, os.strerror(errno.EBUSY))
            self.set_file(rel, "cgroup.type", "domain threaded\n")
        self._set_subtree(rel, (self.subtree(rel) | add) - remove)

    def _kill_tree(self, rel):
        prefix = rel + "/"
        for pid, where in list(self.members.items()):
            if where == rel or where.startswith(prefix):
                self.killed.append((pid, "cgroup.kill"))
                self.exit(pid)

    @_serialized
    def mkdir(self, path):
        self._mutable("mkdir", path)
        if os.path.exists(path):
            raise OSError(errno.EEXIST, os.strerror(errno.EEXIST), path)
        os.mkdir(path)
        parent = self.rel_of(os.path.dirname(path))
        self._populate(self.rel_of(path), self.subtree(parent))

    @_serialized
    def rmdir(self, path):
        self.rmdirs.append(path)
        self._mutable("rmdir", path)
        if not os.path.isdir(path):
            raise OSError(errno.ENOENT, os.strerror(errno.ENOENT), path)
        if self._populated(self.rel_of(path), subtree=True) or self.subdirs(
            path
        ):
            raise OSError(errno.EBUSY, os.strerror(errno.EBUSY), path)
        for name in os.listdir(path):
            os.unlink(os.path.join(path, name))
        os.rmdir(path)

    @_serialized
    def writable(self, path):
        return (
            not self.readonly
            and path not in self.unwritable
            and os.path.exists(path)
        )

    @_serialized
    def getxattr(self, path, name):
        self._fault("getxattr", path)
        try:
            return self.xattrs[(self.rel_of(path), name)]
        except KeyError:
            raise OSError(ENODATA, os.strerror(ENODATA), path) from None

    def geteuid(self):
        return self.euid

    @_serialized
    def open_procs(self, path):
        self._mutable("open", path)
        if not os.path.isfile(path):
            raise OSError(errno.ENOENT, os.strerror(errno.ENOENT), path)
        self.opened.append(path)
        return os.open(os.devnull, os.O_WRONLY)

    @_serialized
    def kill(self, pid, sig):
        self.killed.append((pid, sig))
        if pid not in self.members:
            raise OSError(errno.ESRCH, os.strerror(errno.ESRCH))
        self.exit(pid)

    def page_size(self):
        return self.page

    def nice(self):
        return self.nice_value

    def sleep(self, seconds):
        self.sleeps.append(seconds)
        if self.on_sleep is not None:
            self.on_sleep(len(self.sleeps))
