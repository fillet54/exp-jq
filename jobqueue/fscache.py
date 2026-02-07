"""
`fscache.py` is a tiny filesystem snapshot/cache utility.

It provides two complementary pieces:

- An **integrity index** (`IntegrityIndex`) that scans a working tree and stores,
  for each file, a git-style blob SHA-1 plus a lightweight filesystem
  fingerprint (mtime/ctime/size/inode/etc). The fingerprint lets subsequent
  scans skip re-hashing unchanged files.
- A minimal, git-inspired, **content-addressable object store** under
  `<cache_dir>/objects` plus a simple "tree" object that records `(mode, blob,
  path)` entries. Named snapshots can be recorded under `<cache_dir>/refs/`,
  similar to Git refs, and garbage collection uses refs as the root set.

This is not a full Git implementation: it writes/reads just enough of Git's
object format (header + zlib-compressed payload) to store blobs and a custom
tree representation suitable for restoring snapshots.

It also contains an **object-based sync** implementation (roughly rsync-like)
that transfers cached *objects* instead of re-sending whole files.

**Python compatibility**
- Core library code is Python 2.7+ compatible.
- Remote-side sync is stdlib-only (no third-party dependencies required).
- Host-side CLI uses `docopt`; host-side sync uses `paramiko` for SSH, both
  imported lazily only when needed.

**CLI**
Run `python fscache.py --help` to see the full command list. High-level entry
points:
- `snapshot`: create/update `.fscache` and print the tree SHA
- `restore`: restore a tree SHA into a directory
- `diff` / `verify`: integrity operations using the index
- `sync`: snapshot + push missing objects to a remote over SSH

**Sync protocol (ASCII overview)**

The host snapshots the working tree into a local object store, then only sends
the remote the *objects it is missing*:

    Host                                                     Remote
    ----                                                     ------
    snapshot_tree()  -> tree_sha
    read INDEX       -> host_index

    SSH exec (embedded fscache.py) ------------------------->  remote_sync_main() starts
                                   ready  <------------------  PacketIO.send()
    send host_index + tree_sha ----------------------------->  compute missing SHAs
                                   need_objects <------------  list of SHAs missing locally
    send obj <sha> payloads  ------------------------------->  write into .fscache/objects
    objects_done  ------------------------------------------>  restore_tree(tree_sha)
                                   complete <----------------  tree restored

`remote-sync` is a sentinel command used on the remote side to avoid importing
host-only libraries (like `docopt`/`paramiko`) and to ensure stdout is reserved
for the packet protocol.
"""

import errno
import fnmatch
import hashlib
import io
import json
import os
import sys
import tempfile
import time
import zlib
import shutil
import re
import logging
from collections import namedtuple
from contextlib import contextmanager
from functools import partial
from itertools import chain

try:
    from queue import Queue
except ImportError:  # Python 2.7
    from Queue import Queue

import threading

try:
    TimeoutError
except NameError:  # Python 2.7
    class TimeoutError(RuntimeError):
        pass

PY2 = sys.version_info[0] == 2
if PY2:
    text_type = unicode  # noqa: F821
    binary_type = str
else:
    text_type = str
    binary_type = bytes

try:
    integer_types = (int, long)  # noqa: F821
except NameError:
    integer_types = (int,)


def _coerce_verbosity(value):
    """
    Coerce verbosity into an integer level:
    - 0: silent
    - 1: high-level operations
    - 2+: per-path details
    """
    if value is None:
        env = os.environ.get("FSCACHE_VERBOSE", "")
        if env.strip() == "":
            return 0
        try:
            return int(env)
        except Exception:
            return 1
    if isinstance(value, bool):
        return 1 if value else 0
    if isinstance(value, (text_type, binary_type)):
        s = to_text(value).strip().lower()
        if s in ("0", "false", "no", "off", ""):
            return 0
        if s in ("1", "true", "yes", "on"):
            return 1
        try:
            return int(s)
        except Exception:
            return 1
    if isinstance(value, integer_types):
        return int(value)
    return 1 if value else 0


LOG = logging.getLogger("fscache")
LOG.propagate = False
LOG.setLevel(logging.DEBUG)  # handlers control output


def _level_from_verbose(verbosity):
    verbosity = _coerce_verbosity(verbosity)
    if verbosity >= 2:
        return logging.DEBUG
    if verbosity >= 1:
        return logging.INFO
    return logging.WARNING


def _parse_log_level(value):
    """Parse a log level name/number into a logging level."""
    if value is None:
        env = os.environ.get("FSCACHE_LOG_LEVEL")
        if env is None:
            env = os.environ.get("FSCACHE_VERBOSE")
        if env is None or to_text(env).strip() == "":
            return None
        value = env

    if isinstance(value, integer_types):
        return _level_from_verbose(int(value))

    s = to_text(value).strip().upper()
    if s in ("0", "OFF", "QUIET", "NONE"):
        return logging.WARNING
    if s in ("1",):
        return logging.INFO
    if s in ("2",):
        return logging.DEBUG

    if s in ("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"):
        return getattr(logging, s)
    # Common aliases.
    if s == "WARN":
        return logging.WARNING
    return logging.INFO


def configure_logging(level=None, stream=None):
    """Configure the global `fscache` logger with a single stream handler."""
    if level is None:
        level = logging.WARNING
    if stream is None:
        stream = sys.stdout

    handler_level = level
    for h in list(getattr(LOG, "handlers", [])):
        LOG.removeHandler(h)

    handler = logging.StreamHandler(stream)
    handler.setLevel(handler_level)
    handler.setFormatter(logging.Formatter("%(message)s"))
    LOG.addHandler(handler)
    return LOG


def _human_bytes(num_bytes):
    """Format a byte count as human-readable (e.g., 10.2MiB)."""
    try:
        n = float(num_bytes)
    except Exception:
        return "{0}B".format(num_bytes)
    units = ["B", "KiB", "MiB", "GiB", "TiB", "PiB"]
    unit = units[0]
    for u in units[1:]:
        if n < 1024.0:
            break
        n /= 1024.0
        unit = u
    if unit == "B":
        return "{0}B".format(int(n))
    return "{0:.1f}{1}".format(n, unit)


def to_text(value):
    """Coerce `value` into a unicode/text string (UTF-8 decoding bytes)."""
    if isinstance(value, text_type):
        return value
    if isinstance(value, binary_type):
        return value.decode("utf-8", "replace")
    return text_type(value)


def repo_path_str(path):
    """Normalize a repo/worktree path to an absolute filesystem path string."""
    return os.path.abspath(path)


def mkdir_p(path):
    """Create `path` like `mkdir -p` (no error if it already exists)."""
    if not path:
        return
    try:
        os.makedirs(path)
    except OSError as e:
        if e.errno != errno.EEXIST:
            raise


def read_text(path):
    """Read a UTF-8 text file and return unicode/text."""
    with io.open(path, "r", encoding="utf-8") as f:
        return f.read()


def write_text(path, text):
    """Write unicode/text to a UTF-8 encoded file, creating parent dirs."""
    parent = os.path.dirname(path)
    if parent:
        mkdir_p(parent)
    with io.open(path, "w", encoding="utf-8") as f:
        f.write(to_text(text))


def path_to_posix(path):
    """Convert a local OS path to a `/`-separated relative path string."""
    return path.replace(os.sep, "/")


def path_from_posix(path):
    """Convert a stored `/`-separated relative path to an OS-native path."""
    return path.replace("/", os.sep)


def threaded_map(func, items, max_workers=20):
    """
    A minimal thread-based `map` that works on Python 2.7+ without relying on
    `multiprocessing` or `concurrent.futures`.
    """
    items = list(items)
    if not items:
        return []
    if max_workers <= 1 or len(items) == 1:
        return [func(item) for item in items]

    worker_count = min(int(max_workers), len(items))
    in_q = Queue()
    out = [None] * len(items)

    for idx, item in enumerate(items):
        in_q.put((idx, item))
    for _ in range(worker_count):
        in_q.put(None)

    def worker():
        while True:
            task = in_q.get()
            if task is None:
                return
            idx, item = task
            out[idx] = func(item)

    threads = [threading.Thread(target=worker) for _ in range(worker_count)]
    for t in threads:
        t.daemon = True
        t.start()
    for t in threads:
        t.join()

    return out


def safe_rename(src, dst):
    """
    Rename `src` to `dst`, falling back to copy+unlink on cross-device errors.
    """
    try:
        os.rename(src, dst)
    except OSError as e:
        if e.errno != errno.EXDEV:
            raise
        shutil.copyfile(src, dst)
        try:
            os.unlink(src)
        except OSError:
            pass


def _bin_stdin():
    """Return a binary stdin stream on both Python 2 and 3."""
    return getattr(sys.stdin, "buffer", sys.stdin)


def _bin_stdout():
    """Return a binary stdout stream on both Python 2 and 3."""
    return getattr(sys.stdout, "buffer", sys.stdout)

class PacketIO(object):
    """Framed message transport over file-like byte streams.

    Packet format:
      `<sha1hex> <length> <name>\\n<payload-bytes>`
    """

    def __init__(self, fin, fout):
        self.fin = fin
        self.fout = fout

    def send(self, name, payload):
        payload = payload or b""
        sha_hex = hashlib.sha1(payload).hexdigest()
        header = "{0} {1} {2}\n".format(sha_hex, len(payload), name).encode("utf-8")
        self.fout.write(header)
        if payload:
            self.fout.write(payload)
        try:
            self.fout.flush()
        except Exception:
            pass

    def _read_exact(self, length):
        data = b""
        while len(data) < length:
            chunk = self.fin.read(length - len(data))
            if not chunk:
                raise EOFError("unexpected EOF while reading payload")
            data += chunk
        return data

    def recv(self):
        header = self.fin.readline()
        if not header:
            return None
        try:
            sha_hex, length_str, name = header.decode("utf-8").rstrip("\n").split(" ", 2)
            length = int(length_str)
        except Exception:
            raise RuntimeError("invalid header line: {0!r}".format(header))
        payload = self._read_exact(length) if length else b""
        if hashlib.sha1(payload).hexdigest() != sha_hex:
            raise RuntimeError("sha mismatch for packet {0}".format(name))
        return name, payload


def _encode_need_objects(shas):
    """Encode a list of requested object SHAs as a newline-separated payload."""
    shas = list(shas)
    lines = [str(len(shas))] + shas
    return "\n".join(lines).encode("utf-8")


def _parse_need_objects(payload):
    """Decode the payload created by `_encode_need_objects`."""
    lines = payload.decode("utf-8").splitlines()
    if not lines:
        return []
    count = int(lines[0])
    return lines[1 : 1 + count]


def calculate_sha1(path, chunksize=8192):
    """Compute the git-style blob SHA-1 for a file on disk.

    Git computes blob IDs as SHA-1 over: `b"blob <len>\\0" + <file-bytes>`.
    """
    sha1 = hashlib.sha1()
    size = os.path.getsize(path)
    header = ("blob {0}\x00".format(size)).encode("utf-8")
    sha1.update(header)
    with open(path, "rb") as f:
        while True:
            chunk = f.read(chunksize)
            if not chunk:
                break
            sha1.update(chunk)
    return sha1.hexdigest()


Fingerprint = namedtuple("Fingerprint", "type mode mtime ctime uid gid ino size")
"""A lightweight file identity snapshot used to decide when to re-hash.

Fields:
- `type`: `"file"` or `"link"`
- `mode`: executable bits only (0o000 or 0o111-ish), not full permissions
- `mtime`, `ctime`: timestamps from `stat`
- `uid`, `gid`, `ino`, `size`: selected `stat` fields
"""


class GitObject:
    """A decoded object from the object store.

    `data` is an iterator of decompressed byte chunks (streaming).
    """

    def __init__(self, objtype, length, data):
        self.type = objtype
        self.length = length
        self.data = data

    def __len__(self):
        return self.length

    def __iter__(self):
        return self.data


def read_compressed(fin, block_size=4096):
    """Yield decompressed bytes from a zlib-compressed input stream."""
    zlib_obj = zlib.decompressobj()
    while True:
        chunk = fin.read(block_size)
        if not chunk:
            break
        decompressed_chunk = zlib_obj.decompress(chunk)
        if decompressed_chunk:
            yield decompressed_chunk

    decompressed_chunk = zlib_obj.flush()
    if decompressed_chunk:
        yield decompressed_chunk


def object_header(objtype, length):
    """Return the git-style object header bytes for `objtype` and `length`."""
    return (objtype + "\x20" + str(length) + "\x00").encode()


def stream_length(stream):
    """Compute remaining bytes in a seekable stream without consuming it."""
    cur = stream.tell()
    stream.seek(0, os.SEEK_END)
    length = stream.tell() - cur
    stream.seek(cur)
    return length


@contextmanager
def open_stream(stream_or_path, mode="rb"):
    """Open `stream_or_path` if it's a path; otherwise treat it as a stream."""
    close = False
    stream = stream_or_path
    try:
        # In Python 2.7, builtin file objects are not instances of io.IOBase.
        if not hasattr(stream, "read"):
            stream = open(stream_or_path, mode)
            close = True
        yield stream
    finally:
        if close:
            stream.close()


OBJECT_TYPES = ["blob", "tree", "commit"]


def write_object(repo, objtype, data, block_size=4096):
    """Write an object to `<repo>/objects/` and return `(sha_hex, length)`.

    - `repo`: directory containing an `objects/` subdir
    - `objtype`: `"blob"`, `"tree"`, or `"commit"` (only `"blob"`/`"tree"` are
      used by this module)
    - `data`: a path or binary stream positioned at the start of the payload

    Objects are stored in git's fanout layout: `objects/aa/bb...`.
    """
    return ObjectStore(repo).write_object(objtype, data, block_size=block_size)


@contextmanager
def read_object(repo, sha):
    """Open and stream-decode an object from `<repo>/objects/<sha>`."""
    with ObjectStore(repo).read_object(sha) as obj:
        yield obj


def ensure_repo(repo_path):
    """Ensure `repo_path/objects` exists and return the absolute repo path."""
    repo_path = repo_path_str(repo_path)
    mkdir_p(os.path.join(repo_path, "objects"))
    mkdir_p(os.path.join(repo_path, "refs"))
    return repo_path


@contextmanager
def repo_lock(repo_path, timeout=5):
    """A simple inter-process lock using an exclusive `.lock` file."""
    lock_path = os.path.join(repo_path_str(repo_path), ".lock")
    start = time.time()
    while True:
        try:
            fd = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            os.close(fd)
            break
        except OSError as e:
            if e.errno != errno.EEXIST:
                raise
            if time.time() - start > timeout:
                raise TimeoutError("Could not acquire fscache lock")
            time.sleep(0.1)
    try:
        yield
    finally:
        try:
            os.remove(lock_path)
        except OSError as e:
            if e.errno != errno.ENOENT:
                raise


def _refs_dir(repo_path):
    return os.path.join(repo_path_str(repo_path), "refs")


def _is_safe_ref_name(name):
    name = to_text(name)
    if not name or name.startswith("/") or name.startswith("\\"):
        return False
    parts = [p for p in name.replace("\\", "/").split("/") if p]
    if not parts:
        return False
    if any(p in (".", "..") for p in parts):
        return False
    return True


def _ref_path(repo_path, name):
    if not _is_safe_ref_name(name):
        raise ValueError("invalid ref name: {0!r}".format(name))
    return os.path.join(_refs_dir(repo_path), name.replace("\\", "/"))


def write_ref(repo_path, name, tree_sha):
    """Write/update a ref file pointing at `tree_sha`."""
    ensure_repo(repo_path)
    path = _ref_path(repo_path, name)
    mkdir_p(os.path.dirname(path))
    write_text(path, to_text(tree_sha).strip() + u"\n")


def read_ref(repo_path, name):
    """Read a ref file and return its tree SHA (or None if missing/empty)."""
    path = _ref_path(repo_path, name)
    if not os.path.exists(path):
        return None
    try:
        sha = read_text(path).strip()
    except Exception:
        return None
    return sha or None


def iter_refs(repo_path):
    """Yield `(ref_name, tree_sha)` for all refs under `<repo>/refs`."""
    root = _refs_dir(repo_path)
    if not os.path.exists(root):
        return
    for dirpath, _, files in os.walk(root):
        for fname in files:
            full = os.path.join(dirpath, fname)
            rel = os.path.relpath(full, root).replace(os.sep, "/")
            try:
                sha = read_text(full).strip()
            except Exception:
                continue
            if sha:
                yield rel, sha


def _read_object_bytes(repo, sha):
    """Read an entire object into memory as `(type, bytes)`."""
    return ObjectStore(repo).read_object_bytes(sha)


def _tree_blobs(repo, tree_sha):
    """Return `(mode, blob_sha, rel_path)` entries from a stored tree object."""
    return ObjectStore(repo).read_tree_entries(tree_sha)


def gc_repo(repo_path, keep_last=5, min_age_days=30):
    """Garbage-collect cached objects using refs as the root set.

    Roots:
    - all tree SHAs referenced by `<repo>/refs/**`
    - (optional fallback) if there are no refs and `keep_last > 0`, keeps the
      `keep_last` most-recent tree objects by object-file mtime

    Reachability:
    - all blobs referenced by kept tree objects are retained

    Retention:
    - objects are only eligible for deletion if their object-file mtime is at
      least `min_age_days` days old (default: 30 days)
    """
    ensure_repo(repo_path)
    repo_path = repo_path_str(repo_path)
    objstore = ObjectStore(repo_path)
    min_age_seconds = float(min_age_days) * 24.0 * 60.0 * 60.0
    now = time.time()

    # Determine root tree SHAs from refs.
    root_trees = []
    for _ref_name, sha in iter_refs(repo_path):
        root_trees.append(sha)

    # Optional fallback: keep N newest tree objects if there are no refs.
    if not root_trees and keep_last:
        trees = []
        objects_dir = os.path.join(repo_path, "objects")
        for dirpath, _, files in os.walk(objects_dir):
            for fname in files:
                sha = os.path.basename(dirpath) + fname
                try:
                    with read_object(repo_path, sha) as obj:
                        if obj.type == "tree":
                            st = os.stat(os.path.join(dirpath, fname))
                            trees.append((st.st_mtime, sha))
                except Exception:
                    continue
        trees.sort(reverse=True)
        root_trees = [sha for _mtime, sha in trees[: int(keep_last)]]

    keep_objects = set(root_trees)
    for tree_sha in root_trees:
        for _mode, blob, _rel in objstore.read_tree_entries(tree_sha):
            keep_objects.add(blob)

    objects_dir = os.path.join(repo_path, "objects")
    for dirpath, _, files in os.walk(objects_dir):
        for fname in files:
            obj_path = os.path.join(dirpath, fname)
            sha = os.path.basename(dirpath) + fname
            if sha in keep_objects:
                continue
            try:
                st = os.stat(obj_path)
            except OSError:
                continue
            if (now - st.st_mtime) < min_age_seconds:
                continue
            try:
                os.remove(obj_path)
            except OSError as e:
                if e.errno != errno.ENOENT:
                    raise


def _blob_path(repo_path, sha_hex):
    """Return the object-store path for a given object SHA."""
    return os.path.join(repo_path_str(repo_path), "objects", sha_hex[:2], sha_hex[2:])


class ObjectStore(object):
    """A minimal reader/writer for the git-style `objects/` fanout directory."""

    def __init__(self, repo_path):
        self.repo_path = repo_path_str(repo_path)
        self.objects_dir = os.path.join(self.repo_path, "objects")
        mkdir_p(self.objects_dir)

    def path(self, sha_hex):
        return os.path.join(self.objects_dir, sha_hex[:2], sha_hex[2:])

    def has(self, sha_hex):
        return os.path.exists(self.path(sha_hex))

    def read_object_file(self, sha_hex):
        """Read the raw on-disk object file bytes for `sha_hex`."""
        with open(self.path(sha_hex), "rb") as f:
            return f.read()

    def write_object_file(self, sha_hex, payload):
        """Write raw on-disk object file bytes for `sha_hex` (atomic best-effort)."""
        obj_path = self.path(sha_hex)
        mkdir_p(os.path.dirname(obj_path))
        tmp_path = obj_path + ".tmp"
        with open(tmp_path, "wb") as f:
            f.write(payload)
        safe_rename(tmp_path, obj_path)

    def write_object(self, objtype, data, block_size=4096):
        """Write a decoded object (`objtype` + payload) and return `(sha_hex, length)`."""
        assert objtype in OBJECT_TYPES, "objtype must be one of {0!r}".format(OBJECT_TYPES)
        with open_stream(data, "rb") as stream:
            # Prefer fstat(fileno) when available (real files), but fall back to a
            # seek-based length for in-memory streams like `io.BytesIO`.
            fileno = None
            try:
                fileno = stream.fileno()
            except Exception:
                fileno = None

            if fileno is not None:
                try:
                    length = os.fstat(fileno).st_size - stream.tell()
                except Exception:
                    length = stream_length(stream)
            else:
                length = stream_length(stream)

            header = object_header(objtype, length)
            sha = hashlib.sha1()
            sha.update(header)

            tmp_fd, tmp_path = tempfile.mkstemp(dir=self.objects_dir)
            try:
                with os.fdopen(tmp_fd, "wb") as fout:
                    zlib_obj = zlib.compressobj()
                    fout.write(zlib_obj.compress(header))
                    while True:
                        chunk = stream.read(block_size)
                        if not chunk:
                            break
                        sha.update(chunk)
                        compressed_bytes = zlib_obj.compress(chunk)
                        if compressed_bytes:
                            fout.write(compressed_bytes)

                    compressed_bytes = zlib_obj.flush()
                    if compressed_bytes:
                        fout.write(compressed_bytes)

                sha_hex = sha.hexdigest()
                obj_path = self.path(sha_hex)

                if os.path.exists(obj_path):
                    try:
                        os.remove(tmp_path)
                    except OSError:
                        pass
                    return sha_hex, length

                mkdir_p(os.path.dirname(obj_path))
                safe_rename(tmp_path, obj_path)
                return sha_hex, length
            finally:
                if os.path.exists(tmp_path):
                    try:
                        os.remove(tmp_path)
                    except OSError:
                        pass

    def write_blob(self, path, block_size=4096):
        """Write a blob object from a filesystem path and return `(sha_hex, length)`."""
        return self.write_object("blob", path, block_size=block_size)

    def write_tree(self, entries):
        """Write a tree object from `(mode, blob_sha, rel_path)` entries."""
        formatted = ["'{0}' '{1}' '{2}'".format(mode, sha, rel_path) for mode, sha, rel_path in entries]
        stream = io.BytesIO("\n".join(formatted).encode("utf-8"))
        sha, _length = self.write_object("tree", stream)
        return sha

    def read_object(self, sha_hex):
        """Context manager yielding a decoded `GitObject` for `sha_hex`."""
        @contextmanager
        def _cm():
            path = self.path(sha_hex)
            f = None
            try:
                f = open(path, "rb")
                header_chunks = []
                chunks = read_compressed(f)

                while True:
                    chunk = next(chunks)

                    end_of_header = chunk.find(b"\x00")
                    if end_of_header >= 0:
                        header_chunks.append(chunk[: end_of_header + 1])
                        remaining = chunk[end_of_header + 1 :]

                        header = b"".join(header_chunks).decode("utf-8", "replace")
                        parts = header.split()
                        objtype = parts[0]
                        length = int(parts[1][:-1])

                        yield GitObject(objtype, length, chain([remaining], chunks))
                        break
                    else:
                        header_chunks.append(chunk)
            finally:
                if f:
                    f.close()

        return _cm()

    def read_object_bytes(self, sha_hex):
        """Return `(obj_type, obj_bytes)` for an object (fully materialized)."""
        with self.read_object(sha_hex) as obj:
            data = b"".join(obj.data)
            return obj.type, data

    def read_tree_entries(self, tree_sha):
        """Return `(mode, blob_sha, rel_path)` entries from a tree object."""
        obj_type, data = self.read_object_bytes(tree_sha)
        if obj_type != "tree":
            return []
        blobs = []
        for line in data.decode("utf-8", "replace").splitlines():
            parts = line.strip().strip("'").split("' '")
            if len(parts) == 3:
                mode, sha, rel_path = parts
                blobs.append((mode, sha, rel_path))
        return blobs


def compute_missing_objects(host_index, remote_index, objstore, tree_sha):
    """Compute which object SHAs the remote should request.

    Missing objects are:
    - the `tree_sha` object itself, if absent
    - blob objects for any file whose content differs on the remote (per index),
      or whose object payload is missing from the object store.
    """
    needed_blobs = set()
    for rel_path, (host_sha, _host_fp) in host_index.items():
        remote_entry = remote_index.get(rel_path)
        if remote_entry is None or remote_entry[0] != host_sha:
            needed_blobs.add(host_sha)
        else:
            if not objstore.has(host_sha):
                needed_blobs.add(host_sha)

    missing = []
    if not objstore.has(tree_sha):
        missing.append(tree_sha)
    for sha_hex in sorted(needed_blobs):
        if not objstore.has(sha_hex):
            missing.append(sha_hex)
    return missing


class NexusClient(object):
    """A tiny client for storing/retrieving raw object files from a Nexus URL.

    The Nexus URL is treated as a base prefix. Objects are addressed by SHA in a
    Git-like fanout layout:

      `<nexus_url>/objects/<sha[:2]>/<sha[2:]>`

    The payload is the *raw on-disk object file bytes* (zlib-compressed Git-like
    object format, including header).
    """

    def __init__(self, base_url, timeout=30):
        self.base_url = to_text(base_url).rstrip("/")
        self.timeout = timeout
        try:
            import requests  # lazy import (host-side optional)
        except Exception as e:
            raise RuntimeError("requests is required for --nexus: {0}".format(e))
        self._requests = requests
        self._session = requests.Session()

    def object_url(self, sha_hex):
        return "{0}/objects/{1}/{2}".format(self.base_url, sha_hex[:2], sha_hex[2:])

    def has(self, sha_hex):
        r = self._session.head(self.object_url(sha_hex), timeout=self.timeout)
        if r.status_code == 200:
            return True
        if r.status_code == 404:
            return False
        raise RuntimeError("nexus HEAD {0} -> {1}".format(sha_hex, r.status_code))

    def get(self, sha_hex):
        r = self._session.get(self.object_url(sha_hex), timeout=self.timeout)
        if r.status_code == 200:
            return r.content
        if r.status_code == 404:
            raise KeyError(sha_hex)
        raise RuntimeError("nexus GET {0} -> {1}".format(sha_hex, r.status_code))

    def put(self, sha_hex, payload):
        r = self._session.put(self.object_url(sha_hex), data=payload, timeout=self.timeout)
        if r.status_code in (200, 201, 204):
            return
        raise RuntimeError("nexus PUT {0} -> {1}".format(sha_hex, r.status_code))


def write_tree(repo, entries):
    """Write a tree object from `(mode, sha, rel_path)` entries."""
    return ObjectStore(repo).write_tree(entries)

def file_fingerprint(path):
    """Return a `Fingerprint` for a filesystem path.

    The fingerprint is used as a fast change detector. If it matches the prior
    value, we assume the file contents are unchanged and skip hashing.
    """
    stat = os.lstat(path)
    is_symlink = os.path.islink(path)

    return Fingerprint(
        "link" if is_symlink else "file",
        stat.st_mode & 0o111,
        stat.st_mtime,
        stat.st_ctime,
        stat.st_uid,
        stat.st_gid,
        stat.st_ino,
        stat.st_size
    )

class GitIgnoreSpec(object):
    """A small, gitignore-style ignore matcher.

    Supports:
    - ordered rules (later rules override earlier ones)
    - negation via leading `!`
    - comments via leading `#` (unless escaped as `\\#`)
    - escaping leading `!`/`#` via `\\!` / `\\#`
    - directory-only rules via trailing `/`
    - anchored rules via leading `/`

    This is intentionally a subset of full gitignore behavior, but it supports
    the common pattern:
      `*` then `!keep.txt`
    """

    def __init__(self, rules):
        self.rules = rules

    @staticmethod
    def from_patterns(patterns):
        rules = []
        for raw in patterns or []:
            if raw is None:
                continue
            line = to_text(raw).strip()
            if not line:
                continue
            if line.startswith("\\#") or line.startswith("\\!"):
                line = line[1:]
            elif line.startswith("#"):
                continue

            negated = False
            if line.startswith("!"):
                negated = True
                line = line[1:]
            line = line.strip()
            if not line:
                continue

            directory_only = line.endswith("/")
            if directory_only:
                line = line[:-1]

            anchored = line.startswith("/")
            if anchored:
                line = line[1:]

            # Gitignore patterns use forward slashes.
            line = line.replace("\\", "/")

            regex = GitIgnoreSpec._compile_pattern(line)
            rules.append(
                {
                    "pattern": line,
                    "negated": negated,
                    "directory_only": directory_only,
                    "anchored": anchored,
                    "has_slash": ("/" in line),
                    "regex": regex,
                }
            )
        return GitIgnoreSpec(rules)

    @staticmethod
    def _compile_pattern(pattern):
        # Translate a gitignore-like glob to regex where:
        # - '*' and '?' do not match '/'
        # - '**' matches across '/'
        i = 0
        out = ""
        while i < len(pattern):
            c = pattern[i]
            if c == "*":
                if i + 1 < len(pattern) and pattern[i + 1] == "*":
                    # consume consecutive '*'
                    while i < len(pattern) and pattern[i] == "*":
                        i += 1
                    out += ".*"
                    continue
                out += "[^/]*"
            elif c == "?":
                out += "[^/]"
            elif c == "[":
                j = i + 1
                if j < len(pattern) and pattern[j] in ("!", "^"):
                    j += 1
                if j < len(pattern) and pattern[j] == "]":
                    j += 1
                while j < len(pattern) and pattern[j] != "]":
                    j += 1
                if j >= len(pattern):
                    out += re.escape(c)
                else:
                    stuff = pattern[i + 1 : j]
                    if stuff and stuff[0] in ("!", "^"):
                        stuff = "^" + stuff[1:]
                    stuff = stuff.replace("\\", "\\\\")
                    out += "[" + stuff + "]"
                    i = j
            else:
                out += re.escape(c)
            i += 1
        return re.compile("^" + out + "$")

    def is_ignored(self, rel_path_posix, is_dir=False):
        rel_path_posix = path_to_posix(rel_path_posix)
        # Normalize to a clean relative path without accidentally stripping
        # leading dots from names like `.git/`.
        if rel_path_posix.startswith("./"):
            rel_path_posix = rel_path_posix[2:]
        if rel_path_posix.startswith("/"):
            rel_path_posix = rel_path_posix[1:]
        basename = rel_path_posix.rsplit("/", 1)[-1]
        ignored = False
        for rule in self.rules:
            if rule["directory_only"] and not is_dir:
                continue

            if rule["has_slash"] or rule["anchored"]:
                candidate = rel_path_posix
            else:
                candidate = basename

            if rule["regex"].match(candidate):
                ignored = not rule["negated"]
        return ignored


def filter_files(path, patterns, root_path=None):
    """Yield `(relative_path, direntry)` for files under `path` not ignored.

    Patterns are treated like a very small subset of `.gitignore` matching:
    - directory patterns are recognized by a trailing slash
    - matching checks both the basename and the relative path
    """
    root_path = os.path.abspath(root_path or path)
    scan_root = os.path.abspath(path)
    spec = GitIgnoreSpec.from_patterns(patterns)

    for dirpath, dirnames, filenames in os.walk(scan_root):
        rel_dir = os.path.relpath(dirpath, root_path)
        if rel_dir == os.curdir:
            rel_dir = ""

        # Prune ignored directories in-place.
        kept_dirnames = []
        for dirname in dirnames:
            candidate_rel = os.path.join(rel_dir, dirname) if rel_dir else dirname
            if spec.is_ignored(path_to_posix(candidate_rel), is_dir=True):
                continue
            kept_dirnames.append(dirname)
        dirnames[:] = kept_dirnames

        for filename in filenames:
            rel_path = os.path.join(rel_dir, filename) if rel_dir else filename
            rel_posix = path_to_posix(rel_path)
            if spec.is_ignored(rel_posix, is_dir=False):
                continue
            yield rel_posix, os.path.join(dirpath, filename)


def compute_sha1_and_fingerprint_task(args, progress=None):
    """Worker task for parallel SHA-1+fingerprint computation."""
    base_path, path = args
    full_path = os.path.join(base_path, path_from_posix(path))

    fingerprint = file_fingerprint(full_path)
    sha1 = calculate_sha1(full_path)

    if progress:
        progress(advance=1)

    return path, sha1, fingerprint


class IntegrityIndex:
    """Maintain a cached integrity index for a working tree.

    The index maps relative `Path` -> `(blob_sha1, Fingerprint)` and is stored as
    a plain text file (default `<cache_dir>/INDEX`).

    Typical use:
    - `update()` to write/refresh the index
    - `diff()` to detect added/modified/deleted paths
    - `snapshot_tree()` to store blobs + a tree object in the cache
    - `restore_tree(tree_sha)` to restore a cached tree snapshot to disk
    """

    MAGIC = "INTIDX"
    VERSION = "1"

    def __init__(self, worktree_path, index_path=None, ignore_patterns=None, cache_dir=None, verbose=None):
        self.worktree_path = os.path.abspath(worktree_path)
        self.cache_dir = os.path.abspath(cache_dir) if cache_dir else os.path.join(self.worktree_path, ".fscache")
        self.index_path = os.path.abspath(index_path) if index_path else os.path.join(self.cache_dir, "INDEX")
        self.ignore_patterns = list(ignore_patterns) if ignore_patterns else []
        self.verbose = _coerce_verbosity(verbose)
        # Logging is configured globally (CLI/host/remote entrypoints).

        # Ignore cache directory and index file
        self.ignore_patterns.append("{0}/".format(os.path.basename(self.cache_dir)))
        self.ignore_patterns.append(os.path.basename(self.index_path))


    def generate(self, progress=None):
        """Return an in-memory index without writing it to disk."""
        return self.diff(progress=progress)[0]

    
    def update(self, progress=None):
        """Rebuild and write the current index to `self.index_path`."""
        LOG.info("fscache: updating index {0}".format(self.index_path))
        index = self.generate(progress=progress)
        IntegrityIndex.write(index, self.index_path)
        return index

    def diff(self, index_path=None, progress=None):
        """Compare the on-disk index against the current filesystem state.

        Returns `(index, added, modified, deleted)` where:
        - `index` is the updated mapping (in-memory)
        - `added`/`deleted` are lists of relative `Path`
        - `modified` is a list of relative `Path` whose content hash changed
        """
        LOG.info("fscache: scanning {0}".format(self.worktree_path))
        index = IntegrityIndex.read(index_path or self.index_path)

        added = set()
        modified = []
        deleted = set(index.keys())

        calc_sha = []

        files = list(filter_files(self.worktree_path, self.ignore_patterns, root_path=self.worktree_path))

        if progress:
            progress.reset(start=True, total=len(files))

        for rel_path, full_path in files:
            deleted.discard(rel_path)

            if rel_path not in index:
                added.add(rel_path)
                calc_sha.append((self.worktree_path, rel_path))
            else:
                is_fingerprint = file_fingerprint(full_path)
                _, was_fingerprint = index[rel_path]

                if is_fingerprint != was_fingerprint:
                    calc_sha.append((self.worktree_path, rel_path))
                elif progress:
                    progress(advance=1)

        if calc_sha:
            results = threaded_map(
                partial(compute_sha1_and_fingerprint_task, progress=progress),
                calc_sha,
                max_workers=20,
            )
            for rel_path, is_sha1, is_fingerprint in results:
                if rel_path not in added:
                    was_sha1, _ = index[rel_path]
                    if is_sha1 != was_sha1:
                        modified.append(rel_path)

                index[rel_path] = (is_sha1, is_fingerprint)

        index = dict((k, v) for k, v in index.items() if k not in deleted)
        added = list(added)
        deleted = list(deleted)

        LOG.info(
            "fscache: diff added={0} modified={1} deleted={2}".format(
                len(added), len(modified), len(deleted)
            )
        )
        for p in sorted(added):
            LOG.debug("fscache: + {0}".format(p))
        for p in sorted(modified):
            LOG.debug("fscache: ~ {0}".format(p))
        for p in sorted(deleted):
            LOG.debug("fscache: - {0}".format(p))

        return index, added, modified, deleted

    @staticmethod
    def write(index, index_path):
        """Serialize `index` to `index_path`."""
        lines = [to_text(IntegrityIndex.MAGIC), to_text(IntegrityIndex.VERSION)]

        for path in sorted(index.keys()):
            sha1, fp = index[path]
            fields = [
                fp.type,
                format(fp.mode, "03o"),
                "{0:>20}".format(fp.mtime),
                "{0:>20}".format(fp.ctime),
                fp.uid,
                fp.gid,
                fp.ino,
                "{0:<12}".format(fp.size),
                sha1,
                path
            ]
            lines.append(u" ".join(to_text(field) for field in fields))

        write_text(index_path, u"\n".join(lines))


    @staticmethod
    def loads(text):
        """Parse an INDEX file payload into an index mapping.

        This is the canonical parser for the INDEX *format* and is used both for
        reading from disk (`read`) and decoding payloads received over the sync
        protocol.
        """
        parts = to_text(text).splitlines()
        if len(parts) < 2:
            raise ValueError("index too short")
        magic = parts[0]
        version = parts[1]
        if magic != IntegrityIndex.MAGIC or version != IntegrityIndex.VERSION:
            raise ValueError("index header mismatch")

        index = {}
        for line in parts[2:]:
            if not line.strip():
                continue
            _type, mode, mtime, ctime, uid, gid, ino, size, sha1, pathstr = line.split(None, 9)
            fp = Fingerprint(
                _type,
                int(mode, 8),
                float(mtime),
                float(ctime),
                int(uid),
                int(gid),
                int(ino),
                int(size),
            )
            index[pathstr] = (sha1, fp)
        return index

    @staticmethod
    def loads_bytes(payload):
        """Parse an INDEX payload received as bytes."""
        if isinstance(payload, binary_type):
            return IntegrityIndex.loads(payload.decode("utf-8", "replace"))
        return IntegrityIndex.loads(payload)

    @staticmethod
    def read(index_path):
        """Read and validate an index file, returning an empty mapping on failure."""
        if not os.path.exists(index_path):
            return {}

        try:
            return IntegrityIndex.loads(read_text(index_path))
        except Exception:
            try:
                os.unlink(index_path)
            except Exception:
                pass
            return {}


    def verify_tree(self, was_index_path, update_in_place=True, progress=None):
        """Verify current tree against an existing index file.

        Returns `(is_same, added, modified, deleted)`.
        """
        was_index = IntegrityIndex.read(was_index_path)

        if update_in_place:
            is_index = self.update(progress=progress)
        else:
            is_index = self.generate(progress=progress)

        added = []
        modified = []
        deleted = set(was_index.keys())

        for is_path in sorted(is_index.keys()):
            is_sha1, _ = is_index[is_path]

            deleted.discard(is_path)

            if is_path not in was_index:
                added.append(is_path)
            else:
                was_sha1, _ = was_index[is_path]

                if is_sha1 != was_sha1:
                    modified.append(is_path)

        is_same = len(added + modified + list(deleted)) == 0

        return is_same, added, modified, list(deleted)


    def restore_tree(self, tree_sha, dest_dir=None, gc_after=False, gc_keep_last=5, progress=None, nexus_url=None):
        """
        Restore a tree snapshot into a destination directory, reusing the saved
        integrity index when possible to avoid rewrites and unnecessary hashing.
        """
        repo = ensure_repo(self.cache_dir)
        objstore = ObjectStore(repo)
        nexus = NexusClient(nexus_url) if nexus_url else None

        if nexus and not objstore.has(tree_sha):
            LOG.info("fscache: nexus fetch tree {0}".format(tree_sha))
            objstore.write_object_file(tree_sha, nexus.get(tree_sha))
        dest_root = os.path.abspath(dest_dir) if dest_dir else self.worktree_path
        dest_cache_dir = self.cache_dir if dest_root == self.worktree_path else os.path.join(dest_root, ".fscache")
        dest_index_path = self.index_path if dest_root == self.worktree_path else os.path.join(dest_cache_dir, "INDEX")

        mkdir_p(dest_root)
        mkdir_p(dest_cache_dir)

        current_index = IntegrityIndex.read(dest_index_path)
        target_entries = objstore.read_tree_entries(tree_sha)
        target_map = dict((rel, (mode, sha)) for mode, sha, rel in target_entries)
        to_delete = set(current_index.keys()) - set(target_map.keys())

        LOG.info("fscache: restore tree {0} -> {1}".format(tree_sha, dest_root))
        LOG.debug("fscache: target files {0}, deletes {1}".format(len(target_map), len(to_delete)))

        if progress:
            progress.reset(start=True, total=len(target_map) + len(to_delete))

        new_index = {}
        reused = 0
        written = 0
        for rel_path in sorted(target_map.keys()):
            mode_str, blob_sha = target_map[rel_path]
            if nexus and not objstore.has(blob_sha):
                LOG.debug("fscache: nexus fetch blob {0} {1}".format(blob_sha, rel_path))
                objstore.write_object_file(blob_sha, nexus.get(blob_sha))
            full_path = os.path.join(dest_root, path_from_posix(rel_path))
            existing_entry = current_index.get(rel_path)
            reuse = False

            if existing_entry:
                existing_sha, existing_fp = existing_entry
                if existing_sha == blob_sha:
                    try:
                        if file_fingerprint(full_path) == existing_fp:
                            new_index[rel_path] = (blob_sha, existing_fp)
                            reuse = True
                            reused += 1
                            if progress:
                                progress(advance=1)
                    except OSError as e:
                        if e.errno != errno.ENOENT:
                            raise

            if reuse:
                LOG.debug("fscache: = {0}".format(rel_path))
                continue

            _, blob_data = objstore.read_object_bytes(blob_sha)
            mkdir_p(os.path.dirname(full_path))
            with open(full_path, "wb") as f:
                f.write(blob_data)
            try:
                os.chmod(full_path, int(mode_str, 8))
            except Exception:
                pass
            fp = file_fingerprint(full_path)
            new_index[rel_path] = (blob_sha, fp)
            written += 1
            LOG.debug("fscache: -> {0}".format(rel_path))
            if progress:
                progress(advance=1)

        deleted_count = 0
        for rel_path in to_delete:
            full_path = os.path.join(dest_root, path_from_posix(rel_path))
            try:
                os.unlink(full_path)
            except OSError as e:
                if e.errno == errno.ENOENT:
                    pass
                else:
                    pass
            else:
                deleted_count += 1
                LOG.debug("fscache: x {0}".format(rel_path))
            if progress:
                progress(advance=1)

        IntegrityIndex.write(new_index, dest_index_path)

        LOG.info(
            "fscache: restore done reused={0} written={1} deleted={2}".format(
                reused, written, deleted_count
            )
        )

        if gc_after:
            LOG.info("fscache: gc keep_last={0}".format(gc_keep_last))
            gc_repo(repo, keep_last=gc_keep_last)

        return dest_root


    def snapshot_tree(self, gc_after=False, gc_keep_last=5, progress=None, nexus_url=None, ref=None):
        """
        Snapshot the worktree into a git-style object store.

        Existing integrity index entries already carry blob-style SHA1s, so we
        can reuse them directly and only write missing blob objects.
        """
        repo = ensure_repo(self.cache_dir)
        objstore = ObjectStore(repo)
        nexus = NexusClient(nexus_url) if nexus_url else None
        LOG.info("fscache: snapshot {0} -> {1}".format(self.worktree_path, repo))
        index = self.update(progress=progress)

        entries = []
        wrote_blobs = 0
        snapshot_shas = set()
        for rel_path in sorted(index.keys()):
            blob_sha, _ = index[rel_path]
            if not objstore.has(blob_sha):
                blob_sha, _ = objstore.write_blob(os.path.join(self.worktree_path, path_from_posix(rel_path)))
                wrote_blobs += 1
                LOG.debug("fscache: store + {0} {1}".format(blob_sha, rel_path))

            mode = oct(os.stat(os.path.join(self.worktree_path, path_from_posix(rel_path))).st_mode)[2:]
            entries.append((mode, blob_sha, rel_path))
            snapshot_shas.add(blob_sha)

        tree_sha = objstore.write_tree(entries)
        snapshot_shas.add(tree_sha)

        LOG.info("fscache: tree {0} blobs_written={1}".format(tree_sha, wrote_blobs))

        if ref:
            write_ref(repo, ref, tree_sha)
            LOG.info("fscache: ref {0} -> {1}".format(ref, tree_sha))

        if nexus:
            LOG.info("fscache: nexus upload objects={0}".format(len(snapshot_shas)))
            uploaded = 0
            uploaded_bytes = 0
            for sha_hex in sorted(snapshot_shas):
                if nexus.has(sha_hex):
                    continue
                payload = objstore.read_object_file(sha_hex)
                nexus.put(sha_hex, payload)
                uploaded += 1
                uploaded_bytes += len(payload)
                LOG.debug("fscache: nexus put {0} bytes={1}".format(sha_hex, _human_bytes(len(payload))))
            LOG.info("fscache: nexus uploaded objects={0} bytes={1}".format(uploaded, _human_bytes(uploaded_bytes)))

        if gc_after:
            LOG.info("fscache: gc keep_last={0}".format(gc_keep_last))
            gc_repo(repo, keep_last=gc_keep_last)

        return tree_sha


def snapshot_tree(
    rootdir,
    cache_dir=".fscache",
    gc_after=False,
    gc_keep_last=5,
    progress=None,
    nexus_url=None,
    ref=None,
    ignore_patterns=None,
    verbose=None,
):
    """Back-compat helper: snapshot `rootdir` and return a tree SHA."""
    ii = IntegrityIndex(
        rootdir,
        cache_dir=cache_dir,
        ignore_patterns=ignore_patterns,
        verbose=verbose,
    )
    return ii.snapshot_tree(
        gc_after=gc_after,
        gc_keep_last=gc_keep_last,
        progress=progress,
        nexus_url=nexus_url,
        ref=ref,
    )


def restore_tree(
    rootdir,
    tree_sha,
    dest_dir=None,
    cache_dir=".fscache",
    gc_after=False,
    gc_keep_last=5,
    progress=None,
    nexus_url=None,
    ignore_patterns=None,
    verbose=None,
):
    """Back-compat helper: restore `tree_sha` for `rootdir` into `dest_dir`."""
    ii = IntegrityIndex(
        rootdir,
        cache_dir=cache_dir,
        ignore_patterns=ignore_patterns,
        verbose=verbose,
    )
    return ii.restore_tree(
        tree_sha=tree_sha,
        dest_dir=dest_dir,
        gc_after=gc_after,
        gc_keep_last=gc_keep_last,
        progress=progress,
        nexus_url=nexus_url,
    )


def remote_sync_main(remote_root):
    """
    Remote-side entrypoint for object-based sync.

    Runs without any third-party dependencies. This function is invoked via the
    `remote-sync` sentinel CLI command, and communicates over stdin/stdout.
    """
    level = _parse_log_level(os.environ.get("FSCACHE_REMOTE_LOG_LEVEL") or os.environ.get("FSCACHE_REMOTE_VERBOSE"))
    configure_logging(level=level or logging.WARNING, stream=sys.stderr)
    remote_root = os.path.abspath(os.path.expanduser(remote_root))
    mkdir_p(remote_root)

    cache_dir = os.path.join(remote_root, ".fscache")
    objstore = ObjectStore(cache_dir)

    io = PacketIO(_bin_stdin(), _bin_stdout())
    LOG.info("remote-sync: start root={0}".format(remote_root))
    io.send("ready", remote_root.encode("utf-8"))
    LOG.info("remote-sync: sent ready")

    pkt = io.recv()
    if pkt is None or pkt[0] != "host_index":
        io.send("error", b"expected host_index")
        return 2
    host_index = IntegrityIndex.loads_bytes(pkt[1])
    LOG.info("remote-sync: received host_index entries={0}".format(len(host_index)))
    if LOG.isEnabledFor(logging.INFO):
        try:
            total_bytes = sum(fp.size for _sha, fp in host_index.values())
        except Exception:
            total_bytes = 0
        LOG.info("remote-sync: host tree files={0} bytes={1}".format(len(host_index), _human_bytes(total_bytes)))

    pkt = io.recv()
    if pkt is None or pkt[0] != "tree_sha":
        io.send("error", b"expected tree_sha")
        return 2
    tree_sha = pkt[1].decode("utf-8").strip()
    LOG.info("remote-sync: received tree_sha={0}".format(tree_sha))

    dest_index_path = os.path.join(cache_dir, "INDEX")
    remote_index = IntegrityIndex.read(dest_index_path)
    LOG.info("remote-sync: loaded remote index entries={0}".format(len(remote_index)))

    t0 = time.time()
    missing = compute_missing_objects(host_index, remote_index, objstore, tree_sha)
    LOG.info("remote-sync: computed missing objects count={0} in {1:.3f}s".format(len(missing), time.time() - t0))
    io.send("need_objects", _encode_need_objects(missing))
    for sha in missing:
        LOG.debug("remote-sync: need {0}".format(sha))

    received = 0
    bytes_received = 0
    while True:
        msg = io.recv()
        if msg is None:
            io.send("error", b"unexpected EOF")
            return 2
        name, payload = msg
        if name == "objects_done":
            break
        if not name.startswith("obj "):
            io.send("error", to_text("unexpected packet {0}".format(name)).encode("utf-8"))
            return 2
        sha_hex = name.split(" ", 1)[1].strip()
        LOG.debug("remote-sync: recv obj {0} bytes={1}".format(sha_hex, _human_bytes(len(payload))))
        objstore.write_object_file(sha_hex, payload)
        received += 1
        bytes_received += len(payload)

    ii = IntegrityIndex(remote_root, cache_dir=cache_dir, verbose=0)
    LOG.info("remote-sync: restoring tree {0}".format(tree_sha))
    t1 = time.time()
    ii.restore_tree(tree_sha, dest_dir=remote_root)
    LOG.info("remote-sync: restore done in {0:.3f}s".format(time.time() - t1))
    io.send("complete", tree_sha.encode("utf-8"))
    LOG.info("remote-sync: complete objects_received={0} bytes_received={1}".format(received, _human_bytes(bytes_received)))
    return 0


def host_sync_tree(
    local_dir,
    remote_dir,
    host,
    user,
    password=None,
    key_path=None,
    python_exe="python",
    verbose=1,
    ignore_patterns=None,
):
    """
    Host-side sync: snapshot the local tree, then push missing objects to remote over SSH.
    """
    local_dir = os.path.abspath(os.path.expanduser(local_dir))
    if not os.path.isdir(local_dir):
        raise SystemExit("Local path {0} is not a directory".format(local_dir))

    configure_logging(level=_level_from_verbose(verbose), stream=sys.stdout)
    ii = IntegrityIndex(local_dir, ignore_patterns=ignore_patterns, verbose=verbose)
    t_total = time.time()
    t0 = time.time()
    tree_sha = ii.snapshot_tree()
    LOG.info("sync: snapshot tree_sha={0} in {1:.3f}s".format(tree_sha, time.time() - t0))
    local_cache_dir = os.path.join(local_dir, ".fscache")
    local_index_path = os.path.join(local_cache_dir, "INDEX")

    with open(__file__, "rb") as f:
        fscache_src = f.read()
    with open(local_index_path, "rb") as f:
        host_index = f.read()
    host_index_map = IntegrityIndex.loads_bytes(host_index)
    host_file_count = len(host_index_map)

    try:
        import base64
        import shlex
        import paramiko
    except Exception as e:
        raise RuntimeError("Missing host dependency: {0}".format(e))

    src_b64 = base64.b64encode(fscache_src).decode("ascii")
    remote_runner = (
        "import base64,sys; "
        "src=base64.b64decode('{b64}'); "
        "src = src.decode('utf-8') if not isinstance(src, str) else src; "
        "import os; os.environ['FSCACHE_REMOTE_LOG_LEVEL']='{rl}'; "
        "sys.argv=['fscache.py','remote-sync',sys.argv[1]]; "
        "g={{'__name__':'__main__'}}; "
        "exec(compile(src, '<fscache>', 'exec'), g, g)"
    ).format(
        b64=src_b64,
        rl=("DEBUG" if int(verbose) >= 2 else ("INFO" if int(verbose) >= 1 else "WARNING")),
    )
    cmd = "{py} -u -c {code} {remote}".format(
        py=shlex.quote(python_exe),
        code=shlex.quote(remote_runner),
        remote=shlex.quote(remote_dir),
    )
    LOG.info("sync: ssh exec python={0} host={1} remote_dir={2}".format(python_exe, host, remote_dir))

    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(host, username=user, password=password, key_filename=key_path)
    try:
        transport = ssh.get_transport()
        chan = transport.open_session()
        chan.exec_command(cmd)
        stdin = chan.makefile("wb")
        stdout = chan.makefile("rb")
        stderr = chan.makefile_stderr("rb")

        io = PacketIO(stdout, stdin)

        ready = io.recv()
        if ready is None or ready[0] != "ready":
            err = ""
            try:
                err = stderr.read().decode("utf-8", "replace")
            except Exception:
                pass
            raise RuntimeError("remote did not signal readiness; stderr:\n{0}".format(err))
        LOG.info("sync: remote ready root={0}".format(ready[1].decode("utf-8", "replace")))

        t1 = time.time()
        io.send("host_index", host_index)
        io.send("tree_sha", tree_sha.encode("utf-8"))
        LOG.info("sync: sent host_index+tree_sha in {0:.3f}s".format(time.time() - t1))

        response = io.recv()
        if response is None or response[0] != "need_objects":
            raise RuntimeError("remote did not request objects")
        need_shas = _parse_need_objects(response[1])
        LOG.info("sync: remote requests objects count={0}".format(len(need_shas)))
        for sha in need_shas:
            LOG.debug("sync: need {0}".format(sha))

        local_store = ObjectStore(local_cache_dir)
        bytes_sent = 0
        t2 = time.time()
        for sha_hex in need_shas:
            payload = local_store.read_object_file(sha_hex)
            io.send("obj {0}".format(sha_hex), payload)
            bytes_sent += len(payload)
            LOG.debug("sync: sent obj {0} bytes={1}".format(sha_hex, _human_bytes(len(payload))))

        io.send("objects_done", b"")
        LOG.info(
            "sync: sent objects_done objects={0} bytes={1} in {2:.3f}s".format(
                len(need_shas), _human_bytes(bytes_sent), time.time() - t2
            )
        )

        complete = io.recv()
        if complete is None or complete[0] != "complete":
            raise RuntimeError("sync did not complete")
        LOG.info("sync: complete tree_sha={0}".format(complete[1].decode("utf-8", "replace")))

        stdin.close()
        exit_status = chan.recv_exit_status()
        if exit_status:
            err = stderr.read().decode("utf-8", "replace")
            raise RuntimeError("remote exited with status {0}: {1}".format(exit_status, err))
        sys.stdout.write(
            "sync: summary files={0} objects_sent={1} bytes_sent={2} total_time={3:.3f}s\n".format(
                host_file_count, len(need_shas), _human_bytes(bytes_sent), time.time() - t_total
            )
        )
        return complete[1].decode("utf-8").strip()
    finally:
        ssh.close()


CLI_USAGE = """Usage:
  fscache.py diff <path> [--cache-dir=<dir>] [--index=<path>] [--log-level=<lvl>]
  fscache.py verify <path> <index_path> [--no-update] [--log-level=<lvl>]
  fscache.py snapshot <path> [--log-level=<lvl>] [--nexus=<url>] [--ref=<name>]
  fscache.py restore <path> <tree_sha> [--dest=<dir>] [--log-level=<lvl>] [--nexus=<url>]
  fscache.py sync <local_dir> <remote_dir> --host=<host> --user=<user> [--password=<pwd>] [--key=<key>] [--python=<py>] [--log-level=<lvl>]
  fscache.py remote-sync <remote_dir>

Options:
  --cache-dir=<dir>   Cache directory (default: <path>/.fscache)
  --index=<path>      Index file path (default: <cache-dir>/INDEX)
  --dest=<dir>        Restore destination directory (default: <path>)
  --log-level=<lvl>   Log level: `WARNING` (default), `INFO`, or `DEBUG` (aliases: `1`=INFO, `2`=DEBUG).
  --python=<py>       Remote python executable [default: python].
  --ignore-file=<p>   Load ignore rules from this file (default: `<root>/.fsignore` when present).
  --nexus=<url>       Base URL for a Nexus raw object store.
  --ref=<name>        Write/update a ref in `<cache-dir>/refs/<name>` pointing at the snapshot tree.
"""


def cli_main(argv=None):
    """
    Host-side CLI entrypoint.

    The `remote-sync` sentinel command is used on the remote side and must not
    import any third-party libraries (docopt/paramiko).
    """
    argv = list(sys.argv[1:] if argv is None else argv)
    if argv and argv[0] == "remote-sync":
        remote_dir = argv[1] if len(argv) > 1 else "."
        return remote_sync_main(remote_dir)

    try:
        from docopt import docopt
    except Exception as e:
        raise RuntimeError("docopt is required for CLI usage: {0}".format(e))

    args = docopt(CLI_USAGE, argv=argv)

    # Back-compat: allow old `--verbose` if present in argv even though it's not in docopt usage anymore.
    log_level = args.get("--log-level")
    if log_level is None:
        for a in argv:
            if a.startswith("--verbose"):
                parts = a.split("=", 1)
                log_level = parts[1] if len(parts) == 2 else "1"
                break

    level = _parse_log_level(log_level)
    configure_logging(level=level or logging.WARNING, stream=sys.stdout)

    def _load_ignore_patterns(worktree_root):
        patterns = [".git/", ".venv/"]
        ignore_file = args.get("--ignore-file")
        if ignore_file:
            ignore_file = os.path.abspath(os.path.expanduser(ignore_file))
            if not os.path.exists(ignore_file):
                raise RuntimeError("ignore file not found: {0}".format(ignore_file))
            patterns.extend(read_text(ignore_file).splitlines())
            return patterns

        default_path = os.path.join(os.path.abspath(worktree_root), ".fsignore")
        if os.path.exists(default_path):
            patterns.extend(read_text(default_path).splitlines())
        return patterns

    if args.get("diff"):
        path = args["<path>"]
        cache_dir = args.get("--cache-dir") or os.path.join(os.path.abspath(path), ".fscache")
        index_path = args.get("--index") or os.path.join(cache_dir, "INDEX")
        ii = IntegrityIndex(
            path,
            cache_dir=cache_dir,
            index_path=index_path,
            verbose=None,
            ignore_patterns=_load_ignore_patterns(path),
        )
        _index, added, modified, deleted = ii.diff()
        sys.stdout.write("added {0}\nmodified {1}\ndeleted {2}\n".format(len(added), len(modified), len(deleted)))
        for p in sorted(added):
            sys.stdout.write("+ {0}\n".format(p))
        for p in sorted(modified):
            sys.stdout.write("~ {0}\n".format(p))
        for p in sorted(deleted):
            sys.stdout.write("- {0}\n".format(p))
        return 0

    if args.get("verify"):
        path = args["<path>"]
        was_index_path = args["<index_path>"]
        update_in_place = not args.get("--no-update")
        ii = IntegrityIndex(path, verbose=None, ignore_patterns=_load_ignore_patterns(path))
        same, added, modified, deleted = ii.verify_tree(was_index_path, update_in_place=update_in_place)
        sys.stdout.write("same {0}\n".format("yes" if same else "no"))
        for p in added:
            sys.stdout.write("+ {0}\n".format(p))
        for p in modified:
            sys.stdout.write("~ {0}\n".format(p))
        for p in deleted:
            sys.stdout.write("- {0}\n".format(p))
        return 0 if same else 1

    if args.get("snapshot"):
        path = args["<path>"]
        ii = IntegrityIndex(path, verbose=None, ignore_patterns=_load_ignore_patterns(path))
        tree_sha = ii.snapshot_tree(nexus_url=args.get("--nexus"), ref=args.get("--ref"))
        sys.stdout.write("{0}\n".format(tree_sha))
        return 0

    if args.get("restore"):
        path = args["<path>"]
        tree_sha = args["<tree_sha>"]
        dest = args.get("--dest")
        ii = IntegrityIndex(path, verbose=None, ignore_patterns=_load_ignore_patterns(path))
        ii.restore_tree(tree_sha, dest_dir=dest, nexus_url=args.get("--nexus"))
        return 0

    if args.get("sync"):
        local_dir = args["<local_dir>"]
        remote_dir = args["<remote_dir>"]
        host = args["--host"]
        user = args["--user"]
        password = args.get("--password")
        key_path = args.get("--key")
        python_exe = args.get("--python") or "python"
        host_sync_tree(
            local_dir,
            remote_dir,
            host=host,
            user=user,
            password=password,
            key_path=key_path,
            python_exe=python_exe,
            verbose=1 if (level == logging.INFO) else (2 if level == logging.DEBUG else 0),
            ignore_patterns=_load_ignore_patterns(local_dir),
        )
        return 0

    return 0


if __name__ == "__main__":
    raise SystemExit(cli_main())
