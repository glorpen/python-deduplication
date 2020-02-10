import fcntl
from filecmp import cmp as file_cmp
import os
import logging
import time
import sys
import ctypes
from ioctl_opt import IOC, IOC_WRITE, IOC_READ
import stat
import hashlib
import contextlib
import pathlib
import argparse

class StructureWithDefaults(ctypes.Structure):
    def __init__(self, **kwargs):
        for k, v in self._defaults_.items():
            kwargs.setdefault(k, v)
        super().__init__(**kwargs)

class DynamicStructure(object):
    def __init__(self, fields):
        super().__init__()
        self.fields = fields
        self._cache = {}

    def get(self, **kwargs):
        key = tuple(kwargs.values())
        if key in self._cache:
            return self._cache[key]
        
        fields = []
        defaults = {}
        for f in self.fields:
            if isinstance(f[1], tuple):
                count = kwargs.get(f[1][0], 0)
                fields.append((f[0], f[1][1] * count))
                defaults[f[1][0]] = count
            else:
                fields.append(f)
        cls = type("dynamic_structure", (StructureWithDefaults,), {"_fields_": fields, "_defaults_": defaults})
        self._cache[key] = cls
        return cls
    def __call__(self, **kwargs):
        return self.get(**kwargs)

class xfs_extent_data_info(ctypes.Structure):
    _fields_ = [
        ("fd", ctypes.c_int64),
        ("logical_offset", ctypes.c_uint64),
        ("bytes_deduped", ctypes.c_uint64),
        ("status", ctypes.c_uint32),
        ("reserved", ctypes.c_int32),
    ]

xfs_extent_data = DynamicStructure([
    ("logical_offset", ctypes.c_uint64),
    ("length", ctypes.c_uint64),
    ("dest_count", ctypes.c_uint16),
    ("reserved1", ctypes.c_uint16),
    ("reserved2", ctypes.c_uint16),
    ("info", ("dest_count", xfs_extent_data_info)),
])

class fiemap_extent(ctypes.Structure):
    _fields_ = [
        ("fe_logical", ctypes.c_uint64),
        ("fe_physical", ctypes.c_uint64),
        ("fe_length", ctypes.c_uint64),
        ("fe_reserved1", ctypes.c_uint64 * 2),
        ("fe_flags", ctypes.c_uint32),
        ("fe_reserved2", ctypes.c_uint32 * 3),
    ]

fiemap = DynamicStructure([
    ("fm_start", ctypes.c_uint64),
    ("fm_length", ctypes.c_uint64),
    ("fm_flags", ctypes.c_uint32),
    ("fm_mapped_extents", ctypes.c_uint32),
    ("fm_extent_count", ctypes.c_uint32),
    ("fm_reserved", ctypes.c_uint32),
    ("fm_extents", ("fm_extent_count", fiemap_extent)),
])

XFS_EXTENT_DATA_SAME = 0
XFS_EXTENT_DATA_DIFFERS = 1

XFS_IOC_FILE_EXTENT_SAME = IOC(IOC_WRITE|IOC_READ, 0x94, 54, ctypes.sizeof(xfs_extent_data()))

logger = logging.getLogger("ioctl")

def dedup_file(src_fd, length, dst_fds):
    req = xfs_extent_data(dest_count=len(dst_fds))()

    src_offset = 0
    dst_offset = 0
    
    for i, fd in enumerate(dst_fds):
        req.info[i].fd = fd

    while length > 0:
        req.logical_offset = src_offset
        req.length = length
        
        for info in req.info:
            info.logical_offset = dst_offset
        # raises OSError if not 0
        fcntl.ioctl(src_fd, XFS_IOC_FILE_EXTENT_SAME, req)

        lowest_deduped_bytes = sys.maxsize
        for info in req.info:
            bytes_deduped = info.bytes_deduped
            status = info.status

            lowest_deduped_bytes = min(lowest_deduped_bytes, bytes_deduped)
            
            if status < 0:
                raise OSError(-status, os.strerror(-status))
            if status == XFS_EXTENT_DATA_DIFFERS:
                raise Exception("File differs")
            if bytes_deduped > length:
                raise Exception("Deduped %d bytes of %d requested" % (bytes_deduped, length))
            print(bytes_deduped)
        
        logger.debug("Deduped %d bytes", lowest_deduped_bytes)
        length -= bytes_deduped
        src_offset += bytes_deduped
        dst_offset += bytes_deduped
        
    return length * len(dst_fds)

FS_IOC_FIEMAP = IOC(IOC_WRITE|IOC_READ, ord('f'), 11, ctypes.sizeof(fiemap()))
FIEMAP_FLAG_SYNC = 1
FIEMAP_EXTENT_LAST = 1

def get_map(fd):
    last_logical = 0
    range_end = sys.maxsize
    extent_batch = 10

    req = fiemap(fm_extent_count=extent_batch)()
    ret = []

    done = False
    while not done:
        req.fm_flags = FIEMAP_FLAG_SYNC
        req.fm_start = last_logical
        req.fm_length = range_end - last_logical
        req.fm_extent_count = extent_batch

        fcntl.ioctl(fd, FS_IOC_FIEMAP, req)

        if req.fm_mapped_extents == 0:
            done = True
            break
        
        for e in req.fm_extents[0:req.fm_mapped_extents]:
            # print([e.fe_logical, e.fe_physical, e.fe_length])
            ret.append((e.fe_physical, e.fe_length))
            last_logical = e.fe_logical + e.fe_length
            if e.fe_flags & FIEMAP_EXTENT_LAST:
                done = True
                break
    return ret

class UnsupportedFileException(Exception):
    pass

class Keyer(object):

    def stat(self, fd, stat=None):
        s = os.fstat(fd) if stat is None else stat
        return {"size": s.st_size}

    def extent_map(self, fd):
        return get_map(fd)

    def get_keys(self, fd, stat=None):
        return {
            "stat": self.stat(fd, stat),
            "extent": self.extent_map(fd)
        }

@contextlib.contextmanager
def fdopen(pathlike, flags):
    fd = os.open(pathlike, flags)
    try:
        yield fd
    finally:
        os.close(fd)

"""
Initial:
INFO:Deduplicator:Deduplicated 1312.34 MBytes
real	4m50,181s
user	4m41,533s
sys	0m8,579s

hashing index by "stat":
INFO:Deduplicator:Deduplicated 1312.34 MBytes
real	0m35,667s
user	0m28,077s
sys	0m7,513s
"""
class Deduplicator(object):

    def __init__(self, pretend=False):
        super().__init__()
        self.logger = logging.getLogger(self.__class__.__qualname__)
        self.pretend = pretend

    def get_info_stat_key(self, info):
        return str(info["stat"])

    def create_index(self, path):
        self.logger.info("Creating index for %r", path)
        keyer = Keyer()
        index = {}
        for root, dummy_, files in os.walk(path):
            for fname in files:
                f = pathlib.Path(root) / fname
                with fdopen(f, os.O_PATH|os.O_NOFOLLOW) as fd:
                    s = os.fstat(fd)
                    if stat.S_IFMT(s.st_mode) != stat.S_IFREG:
                        self.logger.warning("Skipping not regular file at %r", f)
                        continue
                try:
                    with fdopen(f, os.O_RDONLY) as fd:
                        keys = keyer.get_keys(fd, stat=s)
                except OSError as e:
                    self.logger.warning("Skipping bad path %r: %s", f, e)
                    continue
                # hash index by most freq used key
                s = self.get_info_stat_key(keys)
                if s not in index:
                    index[s] = {}
                index[s][f] = keys
        self._index = index

    def _find_similar(self, src_info):
        for p,i in self._index[self.get_info_stat_key(src_info)].items():
            if src_info["stat"] == i["stat"]:
                yield p, src_info["extent"] == i["extent"]

    def handle_single(self, index):
        src_path, src_info = index.popitem()
        self.logger.info("Checking file %r", src_path)

        pending = []
        handled = []
        deduplicated_bytes = 0
        
        for p, extent_matches in self._find_similar(src_info):
            if extent_matches:
                handled.append(p)
            else:
                # TODO: when comparing, calculate some simple hash,
                # so later we can find similar files quickier
                if file_cmp(src_path, p, shallow=False):
                    self.logger.debug("Found duplicate: %r", p)
                    pending.append(p)

        for p in handled:
            index.pop(p)

        if self.pretend:
            for p in pending:
                self.logger.info("Would deduplicate %r", p)
                index.pop(p)
            deduplicated_bytes += len(pending) * src_info["stat"]["size"]
        else:
            if pending:
                offset = 0
                batch_size = 10
                with fdopen(src_path, os.O_RDWR) as src_fd:
                    while True:
                        batch = pending[offset:offset+batch_size]
                        fds = [os.open(i, os.O_RDWR) for i in batch]
                        try:
                            deduplicated_bytes += dedup_file(src_fd, src_info["stat"]["size"], fds)
                        finally:
                            for fd in fds:
                                os.close(fd)
                        # remove handled items from index
                        for b in batch:
                            index.pop(b)
                        if len(batch) != batch_size:
                            break
        return deduplicated_bytes
    
    def run(self, path):
        self.create_index(path)
        deduplicated_bytes = 0
        for k in self._index.keys():
            while self._index[k]:
                deduplicated_bytes += self.handle_single(self._index[k])
        self.logger.info("Deduplicated %.2f MBytes", deduplicated_bytes / 1024 / 1024)

if __name__ == "__main__":
    p = argparse.ArgumentParser()
    p.add_argument("--pretend", "-p", action="store_true")
    p.add_argument("path", action="store", type=pathlib.Path)

    ns = p.parse_args()

    logging.basicConfig(level=logging.INFO)
    d = Deduplicator(pretend=ns.pretend)
    d.run(ns.path)
