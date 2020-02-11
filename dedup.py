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
import multiprocessing.pool
import math
import itertools

@contextlib.contextmanager
def fdopen(pathlike, flags):
    fd = os.open(pathlike, flags)
    try:
        yield fd
    finally:
        os.close(fd)

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

            if status < 0:
                raise OSError(-status, os.strerror(-status))
            if status == XFS_EXTENT_DATA_DIFFERS:
                raise Exception("File differs")
            if bytes_deduped > length:
                raise Exception("Deduped %d bytes of %d requested" % (bytes_deduped, length))
            
            lowest_deduped_bytes = min(lowest_deduped_bytes, bytes_deduped)
        
        logger.debug("Deduped %d bytes", lowest_deduped_bytes)
        length -= lowest_deduped_bytes
        src_offset += lowest_deduped_bytes
        dst_offset += lowest_deduped_bytes
        
    return length * len(dst_fds)

FS_IOC_FIEMAP = IOC(IOC_WRITE|IOC_READ, ord('f'), 11, ctypes.sizeof(fiemap()))
FIEMAP_FLAG_SYNC = 1
FIEMAP_EXTENT_LAST = 1

def get_extent_map(fd, range_end = None):
    last_logical = 0
    if range_end is None:
        range_end = sys.maxsize
    
    # buffer to store extents info from kernel
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
    
    return tuple(ret)

class Progress(object):
    def __init__(self, on_update):
        super().__init__()
        self._state = {}
        self.on_update = on_update
    
    def update(self, **kwargs):
        for k,v in kwargs.items():
            if k.endswith("__add"):
                k = k[:-5]
                self._state[k] += v
            else:
                self._state[k] = v
        if self.on_update:
            self.on_update(**self._state)
    
    def __getattr__(self, name):
        return self._state[name]

class Index(object):
    BUFSIZE = 1024 * 1024

    def __init__(self):
        super().__init__()
        self.logger = logging.getLogger(self.__class__.__qualname__)
        self._path_by_size = {}
        self._hash_by_extent = {}
        self._info_by_path = {}

    def put(self, path, info):
        self._info_by_path[path] = info

        # index by most freq used key
        size = info["size"]
        if size not in self._path_by_size:
            self._path_by_size[size] = []
        self._path_by_size[size].append(path)
    
    def _get_info(self, fd, stat):
        # get only first extent since it seems that you cannot
        # dedup unfilled blocks - or it is just not reported by femap,
        # so we cannot check if all of data is deduped
        extents = get_extent_map(fd)
        first_extent = extents[0] if extents else None
        return {
            "size": stat.st_size,
            "extent": first_extent,
        }

    def collect_files(self, path, progress=None):
        self.logger.info("Creating index for %r", path)
        if progress:
            progress.update(found=0, added=0)
        for root, dummy_, files in os.walk(path):
            for fname in files:
                if progress:
                    progress.update(found__add=1)
                f = pathlib.Path(root) / fname
                with fdopen(f, os.O_PATH|os.O_NOFOLLOW) as fd:
                    s = os.fstat(fd)
                if stat.S_IFMT(s.st_mode) != stat.S_IFREG:
                    self.logger.warning("Skipping not regular file at %r", f)
                    continue
                if s.st_size == 0:
                    self.logger.debug("Skipping empty file at %r", f)
                    continue
                try:
                    with fdopen(f, os.O_RDONLY) as fd:
                        info = self._get_info(fd, s)
                except OSError as e:
                    self.logger.warning("Skipping bad path %r: %s", f, e)
                    continue
                if not info["extent"]:
                    self.logger.warning('No extents found for %r, insufficient permissions?', f)
                    continue
                
                self.put(f, info)
                if progress:
                    progress.update(added__add=1)
    
    @classmethod
    def _calculate_hash(cls, path):
        h = hashlib.md5()
        with open(path, "rb") as f:
            while True:
                data = f.read(cls.BUFSIZE)
                if not data:
                    break
                h.update(data)
        return h.digest()
    
    @classmethod
    def _hash_collector_worker(cls, infos):
        ret = []
        for extent, path in infos:
            ret.append((path, extent, cls._calculate_hash(path)))
        return ret

    def collect_hashes(self, workers=None, batch_size=200, progress=None):
        self.logger.info("Collecting hashes")
        
        if progress:
            progress.update(
                total_files=len(self._info_by_path),
                skipped_files=0,
                hashed_files=0,

                total_bytes=sum(i["size"] for i in self._info_by_path.values()),
                hashed_bytes=0,
                skipped_bytes=0,
            )

        def path_iter():
            known_extents = set()
            for ps in self._path_by_size.values():
                # calculate hash only if there is need for comparison
                if len(ps) < 3:
                    if progress:
                        progress.update(skipped_files__add=len(ps), skipped_bytes__add=sum(self._info_by_path[p]["size"] for p in ps))
                    continue
                for p in ps:
                    info = self._info_by_path[p]
                    extent_offset = info["extent"]
                    if extent_offset in known_extents:
                        if progress:
                            progress.update(skipped_files__add=1, skipped_bytes__add=info["size"])
                        continue
                    known_extents.add(extent_offset)
                    yield extent_offset, p
        
        def hash_iter(batch_size):
            i = path_iter()
            while True:
                ret = list(itertools.islice(i, batch_size))
                if not ret:
                    raise StopIteration()
                yield ret
        
        with multiprocessing.pool.Pool(workers) as pool:
            for batch in pool.imap_unordered(self._hash_collector_worker, hash_iter(batch_size)):
                for path, extent, hash_ in batch:
                    if extent not in self._hash_by_extent:
                        self._hash_by_extent[extent] = {}
                    self._hash_by_extent[extent] = hash_
                    if progress:
                        progress.update(hashed_files__add=1, hashed_bytes__add=self._info_by_path[path]["size"])

    def __iter__(self):
        return iter(self._info_by_path.keys())
    def __len__(self):
        return len(self._info_by_path)
    
    def get_size(self, path):
        return self._info_by_path[path]["size"]

    def find_similar(self, src_path):
        src_info = self._info_by_path[src_path]
        src_extent = src_info["extent"]
        src_hash = self._hash_by_extent.get(src_extent, None)

        paths = set(self._path_by_size[src_info["size"]])
        paths.discard(src_path)

        same = []
        similar = []

        for p in paths:
            ex = self._info_by_path[p]["extent"]
            if ex == src_extent:
                same.append(p)
            else:
                # there are few files with this size so no hash was computed
                # if src_hash exist then all files in size group should have it computed
                if src_hash is None or self._hash_by_extent[ex] == src_hash:
                    similar.append(p)
        
        return same, similar

class Deduplicator(object):

    def __init__(self, index, pretend=False):
        super().__init__()
        self.logger = logging.getLogger(self.__class__.__qualname__)
        self.pretend = pretend
        self.index = index

    def run(self, progress=None):
        handled = set()
        
        if progress:
            progress.update(
                total_files=len(self.index),
                handled_files=0,
                deduped_files=0,
                deduped_bytes=0,
                same_files=0,
                same_bytes=0,
                duplicated_files=0
            )

        for src_path in self.index:
            if src_path in handled:
                continue

            self.logger.debug("Checking %r", src_path)
            
            same, similar = self.index.find_similar(src_path)
            handled.update(same)
            handled.add(src_path)
            if progress:
                progress_src_size = self.index.get_size(src_path)
                progress.update(
                    same_files__add=len(same),
                    same_bytes__add=len(same) * progress_src_size,
                    duplicated_files__add=1
                )

            self.logger.debug("Found %d same and %d similar files", len(same), len(similar))

            pending = []
            
            for p in similar:
                if file_cmp(src_path, p, shallow=False):
                    self.logger.debug("Found duplicate: %r", p)
                    pending.append(p)
                    handled.add(p)
                    if progress:
                        progress.update(deduped_files__add=1)
            
            if pending:
                if not self.pretend:
                    self._run_dedupe(src_path, pending)
                if progress:
                    progress.update(
                        deduped_files__add=len(pending),
                        deduped_bytes__add=len(pending) * progress_src_size,
                        duplicated_files__add=1
                    )
            
            if progress:
                progress.update(handled_files=len(handled))

        # self.logger.debug("Deduplicated %.2f MBytes in %d files.", progress.deduped_bytes/1024/1024, progress.deduped_files)
        # return progress

    def _run_dedupe(self, src_path, dst_paths, batch_size=10):
        size = self.index.get_size(src_path)
        
        offset = 0
        with fdopen(src_path, os.O_RDONLY) as src_fd:
            while True:
                batch = dst_paths[offset:offset+batch_size]
                if not batch:
                    break
                self.logger.debug("Deduping %r", batch)
                fds = [os.open(i, os.O_RDONLY) for i in batch]
                try:
                    dedup_file(src_fd, size, fds)
                finally:
                    for fd in fds:
                        os.close(fd)
                if len(batch) != batch_size:
                    break
                offset += batch_size

if __name__ == "__main__":
    import tqdm
    logging.basicConfig(level=logging.ERROR)

    p = argparse.ArgumentParser()
    p.add_argument("--pretend", "-p", action="store_true")
    p.add_argument("--hash-workers", action="store", type=int, default=math.ceil(os.cpu_count()/2))
    p.add_argument("--hash-batch-size", action="store", type=int, default=200)
    p.add_argument("path", action="store", type=pathlib.Path)

    def format_bytes(s):
        return tqdm.tqdm.format_sizeof(s, 'b', 1024)
    
    ns = p.parse_args()

    index = Index()
    d = Deduplicator(pretend=ns.pretend, index=index)

    # bar_steps = tqdm.tqdm(unit=" step")
    bar = tqdm.tqdm(desc="Scanning", unit=" files", leave=False)
    
    def collect_progress(found, added):
        bar.n = found
        bar.update(0)
    progress = Progress(collect_progress)
    index.collect_files(ns.path, progress=progress)
    bar.write("Indexed %d files from %d found in %ss" % (progress.added, progress.found, bar.format_interval(bar.last_print_t - bar.start_t)))

    bar.desc = "Calculating hashes"
    bar.unit="b"
    bar.unit_divisor=1024
    bar.unit_scale=True
    bar.miniters = 0
    bar.reset(1)
    
    def hash_progress(total_bytes, skipped_bytes, hashed_bytes, **kwargs):
        bar.total = total_bytes
        bar.n = skipped_bytes + hashed_bytes
        bar.update(0)
    progress = Progress(hash_progress)
    index.collect_hashes(workers=ns.hash_workers, batch_size=ns.hash_batch_size, progress=progress)
    bar.write(
        "Hashed %s of %s from %d files in %ss" % (
            format_bytes(progress.hashed_bytes),
            format_bytes(progress.total_bytes),
            progress.total_files,
            bar.format_interval(bar.last_print_t - bar.start_t)
        )
    )
    
    bar.desc = "Deduplicating"
    bar.unit = " files"
    bar.unit_scale = False
    bar.miniters = 0
    bar.reset(1)
    
    def dedup_progress(total_files, handled_files, **kwargs):
        bar.total = total_files
        bar.n = handled_files
        bar.update(0)
    progress=Progress(dedup_progress)
    d.run(progress=progress)
    
    bar.close()

    bar.write("""Deduplicate stats:
  - checked {handled_files} files
  - found {duplicated_files} files that have duplicates
  - deduplicated {deduped_bytes} in {deduped_files} files
  - found {same_bytes} of already deduplicated data in {same_files} files
    """.format(
        handled_files=progress.handled_files,
        duplicated_files=progress.duplicated_files,
        deduped_files=progress.deduped_files,
        deduped_bytes=format_bytes(progress.deduped_bytes),
        same_bytes=format_bytes(progress.same_bytes),
        same_files=progress.same_files
    ))
