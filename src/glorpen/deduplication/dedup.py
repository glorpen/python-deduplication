'''
@author: Arkadiusz Dzięgiel <arkadiusz.dziegiel@glorpen.pl>
'''

import hashlib
import itertools
import logging
import multiprocessing.pool
import os
import pathlib
import stat
from filecmp import cmp as file_cmp

from glorpen.deduplication.ioctl import dedup_file, fdopen, get_extent_map


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

        pref_block_size = os.stat(path / ".").st_blksize
        self.logger.info("Checking files bigger or equal to %d bytes", pref_block_size)

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
                if s.st_size < pref_block_size:
                    self.logger.debug("Skipping empty or small file at %r", f)
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
                    return
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
                    )
            
            if progress:
                progress.update(
                    handled_files=len(handled),
                    duplicated_files__add=1 if same or pending else 0
                )

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
