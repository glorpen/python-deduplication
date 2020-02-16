'''
@author: Arkadiusz Dzięgiel <arkadiusz.dziegiel@glorpen.pl>
'''

import contextlib
import ctypes
import fcntl
import logging
import os
import sys

from ioctl_opt import IOC, IOC_READ, IOC_WRITE


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
