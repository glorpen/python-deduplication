=====================
Glorpen Deduplication
=====================

Finds duplicated files and passes it to kernel for deduplication.

Files are always open in readonly mode, destructive tasks are left to kernel.

Supports filesystems with ``XFS_IOC_FILE_EXTENT_SAME`` ioctl.

