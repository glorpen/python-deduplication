=====================
Glorpen Deduplication
=====================

Repositories
============

https://github.com/glorpen/python-deduplication

https://bitbucket.org/glorpen/python-deduplication

https://gitlab.com/glorpen/python-deduplication

About
=====

Finds duplicated files and passes it to kernel for deduplication.

Files are always open in readonly mode, destructive tasks are left to kernel.

Supports filesystems with ``XFS_IOC_FILE_EXTENT_SAME`` ioctl.

