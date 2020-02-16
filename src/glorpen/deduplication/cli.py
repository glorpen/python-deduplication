import argparse
import functools
import logging
import math
import os
import pathlib

import tqdm
from glorpen.deduplication.dedup import Deduplicator, Index, Progress


def format_bytes(s):
    return tqdm.tqdm.format_sizeof(s, 'b', 1024)

class TqdmLoggingHandler(logging.Handler):
    def __init__(self, bar, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._bar = bar
    def emit(self, record):
        bar.write(self.format(record))

def collect_progress(bar, found, added):
    bar.n = found
    bar.update(0)

def hash_progress(bar, total_bytes, skipped_bytes, hashed_bytes, **kwargs):
    bar.total = total_bytes
    bar.n = skipped_bytes + hashed_bytes
    bar.update(0)

def dedup_progress(bar, total_files, handled_files, **kwargs):
    bar.total = total_files
    bar.n = handled_files
    bar.update(0)

def run_collect_files(bar, index):
    progress = Progress(functools.partial(collect_progress, bar=bar))
    index.collect_files(ns.path, progress=progress)
    bar.write("Indexed %d files from %d found in %ss" % (progress.added, progress.found, bar.format_interval(bar.last_print_t - bar.start_t)))

def run_calculate_hashes(bar, index):
    bar.desc = "Calculating hashes"
    bar.unit="b"
    bar.unit_divisor=1024
    bar.unit_scale=True
    bar.miniters = 0
    bar.reset(1)
    
    progress = Progress(functools.partial(hash_progress, bar=bar))
    index.collect_hashes(workers=ns.hash_workers, batch_size=ns.hash_batch_size, progress=progress)
    bar.write(
        "Hashed %s of %s from %d files in %ss" % (
            format_bytes(progress.hashed_bytes),
            format_bytes(progress.total_bytes),
            progress.total_files,
            bar.format_interval(bar.last_print_t - bar.start_t)
        )
    )

def run_deduplicate(bar, dedup):
    bar.desc = "Deduplicating"
    bar.unit = " files"
    bar.unit_scale = False
    bar.miniters = 0
    bar.reset(1)
    
    progress=Progress(functools.partial(dedup_progress, bar=bar))
    dedup.run(progress=progress)
    
    bar.close()

    bar.write("""Deduplicated {deduped_bytes} in {run_time}s:
  - checked {handled_files} files
  - found {duplicated_files} unique files that have duplicates
  - deduplicated {deduped_bytes} in {deduped_files} files
  - found {same_bytes} of already deduplicated data in {same_files} files
    """.format(
        handled_files=progress.handled_files,
        duplicated_files=progress.duplicated_files,
        deduped_files=progress.deduped_files,
        deduped_bytes=format_bytes(progress.deduped_bytes),
        same_bytes=format_bytes(progress.same_bytes),
        same_files=progress.same_files,
        run_time=bar.format_interval(bar.last_print_t - bar.start_t)
    ))

if __name__ == "__main__":
    log_levels=[logging.ERROR, logging.WARNING, logging.INFO, logging.DEBUG]

    p = argparse.ArgumentParser()
    p.add_argument("--pretend", "-p", action="store_true")
    p.add_argument("--hash-workers", action="store", type=int, default=math.ceil(os.cpu_count()/2))
    p.add_argument("--hash-batch-size", action="store", type=int, default=200)
    p.add_argument("--verbose", "-v", action="count", default=0)
    p.add_argument("path", action="store", type=pathlib.Path)
    
    ns = p.parse_args()

    bar = tqdm.tqdm(desc="Scanning", unit=" files", leave=False)

    log_level = log_levels[min(ns.verbose, len(log_levels)-1)]

    handler = TqdmLoggingHandler(bar)
    handler.setFormatter(logging.Formatter(logging.BASIC_FORMAT))
    logging.root.addHandler(handler)
    logging.root.setLevel(log_level)

    index = Index()
    d = Deduplicator(pretend=ns.pretend, index=index)

    run_collect_files(bar, index)
    run_calculate_hashes(bar, index)
    run_deduplicate(bar, d)
    
