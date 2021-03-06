# =============================================================================
# 2013+ Copyright (c) Alexey Ivanov <rbtz@ph34r.me>
# All rights reserved.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
# =============================================================================

"""
Misc. routines
"""

import errno
import hashlib
import logging as log
import msgpack
import time
import traceback

import elliptics


def logged_class(klass):
    """
    This decorator adds 'log' method to passed class
    """
    klass.log = log.getLogger(klass.__name__)
    return klass


def id_to_int(key_id):
    """Returns numerical equivalent of key"""
    return int(''.join('%02x' % b for b in key_id.id[:64]), 16)


def mk_container_name(address, backend_id, prefix="iterator_"):
    """
    Makes filename for iterators' results
    """
    return "{0}{1}.{2}".format(prefix, hashlib.sha256(str(address)).hexdigest(), backend_id)


def elliptics_create_node(address=None, elog=None, wait_timeout=3600, check_timeout=60, flags=0, io_thread_num=1,
                          net_thread_num=1, nonblocking_io_thread_num=1, remotes=[]):
    """
    Connects to elliptics cloud
    """
    log.debug("Creating node using: {0}, wait_timeout: {1}, remotes: {2}".format(address, wait_timeout, remotes))
    cfg = elliptics.Config()
    cfg.config.wait_timeout = wait_timeout
    cfg.config.check_timeout = check_timeout
    cfg.config.flags = flags
    cfg.config.io_thread_num = io_thread_num
    cfg.config.nonblocking_io_thread_num = nonblocking_io_thread_num
    cfg.config.net_thread_num = net_thread_num
    node = elliptics.Node(elog, cfg)
    node.add_remotes([address] + remotes)
    log.debug("Created node: {0}".format(node))
    return node


def worker_init():
    """Do not catch Ctrl+C in worker"""
    from signal import signal, SIGINT, SIG_IGN
    signal(SIGINT, SIG_IGN)


# common class for collecting statistics of recovering one key
# TODO(shaitan): try to use default dict with counters enums
class RecoverStat(object):
    def __init__(self):
        self.reset()

    def reset(self):
        self.skipped = 0
        self.lookup = 0
        self.lookup_failed = 0
        self.lookup_retries = 0
        self.read = 0
        self.read_failed = 0
        self.read_retries = 0
        self.read_bytes = 0
        self.write = 0
        self.write_failed = 0
        self.write_retries = 0
        self.written_bytes = 0
        self.remove = 0
        self.remove_failed = 0
        self.remove_retries = 0
        self.removed_bytes = 0
        self.remove_old = 0
        self.remove_old_failed = 0
        self.remove_old_bytes = 0
        self.removed_uncommitted_keys = 0
        self.merged_indexes = 0
        self.skip_write_to_ro_group = 0
        self.skip_remove_corrupted_key_from_ro_group = 0

    def apply(self, stats):
        if self.skipped:
            stats.counter("skipped_keys", self.skipped)
        if self.lookup:
            stats.counter("remote_lookups", self.lookup)
        if self.lookup_failed:
            stats.counter("remote_lookups", -self.lookup_failed)
        if self.lookup_retries:
            stats.counter("remote_lookup_retries", self.lookup_retries)
        if self.read:
            stats.counter("local_reads", self.read)
        if self.read_failed:
            stats.counter("local_reads", -self.read_failed)
        if self.read_retries:
            stats.counter("local_read_retries", self.read_retries)
        if self.read_bytes:
            stats.counter("local_read_bytes", self.read_bytes)
        if self.write:
            stats.counter("remote_writes", self.write)
        if self.write_failed:
            stats.counter("remote_writes", -self.write_failed)
        if self.write_retries:
            stats.counter("remote_write_retries", self.write_retries)
        if self.written_bytes:
            stats.counter("remote_written_bytes", self.written_bytes)
        if self.remove:
            stats.counter("local_removes", self.remove)
        if self.remove_failed:
            stats.counter("local_removes", -self.remove_failed)
        if self.remove_retries:
            stats.counter("local_remove_retries", self.remove_retries)
        if self.removed_bytes:
            stats.counter("local_removed_bytes", self.removed_bytes)
        if self.remove_old:
            stats.counter('local_removes_old', self.remove_old)
        if self.remove_old_failed:
            stats.counter('local_removes_old', -self.remove_old_failed)
        if self.remove_old_bytes:
            stats.counter('local_removes_old_bytes', self.remove_old_bytes)
        if self.removed_uncommitted_keys:
            stats.counter('removed_uncommitted_keys', self.removed_uncommitted_keys)
        if self.merged_indexes:
            stats.counter("merged_indexes", self.merged_indexes)
        if self.skip_write_to_ro_group:
            stats.counter('skip_write_to_ro_group', self.skip_write_to_ro_group)
        if self.skip_remove_corrupted_key_from_ro_group:
            stats.counter('skip_remove_corrupted_key_from_ro_group', self.skip_remove_corrupted_key_from_ro_group)

        self.reset()

    def __add__(self, b):
        ret = RecoverStat()
        ret.skipped = self.skipped + b.skipped
        ret.lookup = self.lookup + b.lookup
        ret.lookup_failed = self.lookup_failed + b.lookup_failed
        ret.lookup_retries = self.lookup_retries + b.lookup_retries
        ret.read = self.read + b.read
        ret.read_failed = self.read_failed + b.read_failed
        ret.read_retries = self.read_retries + b.read_retries
        ret.read_bytes = self.read_bytes + b.read_bytes
        ret.write = self.write + b.write
        ret.write_failed = self.write_failed + b.write_failed
        ret.write_retries = self.write_retries + b.write_retries
        ret.written_bytes = self.written_bytes + b.written_bytes
        ret.remove = self.remove + b.remove
        ret.remove_failed = self.remove_failed + b.remove_failed
        ret.remove_retries = self.remove_retries + b.remove_retries
        ret.removed_bytes = self.removed_bytes + b.removed_bytes
        ret.remove_old = self.remove_old + b.remove_old
        ret.remove_old_failed = self.remove_old_failed + b.remove_old_failed
        ret.remove_old_bytes = self.remove_old_bytes + b.remove_old_bytes
        ret.removed_uncommitted_keys = self.removed_uncommitted_keys + b.removed_uncommitted_keys
        ret.merged_indexes = self.merged_indexes + b.merged_indexes
        ret.skip_write_to_ro_group = self.skip_write_to_ro_group + b.skip_write_to_ro_group
        ret.skip_remove_corrupted_key_from_ro_group = (self.skip_remove_corrupted_key_from_ro_group
                                                       + b.skip_remove_corrupted_key_from_ro_group)
        return ret


class KeyInfo(object):
    def __init__(self, address, group_id, timestamp, size, user_flags, flags, data_offset, blob_id):
        self.address = address
        self.group_id = group_id
        self.timestamp = timestamp
        self.size = size
        self.user_flags = user_flags
        self.flags = flags
        self.data_offset = data_offset
        self.blob_id = blob_id

    def dump(self):
        return (
            (self.address.host, self.address.port, self.address.family),
            self.group_id,
            (self.timestamp.tsec, self.timestamp.tnsec),
            self.size,
            self.user_flags,
            self.flags,
            self.data_offset,
            self.blob_id)

    def same_meta(self, other):
        """Check whether @self and @other have the same meta
        :arg other: other instance of KeyInfo
        :type other: KeyInfo
        :return: whether self and other have the same meta
        :rtype: bool
         """
        return (self.timestamp, self.size, self.user_flags) == (other.timestamp, other.size, other.user_flags)

    @classmethod
    def load(cls, data):
        return cls(elliptics.Address(data[0][0], data[0][1], data[0][2]),
                   data[1],
                   elliptics.Time(data[2][0], data[2][1]),
                   data[3],
                   data[4],
                   data[5],
                   data[6],
                   data[7])


def dump_key_data(key_data, file):
    msgpack.pack((key_data[0].id, tuple(ki.dump() for ki in key_data[1])), file)


def load_key_data_from_file(keys_file):
    unpacker = msgpack.Unpacker(keys_file)
    for data in unpacker:
        yield (elliptics.Id(data[0], 0), tuple(KeyInfo.load(d) for d in data[1]))


def load_key_data(filepath):
    with open(filepath, 'r') as input_file:
        for r in load_key_data_from_file(input_file):
            yield r


class WindowedRecovery(object):
    def __init__(self, ctx, stats):
        import multiprocessing.pool
        import threading
        self.ctx = ctx
        self.stats = stats

        self.lock = threading.Lock()
        self.complete = threading.Event()
        self.result = True
        self.need_exit = False
        self.recovers_in_progress = 0
        self.processed_keys = 0
        self.pool = multiprocessing.pool.ThreadPool(processes=ctx.pool_size)

    def run(self):
        self.start_time = time.time()

        for _ in xrange(self.ctx.batch_size):
            if not self._try_run_one():
                break

        while not self.complete.is_set():
            self.complete.wait()

        self.pool.close()
        self.pool.join()

        speed = self.processed_keys / (time.time() - self.start_time)
        self.stats.set_counter('recovery_speed', round(speed, 2))
        self.stats.set_counter('recovers_in_progress', self.recovers_in_progress)
        return self.result

    def _try_run_one(self):
        with self.lock:
            if self.need_exit:
                return False
            self.recovers_in_progress += 1
        started = False
        try:
            started = self.run_one()
        except:
            log.exception("Failed to recover next key")
            with self.lock:
                self.result = False
                self.need_exit = True
        finally:
            if started:
                return True
            with self.lock:
                self.recovers_in_progress -= 1
                if not self.recovers_in_progress:
                    self.complete.set()
        return False

    def _callback(self, result, stat):
        self._try_run_one()

        with self.lock:
            self.result &= result
            self.processed_keys += 1
            self.recovers_in_progress -= 1
            if not self.recovers_in_progress:
                self.complete.set()

        stat.apply(self.stats)
        speed = self.processed_keys / (time.time() - self.start_time)
        self.stats.set_counter('recovery_speed', round(speed, 2))
        self.stats.set_counter('recovers_in_progress', self.recovers_in_progress)
        self.stats.counter('recovered_keys', 1 if result else -1)
        self.ctx.stats.counter('recovered_keys', 1 if result else -1)

    def async_callback(self, result, stat):
        """Asynchronous start of callback is protection from stack overflow"""

        self.pool.apply_async(self._callback, (result, stat))
