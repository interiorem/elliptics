# =============================================================================
# 2013+ Copyright (c) Kirill Smorodinnikov <shaitkir@gmail.com>
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

import errno
import logging
import logging.handlers
import os
import sys
import threading
import traceback
import weakref

from elliptics_recovery.dc_server_send import ServerSendRecovery
from elliptics_recovery.utils.misc import RecoverStat
from elliptics_recovery.utils.misc import WindowedRecovery
from elliptics_recovery.utils.misc import elliptics_create_node
from elliptics_recovery.utils.misc import load_key_data

import elliptics

from elliptics import Address
from elliptics.log import convert_elliptics_log_level
from elliptics.log import formatter

log = logging.getLogger()


class KeyRemover(object):
    def __init__(self, key, key_size, remove_session, groups, ctx, stats, callback):
        self.key = key
        self.session = remove_session.clone()
        self.session.groups = groups
        self.session.timeout = max(60, key_size / ctx.data_flow_rate)
        self.ctx = ctx
        self.stats = stats
        self.stats_cmd = ctx.stats['commands']
        self.stats_cmd_groups = ctx.stats['commands_by_groups']
        self.callback = callback
        self.attempt = 0

    def remove(self):
        if self.ctx.safe or self.ctx.dry_run:
            if self.ctx.safe:
                log.info("Safe mode is turned on. Skip removing key: {0}".format(repr(self.key)))
            else:
                log.info("Dry-run mode is turned on. Skip removing key: {0}.".format(repr(self.key)))
        else:
            try:
                log.info("Removing key: {0} from group: {1}".format(self.key, self.session.groups))
                remove_result = self.session.remove(self.key)
                remove_result.connect(self.on_remove)
                return
            except:
                log.exception("Failed to remove key: {0} from groups: {1}".format(self.key, self.session.groups))

        self.on_complete()

    def on_remove(self, results, error):
        try:
            for r in results:
                if r.status:
                    self.stats_cmd.counter('remove.{0}'.format(r.status), 1)
                    self.stats_cmd_groups.counter('remove.{0}.{1}'.format(r.group_id, r.status), 1)

            if error.code:
                self.stats.remove_failed += 1
                failed_groups = [r.group_id for r in results if r.status not in (0, -errno.ENOENT, -errno.EBADFD)]
                log.error("Failed to remove key: {0}: from groups: {1}: {2}"
                          .format(self.key, failed_groups, error))
                if failed_groups and self.attempt < self.ctx.attempts:
                    self.session.groups = failed_groups
                    old_timeout = self.session.timeout
                    self.session.timeout *= 2
                    self.attempt += 1
                    log.info("Retry to remove key: {0} attempts: {1}/{2} "
                             "increased timeout: {3}/{4}"
                             .format(repr(self.key),
                                     self.attempt, self.ctx.attempts,
                                     self.session.timeout,
                                     old_timeout))
                    self.stats.remove_retries += 1

                    self.remove()
                    return
            else:
                self.stats.remove += len(results)
        except:
            log.exception("Failed to handle remove result key: {0} from groups: {1}"
                          .format(self.key, self.session.groups))

        self.on_complete()

    def on_complete(self):
        del self.session
        self.callback()


class KeyRecover(object):
    def __init__(self, ctx, key, key_infos, missed_groups, node, callback):
        self.ctx = ctx
        self.complete = threading.Event()
        self.callback = callback
        self.stats = RecoverStat()
        self.stats_cmd = ctx.stats['commands']
        self.stats_cmd_groups = ctx.stats['commands_by_groups']
        self.key = key
        self.key_flags = 0
        self.key_infos = key_infos
        self.diff_groups = []
        self.missed_groups = list(missed_groups)

        self.read_session = elliptics.newapi.Session(node)
        self.read_session.trace_id = ctx.trace_id
        self.read_session.set_filter(elliptics.filters.all)

        self.write_session = elliptics.newapi.Session(node)
        self.write_session.trace_id = ctx.trace_id
        self.write_session.set_checker(elliptics.checkers.all)
        self.write_session.ioflags |= elliptics.io_flags.cas_timestamp

        self.remove_session = elliptics.newapi.Session(node)
        self.remove_session.trace_id = ctx.trace_id
        self.remove_session.set_filter(elliptics.filters.all_final)
        self.remove_session.ioflags |= elliptics.io_flags.cas_timestamp
        self.remove_session.timestamp = ctx.prepare_timeout

        self.result = False
        self.attempt = 0
        self.pending_operations = 1
        self.pending_operations_lock = threading.Lock()

        log.debug("Recovering key: {0} from nonempty groups: {1} and missed groups: {2}"
                  .format(repr(self.key), [k.group_id for k in self.key_infos], self.missed_groups))
        self.run()

    def run(self):
        self.total_size = self.key_infos[0].size
        self.chunked = self.total_size > self.ctx.chunk_size
        self.recovered_size = 0

        same_meta = lambda lhs, rhs: (lhs.timestamp, lhs.size, lhs.user_flags) == (rhs.timestamp, rhs.size, rhs.user_flags)
        same_infos = [info for info in self.key_infos if same_meta(info, self.key_infos[0])]
        self.key_flags = same_infos[0].flags

        self.same_groups = [info.group_id for info in same_infos]
        self.key_infos = [info for info in self.key_infos if info.group_id not in self.same_groups]
        self.diff_groups += [info.group_id for info in self.key_infos]
        self.diff_groups = list(set(self.diff_groups).difference(self.same_groups))

        if not self.diff_groups and not self.missed_groups:
            log.debug("Key: {0} already up-to-date in all groups: {1}".format(self.key, self.same_groups))
            self.stop(False)
            return

        log.debug("Try to recover key: {0} from groups: {1} to groups: {2}: diff groups: {3}, missed groups: {4}"
                  .format(self.key, self.same_groups, self.diff_groups + self.missed_groups,
                          self.diff_groups, self.missed_groups))

        self.read_session.groups = self.same_groups

        groups_for_write = []
        for group in self.diff_groups + self.missed_groups:
            if group in self.ctx.ro_groups:
                self.stats.skip_write_to_ro_group += 1
                continue

            groups_for_write.append(group)

        self.write_session.groups = groups_for_write
        self.read()

    def stop(self, result):
        self.result = result
        log.debug("Finished recovering key: {0} with result: {1}".format(self.key, self.result))
        self.on_complete()

    def on_complete(self):
        with self.pending_operations_lock:
            self.pending_operations -= 1
            if self.pending_operations:
                return

        # Remove all sessions to release links to elliptics node. Main thread must hold last reference to
        # elliptics node after recovery completion. Otherwise, node may be destroyed in i/o thread.
        del self.read_session, self.write_session, self.remove_session
        self.complete.set()
        self.callback(self.result, self.stats)

    def read(self):
        try:
            size = self.total_size
            log.debug("Reading key: {0} from groups: {1}, chunked: {2}"
                      .format(self.key, self.read_session.groups, self.chunked))
            if self.chunked:
                size = min(self.total_size - self.recovered_size, self.ctx.chunk_size)
            # do not check checksum for all but the first chunk
            if self.recovered_size != 0:
                if self.key_flags & elliptics.record_flags.chunked_csum:
                    # if record was checksummed by chunks there is no need to disable checksum verification
                    self.read_session.ioflags &= ~elliptics.io_flags.nocsum
                else:
                    self.read_session.ioflags |= elliptics.io_flags.nocsum
            else:
                size = min(self.total_size, size)
                self.read_session.ioflags = 0

            log.debug("Reading key: {0} from groups: {1}, chunked: {2}, offset: {3}, size: {4}, total_size: {5}"
                      .format(self.key, self.read_session.groups, self.chunked,
                              self.recovered_size, size, self.total_size))

            if self.recovered_size == 0:
                # read first chunk of data with json
                read_result = self.read_session.read(key=self.key,
                                                     offset=0,
                                                     size=size)
            else:
                # read none-first chunks of data without json
                read_result = self.read_session.read_data(key=self.key,
                                                          offset=self.recovered_size,
                                                          size=size)
            read_result.connect(self.onread)
        except Exception as e:
            log.error("Read key: {0} by offset: {1} and size: {2} raised exception: {3}, traceback: {4}"
                      .format(self.key, self.recovered_size, size, repr(e), traceback.format_exc()))
            self.stop(False)

    def write(self):
        try:
            log.debug('Writing key: %s to groups: %s',
                      repr(self.key), self.diff_groups + self.missed_groups)
            params = {'key': self.key,
                      'json': self.json,
                      'data': self.write_data,
                      'data_offset': self.recovered_size}
            if self.chunked:
                if self.recovered_size == 0:
                    params['json_capacity'] = self.json_capacity
                    params['data_capacity'] = self.total_size
                    log.debug('Write_prepare key: %s to groups: %s, json: %d, json_capacity: %d, '
                              'data: %d, data_offset: %d, data_capacity: %d',
                              params['key'], self.write_session.groups, len(params['json']),
                              params['json_capacity'], len(params['data']), params['data_offset'],
                              params['data_capacity'])
                    write_result = self.write_session.write_prepare(**params)
                elif self.recovered_size + len(params['data']) < self.total_size:
                    log.debug('Write_plain key: %s to groups: %s, json: %d, data: %d, data_offset: %d',
                              params['key'], self.write_session.groups,
                              len(params['json']), len(params['data']), params['data_offset'])
                    write_result = self.write_session.write_plain(**params)
                else:
                    params['data_commit_size'] = self.total_size
                    log.debug('Write_commit key: %s to groups: %s, json: %d, data: %d, data_offset: %d'
                              'data_commit_size: %d',
                              params['key'], self.write_session.groups,
                              len(params['json']), len(params['data']), params['data_offset'],
                              params['data_commit_size'])
                    write_result = self.write_session.write_commit(**params)
            else:
                del params['data_offset']
                params['json_capacity'] = self.json_capacity
                params['data_capacity'] = self.total_size
                log.debug('Write key: %s to groups: %s, json: %d, json_capacity: %d, '
                          'data: %d, data_capacity: %d',
                          params['key'], self.write_session.groups,
                          len(params['json']), params['json_capacity'], len(params['data']),
                          params['data_capacity'])
                write_result = self.write_session.write(**params)
            write_result.connect(self.onwrite)
        except Exception as e:
            log.error('Writing key: %s raised exception: %s, traceback: %s',
                      self.key, repr(e), traceback.format_exc())
            self.stop(False)

    def onread(self, results, error):
        try:
            corrupted_groups = []
            for result in results:
                if result.status:
                    self.stats_cmd_groups.counter('read.{0}.{1}'.format(result.group_id, result.status), 1)

                if result.status != -errno.EILSEQ:
                    continue

                if result.group_id in self.ctx.ro_groups:
                    self.stats.skip_remove_corrupted_key_from_ro_group += 1
                    continue

                corrupted_groups.append(result.group_id)

            if corrupted_groups:
                with self.pending_operations_lock:
                    self.pending_operations += 1
                KeyRemover(self.key, self.total_size, self.remove_session, corrupted_groups,
                           self.ctx, self.stats, self.on_complete).remove()

            if error.code:
                log.error("Failed to read key: {0} from groups: {1}: {2}".format(self.key, self.same_groups, error))
                self.stats_cmd.counter('read.{0}'.format(error.code), 1)
                self.stats.read_failed += len(results)
                if error.code == -errno.ETIMEDOUT:
                    if self.attempt < self.ctx.attempts:
                        self.attempt += 1
                        old_timeout = self.read_session.timeout
                        self.read_session.timeout *= 2
                        log.error("Read has been timed out. Try to reread key: {0} from groups: {1}, attempt: {2}/{3} "
                                  "with increased timeout: {4}/{5}"
                                  .format(self.key, self.same_groups, self.attempt, self.ctx.attempts,
                                          self.read_session.timeout, old_timeout))
                        self.read()
                    else:
                        log.error("Read has been timed out {0} times, all {1} attempts are used. "
                                  "The key: {1} can't be recovery now. Skip it"
                                  .format(self.attempt, self.key))
                        self.stats.skipped += 1
                        self.stop(False)
                elif len(self.key_infos) > 1:
                    log.error("Key: {0} has available replicas in other groups. Try to recover the key from them"
                              .format(self.key))
                    self.diff_groups += self.read_session.groups
                    self.run()
                else:
                    log.error("Failed to read key: {0} from any available group. "
                              "This key can't be recovered now. Skip it"
                              .format(self.key))
                    self.stats.skipped += 1
                    self.stop(False)
                return

            self.stats.read_failed += len(results) - 1
            self.stats.read += 1
            self.stats.read_bytes += results[-1].size

            if self.recovered_size == 0:
                self.write_session.user_flags = results[-1].record_info.user_flags
                self.write_session.timestamp = results[-1].record_info.data_timestamp
                self.write_session.json_timestamp = results[-1].record_info.json_timestamp
                self.read_session.ioflags |= elliptics.io_flags.nocsum
                self.read_session.groups = [results[-1].group_id]
                self.key_flags = results[-1].record_info.record_flags
                self.json_capacity = results[-1].record_info.json_capacity
                if self.total_size != results[-1].record_info.data_size:
                    self.total_size = results[-1].record_info.data_size
                    self.chunked = self.total_size > self.ctx.chunk_size
            self.attempt = 0

            if self.chunked and len(results) > 1:
                self.missed_groups += [r.group_id for r in results if r.error.code]

            log.debug("Copy key: %s from groups: %s to groups: %s",
                      repr(self.key), self.same_groups, self.write_session.groups)
            self.write_data = results[-1].data
            self.json = results[-1].json
            self.write()
        except Exception as e:
            log.error("Failed to handle origin key: {0}, exception: {1}, traceback: {2}"
                      .format(self.key, repr(e), traceback.format_exc()))
            self.stop(False)

    def onwrite(self, results, error):
        try:
            for result in results:
                if result.status:
                    self.stats_cmd_groups.counter('write.{0}.{1}'.format(result.group_id, result.status), 1)

            if error.code:
                self.stats_cmd.counter('write.{0}'.format(error.code), 1)
                self.stats.write_failed += 1
                log.error("Failed to write key: {0}: to groups: {1}: {2}"
                          .format(self.key, self.write_session.groups, error))
                if self.attempt < self.ctx.attempts and error.code in (-errno.ETIMEDOUT, -errno.ENXIO):
                    old_timeout = self.write_session.timeout
                    self.write_session.timeout *= 2
                    self.attempt += 1
                    log.info("Retry to write key: {0} attempts: {1}/{2} increased timeout: {3}/{4}"
                             .format(repr(self.key), self.attempt, self.ctx.attempts,
                                     self.write_session.timeout, old_timeout))
                    self.stats.write_retries += 1
                    self.write()
                    return
                self.stop(False)
                return

            self.stats.write += len(results)
            self.recovered_size += len(self.write_data)
            self.stats.written_bytes += len(self.write_data) * len(results)
            self.attempt = 0
            if self.recovered_size < self.total_size:
                self.read()
            else:
                log.debug("Key: {0} has been successfully copied to groups: {1}"
                          .format(repr(self.key), [r.group_id for r in results]))
                self.stop(True)
        except Exception as e:
            log.error("Failed to handle write result key: {0}: {1}, traceback: {2}"
                      .format(self.key, repr(e), traceback.format_exc()))
            self.stop(False)

    def wait(self):
        if not self.complete.is_set():
            self.complete.wait()

    def succeeded(self):
        self.wait()
        return self.result


def iterate_key(filepath, groups):
    '''
    Iterates key and sort each key key_infos by timestamp and size
    '''
    groups = set(groups)
    for key, key_infos in load_key_data(filepath):
        if len(key_infos) + len(groups) > 1:
            key_infos = sorted(key_infos, key=lambda x: (x.timestamp, x.size), reverse=True)
            missed_groups = tuple(groups.difference([k.group_id for k in key_infos]))

            # if all key_infos has the same timestamp and size and there is no missed groups -
            # skip key, it is already up-to-date in all groups
            same_meta = lambda lhs, rhs: (lhs.timestamp, lhs.size, lhs.user_flags) == (rhs.timestamp, rhs.size, rhs.user_flags)
            if same_meta(key_infos[0], key_infos[-1]) and not missed_groups:
                continue

            yield (key, key_infos, missed_groups)
        else:
            log.error("Invalid number of replicas for key: {0}: infos_count: {1}, groups_count: {2}"
                      .format(key, len(key_infos), len(groups)))


class WindowedDC(WindowedRecovery):
    def __init__(self, ctx, node):
        super(WindowedDC, self).__init__(ctx, ctx.stats['recover'])
        self.node_ref = weakref.ref(node)
        ctx.rest_file.flush()
        self.keys = iterate_key(self.ctx.rest_file.name, self.ctx.groups)

    def run_one(self):
        try:
            with self.lock:
                key = next(self.keys)
            KeyRecover(self.ctx, *key, node=self.node_ref(), callback=self.async_callback)
            return True
        except StopIteration:
            return False


def cleanup(ctx):
    for f in ctx.bucket_files.itervalues():
        os.remove(f.name)
    del ctx.bucket_files
    os.remove(ctx.rest_file.name)
    del ctx.rest_file


def recover(ctx):
    stats = ctx.stats['recover']

    stats.timer('recover', 'started')
    node = elliptics_create_node(address=ctx.address,
                                 elog=elliptics.Logger(ctx.log_file, int(ctx.log_level), True),
                                 wait_timeout=ctx.wait_timeout,
                                 flags=elliptics.config_flags.no_route_list,
                                 net_thread_num=4,  # TODO(shaitan): why 4?
                                 io_thread_num=24,  # TODO(shaitan): why 24?
                                 remotes=ctx.remotes)
    result = ServerSendRecovery(ctx, node).recover()
    result &= WindowedDC(ctx, node).run()
    cleanup(ctx)
    stats.timer('recover', 'finished')

    return result


if __name__ == '__main__':
    from elliptics_recovery.ctx import Ctx
    from optparse import OptionParser
    parser = OptionParser()
    parser.usage = "%prog [options]"
    parser.description = __doc__
    parser.add_option("-i", "--merged-filename", dest="merged_filename",
                      default='merged_result', metavar="FILE",
                      help="Input file which contains information about keys "
                      "in groups [default: %default]")
    parser.add_option("-d", "--debug", action="store_true",
                      dest="debug", default=False,
                      help="Enable debug output [default: %default]")
    parser.add_option("-D", "--dir", dest="tmp_dir",
                      default='/var/tmp/dnet_recovery_%TYPE%', metavar="DIR",
                      help="Temporary directory for iterators' results "
                      "[default: %default]")
    parser.add_option("-l", "--log", dest="elliptics_log",
                      default='dnet_recovery.log', metavar="FILE",
                      help="Output log messages from library to file "
                      "[default: %default]")
    parser.add_option("-L", "--log-level", action="store",
                      dest="elliptics_log_level", default="1",
                      help="Elliptics client verbosity [default: %default]")
    parser.add_option("-w", "--wait-timeout", action="store",
                      dest="wait_timeout", default="3600",
                      help="[Wait timeout for elliptics operations "
                      "default: %default]")
    parser.add_option("-r", "--remote", action="store",
                      dest="elliptics_remote", default=None,
                      help="Elliptics node address [default: %default]")
    parser.add_option("-g", "--groups", action="store",
                      dest="elliptics_groups", default=None,
                      help="Comma separated list of groups [default: all]")
    parser.add_option("-b", "--batch-size", action="store",
                      dest="batch_size", default="1024",
                      help="Number of keys in read_bulk/write_bulk "
                      "batch [default: %default]")
    parser.add_option("-p", "--pool-size", action="store",
                      dest="pool_size", default=1,
                      help="Size of internal pool for asynchronously "
                      "running callbacks [default: %default]")

    (options, args) = parser.parse_args()
    ctx = Ctx()

    log.setLevel(logging.DEBUG)

    ch = logging.StreamHandler(sys.stderr)
    ch.setFormatter(formatter)
    ch.setLevel(logging.WARNING)
    log.addHandler(ch)

    if not os.path.exists(ctx.tmp_dir):
        try:
            os.makedirs(ctx.tmp_dir, 0755)
            log.warning("Created tmp directory: {0}".format(ctx.tmp_dir))
        except Exception as e:
            raise ValueError("Directory: {0} does not exist and could not be created: {1}, traceback: {2}"
                             .format(ctx.tmp_dir, repr(e), traceback.format_exc()))
    os.chdir(ctx.tmp_dir)

    try:
        ctx.log_file = os.path.join(ctx.tmp_dir, options.elliptics_log)
        ctx.log_level = int(options.elliptics_log_level)
        ctx.merged_filename = os.path.join(ctx.tmp_dir,
                                           options.merged_filename)

        # FIXME: It may be inappropriate to use one log for both
        # elliptics library and python app, esp. in presence of auto-rotation
        fh = logging.handlers.WatchedFileHandler(ctx.log_file)
        fh.setFormatter(formatter)
        fh.setLevel(convert_elliptics_log_level(ctx.log_level))
        log.addHandler(fh)
        log.setLevel(convert_elliptics_log_level(ctx.log_level))

        if options.debug:
            log.setLevel(logging.DEBUG)
            ch.setLevel(logging.DEBUG)
    except Exception as e:
        raise ValueError("Can't parse log_level: '{0}': {1}, traceback: {2}"
                         .format(options.elliptics_log_level, repr(e), traceback.format_exc()))
    log.info("Using elliptics client log level: {0}".format(ctx.log_level))

    if options.elliptics_remote is None:
        raise ValueError("Recovery address should be given (-r option).")
    try:
        ctx.address = Address.from_host_port_family(options.elliptics_remote)
    except Exception as e:
        raise ValueError("Can't parse host:port:family: '{0}': {1}, traceback: {2}"
                         .format(options.elliptics_remote, repr(e), traceback.format_exc()))
    log.info("Using host:port:family: {0}".format(ctx.address))

    try:
        if options.elliptics_groups:
            ctx.groups = map(int, options.elliptics_groups.split(','))
        else:
            ctx.groups = []
    except Exception as e:
        raise ValueError("Can't parse grouplist: '{0}': {1}, traceback: {2}"
                         .format(options.elliptics_groups, repr(e), traceback.format_exc()))

    try:
        ctx.batch_size = int(options.batch_size)
        if ctx.batch_size <= 0:
            raise ValueError("Batch size should be positive: {0}"
                             .format(ctx.batch_size))
    except Exception as e:
        raise ValueError("Can't parse batchsize: '{0}': {1}, traceback: {2}"
                         .format(options.batch_size, repr(e), traceback.format_exc()))
    log.info("Using batch_size: {0}".format(ctx.batch_size))

    try:
        ctx.pool_size = int(options.pool_size)
        if ctx.pool_size <= 0:
            raise ValueError("Pool size should be positive: {0}"
                             .format(ctx.pool_size))
    except Exception as e:
        raise ValueError("Can't parse poolsize: '{0}': {1}, traceback: {2}"
                         .format(options.pool_size, repr(e), traceback.format_exc()))
    log.info("Using pool_size: %s", ctx.pool_size)

    try:
        ctx.wait_timeout = int(options.wait_timeout)
    except Exception as e:
        raise ValueError("Can't parse wait_timeout: '{0}': {1}, traceback: {2}"
                         .format(options.wait_timeout, repr(e), traceback.format_exc()))

    log.debug("Creating logger")
    elog = elliptics.Logger(ctx.log_file, int(ctx.log_level), True)

    result = recover(ctx)

    rc = int(not result)
    exit(rc)
