# =============================================================================
# 2013+ Copyright (c) Kirill Smorodinnikov <shaitkir@gmail.com>
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
Wrapper for monitoring data and working with it in user code
"""

import os
import socket

from datetime import datetime
from threading import Thread
from multiprocessing import Manager

from .utils.misc import logged_class
from .stat import Stats

from BaseHTTPServer import HTTPServer
from SimpleHTTPServer import SimpleHTTPRequestHandler

STAT_NONE = 'none'
STAT_TEXT = 'text'
STAT_JSON = 'json'
ALLOWED_STAT_FORMATS = (STAT_NONE, STAT_TEXT, STAT_JSON)


@logged_class
class StatsProxy(object):
    """
    Very simple wrapper that forwards counter and timer methods to queue.
    Also it provides access to sub-stats via []
    """
    COUNTER = 1
    SET_COUNTER = 2
    TIMER = 3
    ATTRIBUTE = 4

    def __init__(self, queue, prefix=''):
        self.queue = queue
        self.prefix = prefix

    def counter(self, name, value):
        try:
            self.queue.put_nowait((self.prefix, self.COUNTER, name, value))
        except:
            self.log.exception('Failed to update counter')
            raise

    def set_counter(self, name, value):
        try:
            self.queue.put_nowait((self.prefix, self.SET_COUNTER, name, value))
        except:
            self.log.exception('Failed to set counter')
            raise

    def timer(self, name, milestone):
        try:
            self.queue.put_nowait((self.prefix, self.TIMER, name, milestone, datetime.now()))
        except:
            self.log.exception('Failed to update timer')
            raise

    def attribute(self, name, value):
        try:
            self.queue.put_nowait((self.prefix, self.ATTRIBUTE, name, value))
        except:
            self.log.exception('Failed to set attribute')
            raise

    def __getitem__(self, item):
        prefix = item
        if self.prefix:
            prefix = '\\'.join([self.prefix, prefix])
        return StatsProxy(self.queue, prefix=prefix)


@logged_class
class Monitor(object):
    """
    Contains monitoring data and provides interface for manipulating it
    from detached threads/processes
    """
    # TODO: remove @ctx when no one specifies it
    def __init__(self, ctx=None, port=None, stat_format=None, path=None):
        """Initializes Monitor

        Args:
            ctx (obj): old way to pass arguments
            port (int): port number to serve by http server
            stat_format (str): one of ALLOWED_STAT_FORMATS
            path (str): path for dumping statistics

        Note:
            `ctx` and current order is kept for backward compatibility. If `ctx` is specified then
            `ctx.stat_format` and `ctx.tmp_dir` will be used instead of `stat_format` and `path`.
        """
        if ctx:
            stat_format = ctx.stat_format
            path = ctx.tmp_dir
        self.port = port
        self.stat_format = stat_format
        self.manager = Manager()
        self.queue = self.manager.Queue()
        self.stats = StatsProxy(self.queue)
        self.__shutdown_request = False
        self.__stats = Stats('monitor')
        if self.stat_format == STAT_TEXT:
            self.stats_file = 'stats.txt'
        elif self.stat_format == STAT_JSON:
            self.stats_file = 'stats.json'
        else:
            self.stats_file = 'stats'
        self.stats_file = os.path.join(path, self.stats_file)

        self.d_thread = Thread(target=self.data_thread, name="MonitorDataThread")
        self.d_thread.daemon = True

        if self.port:
            if socket.has_ipv6:
                HTTPServer.address_family = socket.AF_INET6
                address = '::'
            else:
                HTTPServer.address_family = socket.AF_INET
                address = '0.0.0.0'
            server_address = (address, self.port)
            self.httpd = HTTPServer(server_address, SimpleHTTPRequestHandler)
            self.l_thread = Thread(target=self.listen_thread, name="MonitorListenThread")
            self.l_thread.daemon = True

        self.u_thread = Thread(target=self.update_thread, name="MonitorUpdateThread")
        self.u_thread.daemon = True

        self.d_thread.start()
        if self.port:
            self.l_thread.start()
        self.u_thread.start()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.shutdown()
        self.update()
        self.print_stat()

    def update(self):
        """
        Writes to file current stats
        """
        stats_file_tmp = os.path.join(self.stats_file + '.tmp')
        with open(stats_file_tmp, 'w') as f:
            if self.stat_format == STAT_TEXT:
                f.write(str(self.__stats))
            elif self.stat_format == STAT_JSON:
                f.write(self.__stats.json())
            f.write('\n')
        try:
            if os.path.exists(stats_file_tmp):
                os.rename(stats_file_tmp, self.stats_file)
        except Exception as e:
            self.log.error("Failed to rename {0} to {1}: {2}".format(stats_file_tmp, self.stats_file, e))

    def data_thread(self):
        """
        TODO: Not very pythonish interface, but OK for now.
        """
        import Queue
        while not self.__shutdown_request:
            try:
                data = self.queue.get(block=True, timeout=1)
            except Queue.Empty:
                continue
            except:
                self.log.exception('Failed to wait on queue')
                break

            try:
                prefix = data[0]
                stats = self.__stats

                # If prefix was set then use sub stat
                if prefix:
                    for sub in prefix.split('\\'):
                        stats = stats[sub]

                # Use different handling for different stat flavours
                flavour = data[1]
                if flavour == StatsProxy.COUNTER:
                    _, _, name, value = data
                    counter = getattr(stats.counter, name)
                    if value > 0:
                        counter += value
                    else:
                        counter -= -value
                elif flavour == StatsProxy.SET_COUNTER:
                    _, _, name, value = data
                    counter = getattr(stats.counter, name)
                    if value > 0:
                        counter.success = value
                    elif value < 0:
                        counter.failures = -value
                    else:
                        counter.success, counter.failures = 0, 0
                elif flavour == StatsProxy.TIMER:
                    _, _, name, milestone, ts = data
                    timer = getattr(stats.timer, name, ts)
                    timer(milestone)
                elif flavour == StatsProxy.ATTRIBUTE:
                    _, _, name, value = data
                    attribute = getattr(stats.attributes, name)
                    attribute.append(value)
                else:
                    raise RuntimeError("Unknown flavour: {0}".format(data))
            except Exception as e:
                self.log.error("Failed to process: {0}: {1}".format(data, e))

    def listen_thread(self):
        sa = self.httpd.socket.getsockname()
        self.log.debug("Serving HTTP on {0}:{1} port...".format(sa[0], sa[1]))
        self.httpd.serve_forever()

    def update_thread(self, seconds=1):
        """
        Periodically saves stats to file
        """
        from time import sleep
        while not self.__shutdown_request:
            try:
                self.update()
            except Exception as e:
                self.log.error("Got an error during stats update: {0}".format(e))
            sleep(seconds)

    def shutdown(self):
        """
        FIXME: We also need a condition variable per thread to really check that thread is finished
        """
        self.__shutdown_request = True
        if self.port:
            self.httpd.shutdown()
        self.d_thread.join()
        if self.port:
            self.l_thread.join()
        self.u_thread.join()
        self.manager.shutdown()

    def print_stat(self):
        '''
        Prints to stdout final statistics in requested format
        '''
        with open(self.stats_file) as lines:
            for line in lines:
                print line,
