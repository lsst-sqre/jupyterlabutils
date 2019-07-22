from .utils import get_proxy_url, format_bytes
import dask
from dask.distributed import Client, sync
from datetime import timedelta
from distributed.utils import thread_state
import logging
import os
from tornado import gen
from tornado.ioloop import IOLoop
logger = logging.getLogger(__name__)


class LSSTDaskClient(Client):
    """This uses the proxy info to provide an externally-reachable dashboard
    URL for the Client.  It assumes the LSST JupyterLab environment, and uses
    an "overall_timeout" parameter for how long to wait on a cell's results
    before giving up; this parameter is an integer, represents the wait time
    in seconds, and defaults to 3600 (-1 is "never time out).  Otherwise it
    is a standard Dask client.
    """
    proxy_url = None
    overall_timeout = 3600

    def __init__(self, *args, **kwargs):
        ot = kwargs.pop('overall_timeout', None)
        super().__init__(*args, **kwargs)
        if ot:
            if ot == -1:
                self.overall_timeout = None
            else:
                self.overall_timeout = ot

    def sync(self, func, *args, asynchronous=None, callback_timeout=None,
             **kwargs):
        if (
            asynchronous
            or self.asynchronous
            or getattr(thread_state, "asynchronous", False)
        ):
            future = func(*args, **kwargs)
            if callback_timeout is not None:
                future = gen.with_timeout(timedelta(seconds=callback_timeout),
                                          future)
            return future
        else:
            if callback_timeout is None:
                callback_timeout = self.overall_timeout
            so_far = 0
            attempt_timeout = self._timeout or 15
            while so_far < callback_timeout:
                so_far += attempt_timeout
                try:
                    return sync(
                        self.loop, func, *args,
                        callback_timeout=attempt_timeout,
                        **kwargs
                    )
                except gen.TimeoutError as exc:
                    logger.debug("Timeout: {}".format(exc))
                    attempt_timeout = attempt_timeout * 2
                    if so_far + attempt_timeout >= callback_timeout:
                        attempt_timeout = callback_timeout - so_far
                    if attempt_timeout > 0:
                        logger.debug(
                            "Retry: {}s timeout.".format(attempt_timeout))
            raise TimeoutError("timed out after {} s.".format(attempt_timeout))

    @gen.coroutine
    def _update_scheduler_info(self):
        if self.status not in ('running', 'connecting'):
            logger.debug("Unexpected status '%s'" % self.status)
            return
        try:
            self._scheduler_identity = yield self.scheduler.identity()
        except EnvironmentError:
            logger.debug("Not able to query scheduler for identity")
        srv = self._scheduler_identity.get("services")
        if srv:
            self.proxy_url = get_proxy_url(srv.get("dashboard"))

    def __repr__(self):
        # Note: avoid doing I/O here...
        self._update_scheduler_info()
        info = self._scheduler_identity
        addr = self.proxy_url or info.get('address')
        if addr:
            workers = info.get('workers', {})
            nworkers = len(workers)
            nthreads = sum(w['nthreads'] for w in workers.values())
            return '<%s: scheduler=%r processes=%d threadss=%d>' % (
                self.__class__.__name__, addr, nworkers, nthreads)
        elif self.scheduler is not None:
            return '<%s: scheduler=%r>' % (
                self.__class__.__name__, self.scheduler.address)
        else:
            return '<%s: not connected>' % (self.__class__.__name__,)

    def _repr_html_(self):
        self._update_scheduler_info()
        if (self.cluster and hasattr(self.cluster, 'scheduler') and
                self.cluster.scheduler):
            info = self.cluster.scheduler.identity()
            scheduler = self.cluster.scheduler
        elif (self._loop_runner.is_started() and
                self.scheduler and
                not (self.asynchronous and self.loop is IOLoop.current())):
            info = sync(self.loop, self.scheduler.identity,
                        callback_timeout=(self._timeout or 15))
            scheduler = self.scheduler
        else:
            info = False
            scheduler = self.scheduler

        if scheduler is not None:
            text = ("<h3>Client</h3>\n"
                    "<ul>\n"
                    "  <li><b>Scheduler: </b>%s\n") % scheduler.address
        else:
            text = ("<h3>Client</h3>\n"
                    "<ul>\n"
                    "  <li><b>Scheduler: not connected</b>\n")
        text += "  <li><b>Dashboard: </b>"
        if self.proxy_url:
            url = self.proxy_url + "/status"
            text += "<br> "
            text += "<a href='{addr}' target='_blank'>{addr}</a>\n".format(
                addr=url)
        elif info and 'dashboard' in info['services']:
            protocol, rest = scheduler.address.split('://')
            port = info['services']['dashboard']
            if protocol == 'inproc':
                host = 'localhost'
            else:
                host = rest.split(':')[0]
            template = dask.config.get('distributed.dashboard.link')
            address = template.format(host=host, port=port, **os.environ)
            text += "<a href='%(web)s' target='_blank'>%(web)s</a>\n" % {
                'web': address}

        text += "</ul>\n"

        if info:
            workers = len(info['workers'])
            threads = sum(w['nthreads'] for w in info['workers'].values())
            memory = sum(w['memory_limit'] for w in info['workers'].values())
            memory = format_bytes(memory)
            text2 = ("<h3>Cluster</h3>\n"
                     "<ul>\n"
                     "  <li><b>Workers: </b>%d</li>\n"
                     "  <li><b>Threads: </b>%d</li>\n"
                     "  <li><b>Memory: </b>%s</li>\n"
                     "</ul>\n") % (workers, threads, memory)

            return ('<table style="border: 2px solid white;">\n'
                    '<tr>\n'
                    '<td style="vertical-align: top; '
                    'border: 0px solid white">\n%s</td>\n'
                    '<td style="vertical-align: top; '
                    'border: 0px solid white">\n%s</td>\n'
                    '</tr>\n</table>') % (text, text2)

        else:
            return text
