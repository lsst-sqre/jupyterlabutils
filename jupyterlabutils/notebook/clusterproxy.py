import logging
import os
from .forwarder import Forwarder
from dask.distributed import Client


def _remove_prefix(s, prefix):
    return s[len(prefix):] if s.startswith(prefix) else s


def _get_hostname():
    return os.environ.get('HOSTNAME') or 'localhost'


class ClusterProxy(object):
    """Provides a proxy service to map a local port to a worker node's
    dashboard, which is on port 8989.  This allows us to proxy to the
    worker even though the k8s network is not accessible externally.

    It must be created with an instance of a dask.distributed.Client
    as its argument.
    """
    client = None
    cluster = None
    ioloop = None
    workers = {}

    def __init__(self, client):
        if not isinstance(client, Client):
            estr = "'client' argument must be dask.distributed.Client!"
            raise RuntimeError(estr)
        self.client = client
        self.logger = logging.getLogger(__name__)
        self.cluster = client.cluster
        self.ioloop = client.io_loop
        self.refresh_workers()

    def refresh_workers(self):
        """Rebuild current worker map from actual state.
        """
        coremap = self.client.ncores()
        current_workerlist = list(coremap.keys())
        current_workerlist = [_remove_prefix(x, "tcp://")
                              for x in current_workerlist]
        removed_workers = []
        for worker_id in self.workers:
            if worker_id not in current_workerlist:
                # Get rid of removed workers
                self._remove_worker_proxy(worker_id)
                removed_workers.append(worker_id)
        for worker_id in removed_workers:
            del self.workers[worker_id]
        for worker_id in current_workerlist:
            if worker_id not in self.workers:
                self.workers[worker_id] = self._create_worker_proxy(worker_id)
            # Otherwise it hasn't changed.

    def _remove_worker_proxy(self, worker_id):
        worker = self.workers.get(worker_id)
        if not worker:
            return
        forwarder = worker["forwarder"]
        forwarder.stop()

    def _create_worker_proxy(self, worker_id):
        host = worker_id.split(":")[0]
        port = 8989
        proxy = Forwarder(host, port, ioloop=self.ioloop)
        proxy.start()
        local_port = proxy.get_port()
        urlprefix = os.environ.get("EXTERNAL_URL") or "http://localhost:8888"
        user = os.environ.get("USER") or "jovyan"
        url = "/".join([urlprefix, "user", user, "proxy", str(local_port)])
        worker = {"forwarder": proxy,
                  "url": url,
                  "local_port": local_port}
        return worker

    def get_proxies(self, workers):
        rval = {}
        if not workers:
            workers = list(self.workers.keys())
        if workers:
            if type(workers) is str:
                workers = [workers]
        for worker in workers:
            rval[worker] = {"url": self.workers[worker]["url"],
                            "local_port": self.workers[worker]["local_port"]}
        return rval

    def __repr__(self):

        s = "ClusterProxy {name} (workers={count:d})".format(
            name=_get_hostname(), count=len(self.workers))
        sw = self.workers
        for worker in sw:
            s += "\n  {worker}: {url}".format(worker=worker,
                                              url=sw[worker]["url"])
        return s

    def _repr_html_(self):
        s = "<h3>ClusterProxy {name}<h3>", format(name=_get_hostname())
        if len(self.workers) > 0:
            s += "<dl>"
            sw = self.workers
            for worker, details in sw:
                s += "<dt>{w}</dt><dd>{u}</dd>".format(w=worker,
                                                       u=sw[worker]["url"])
            s += "</dl>"
        return s
