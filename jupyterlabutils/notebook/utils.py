"""Utility functions for LSST JupyterLab notebook environment
"""
import bokeh
import os
from kubernetes import client, config
from urllib.parse import urljoin


def format_bytes(n):
    """ Format bytes as text

    >>> format_bytes(1)
    '1 B'
    >>> format_bytes(1234)
    '1.23 kB'
    >>> format_bytes(12345678)
    '12.35 MB'
    >>> format_bytes(1234567890)
    '1.23 GB'
    >>> format_bytes(1234567890000)
    '1.23 TB'
    >>> format_bytes(1234567890000000)
    '1.23 PB'

    (taken from dask.distributed, where it is not exported)
    """
    if n > 1e15:
        return '%0.2f PB' % (n / 1e15)
    if n > 1e12:
        return '%0.2f TB' % (n / 1e12)
    if n > 1e9:
        return '%0.2f GB' % (n / 1e9)
    if n > 1e6:
        return '%0.2f MB' % (n / 1e6)
    if n > 1e3:
        return '%0.2f kB' % (n / 1000)
    return '%d B' % n


def get_hostname():
    """Utility function to return hostname or, failing that, "localhost".
    """
    return os.environ.get('HOSTNAME') or 'localhost'


def show_with_bokeh_server(obj):
    """Method to wrap bokeh with proxy URL
    """
    def jupyter_proxy_url(port):
        """Construct proxy URL from environment
        """
        # If port is None we're asking about the URL
        # for the origin header.
        return get_proxy_url(port) or '*'

    bokeh.io.show(obj, notebook_url=jupyter_proxy_url)


def get_pod():
    """Get pod record.  Throws an error if you're not running in a cluster.
    """
    config.load_incluster_config()
    api = client.CoreV1Api()
    namespace = 'default'
    with open('/var/run/secrets/kubernetes.io/serviceaccount/namespace',
              'r') as f:
        namespace = f.readlines()[0]
    pod = api.read_namespaced_pod(get_hostname(), namespace)
    return pod


def get_node():
    """Extract node name from pod."""
    return get_pod().spec.node_name
