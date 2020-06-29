import os

import requests
import pyvo
import pyvo.auth.authsession

from jupyterhubutils.utils import get_access_token, parse_access_token


def _get_tap_url():
    if 'EXTERNAL_TAP_URL' in os.environ:
        return os.environ['EXTERNAL_TAP_URL']
    else:
        return os.environ['EXTERNAL_INSTANCE_URL'] + os.environ['TAP_ROUTE']


def _get_token():
    '''Returns access token if (and only if) it is valid; otherwise throws
    exception.'''
    token = get_access_token()
    gfendpoint = os.getenv("EXTERNAL_GAFAELFAWR_URL", None)
    # parse_access_token() will throw an exception if the token is not
    #  valid.
    parse_access_token(endpoint=gfendpoint, token=token)
    return token


def _get_auth():
    tap_url = _get_tap_url()
    s = requests.Session()
    s.headers['Authorization'] = 'Bearer ' + _get_token()
    auth = pyvo.auth.authsession.AuthSession()
    auth.credentials.set('lsst-token', s)
    auth.add_security_method_for_url(tap_url, 'lsst-token')
    auth.add_security_method_for_url(tap_url + '/sync', 'lsst-token')
    auth.add_security_method_for_url(tap_url + '/async', 'lsst-token')
    auth.add_security_method_for_url(tap_url + '/tables', 'lsst-token')
    return auth


def get_catalog():
    return pyvo.dal.TAPService(_get_tap_url(), _get_auth())


def retrieve_query(query_url):
    return pyvo.dal.AsyncTAPJob(query_url, _get_auth())
