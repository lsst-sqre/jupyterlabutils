import requests
import os
import pyvo
import pyvo.auth.authsession

def _get_tap_url():
    if 'EXTERNAL_TAP_URL' in os.environ:
        return os.environ['EXTERNAL_TAP_URL']
    else:
        return os.environ['EXTERNAL_INSTANCE_URL'] + os.environ['TAP_ROUTE']

def _get_auth():
    tap_url = _get_tap_url()
    s = requests.Session()
    s.headers['Authorization'] = 'Bearer ' + os.environ['ACCESS_TOKEN']
    auth = pyvo.auth.authsession.AuthSession()
    auth.credentials.set('lsst-token', s)
    auth.add_security_method_for_url(tap_url, 'lsst-token')
    auth.add_security_method_for_url(tap_url + '/sync', 'lsst-token')
    auth.add_security_method_for_url(tap_url + '/async', 'lsst-token')
    return auth

def get_catalog():
    return pyvo.dal.TAPService(_get_tap_url(), _get_auth())

def retrieve_query(query_url):
    return pyvo.dal.AsyncTAPJob(query_url, _get_auth())
