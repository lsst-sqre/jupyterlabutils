import requests
import os
import pyvo
import pyvo.auth.authsession


def _get_tap_url():
    if 'EXTERNAL_TAP_URL' in os.environ:
        return os.environ['EXTERNAL_TAP_URL']
    else:
        return os.environ['EXTERNAL_INSTANCE_URL'] + os.environ['TAP_ROUTE']


def _get_token():
    # Start with environment variable, if it's there.
    token = os.getenv('ACCESS_TOKEN', default="")
    # But if we have a token file, use it preferentially.
    try:
        with open(os.path.join(os.getenv('HOME'), ".access_token"), "r") as f:
            # We could do token age calculations here based on PPID age and
            #  file age to determine which one to use.
            token = f.read().strip()
    except FileNotFoundError:
        pass  # We might have it in the environment, and if not, the token
        # will fail validation
    _validate_token(token)
    return token


def _validate_token(token):
    # We can get more sophisticated later
    if not token:
        raise ValueError("No access token")


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
