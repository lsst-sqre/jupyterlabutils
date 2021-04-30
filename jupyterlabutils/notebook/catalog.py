import os
from pathlib import Path

import requests
import pyvo
import pyvo.auth.authsession

TOKEN_PATH = "/opt/lsst/software/jupyterlab/environment/ACCESS_TOKEN"
"""Nublado 2 mounts the user's token here."""


class TokenNotFoundError(Exception):
    """No access token was found."""


def _get_tap_url():
    if "EXTERNAL_TAP_URL" in os.environ:
        return os.environ["EXTERNAL_TAP_URL"]
    else:
        return os.environ["EXTERNAL_INSTANCE_URL"] + os.environ["TAP_ROUTE"]


def _get_token() -> str:
    """Return notebook access token."""
    token_path = Path(TOKEN_PATH)
    if token_path.exists():
        return token_path.read_text()
    else:
        token = os.environ.get("ACCESS_TOKEN", None)
        if not token:
            msg = f"{TOKEN_PATH} missing and ACCESS_TOKEN not set"
            raise TokenNotFoundError(msg)
        return token


def _get_auth():
    tap_url = _get_tap_url()
    s = requests.Session()
    s.headers["Authorization"] = "Bearer " + _get_token()
    auth = pyvo.auth.authsession.AuthSession()
    auth.credentials.set("lsst-token", s)
    auth.add_security_method_for_url(tap_url, "lsst-token")
    auth.add_security_method_for_url(tap_url + "/sync", "lsst-token")
    auth.add_security_method_for_url(tap_url + "/async", "lsst-token")
    auth.add_security_method_for_url(tap_url + "/tables", "lsst-token")
    return auth


def get_catalog():
    return pyvo.dal.TAPService(_get_tap_url(), _get_auth())


def retrieve_query(query_url):
    return pyvo.dal.AsyncTAPJob(query_url, _get_auth())
