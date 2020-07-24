import os
from .labhubapp import SingleUserLabApp
from traitlets import (
    default
)
from urllib.parse import urlparse


class RubinLabApp(SingleUserLabApp):
    '''This is a Rubin Observatory-specific class that takes advantage of
    the very particular environment its Science Platform sets up in order
    to perform its tasks.
    '''

    def init_webapp(self, *args, **kwargs):
        super().init_webapp(*args, **kwargs)
        s = self.tornado_settings
        # These probably should be traitlets.
        jh = os.environ.get('JUPYTERHUB_API_URL') or ''
        s['rubin_hub_api_url'] = jh
        ph = urlparse(jh)
        s['rubin_hub_api_host'] = ph.hostname
        s['rubin_hub_api_path'] = ph.path
        s['rubin_hub_api_scheme'] = ph.scheme
        s['rubin_hub_api_port'] = ph.port
        s['rubin_hub_api_token'] = os.environ.get('JUPYTERHUB_API_TOKEN') or ''
        s['rubin_hub_user'] = os.environ.get('JUPYTERHUB_USER') or ''
        # FIXME just want to see this work
        self.log.debug("Tornado settings: {}".format(s))


def main(argv=None):
    return RubinLabApp.launch_instance(argv)


if __name__ == "__main__":
    main()
