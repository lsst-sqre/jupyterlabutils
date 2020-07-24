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

    @default('rubin_hub_api_url')
    def _rubin_hub_api_url_default(self):
        return os.environ.get('JUPYTERHUB_API_URL') or ''

    @default('rubin_hub_api_host')
    def _rubin_hub_api_host_default(self):
        return urlparse(os.environ.get('JUPYTERHUB_API_URL')).hostname or ''

    @default('rubin_hub_api_path')
    def _rubin_hub_api_path_default(self):
        return urlparse(os.environ.get('JUPYTERHUB_API_URL')).path or ''

    @default('rubin_hub_api_scheme')
    def _rubin_hub_api_scheme_default(self):
        return urlparse(os.environ.get('JUPYTERHUB_API_URL')).scheme or ''

    @default('rubin_hub_api_port')
    def _rubin_hub_api_port_default(self):
        return urlparse(os.environ.get('JUPYTERHUB_API_URL')).port or ''

    @default('rubin_hub_api_token')
    def _rubin_hub_api_token_default(self):
        return os.getenv('JUPYTERHUB_API_TOKEN') or ''

    @default('rubin_hub_user')
    def _rubin_hub_user_default(self):
        return os.environ.get('JUPYTERHUB_USER') or ''

    def init_webapp(self, *args, **kwargs):
        super().init_webapp(*args, **kwargs)
        s = self.tornado_settings
        s['rubin_hub_api_url'] = self.rubin_hub_api_url
        s['rubin_hub_api_host'] = self.rubin_hub_api_host
        s['rubin_hub_api_path'] = self.rubin_hub_api_path
        s['rubin_hub_api_scheme'] = self.rubin_hub_api_scheme
        s['rubin_hub_api_port'] = self.rubin_hub_api_port
        s['rubin_hub_api_token'] = self.rubin_hub_api_token
        s['rubin_hub_user'] = self.rubin_hub_user
        # FIXME just want to see this work
        self.log.debug("Tornado settings: {}".format(s))


def main(argv=None):
    return RubinLabApp.launch_instance(argv)


if __name__ == "__main__":
    main()
