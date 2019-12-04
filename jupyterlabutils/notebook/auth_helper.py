from lsst.daf.butler import DbAuth


class NotebookAuth(DbAuth):
    def __init__(self, path='~/.lsst/notebook_auth.yaml', env_var=None):
        super().__init__(path, env_var)

    def get_auth(self, alias):
        return (self.authList[alias]['host'], self.authList[alias]['username'],
                self.authList[alias]['password'])
