from lsst.daf.butler import DbAuth


class NotebookAuth(DbAuth):
    def __init__(self, path='~/.lsst/notebook_auth.yaml', envVar=None, authList=None):
        super().__init__(path, envVar, authList)

    def getAuth(self, alias):
        return (self.authList[alias]['host'], self.authList[alias]['username'],
                self.authList[alias]['password'])
