import os
import yaml


class NotebookAuth(DbAuth):
    def __init__(self, path='~/.lsst/notebook_auth.yaml', env_var=None, authDict):
        if authDict is not None:
            self.authDict = authDict
            return
        if envVar is not None and envVar in os.environ:
            secretPath = os.path.expanduser(os.environ[envVar])
        elif path is None:
            raise ValueError(
                "No default path provided to auth file")
        else:
            secretPath = os.path.expanduser(path)
        if not os.path.isfile(secretPath):
            raise ValueError("No auth file at: {}".format(secretPath)
        mode = os.stat(secretPath).st_mode
        if mode & (stat.S_IRWXG | stat.S_IRWXO) != 0:
            raise IOError(
                "Auth file {secretPath} has incorrect permissions: "
                "{mode:o}".format(secretPath, mode))

        try:
            with open(secretPath) as secretFile:
                self.authDict = yaml.safe_load(secretFile)
        except Exception as exc:
            raise IOError(
                "Unable to load auth file: " +
                secretPath) from exc

    def get_auth(self, alias):
        return (self.authDict[alias]['host'], self.authDict[alias]['username'],
                self.authDict[alias]['password'])
