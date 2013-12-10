"""
Storage of large binary data.

"""

class Store (object):
    """
    Define the store interface.
    """

    def mkdir (self, path):
        """ Make a directory including parent directories """
        raise NotImplementedError()

    def read (self, path):
        """ Get a reader for a store file. Use in with statement """
        raise NotImplementedError()

    def append (self, path):
        """ Get an appender for a store file. Use in with statement """
        raise NotImplementedError()

    def write (self, path):
        """ Get a writer for a store file. Use in with statement """
        raise NotImplementedError()

    def exists (self, path):
        """ Return true if a file exists """
        raise NotImplementedError()

    def walk (self, path, visitor, recursive = False):
        """ Walk files in a path. Use recursive=True to include subdirs """
        raise NotImplementedError()


class StoreFile (object):
    """
    Define the store file interface.
    """

    def __enter__ (self):
        """ Support 'with' statement """
        raise NotImplementedError()

    def __exit__ (self, typ, value, tb):
        """ Close and remove temp file """
        raise NotImplementedError()

    def local (self):
        """ Get a local file instance """
        raise NotImplementedError()

    def save (self):
        """ Write file to store """
        raise NotImplementedError()

