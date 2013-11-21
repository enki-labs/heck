"""
Common config, logging etc.

"""


import sys
import logging
import autologging
from autologging import logged, traced, TracedMethods

log = logging.getLogger("heck")
log.setLevel(autologging.TRACE)
__stdout_handler = logging.StreamHandler(sys.stdout)
__stdout_handler.setLevel(autologging.TRACE)
__formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
__stdout_handler.setFormatter(__formatter)
log.addHandler(__stdout_handler)


class Path (object):
    """
    Paths.
    """

    @staticmethod
    def get (typ, name):
        """
        Get path.
        """
        return "data/" + name


