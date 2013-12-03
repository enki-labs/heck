"""
Intialise the database and other static configuration.

"""

import unittest
import os
from pywebhdfs.webhdfs import PyWebHdfsClient

import schema


def test_hdfs_paths ():
    """ Build default HDFS paths """
    hdfs = PyWebHdfsClient(host='localhost',port='50070', user_name='hdfs')
    hdfs.make_dir("inbound/bloomberg")    
    hdfs.make_dir("inbound/bloomberg/ohlcv")
    hdfs.make_dir("inbound/bloomberg/instrument")
    hdfs.make_dir("inbound/reuters")
    hdfs.make_dir("inbound/reuters/tick")
    hdfs.make_dir("inbound/reuters/ohlcv")
    hdfs.make_dir("inbound/reuters/depth")


def test_reuters_map ():
    """ Reuters code mapping """
    with open(os.path.join(os.path.dirname(os.path.realpath(__file__)), "static/reuters_code_map.txt")) as rfile:
        for line in rfile.readlines():
            codes = line.strip().split(",")
            symbol_resolve = schema.table("symbol_resolve")
            symbol_resolve.symbol=codes[1]
            symbol_resolve.source="reuters"
            symbol_resolve.resolve=codes[0]
            schema.save(symbol_resolve)


