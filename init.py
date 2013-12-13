"""
Intialise the database and other static configuration.

"""


import unittest
import os
from datetime import datetime
import pytz
from pywebhdfs.webhdfs import PyWebHdfsClient
from nose.tools import with_setup
from config import prod
from lib import schema
from lib import common


def test_hdfs_paths ():
    """ Build default HDFS paths """
    """
    hdfs = PyWebHdfsClient(host='localhost',port='50070', user_name='hdfs')
    hdfs.make_dir("inbound/bloomberg")    
    hdfs.make_dir("inbound/bloomberg/ohlcv")
    hdfs.make_dir("inbound/bloomberg/instrument")
    hdfs.make_dir("inbound/reuters")
    hdfs.make_dir("inbound/reuters/tick")
    hdfs.make_dir("inbound/reuters/ohlcv")
    hdfs.make_dir("inbound/reuters/depth")
    hdfs.make_dir("data/store")
    """
    pass

def test_reuters_map ():
    """ Reuters code mapping """
    with open(os.path.join(os.path.dirname(os.path.realpath(__file__)), "static/reuters_code_map.txt")) as rfile:
        for line in rfile.readlines():
            codes = line.strip().split(",")
            symbol_resolve = schema.table.symbol_resolve()
            symbol_resolve.symbol=codes[1]
            symbol_resolve.source="reuters"
            symbol_resolve.resolve=codes[0]
            index = 2
            symbol_resolve.adjust_clear()
            price_adjustments = []
            while index < len(codes):
                start = common.Time.tick(datetime.strptime(codes[index], "%Y-%m-%d").replace(tzinfo=pytz.utc))
                multiplier = float(codes[index+1])
                price_adjustments.append(dict(start=start, multiplier=multiplier))
                index += 2
            symbol_resolve.adjust_add("price_adjustments", price_adjustments)
                    
            schema.save(symbol_resolve)
    raise "abc"

