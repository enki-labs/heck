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


def test_reuters_map ():
    """ Reuters code mapping """
    with open(os.path.join(os.path.dirname(os.path.realpath(__file__)), "static/reuters_code_map.txt")) as rfile:
        for line in rfile.readlines():
            codes = line.strip().split(",")
            vals = dict(symbol=codes[1], source="reuters", resolve=codes[0])
            schema.insert("symbol_resolve", values=vals)


#if __name__ == '__main__':
#    unittest.main()

