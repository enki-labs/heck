
from nose.tools import eq_, ok_
import config.test
import common

def test_path_encode_decode ():
    
    testinfo = dict(provider="test", symbol="AB%e ", year=2004)
    path = common.Path.get("series_store", testinfo)
    print(path)
    
    outinfo = common.Path.info(path)
    eq_(len(testinfo), 3)
    eq_(testinfo["provider"], outinfo["provider"])
    eq_(testinfo["symbol"], outinfo["symbol"])
    eq_(testinfo["year"], outinfo["year"])

