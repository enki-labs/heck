
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

def test_store ():

    common.store.mkdir("test/test2")

    with common.store.write("test/testfile.txt") as testfile:
        content = testfile.local().file
        content.write(b"line 1\n")
        content.write(b"line 2\n")
        testfile.save()
    with common.store.read("test/testfile.txt") as testfile:
        content = testfile.local().file
        eq_(content.readline(), b"line 1\n")
        eq_(content.readline(), b"line 2\n")
        eq_(content.readline(), b"")

