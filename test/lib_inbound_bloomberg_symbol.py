
from nose.tools import eq_, ok_
from config import test
from lib.inbound import bloomberg_symbol
from io import StringIO
from lib import schema

def test_parse_future_contract_2000 ():
    symbol = "SIM07"
    contract = bloomberg_symbol.parse_future_contract(symbol)
    ok_(contract != None)
    eq_(contract["month"], "6")
    eq_(contract["year"], "2007")
    eq_(contract["symbol"], "SI")

def test_parse_future_contract_1990 ():
    symbol = "L F97"
    contract = bloomberg_symbol.parse_future_contract(symbol)
    ok_(contract != None)
    eq_(contract["month"], "1")
    eq_(contract["year"], "1997")
    eq_(contract["symbol"], "L ")

def test_parse_future_contract_2013 ():
    symbol = "ESZ5"
    contract = bloomberg_symbol.parse_future_contract(symbol)
    ok_(contract != None)
    eq_(contract["month"], "12")
    eq_(contract["year"], "2015")
    eq_(contract["symbol"], "ES")

def test_parse_future_contract_not ():
    eq_(bloomberg_symbol.parse_future_contract("NMCMFUS"), None)
    eq_(bloomberg_symbol.parse_future_contract("USSP30"), None)
    eq_(bloomberg_symbol.parse_future_contract("USFS019"), None)

def test_parse_symbol_short ():
    ret = bloomberg_symbol.parse_symbol("ES1 Index")
    ok_("class" in ret)
    ok_("symbol" in ret)
    ok_("year" not in ret)
    eq_(ret["class"], "index")
    eq_(ret["symbol"], "ES1")

def test_parse_symbol_long ():
    ret = bloomberg_symbol.parse_symbol("USFS019 BLC Curncy")
    ok_("class" in ret)
    ok_("symbol" in ret)
    ok_("year" not in ret)
    ok_("source" in ret)
    eq_(ret["class"], "curncy")
    eq_(ret["symbol"], "USFS019")
    eq_(ret["source"], "BLC")

def test_parse_symbol_future ():
    ret = bloomberg_symbol.parse_symbol("USU90 Comdty")
    ok_("class" in ret)
    ok_("symbol" in ret)
    ok_("year" in ret)
    eq_(ret["class"], "comdty")
    eq_(ret["symbol"], "US")
    eq_(ret["year"], "1990")
    eq_(ret["month"], "9")

def test_import ():
    testdata = """
test
test
test
START-OF-DATA
ABCM10 Index||||ABC DESC|CODE|||CUR|Desc|||||EXCH|||CODE2||||||Future||
DEFG Index||||DEFG DESC|CODE|||CUR|Desc|||||EXCH|||CODE2||||||Index||
END-OF-DATA
test
test
"""
    bloomberg_symbol.Import.parse(StringIO(testdata))
    ok_(schema.select_one("symbol", schema.table.symbol.symbol=="ABC") != None)
    ok_(schema.select_one("symbol", schema.table.symbol.symbol=="DEFG") != None)

