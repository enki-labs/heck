
from nose.tools import eq_, ok_
import config.test
from lib import common
from lib import schema


def test_insert ():
    
    unsorted = [456, 123, 980]
    sortd = [123, 456, 980]

    insert = schema.table.series()
    insert.tags = unsorted
    schema.save(insert)

    inserted = schema.select_one("series", schema.table.series.tags==sortd)
    ok_(inserted != None)
    eq_(inserted.tags, sortd)

