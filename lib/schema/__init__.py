"""
Storage schema and connections.

"""

from sqlalchemy.engine import create_engine
from sqlalchemy import schema, types
import os
from lib import common
from sqlalchemy import sql

""" Initial loading """

common.log.debug("load database engine")
#postgresql://me@localhost/mydb
metadata = schema.MetaData()
engine = create_engine("sqlite:///:memory:", pool_size=20)
metadata.bind = engine

common.log.debug("load data schema")
for _,_,files in os.walk(__path__[0]):
    for fname in files:
        parts = os.path.splitext(fname)
        if len(parts) == 2 and parts[1] == ".py" and not parts[0] == "__init__":
            common.log.debug("import %s" % parts[0])
            imported_schema = __import__("schema." + parts[0], globals(), locals(), ["table"], -1)
            imported_schema.table(metadata)
    break

common.log.debug("init schema")
metadata.create_all(checkfirst=True)


def table (name):
    """ Return a table schema """
    return metadata.tables[name]

def select (table, *args):
    """ Select from a table """
    return engine.execute(sql.select([table], *args))

def insert (table, values):
    """ Insert into a table """
    return engine.execute(metadata.tables[table].insert(values=values))

    
