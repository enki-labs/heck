"""
Storage schema and connections.

"""

from sqlalchemy.engine import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy import schema, types
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import sql
from bunch import Bunch
import os
from lib import common

""" Initial loading """

common.log.debug("load database engine")
engine = create_engine(common.db_connection, pool_size=20)
session_factory = sessionmaker(bind=engine)
base = declarative_base()
base.metadata.bind = engine

common.log.debug("load data schema")
table = Bunch()
for _,_,files in os.walk(__path__[0]):
    for fname in files:
        parts = os.path.splitext(fname)
        if len(parts) == 2 and parts[1] == ".py" and not parts[0] == "__init__":
            common.log.debug("import %s" % parts[0])
            imported_schema = __import__("lib.schema." + parts[0], globals(), locals(), ["table"], -1)
            table.update({parts[0]: imported_schema.table(base)})
    break

common.log.debug("init schema")
base.metadata.create_all(checkfirst=True)


class SessionWrapper (object):
    """ Wapper to enable use of sessions in the 'with' context """

    def __init__ (self, session, action):
        self._session = session
        self._action = action
    
    def __enter__ (self):
        return self._action

    def __exit__ (self, typ, value, tb):
        if tb is None:
            self._session.commit()
        else:
            self._session.rollback()
        self._session.close()


def select (table_name, *criterion):
    """ Select one or more elements from a table """
    session = session_factory()
    return SessionWrapper(session, session.query(table[table_name]).filter(*criterion))
    
def select_one (table_name, *criterion):
    """ Select one element from a table """
    session = session_factory()
    res = None
    try:
        res = session.query(table[table_name]).filter(*criterion).first()
    finally:
        session.close()
    return res

def save (obj):
    """ Upsert table """
    session = session_factory()
    try:
        session.add(obj)
        session.commit()
        session.refresh(obj)
    except:
        session.rollback()
        raise
    finally:
        session.close()
    
