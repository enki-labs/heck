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
import json
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


def query (table_name, *criterion):
    """ Run a complex query """
    session = session_factory()
    query = session.query(*criterion)
    return SessionWrapper(session, query)

def select (table_name, *criterion):
    """ Select one or more elements from a table """
    session = session_factory()
    query = session.query(table[table_name]).filter(*criterion)
    #if limit: query = query.limit(limit)
    return SessionWrapper(session, query)
    
def select_one (table_name, *criterion):
    """ Select one element from a table """
    session = session_factory()
    res = None
    try:
        res = session.query(table[table_name]).filter(*criterion).first()
    finally:
        session.close()
    return res

def update (table_name, *where_clause, **update_values):
    """ Update using a where clause """
    session = session_factory()
    try:
        session.query(table[table_name]).filter(*where_clause).update(update_values)
        session.commit()
    except:
        session.rollback()
        raise
    finally:
        session.close()

def delete (obj):
    """ Delete from table """
    create_session = obj._sa_instance_state.session == None
    session = session_factory() if create_session else obj._sa_instance_state.session
    try:
        session.delete(obj)
        session.commit()
    except:
        session.rollback()
        raise
    finally:
        if create_session: session.close()
        
def save (obj):
    """ Upsert table """
    create_session = obj._sa_instance_state.session == None
    session = session_factory() if create_session else obj._sa_instance_state.session
    try:
        session.add(obj)
        session.commit()
        session.refresh(obj)
    except:
        session.rollback()
        raise
    finally:
        if create_session: session.close()

def refresh (obj):
    """ Refresh the object """
    if obj._sa_instance_state.session:
        obj._sa_instance_state.session.refresh(obj)
   

class StringDictionaryWrapper (object):
    """
    Provide a dictionary-like interface to string fields 
    serialized as JSON.
    """

    def __init__ (self, target, attribute):
        """
        Init with a target string field.
        The field value will be defaulted to an empty dictionary.
        """
        self._target = target
        self._attribute = attribute
        if not getattr(target, attribute):
            setattr(target, attribute, "{}")

    def _parse (self):
        """ Parse JSON serialized dictionary """
        return json.loads(getattr(self._target, self._attribute))

    def _encode (self, value):
        """ Encode dictionary as JSON serialized string """
        setattr(self._target, self._attribute, json.dumps(value))

    def __getitem__ (self, key):
        return self._parse().__getitem__(key)

    def __setitem__ (self, key, value):
        temp = self._parse()
        temp.__setitem__(key, value)
        self._encode(temp)

    def __delitem__ (self, key):
        temp = self._parse()
        temp.__delitem__(key)
        self._encode(temp)

    def __iter__ (self):
        return self._parse().__iter__()

    def __len__ (self):
        return self._parse().__len__()

    def __contains__ (self, x):
        return self._parse().__contains__(x) 

