"""
Define the inbound table.

"""

from sqlalchemy import Column, types
from sqlalchemy.ext.declarative import declarative_base

def table (base):
    class table_def (base):
        __tablename__ = 'inbound'
        
        path = Column(types.Unicode(10000), primary_key=True)
        status = Column(types.Unicode(1000))
        meta = Column(types.UnicodeText())

        def __repr__ (self):
            return "<file(path='%s', status='%s', meta='%s')>" % (
                   path, status, meta)

    return table_def

