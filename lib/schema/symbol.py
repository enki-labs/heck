"""
Define the symbol table.

"""

from sqlalchemy import Column, types
from sqlalchemy.ext.declarative import declarative_base

def table (base):
    class table_def (base):
        __tablename__ = 'symbol'
        
        symbol = Column(types.Unicode(255), primary_key=True)
        meta = Column(types.UnicodeText())

        def __repr__ (self):
            return "<series(symbol='%s', meta='%s')>" % (
                   self.symbol, self.meta)

    return table_def

