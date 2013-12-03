"""
Define the symbol resolution table.

"""

from sqlalchemy import Column, types
from sqlalchemy.ext.declarative import declarative_base

def table (base):
    class table_def (base):
        __tablename__ = 'symbol_resolve'
    
        symbol = Column(types.Unicode(255), primary_key=True)
        source = Column(types.Unicode(255), primary_key=True)
        resolve = Column(types.Unicode(255))

        def __repr__ (self):
            return "<symbol_resolve(symbol='%s', source='%s', resolve='%s')>" % (
                   self.symbol, self.source, self.resolve)

    return table_def

