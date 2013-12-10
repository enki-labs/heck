"""
Define the lock table.

"""

from sqlalchemy import Column, types
from sqlalchemy.ext.declarative import declarative_base

def table (base):
    class table_def (base):
        __tablename__ = 'lock'
        
        name = Column(types.Unicode(10000), primary_key=True)
        instance = Column(types.Unicode(1000))
        acquired = Column(types.BigInteger)

        def __repr__ (self):
            return "<lock(name='%s', acquired='%s', instance='%s')>" % (
                   self.name, self.acquired, self.instance)

    return table_def

