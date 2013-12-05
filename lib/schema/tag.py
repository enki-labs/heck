"""
Define the series table.

"""

from sqlalchemy import Column, Sequence, types
from sqlalchemy.ext.declarative import declarative_base


def table (base):
    class table_def (base):
        __tablename__ = 'tag'
        
        id = Column(types.Integer, Sequence('tag_id_seq'), primary_key=True)
        name = Column(types.Unicode(1000))
        value = Column(types.Unicode(1000))

        def __repr__ (self):
            return "<tag(id='%s', name='%s', value='%s')>" % (
                   self.id, self.name, self.value)

    return table_def

