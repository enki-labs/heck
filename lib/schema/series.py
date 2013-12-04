"""
Define the series table.

"""

from sqlalchemy import Column, Sequence, types
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import validates
from sqlalchemy.dialects.postgresql import ARRAY


def table (base):
    class table_def (base):
        __tablename__ = 'series'
        
        id = Column(types.Integer, Sequence('series_id_seq'), primary_key=True)
        tags = Column(ARRAY(types.Integer))

        @validates('tags')
        def _set_tags (self, key, value):
            """ Ensure tag array always sorted """
            return sorted(value)

        def __repr__ (self):
            return "<series(id='%s', tags='%s')>" % (
                   self.id, self.tags)

    return table_def

