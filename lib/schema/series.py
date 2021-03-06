"""
Define the series table.

"""

from sqlalchemy import Column, Sequence, types
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import validates
from sqlalchemy.dialects.postgresql import ARRAY, JSON


def table (base):
    class table_def (base):
        __tablename__ = 'series'
        
        id = Column(types.Integer, Sequence('series_id_seq'), primary_key=True)
        symbol = Column(types.Unicode(1000))
        tags = Column(ARRAY(types.Integer))
        start = Column(types.BigInteger, default=-1)
        end = Column(types.BigInteger, default=-1)
        count = Column(types.BigInteger, default=0)
        last_modified = Column(types.BigInteger, default=-1)
        meta = Column(types.UnicodeText, default="{}")
        _meta_dict = None

        @property
        def meta_dict (self):
            from lib.schema import StringDictionaryWrapper
            if not self._meta_dict:
                self._meta_dict = StringDictionaryWrapper(self, "meta")
            return self._meta_dict

        @validates('tags')
        def _set_tags (self, key, value):
            """ Ensure tag array always sorted """
            return sorted(value)

        def __repr__ (self):
            return "<series(id='%s', symbol='%s', tags='%s')>" % (
                   self.id, self.symbol, self.tags)

    return table_def

