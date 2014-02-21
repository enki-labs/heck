"""
Define the config table.

"""

from sqlalchemy import Column, Sequence, types
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import validates
from sqlalchemy.dialects.postgresql import ARRAY


def table (base):
    class table_def (base):
        __tablename__ = 'config'
        
        id = Column(types.Integer, Sequence('config_id_seq'), primary_key=True)
        tags = Column(ARRAY(types.Integer))
        last_modified = Column(types.BigInteger, default=-1)
        content = Column(types.UnicodeText())

        @validates('tags')
        def _set_tags (self, key, value):
            """ Ensure tag array always sorted """
            return sorted(value)

        def __repr__ (self):
            return "<config(id='%s', tags='%s', last_modified='%s', content='%s')>" % (
                   self.id, self.tags, self.last_modified, self.content)

        def to_dict (self):
            return {"id": self.id
                   ,"tags": self.tags
                   ,"last_modified": self.last_modified
                   ,"content": self.content};

    return table_def

