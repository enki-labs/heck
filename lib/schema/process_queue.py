"""
Define the process queue table.

"""

from sqlalchemy import Column, Sequence, types
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy.orm import validates
import json
from collections import OrderedDict


def table (base):
    class table_def (base):
        __tablename__ = 'process_queue'
        
        tags = Column(types.Unicode(1000), primary_key=True)
        process = Column(types.Integer)
        depend = Column(ARRAY(types.Integer))
        status = Column(types.Unicode(1000))

        _tags_dict = None

        @property
        def tags_dict (self):
            if not self._tags_dict:
                from lib.schema import StringDictionaryWrapper
                self._tags_dict = StringDictionaryWrapper(self, "tags")
            return self._tags_dict

        @validates('depend')
        def _set_depend (self, key, value):
            """ Ensure depend array always sorted """
            return sorted(value)

        @validates('tags')
        def _set_tags (self, key, value):
            """ Ensure tags dictionary sorted """
            return json.dumps(OrderedDict(sorted(value.items())))

        def __repr__ (self):
            return "<process_queue(tags='%s', process='%s', depend='%s', status='%s')>" % (
                   self.tags, self.process, self.depend, self.status)

    return table_def

