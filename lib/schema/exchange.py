"""
Define the exchange table.

"""

from sqlalchemy import Column, Sequence, types
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import validates
from sqlalchemy.dialects.postgresql import ARRAY


def table (base):
    class table_def (base):
        __tablename__ = 'exchange'
        
        id = Column(types.Integer, Sequence('exchange_id_seq'), primary_key=True)
        code = Column(types.Unicode(1000))
        timezone = Column(types.Unicode(1000))
        last_modified = Column(types.BigInteger, default=-1)

        def __repr__ (self):
            return "<exchange(id='%s', code='%s', timezone='%s', last_modified='%s')>" % (
                   self.id, self.code, self.timezone, self.last_modified)

        def from_dict (inst):
            instance = table_def()
            instance.id = inst["id"]
            instance.code = inst["code"]
            instance.timezone = inst["timezone"]
            instance.last_modified = inst["last_modified"]
            return instance

        def to_dict (self):
            return {"id": self.id
                   ,"code": self.code
                   ,"timezone": self.timezone
                   ,"last_modified": self.last_modified}

    return table_def

