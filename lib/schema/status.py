"""
Define the status table.

"""

from sqlalchemy import Column, Sequence, types
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import validates
from sqlalchemy.dialects.postgresql import ARRAY


def table (base):
    class table_def (base):
        __tablename__ = 'status'
        
        id = Column(types.Integer, Sequence('status_id_seq'), primary_key=True)
        node = Column(types.Unicode(1000))
        name = Column(types.Unicode(1000))
        arguments = Column(types.UnicodeText())
        task_id = Column(types.Unicode(1000))
        instance_id = Column(types.Unicode(1000))
        status = Column(types.Unicode(1000))
        started = Column(types.BigInteger, default=-1)
        last_modified = Column(types.BigInteger, default=-1)
        result = Column(types.UnicodeText())

        def __repr__ (self):
            return "<status(id='%s', status='%s')>" % (
                   self.id, self.status)

    return table_def

