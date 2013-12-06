"""
Define the symbol table.

"""

from sqlalchemy import Column, types
from sqlalchemy.ext.declarative import declarative_base
import json

def table (base):
    class table_def (base):
        __tablename__ = 'symbol'
        
        symbol = Column(types.Unicode(1000), primary_key=True)
        category = Column(types.Unicode(1000))
        name = Column(types.Unicode(1000))
        meta = Column(types.UnicodeText(), default="{}")

        def meta_dict (self):
            if self.meta:
                return json.loads(self.meta)
            else:
                return dict()

        def meta_add (self, key, value):
            meta = None
            if self.meta:
                meta = json.loads(self.meta)
            else:
                meta = dict()
            meta[key] = value
            self.meta = json.dumps(meta)

        def meta_del (self, key):
            if self.meta:
                meta = json.loads(self.meta)
                try:
                    del meta[key]
                    self.meta = json.dumps(self.meta)
                except KeyError:
                    pass

        def __repr__ (self):
            return "<series(symbol='%s', meta='%s')>" % (
                   self.symbol, self.meta)

    return table_def

