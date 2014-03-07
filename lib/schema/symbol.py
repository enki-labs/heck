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
        last_modified = Column(types.BigInteger, default = -1)

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

        def to_dict (self):
            return {"symbol": self.symbol
                   ,"category": self.category
                   ,"name": self.name
                   ,"meta": self.meta_dict()
                   ,"last_modified": self.last_modified}

        def from_dict (dictionary):
            from lib.schema import StringDictionaryWrapper
            output = table_def()
            output.symbol = dictionary["symbol"]
            output.category = dictionary["category"]
            output.name = dictionary["name"]
            StringDictionaryWrapper(output, "meta")._encode(dictionary["meta"])
            output.last_modified = dictionary["last_modified"]
            return output

    return table_def

