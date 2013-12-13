"""
Define the symbol resolution table.

"""

import json
from sqlalchemy import Column, types
from sqlalchemy.ext.declarative import declarative_base

def table (base):
    class table_def (base):
        __tablename__ = 'symbol_resolve'
    
        symbol = Column(types.Unicode(255), primary_key=True)
        source = Column(types.Unicode(255), primary_key=True)
        resolve = Column(types.Unicode(255))
        adjust = Column(types.UnicodeText(), default="{}")

        def adjust_dict (self):
            if self.adjust:
                return json.loads(self.adjust)
            else:
                return dict()

        def adjust_clear (self):
            self.adjust = "{}"

        def adjust_add (self, key, value):
            adjust = None
            if self.adjust:
                adjust = json.loads(self.adjust)
            else:
                adjust = dict()
            adjust[key] = value
            self.adjust = json.dumps(adjust)

        def adjust_del (self, key):
            if self.adjust:
                adjust = json.loads(self.adjust)
                try:
                    del adjust[key]
                    self.adjust = json.dumps(self.adjust)
                except KeyError:
                    pass 

        def __repr__ (self):
            return "<symbol_resolve(symbol='%s', source='%s', resolve='%s', adjust='%s')>" % (
                   self.symbol, self.source, self.resolve, self.adjust)

    return table_def

