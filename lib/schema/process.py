"""
Define the process table.

"""

from sqlalchemy import Column, Sequence, types
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.ext.declarative import synonym_for

def table (base):
    class table_def (base):
        __tablename__ = 'process'
        
        id = Column(types.Integer, Sequence('process_id_seq'), primary_key=True)
        name = Column(types.Unicode(1000))
        processor = Column(types.Unicode(1000))
        search = Column(types.UnicodeText())
        output = Column(types.UnicodeText())
        last_modified = Column(types.BigInteger, default=-1)
        _search_dict = None
        _output_dict = None

        @property
        def search_dict (self):
            from lib.schema import StringDictionaryWrapper
            if not self._search_dict:
                self._search_dict = StringDictionaryWrapper(self, "search")
            return self._search_dict

        @property
        def output_dict (self):
            from lib.schema import StringDictionaryWrapper
            if not self._output_dict:
                self._output_dict = StringDictionaryWrapper(self, "output")
            return self._output_dict

        def __repr__ (self):
            return "<process(id='%s', name='%s', processor='%s', search='%s', output='%s', last_modified='%s')>" % (
                   self.id, self.name, self.processor, self.search, self.output, self.last_modified)

        def to_dict (self):
            return {"id": self.id
                   ,"name": self.name
                   ,"processor": self.processor
                   ,"search": self.search_dict._parse()
                   ,"output": self.output_dict._parse()
                   ,"last_modified": self.last_modified}

        def from_dict (dictionary):
            from lib.schema import StringDictionaryWrapper
            output = table_def()
            output.id = dictionary["id"]
            output.name = dictionary["name"]
            output.processor = dictionary["processor"]
            StringDictionaryWrapper(output, "search")._encode(dictionary["search"])
            StringDictionaryWrapper(output, "output")._encode(dictionary["output"])
            output.last_modified = dictionary["last_modified"]
            return output

    return table_def

