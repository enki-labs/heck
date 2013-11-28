"""
Define the symbol resolution table.

"""

from sqlalchemy import schema, types

def table (metadata):
    return schema.Table('symbol_resolve', metadata,
           schema.Column('symbol', types.Unicode(255), primary_key=True),
           schema.Column('source', types.Unicode(255), primary_key=True),
           schema.Column('bloomberg', types.Unicode(255)))


