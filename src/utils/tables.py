import pandas as pd
from src.utils.schema import schema_chooser

def create_command(tablename):
    """
    creates a complete CREATE TABLE postgres statement
    by only supplying tablename
    currently handles: HEADER, Gap
    """

    str = f'CREATE TABLE "{tablename}" '
    str+=field_appender(tablename)

    if "Header" in tablename:
        str = header_fix(str)

    elif "Gap" in tablename:
        str = gap_fix(str)
        str = rid_adder(str)

    return str

def field_appender(tablename):
    """
    uses schema_chooser to pull a schema for a specific table
    and create a postgres-ready string of fields and field types
    """

    str = "( "
    count = 0
    di = pd.Series(
            schema_chooser(tablename).DataType.values,
            index=schema_chooser(tablename).Field).to_dict()

    for k,v in di.items():
        if count<(len(di.items())-1):
            str+= f'"{k}" {v.upper()}, '
            count+=1
        else:
            str+= f'"{k}" {v.upper()} );'
    return str


def header_fix(str):
    """
    adds primary key constraint
    geometry type fix: public_dev is not postgis enabled; https://stackoverflow.com/a/55408170
    """

    fix = str.replace('"PrimaryKey" TEXT,', '"PrimaryKey" TEXT PRIMARY KEY,')
    fix = fix.replace('POSTGIS.GEOMETRY,', 'postgis.GEOMETRY,')
    return fix

def gap_fix(str):
    """
    adds foreign key constraint
    """

    fix = str.replace('"PrimaryKey" TEXT,', '"PrimaryKey" TEXT REFERENCES gisdb.public_dev."dataHeader"("PrimaryKey"), ')
    return fix

def rid_adder(str):
    """
    adds an autoincrement field (row id) and sets it as Primarykey
    """
    
    fix = str.replace('" ( ', '" ( rid SERIAL PRIMARY KEY,')
    return fix
