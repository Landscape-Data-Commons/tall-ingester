import pandas as pd
import os
from psycopg2 import sql
import numpy as np
import src.utils.dbconfig as dbc
import src.utils.schema as stools
import src.utils.tables as tables

def table_create(tablename: str, conn:str=None):
    """
    pulls all fields from dataframe and constructs a postgres table schema;
    using that schema, create new table in postgres.
    """
    d = dbc.db('maindev')

    try:
        print("checking fields")
        comm = tables.create_command(tablename)
        con = d.str
        cur = con.cursor()
        # return comm
        cur.execute(comm)
        cur.execute(tables.set_srid()) if "Header" in tablename else None
        con.commit()

    except Exception as e:
        print(e)
        d = dbc.db('maindev')
        con = d.str
        cur = con.cursor()

def tablecheck(tablename, conn="maindev"):
    """
    receives a tablename and returns true if table exists in postgres table
    schema, else returns false

    """
    tableschema = "public_dev" if conn=="maindev" else "public"
    try:
        d = db(f'{conn}')
        con = d.str
        cur = con.cursor()
        cur.execute("select exists(select * from information_schema.tables where table_name=%s and table_schema=%s)", (f'{tablename}',f'{tableschema}',))
        if cur.fetchone()[0]:
            return True
        else:
            return False

    except Exception as e:
        print(e)
        d = db(f'{conn}')
        con = d.str
        cur = con.cursor()

def todict(tablename):
    sche = stools.schema_chooser(tablename)
    di = pd.Series(
            sche.DataType.values,
            index=sche.Field).to_dict()
    return di
