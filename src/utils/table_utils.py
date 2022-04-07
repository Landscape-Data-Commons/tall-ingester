import pandas as pd
import os
from psycopg2 import sql
import numpy as np
from src.utils.dbconfig import db
from src.utils.schema import schema_chooser
from src.utils.tables import create_command

def table_create(tablename: str, conn:str=None):
    """
    pulls all fields from dataframe and constructs a postgres table schema;
    using that schema, create new table in postgres.
    """
    d = db('maindev')

    try:
        print("checking fields")
        comm = create_command(tablename)
        con = d.str
        cur = con.cursor()
        # return comm
        cur.execute(comm)
        con.commit()

    except Exception as e:
        print(e)
        d = db('maindev')
        con = d.str
        cur = con.cursor()


def to_dict(tablename):
    sche = schema_chooser(tablename)
    di = pd.Series(
            sche.DataType.values,
            index=sche.Field).to_dict()
    return di
