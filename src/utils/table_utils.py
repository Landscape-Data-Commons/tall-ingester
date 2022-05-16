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
        d = dbc.db(f'{conn}')
        con = d.str
        cur = con.cursor()
        cur.execute("select exists(select * from information_schema.tables where table_name=%s and table_schema=%s)", (f'{tablename}',f'{tableschema}',))
        if cur.fetchone()[0]:
            return True
        else:
            return False

    except Exception as e:
        print(e)
        d = dbc.db(f'{conn}')
        con = d.str
        cur = con.cursor()

def todict(tablename):
    sche = stools.schema_chooser(tablename)
    di = pd.Series(
            sche.DataType.values,
            index=sche.Field).to_dict()
    return di


def geoind_postingest(conn="maindev"):
    """
    fixes geoindicators after it has been ingested

    """
    tableschema = "public_dev" if conn=="maindev" else "public"

    create_mlra_name = r""" alter table public_dev."geoIndicators"
                            add column mlra_name VARCHAR(200);"""

    update_mlra_name =r""" update public_dev."geoIndicators" as target
                            set mlra_name = src.mlra_name
                            from (
                            	  select geo.mlra_name, dh."PrimaryKey", geo.mlrarsym
                            	  from gis.mlra_v42_wgs84 as geo
                            	  join public_dev."dataHeader" as dh
                            	  on ST_WITHIN(dh.wkb_geometry, geo.geom)
                            	) as src
                            where target."PrimaryKey" = src."PrimaryKey"; """

    create_mlrasym = r""" alter table public_dev."geoIndicators"
                            add column mlrarsym VARCHAR(4);  """

    update_mlrasym = r"""update public_dev."geoIndicators" as target
                            set mlrarsym = src.mlrarsym
                            from (
                            	  select geo.mlra_name, dh."PrimaryKey", geo.mlrarsym
                            	  from gis.mlra_v42_wgs84 as geo
                            	  join public_dev."dataHeader" as dh
                            	  on ST_WITHIN(dh.wkb_geometry, geo.geom)
                            	) as src
                            where target."PrimaryKey" = src."PrimaryKey";"""

    create_nanames = r"""alter table public_dev."geoIndicators"
                             add column na_l1name VARCHAR(100),
                             add column na_l2name VARCHAR(100),
                             add column us_l3name VARCHAR(100),
                             add column us_l4name VARCHAR(100);"""

    update_nanames = r"""update public_dev."geoIndicators" as target
                            set na_l1name = src.na_l1name,
                             na_l2name = src.na_l2name,
                             us_l3name = src.us_l3name,
                             us_l4name = src.us_l4name
                            from
                                (select geo.us_l4name, geo.us_l3name, geo.na_l2name, geo.na_l1name,
                                dh."PrimaryKey"
                            	  from gis.us_eco_level_4 as geo
                            	  join public_dev."dataHeader" as dh
                            	  on ST_WITHIN(dh.wkb_geometry, geo.geom)) as src
                            where target."PrimaryKey" = src."PrimaryKey";"""
    create_state = r"""alter table public_dev."geoIndicators"
                        add column "State" TEXT; """
    update_state = r""" update public_dev."geoIndicators" as target
                        set "State" = src.stusps
                        from (
                        	 select geo.stusps, dh."PrimaryKey"
                        	 from gis.tl_2017_us_state_wgs84 as geo
                        	 join public_dev."dataHeader" as dh
                        	 on ST_WITHIN(dh.wkb_geometry, geo.geom)
                        	) as src
                        where target."PrimaryKey" = src."PrimaryKey";"""
    try:
        d = dbc.db(f'{conn}')
        constring = engine_conn_string('maindev')

        con = d.str
        if tablecheck("geoIndicators"): # table exists
            cur = con.cursor()
            # if field does not exist
            cur.execute(create_mlra_name)
            cur.execute(update_mlra_name)
            # if field does not exist
            cur.execute(create_mlrasym)
            cur.execute(update_mlrasym)
            # if field does not exist
            cur.execute(create_nanames)
            cur.execute(update_nanames)
            # if field does not exist
            cur.execute(create_state)
            cur.execute(update_state)
            #  geotif processing
            gi = gpd.read_postgis('select * from public_dev."geoIndicators";', eng, geom_col="wkb_geometry")
            classes = pd.read_sql('select * from public.modis_classes;', d.str)

            pgdf = extract_modis_values(gi, tif)
            pg = pgdf.copy(deep=True)
            pg.rename(columns={"modis_val":"Value"}, inplace=True)
            final = pg.merge(classes, on="Value", how="inner").filter(["PrimaryKey", "Name"])
            final.to_sql('modis_values', eng, schema='public_dev')


        # if cur.fetchone()[0]:
        #     return True
        # else:
        #     return False

    except Exception as e:
        print(e)
        d = dbc.db(f'{conn}')
        con = d.str
        cur = con.cursor()
