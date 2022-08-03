import pandas as pd
import os
from datetime import datetime
import json
import geopandas as gpd
from pyproj import CRS

import src.project.project as proj
import src.utils.schema as stools # schema_chooser
import src.utils.table_utils as tutils # table_create, todict

def assemble(tablename):
    dir = json.load(open(file=os.path.normpath(os.path.join(os.getcwd(),"src","utils","config.json") )))["tall_dir"]
    tall_files = {
        os.path.splitext(i)[0]:os.path.normpath(f"{dir}/{i}") for
            i in os.listdir(dir) if not i.endswith(".xlsx")
            and not i.endswith(".ldb")}
    if "tblProject" in tablename:
        return proj.read_template()



    # FOR JOE: csv encoding
    enc = 'utf-8' if 'dataSoilStability' in tablename else 'cp1252'

    # pulling schemas from xlsx and using them to readcsv
    scheme = tutils.todict(tablename)
    translated = pg2pandas(scheme)
    tempdf = pd.read_csv(

        tall_files[tablename],
        encoding=enc,
        low_memory=False,
        index_col=False,
        dtype = translated,
        parse_dates = date_finder(scheme)

        )
    # adding date loaded in db
    tempdf = dateloaded(tempdf)

    # adding projectkey value
    if "tblProject" not in tablename:
        prj = proj.read_template()
        project_key = prj.loc[0,"project_key"]
        tempdf.project_key = project_key

    if "dataHeader" in tablename:
        schema = tutils.todict(tablename).keys()
        tempdf.rename(columns={"EcologicalSiteId": "EcologicalSiteID"}, inplace=True)
        tempdf = tempdf.filter(schema)
        tempdf = tempdf.drop_duplicates()
        tempdf = geoenable(tempdf)

    if "dataLPI" in tablename:

        # tempdf.drop(columns=["X"], inplace=True)
        schema = tutils.todict(tablename).keys()
        tempdf = tempdf.filter(schema)
        tempdf = tempdf.drop_duplicates()

    if "dataGap" in tablename:
        #
        #
        tempdf = tempdf.drop_duplicates()
        schema = tutils.todict(tablename).keys()
        tempdf = tempdf.filter(schema)
        #
        # # for i in integers:
        # #     tempdf[i] = tempdf[i].apply(lambda x: pd.NA if pd.isnull(x)==True else x).astype('Int64')
        # tempdf  = tempdf[~tempdf.PrimaryKey.isin(gap)]

    if "dataHeight" in tablename:
        # integers = [ "Measure", "LineLengthAmount", 'PointNbr' , 'Chkbox','ShowCheckbox']


        schema = tutils.todict(tablename).keys()
        tempdf = tempdf.filter(schema)
        tempdf = tempdf.drop_duplicates()
        # for i in integers:
        #     tempdf[i] = tempdf[i].apply(lambda x: pd.NA if pd.isnull(x)==True else x).astype('Int64')


    if "dataSoilStability" in tablename:

        tempdf = tempdf.drop_duplicates()
        schema = tutils.todict(tablename).keys()
        tempdf = tempdf.filter(schema)


    if "dataSpeciesInventory" in tablename:


        tempdf = tempdf.drop_duplicates()
        schema = tutils.todict(tablename).keys()
        tempdf = tempdf.filter(schema)


    if "geoIndicators" in tablename:
        tempdf.drop_duplicates()
        schema = tutils.todict(tablename).keys()
        tempdf = tempdf.filter(schema)

    if "geoSpecies" in tablename:
        tempdf.drop_duplicates()
        schema = tutils.todict(tablename).keys()
        tempdf = tempdf.filter(schema)


    return tempdf

def geoenable(df):
    df.drop(columns=["wkb_geometry"], inplace=True)
    tempdf = gpd.GeoDataFrame(df,
                crs =CRS("EPSG:4326"),
                geometry=gpd.points_from_xy(
                    df.Longitude_NAD83,
                    df.Latitude_NAD83
                    ))
    tempdf.rename(columns={'geometry':'wkb_geometry'}, inplace=True)
    return tempdf


def dateloaded(df):
    """ appends DateLoadedInDB and dbkey to the dataframe
    """
    if 'DateLoadedInDb' in df.columns:
        df['DateLoadedInDb'] = df['DateLoadedInDb'].astype('datetime64')
        df['DateLoadedInDb'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    else:
        df['DateLoadedInDb'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    return df

def pg2pandas(pg_schema):
    trans = {
        "text":"object",
        "integer":"Int64",
        "bigint":"Int64",
        "bit":"Int64",
        "smallint":"Int64",
        "double precision":"float64",
        "numeric":"float64",
        "postgis.geometry":"object",
        "date":"str", #important
        "timestamp":"str" #important,
    }
    return {k:trans[v.lower()] for k,v in pg_schema.items()}

def date_finder(trans):
    return [k for k,v in trans.items() if
        ('date' in trans[k].lower()) or
        ('timestamp' in trans[k].lower())]

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
        str = nonheader_fix(str)
        str = rid_adder(str)

    elif "Height" in tablename:
        str = nonheader_fix(str)
        str = rid_adder(str)

    elif "LPI" in tablename:
        str = nonheader_fix(str)
        str = rid_adder(str)

    elif "SoilStability" in tablename:
        str = nonheader_fix(str)
        str = rid_adder(str)

    elif "SpeciesInventory" in tablename:
        str = nonheader_fix(str)
        str = rid_adder(str)

    elif "Indicators" in tablename:
        str = nonheader_fix(str)
        str = rid_adder(str)

    elif "geoSpecies" in tablename:
        str = nonheader_fix(str)
        str = rid_adder(str)

    elif "Project" in tablename:
        str = project_fix(str)
        # str = rid_adder(str)

    return str

def set_srid():
    return r"""ALTER TABLE public_test."dataHeader"
                ALTER COLUMN wkb_geometry TYPE postgis.geometry(Point, 4326)
                USING postgis.ST_SetSRID(wkb_geometry,4326);"""

def field_appender(tablename):
    """
    uses schema_chooser to pull a schema for a specific table
    and create a postgres-ready string of fields and field types
    """

    str = "( "
    count = 0
    di = pd.Series(
            stools.schema_chooser(tablename).DataType.values,
            index= stools.schema_chooser(tablename).Field).to_dict()

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
    fix = fix.replace('POSTGIS.GEOMETRY,', 'postgis.GEOMETRY(POINT, 4326),')
    return fix

def nonheader_fix(str):
    """
    adds foreign key constraint
    """

    fix = str.replace('"PrimaryKey" TEXT,', '"PrimaryKey" TEXT REFERENCES gisdb.public_test."dataHeader"("PrimaryKey"), ')
    return fix



def project_fix(str):
    """
    adds foreign key contraint
    """
    # fix = str.replace('"PrimaryKey" TEXT,', '"PrimaryKey" TEXT PRIMARY KEY,')
    fix = str.replace('"ProjectKey" TEXT, ', '"ProjectKey" TEXT PRIMARY KEY REFERENCES gisdb.public_test."dataHeader"("ProjectKey"), ')

    return fix

def rid_adder(str):
    """
    adds an autoincrement field (row id) and sets it as Primarykey
    """

    fix = str.replace('" ( ', '" ( rid SERIAL PRIMARY KEY,')
    return fix


def height_fix(str):
    fix = str.replace('" ( ', '" ( ri)"')
