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
    dir = json.load(open(file=os.path.join(os.getcwd(),"src","utils","config.json")))["schema_dir"]
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
        # index_col=0,
        dtype = translated,
        parse_dates = date_finder(scheme)

        )
    # adding date loaded in db
    tempdf = dateloaded(tempdf)

    # adding projectkey value
    if "tblProject" not in tablename:
        prj = proj.read_template()
        project_key = prj.loc[0,"project_key"]
        tempdf.ProjectKey = project_key

    if "dataHeader" in tablename:
        schema = tutils.todict(tablename).keys()
        tempdf.rename(columns={"EcologicalSiteId": "EcologicalSiteID"}, inplace=True)
        tempdf = tempdf.filter(schema)
        tempdf = tempdf.drop_duplicates()
        tempdf = geoenable(tempdf)

    if "dataLPI" in tablename:
        lpi = ['NML00000_2019_Sandy_5012019-09-01',
                 'UT_GSENM-LUP-Random-2020_Shade-693_V12020-09-01',
                 'UT_SLFO-ESR-Random-SHINY-2019_HW-2_V12020-09-01',
                 'UT_SLFO-ESR-Random-SHINY-2019_SP-2_V12020-09-01',
                 'UT_SLFO-ESR-Random-SHINY-2019_HW-3_V12020-09-01',
                 'UT_SLFO-ESR-Random-SHINY-2019_SP-3_V12020-09-01',
                 'MT_DFO-Watershed-2020_GH-40-S_V12020-09-01',
                 'MT_DFO-Watershed-2020_GH-23-S_V12020-09-01']
        # tempdf.drop(columns=["X"], inplace=True)
        schema = tutils.todict(tablename).keys()
        tempdf = tempdf.filter(schema)
        tempdf = tempdf.drop_duplicates()
        tempdf  = tempdf[~tempdf.PrimaryKey.isin(lpi)]



    if "dataGap" in tablename:
        # pass
        # integers = ["SeqNo", "GapStart", "GapEnd", "Gap", "Measure", "LineLengthAmount",
        # "GapData", "PerennialsCanopy","AnnualGrassesCanopy", "AnnualForbsCanopy", "OtherCanopy",
        # "NoCanopyGaps","NoBasalGaps", "PerennialsBasal", "AnnualGrassesBasal", "AnnualForbsBasal",
        # "OtherBasal"]

        gap =    ['17071207510493982017-09-01',
                     '17071912364675632017-09-01',
                     '17081815533768722017-09-01',
                     '15082015083892752016-09-01',
                     'NML00000_2019_Sandy_5012019-09-01',
                     'UT_GSENM-LUP-Random-2020_Shade-693_V12020-09-01',
                     'UT_SLFO-ESR-Random-SHINY-2019_HW-2_V12020-09-01',
                     'UT_SLFO-ESR-Random-SHINY-2019_HW-3_V12020-09-01',
                     'UT_SLFO-ESR-Random-SHINY-2019_SP-2_V12020-09-01',
                     'UT_SLFO-ESR-Random-SHINY-2019_SP-3_V12020-09-01',
                     'MT_DFO-Watershed-2020_GH-40-S_V12020-09-01',
                     'MT_DFO-Watershed-2020_GH-23-S_V12020-09-01']
        #
        #
        #
        tempdf = tempdf.drop_duplicates()
        schema = tutils.todict(tablename).keys()
        tempdf = tempdf.filter(schema)
        #
        # # for i in integers:
        # #     tempdf[i] = tempdf[i].apply(lambda x: pd.NA if pd.isnull(x)==True else x).astype('Int64')
        tempdf  = tempdf[~tempdf.PrimaryKey.isin(gap)]

    if "dataHeight" in tablename:
        # integers = [ "Measure", "LineLengthAmount", 'PointNbr' , 'Chkbox','ShowCheckbox']
        height = ['17071912364675632017-09-01']

        schema = tutils.todict(tablename).keys()
        tempdf = tempdf.filter(schema)
        tempdf = tempdf.drop_duplicates()
        # for i in integers:
        #     tempdf[i] = tempdf[i].apply(lambda x: pd.NA if pd.isnull(x)==True else x).astype('Int64')
        tempdf  = tempdf[~tempdf.PrimaryKey.isin(height)]

    if "dataSoilStability" in tablename:
        soil = ['15061107204117352015-09-10',
             '15072117415644452015-09-10',
             '1606231547451562016-09-01',
             '15070115065570822015-09-10',
             '15070817194671232015-09-10',
             '15071516212644512015-09-10',
             '15061109502352762015-09-10',
             '15061109503858772015-09-10',
             '15061012474022422015-09-10',
             '15070116214517692015-09-10',
             '1507201842135732015-09-10',
             '15072118230793402015-09-10',
             '15072307072476792015-09-10',
             '15072308002739522015-09-10',
             '15061109503025512015-09-10',
             '15061109502685032015-09-10',
             '15061109501930562015-09-10',
             '15061806553313572015-09-10',
             '15062508012828012015-09-10',
             '15062509535650522015-09-10',
             '15062510492972452015-09-10',
             '15063017120819272015-09-10',
             '15063018160041252015-09-10',
             '17060911393164762017-09-01',
             '17060612044890902017-09-01',
             '17042608593769502017-09-01',
             '17060115430011702017-09-01',
             '18090209051646112018-09-01',
             '18091708400288932018-09-01',
             '18091710495682832018-09-01',
             '18091713033286092018-09-01',
             '18091714593735982018-09-01',
             '17060506454387822018-09-01',
             '17060606191768862018-09-01',
             '17060616532875532018-09-01',
             '17060809112813472018-09-01',
             '17061209222182522018-09-01',
             '17061308574183542018-09-01',
             '17061416352297852018-09-01',
             '17061910100826462018-09-01',
             '17062609240652612018-09-01',
             '17061508304887032018-09-01',
             '17062209010859702018-09-01',
             '17062614033671812018-09-01',
             '17062109501325502018-09-01',
             '17062709145060962018-09-01',
             '17062112502073572018-09-01',
             '17062712435967572018-09-01',
             '17061410543228912018-09-01',
             '17062010165485062018-09-01',
             '17062908371387452018-09-01',
             '1706201449386592018-09-01',
             '17070509424791132018-09-01',
             '17070715414673252018-09-01',
             '1707110759093502018-09-01',
             '17070608581093812018-09-01',
             '17070613111843332018-09-01',
             '17071916331970032018-09-01',
             '1707111324077122018-09-01',
             '17071214220832932018-09-01',
             '17071311182567242018-09-01',
             '17071714394361392018-09-01',
             '17071710384935892018-09-01',
             '17071811375217352018-09-01',
             '17071911292966862018-09-01',
             '17072010411297852018-09-01',
             '17072412535259822018-09-01',
             '17072410040453202018-09-01',
             '17072510382158332018-09-01',
             '17072512541431622018-09-01',
             '17072710263517652018-09-01',
             '17080312323724372018-09-01',
             '17080111044839242018-09-01',
             '17073110363956752018-09-01',
             '17073113135399092018-09-01',
             '17080714234743502018-09-01',
             '17080709570176482018-09-01',
             '17080811511320052018-09-01',
             '17080911020196772018-09-01',
             '170810101241242018-09-01',
             '17080211315185282018-09-01',
             '17081410200798022018-09-01',
             '17081415205215742018-09-01',
             'NML00000_2019_Sandy_5012019-09-01',
             'UT_GSENM-LUP-Random-2020_Shade-693_V12020-09-01',
             'UT_SLFO-ESR-Random-SHINY-2019_HW-2_V12020-09-01',
             'UT_SLFO-ESR-Random-SHINY-2019_HW-3_V12020-09-01',
             'UT_SLFO-ESR-Random-SHINY-2019_SP-3_V12020-09-01',
             'UT_SLFO-ESR-Random-SHINY-2019_SP-2_V12020-09-01',
             'MT_DFO-Watershed-2020_GH-40-S_V12020-09-01',
             'MT_DFO-Watershed-2020_GH-23-S_V12020-09-01',
             '17072812250668882018-09-01']
        tempdf = tempdf.drop_duplicates()
        schema = tutils.todict(tablename).keys()
        tempdf = tempdf.filter(schema)
        tempdf  = tempdf[~tempdf.PrimaryKey.isin(soil)]

    if "dataSpeciesInventory" in tablename:
        spec = ['UT_GSENM-LUP-Random-2020_Shade-693_V12020-09-01',
                 'UT_SLFO-ESR-Random-SHINY-2019_HW-2_V12020-09-01',
                 'UT_SLFO-ESR-Random-SHINY-2019_SP-2_V12020-09-01',
                 'UT_SLFO-ESR-Random-SHINY-2019_SP-3_V12020-09-01',
                 'UT_SLFO-ESR-Random-SHINY-2019_HW-3_V12020-09-01',
                 'MT_DFO-Watershed-2020_GH-40-S_V12020-09-01',
                 'MT_DFO-Watershed-2020_GH-23-S_V12020-09-01']

        tempdf = tempdf.drop_duplicates()
        schema = tutils.todict(tablename).keys()
        tempdf = tempdf.filter(schema)
        tempdf  = tempdf[~tempdf.PrimaryKey.isin(header)]

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

    # elif "Project" in tablename:
    #     str = project_fix(str)
    #     str = rid_adder(str)

    return str

def set_srid():
    return r"""ALTER TABLE public_dev."dataHeader"
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

    fix = str.replace('"PrimaryKey" TEXT,', '"PrimaryKey" TEXT REFERENCES gisdb.public_dev."dataHeader"("PrimaryKey"), ')
    return fix

def project_fix(str):
    """
    adds foreign key contraint
    """

    fix = str.replace('"ProjectKey" TEXT, ', '"ProjectKey" TEXT REFERENCES gisdb.public_dev."dataHeader"("ProjectKey"), ')
    return fix

def rid_adder(str):
    """
    adds an autoincrement field (row id) and sets it as Primarykey
    """

    fix = str.replace('" ( ', '" ( rid SERIAL PRIMARY KEY,')
    return fix


def height_fix(str):
    fix = str.replace('" ( ', '" ( ri)"')
