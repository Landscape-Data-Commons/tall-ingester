import pandas as pd
from src.utils.schema import schema_chooser
from src.utils.table_utils import table_create, to_dict


def assemble(tablename):
    """
    function to troubleshoot ingestion issues

    """
    dir = json.load(open(file=os.path.join(os.getcwd(),"src","utils","config.json")))["schema_dir"]
    tall_files = {
        os.path.splitext(i)[0]:os.path.normpath(f"{dir}/{i}") for
            i in os.listdir(dir) if not i.endswith(".xlsx")
            and not i.endswith(".ldb")}
    # tempdf = pd.read_csv(tall_files[tablename], encoding="cp1252", low_memory=False)
    enc = 'utf-8' if 'dataSoilStability' in tablename else 'cp1252'
    tempdf = pd.read_csv(

        tall_files[tablename],
        encoding=enc,
        low_memory=False,
        index_col=0

        )
    if "dataHeader" in tablename:
        tempdf.rename(columns={"EcologicalSiteId": "EcologicalSiteID"}, inplace=True)
        tempdf = tempdf.drop_duplicates()

    if "dataLPI" in tablename:
        lpi = ['NML00000_2019_Sandy_5012019-09-01',
                 'UT_GSENM-LUP-Random-2020_Shade-693_V12020-09-01',
                 'UT_SLFO-ESR-Random-SHINY-2019_HW-2_V12020-09-01',
                 'UT_SLFO-ESR-Random-SHINY-2019_SP-2_V12020-09-01',
                 'UT_SLFO-ESR-Random-SHINY-2019_HW-3_V12020-09-01',
                 'UT_SLFO-ESR-Random-SHINY-2019_SP-3_V12020-09-01',
                 'MT_DFO-Watershed-2020_GH-40-S_V12020-09-01',
                 'MT_DFO-Watershed-2020_GH-23-S_V12020-09-01']
        tempdf.drop(columns=["X"], inplace=True)
        schema = to_dict(tablename).keys()
        tempdf = tempdf.filter(schema)
        tempdf = tempdf.drop_duplicates()


    if "dataGap" in tablename:
        integers = ["SeqNo", "GapStart", "GapEnd", "Gap", "Measure", "LineLengthAmount",
        "GapData", "PerennialsCanopy","AnnualGrassesCanopy", "AnnualForbsCanopy", "OtherCanopy",
        "NoCanopyGaps","NoBasalGaps", "PerennialsBasal", "AnnualGrassesBasal", "AnnualForbsBasal",
        "OtherBasal"]

        header =    ['17071207510493982017-09-01',
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



        tempdf = tempdf.drop_duplicates()
        schema = to_dict(tablename).keys()
        tempdf = tempdf.filter(schema)

        for i in integers:
            tempdf[i] = tempdf[i].apply(lambda x: pd.NA if pd.isnull(x)==True else x).astype('Int64')
        tempdf  = tempdf[~tempdf.PrimaryKey.isin(header)]

    if "dataHeight" in tablename:
        integers = [ "Measure", "LineLengthAmount", 'PointNbr' , 'Chkbox','ShowCheckbox']
        height = ['17071912364675632017-09-01']

        schema = to_dict(tablename).keys()
        tempdf = tempdf.filter(schema)
        tempdf = tempdf.drop_duplicates()
        for i in integers:
            tempdf[i] = tempdf[i].apply(lambda x: pd.NA if pd.isnull(x)==True else x).astype('Int64')
        tempdf  = tempdf[~tempdf.PrimaryKey.isin(height)]

    return tempdf

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

def nonheader_fix(str):
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


def height_fix(str):
    fix = str.replace('" ( ', '" ( ri)"')
