import src.utils.table_utils as tutils # table_create, to_dict
import src.utils.ingester as ing # Ingester
import src.utils.dbconfig as dbc #db
import src.utils.tables as tbl

def batcher(whichschema):

    tablelist = [
        "dataGap",
        "dataHeight",
        "dataLPI",
        "dataSoilStability",
        "dataSpeciesInventory",
        "geoIndicators",
        "geoSpecies",
        "tblProject"
        ]

    if whichschema:
        if "newtall" in whichschema:
            targetschema = "public_test"
        elif ("gis" in whichschema) or
            ("mainapi" in whichschema):
            targetschema = "public"
        elif "maindev" in whichschema:
            targetschema = "public_dev"
        elif "localpg" in whichschema:
            targetschema = "localpg"
        else:
            targetschema = "ingestion to this schema not implemented."

        print(f"Ingesting to: '{targetschema}' schema")

    else:
        sys.exit("error: target database schema is required!")

    def filterpks(df,pkunavailable = None):
        if pkunavailable is not None:
            df  = df[~df.PrimaryKey.isin(pkunavailable)]
        return df

    unavailablepks = {}
    complete={}
    # need to create dataheader first for pk filtering below
    complete['dataHeader'] = tbl.assemble('dataHeader')

    print("assembling...")
    for table in tablelist:
        df = tbl.assemble(table)

        print(f'finding unavailable primarykeys for {table}...')
        if 'tblProject' not in table:
            unavailablepks[table] = [i for i in df.PrimaryKey.unique() if i not in complete['dataHeader'].PrimaryKey.unique() ]
            df = filterpks(df, unavailablepks[table])
        complete[table] = df
        print(f"assembled {table}!")

    print('finished assembling tables! ingesting..')
    # todo: ingesting dataHeader first then deleting from
    # table queue!
    # handling data header if it exists
    # if tutils.tablecheck('dataHeader'):
    #     del complete['dataHeader']

    for tablename,dataframe in complete.items():
        try:
            d = dbc.db(whichschema)
            if tutils.tablecheck(tablename):
                ing.Ingester.main_ingest(dataframe, tablename, d.str)
            else:
                tutils.table_create(tablename, d.str)
                ing.Ingester.main_ingest(dataframe, tablename, d.str)
                print(f" ingested '{tablename}'!")
        except Exception as e:
            print(e)
            d = dbc.db(whichschema)
