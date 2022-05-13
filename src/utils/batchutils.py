import src.utils.table_utils as tutils # table_create, to_dict
import src.utils.ingester as ing # Ingester
import src.utils.dbconfig as dbc #db
import src.utils.tables as tbl

def batcher():

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
    # handling data header if it exists
    if tutils.tablecheck('dataHeader'):
        del complete['dataHeader']

    for tablename,dataframe in complete.items():
        try:
            d = dbc.db('maindev')
            if tutils.tablecheck(tablename):
                ing.Ingester.main_ingest(dataframe, tablename, d.str)
            else:
                tutils.table_create(tablename, d.str)
                ing.Ingester.main_ingest(dataframe, tablename, d.str)
                print(f" ingested '{tablename}'!")
        except Exception as e:
            print(e)
            d = dbc.db("maindev")
