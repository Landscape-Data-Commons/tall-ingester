import pandas as pd
import os
import json
# parse types per table
# schema_chooser("aero_runs")


def schema_chooser(tablename):
    #  PATH TO EXCEL FILE WITH SCHEMA
    schema_dir = json.load(open(file=os.path.normpath(os.path.join(os.getcwd(),"src","utils","config.json") )))["schema_dir"]

    # SCHEMA PATH LOADER
    schema_file = [
        os.path.normpath(f"{schema_dir}/{i}") for i in os.listdir(schema_dir)
        if "Schema" in i and
        os.path.splitext(i)[1].endswith(".xlsx") and
        "kbf" in i][0]

    # create dataframe with path
    excel_dataframe = pd.read_excel(schema_file)
    # quick table name fix
    # excel_dataframe['Table'] = excel_dataframe['Table'].apply(lambda x: x.replace('\xa0', ''))

    # strip whitespace from columns with string values to make them selectable
    for i in excel_dataframe.columns:

        if excel_dataframe.dtypes[i]=="object":
            excel_dataframe[i] = excel_dataframe[i].apply(
                lambda x: str(x).replace('\xa0', '').strip() if
                    (type(x)!='float') and
                    # (type(x)=='int') and
                    (pd.isnull(x)!=True)
                    else x
                )

    # return dataframe
    return excel_dataframe[excel_dataframe['Table']==tablename]
