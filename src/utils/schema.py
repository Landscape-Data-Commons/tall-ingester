import pandas as pd
import os
# parse types per table

def schema_chooser(tablename):
    #  PATH TO EXCEL FILE WITH SCHEMA
    schema_dir = r""

    # SCHEMA PATH LOADER
    schema_file = [
        os.path.normpath(f"{schema_dir}/{i}") for i in os.listdir(schema_dir)
        if "Schema" in i and
        os.path.splitext(i)[1].endswith(".xlsx")][0]

    # create dataframe with path
    excel_dataframe = pd.read_excel(schema_file)

    # strip whitespace from columns with string values to make them selectable
    for i in excel_dataframe.columns:
        if excel_dataframe.dtypes[i]=="object":
            excel_dataframe[i] = excel_dataframe[i].apply(
                lambda x: x.strip() if
                    (type(x)!='float') and
                    (pd.isnull(x)!=True)
                    else x
                )

    # return dataframe
    return excel_dataframe[excel_dataframe['Table']==tablename]
