# leverages some functions in the tall ingester app to create
# sql statements which finds duplicates
import src.utils.schema as stools # schema_chooser

def produce_counts(table, schema):
    init="SELECT "
    df = stools.schema_chooser("dataHeight")
    col_list = " ".join([f'"{i}",' if (df.Field.to_list().index(i)!=len(df.Field.to_list())-1) else f'"{i}"' for i in df.Field ])
    init+= col_list + ', COUNT(*) FROM {0}."{1}" GROUP BY '.format(schema, table)
    init+= col_list
    init+=' ORDER BY count DESC;'
    return init

"""
example usage:
produce_counts("dataHeight", "public_test")

output:
'SELECT
    "ProjectKey", "PrimaryKey", "LineKey", "PointLoc", "PointNbr", "RecKey", "Height", "Species", "Chkbox", "type", "GrowthHabit_measured", "FormType", "FormDate", "DateVisited", "Direction", "Measure", "LineLengthAmount", "SpacingIntervalAmount", "SpacingType", "HeightOption", "HeightUOM", "ShowCheckbox", "CheckboxLabel", "source", "DBKey", "DateLoadedInDb",
        COUNT(*) as count
        FROM public_test."dataHeights"
        GROUP BY
    "ProjectKey", "PrimaryKey", "LineKey", "PointLoc", "PointNbr", "RecKey", "Height", "Species", "Chkbox", "type", "GrowthHabit_measured", "FormType", "FormDate", "DateVisited", "Direction", "Measure", "LineLengthAmount", "SpacingIntervalAmount", "SpacingType", "HeightOption", "HeightUOM", "ShowCheckbox", "CheckboxLabel", "source", "DBKey", "DateLoadedInDb"
        ORDER BY count DESC;'
"""
def duplicate_delete_statement(table,schema):
    init= '''DELETE FROM {0}."{1}" a USING {0}."{1}" b WHERE a.rid > b.rid AND '''.format(schema, table)
    str = init
    df = stools.schema_chooser(table)
    type_examples = {
        "TEXT": "[NULL]",
        "DOUBLE PRECISION": 0,
        "INTEGER": 0,
        "BIT": 0,
        "DATE": '1999-01-01'
    }
    for fi in df.Field:
        preval =df[df.Field==fi]["DataType"].values[0]
        value = type_examples[preval]
        quoter = ''
        if ("TEXT" in preval) or ("DATE" in preval):
            quoter+=f" '{value}' "
        elif "BIT" in preval:
            quoter+=f" {value}::bit(1) "
        else:
            quoter+=f'{value}'
        if df.Field.to_list().index(fi)!=len(df.Field.to_list())-1:
            # original attempt does not take into account possible nulls:
            str+= f'a."{fi}" = b."{fi}" AND '

            # no coalesce:
            # str+=f'( (a."{fi}" = b."{fi}") OR (a."{fi}" IS NULL AND b."{fi}" IS NULL) ) AND '

            # with coalesce:
            # str+= f'COALESCE(a."{fi}", {value}) = COALESCE(b."{fi}", {value}) AND '

            # with coalesce and type-specific coalescing values:
            # str+= f'COALESCE(a."{fi}",'
            # str+=quoter
            # str+=f') = COALESCE(b."{fi}",'
            # str+=quoter
            # str+=') AND '

        else:
            # original attempt does not take into account possible nulls:
            str+= f'a."{fi}" = b."{fi}";'

            # no coalesce:
            # str+=f'( (a."{fi}" = b."{fi}") OR (a."{fi}" IS NULL AND b."{fi}" IS NULL) );'

            # with coalesce:
            # str+= f'COALESCE(a."{fi}", {value}) = COALESCE(b."{fi}", {value});'

            # with coalesce and type-specific coalescing values:
            # str+= f'COALESCE(a."{fi}",'
            # str+=quoter
            # str+=f') = COALESCE(b."{fi}",'
            # str+=quoter
            # str+=');'


    return str
"""
example usage:
duplicate_delete_statement("dataHeight", "public_test")
output:
DELETE FROM public_test."dataHeight" a
    USING public_test."dataHeight" b
    WHERE a.rid > b.rid AND
    COALESCE(a."ProjectKey", '[NULL]' ) = COALESCE(b."ProjectKey", '[NULL]' ) AND
    COALESCE(a."PrimaryKey", '[NULL]' ) = COALESCE(b."PrimaryKey", '[NULL]' ) AND
    COALESCE(a."LineKey", '[NULL]' ) = COALESCE(b."LineKey", '[NULL]' ) AND
    COALESCE(a."PointLoc",0) = COALESCE(b."PointLoc",0) AND COALESCE(a."PointNbr",0) = COALESCE(b."PointNbr",0) AND
    COALESCE(a."RecKey", '[NULL]' ) = COALESCE(b."RecKey", '[NULL]' ) AND COALESCE(a."Height",0) = COALESCE(b."Height",0) AND
    COALESCE(a."Species", '[NULL]' ) = COALESCE(b."Species", '[NULL]' ) AND
    COALESCE(a."Chkbox", 0::bit(1) ) = COALESCE(b."Chkbox", 0::bit(1) ) AND
    COALESCE(a."type", '[NULL]' ) = COALESCE(b."type", '[NULL]' ) AND
    COALESCE(a."GrowthHabit_measured", '[NULL]' ) = COALESCE(b."GrowthHabit_measured", '[NULL]' ) AND
    COALESCE(a."FormType", '[NULL]' ) = COALESCE(b."FormType", '[NULL]' ) AND
    COALESCE(a."FormDate", '1999-01-01' ) = COALESCE(b."FormDate", '1999-01-01' ) AND
    COALESCE(a."DateVisited", '1999-01-01' ) = COALESCE(b."DateVisited", '1999-01-01' ) AND
    COALESCE(a."Direction", '[NULL]' ) = COALESCE(b."Direction", '[NULL]' ) AND
    COALESCE(a."Measure",0) = COALESCE(b."Measure",0) AND
    COALESCE(a."LineLengthAmount",0) = COALESCE(b."LineLengthAmount",0) AND
    COALESCE(a."SpacingIntervalAmount",0) = COALESCE(b."SpacingIntervalAmount",0) AND
    COALESCE(a."SpacingType", '[NULL]' ) = COALESCE(b."SpacingType", '[NULL]' ) AND
    COALESCE(a."HeightOption", '[NULL]' ) = COALESCE(b."HeightOption", '[NULL]' ) AND
    COALESCE(a."HeightUOM", '[NULL]' ) = COALESCE(b."HeightUOM", '[NULL]' ) AND
    COALESCE(a."ShowCheckbox", 0::bit(1) ) = COALESCE(b."ShowCheckbox", 0::bit(1) ) AND
    COALESCE(a."CheckboxLabel", '[NULL]' ) = COALESCE(b."CheckboxLabel", '[NULL]' ) AND
    COALESCE(a."source", '[NULL]' ) = COALESCE(b."source", '[NULL]' ) AND
    COALESCE(a."DBKey", '[NULL]' ) = COALESCE(b."DBKey", '[NULL]' ) AND
    COALESCE(a."DateLoadedInDb", '1999-01-01' ) = COALESCE(b."DateLoadedInDb", '1999-01-01' );
"""
def counting_rows_dbkey(table, schema, dbkey):
    init="SELECT "
    # df = stools.schema_chooser("dataHeight")
    # col_list = " ".join([f'"{i}",' if (df.Field.to_list().index(i)!=len(df.Field.to_list())-1) else f'"{i}"' for i in df.Field ])
    init+= '''COUNT(*) FROM {0}."{1}" WHERE "DBKey" = '{2}' '''.format(schema, table, dbkey)
    # init+= col_list
    # init+=' ORDER BY count DESC;'
    return init

def delete_rows_dbkey(table, schema, dbkey):
    init="DELETE FROM "
    init+= f'''{schema}."{table}" WHERE "DBKey" = \'{dbkey}\'AND "DateLoadedInDb" = \'2023-04-10\';'''
    print(init)
counting_rows_dbkey("dataLPI", "public_test", "REPORT19Apr21RedHillsDIMA5.6asof2022-11-07")
delete_rows_dbkey("dataLPI", "public_test", "REPORT19Apr21RedHillsDIMA5.6asof2022-11-07")
