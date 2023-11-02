-- function to remove blm_aim data from all tables

call public_test.util_project_key_drops(
  'public_test',
  ARRAY[
    'dataDustDeposition',
    'dataGap',
    'dataHeader',
    'dataHeight',
    'dataHorizontalFlux',
    'dataLPI',
    'dataSoilStability',
    'dataSpeciesInventory',
    'geoIndicators',
    'geoSpecies'
  ],
  ARRAY['BLM_AIM']
)

-- post ingest blm-aim count 

CREATE OR REPLACE FUNCTION public_test.count_blm_aim(schema_name TEXT)

RETURNS TABLE(
	table_name text,
	cnt BIGINT
) AS
$func$
DECLARE

 table_list TEXT[]:= ARRAY['dataGap','dataHeight', 'dataHeader', 'dataLPI', 'dataHorizontalFlux', 'dataSoilStability', 'dataSpeciesInventory', 'geoIndicators','geoSpecies'];

BEGIN

	for table_name in SELECT c.relname FROM pg_class c
    	JOIN pg_namespace s ON (c.relnamespace=s.oid)
    	WHERE c.relkind = 'r' AND s.nspname=schema_name AND c.relname = ANY(table_list)
  LOOP
    RETURN QUERY EXECUTE format(E'select cast(%L as text),count(*) from %I.%I where "ProjectKey" = \'BLM_AIM\' ',
       table_name, schema_name, table_name);
  END LOOP;
	
END
$func$ LANGUAGE plpgsql;