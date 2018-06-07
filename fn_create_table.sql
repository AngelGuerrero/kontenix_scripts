CREATE OR REPLACE FUNCTION XXKON_FN_CREATE_TABLE(p_tablename VARCHAR(200), p_concept_type VARCHAR(200))
  RETURNS BOOLEAN AS $$
DECLARE
  l_tmp_exists BOOLEAN;

  l_sql_stmt TEXT;

  retval BOOLEAN DEFAULT TRUE;

BEGIN
  SELECT EXISTS
  (
    SELECT 1
      INTO l_tmp_exists
      FROM pg_tables
     WHERE tablename = 'tmp1'
  );

  RAISE NOTICE 'Tabla tmp1 existe?: %', l_tmp_exists;

  IF l_tmp_exists THEN
    RAISE NOTICE '--- Creando la tabla de % ---', p_tablename;

    BEGIN
      EXECUTE 'CREATE TABLE IF NOT EXISTS ? AS
                     SELECT tmp1.*
                       FROM tmp1 LEFT JOIN terminologicals
                         ON eccma_eotd = tmp1.term_id
                      WHERE tmp1.concept_id =  ? ;' USING p_tablename, p_concept_type;

    EXCEPTION
      WHEN OTHERS THEN
        raise notice 'Ha ocurrido un error al tratar de crear la tabla: %, Error: % %', p_tablename, sqlstate, sqlerrm;
        retval = FALSE;
    END;
  END IF;

  RETURN retval;
EXCEPTION
  WHEN OTHERS THEN
    raise notice 'Ha ocurrido un error en la función XXKON_FN_UPDATE_EOTD: % %', sqlstate, sqlerrm;
    RETURN FALSE;
END;
$$ LANGUAGE plpgsql;

DROP FUNCTION xxkon_fn_create_table(character varying,character varying);

DO
$$
BEGIN
  PERFORM xxkon_fn_create_table('xx_eccma_others', '0161-1#CT-00#1');
END;
$$;
