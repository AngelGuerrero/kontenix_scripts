/*===============================================================+
PROCEDURE:     XXKON_FN_UPDATE_ECCMA_EOTD
DESCRIPTION:   Procedimiento para actualizar eOTD general.
RETURNS:       Void

NOTES:         Script para realizar una actualización de registros en el eOTD
               general, este script usa el principio que se había implementado en
               Ruby, más sin embargo en vez de trabajar con archivos, trabaja con
               tablas temporales, la tabla tmp1 es la tabla temporal donde se
               encuentran los datos venidos de la India.

HISTORY
Version     Date         Author                    Change Reference
1.0    12/Junio/2018    Ángel Guerrero           Creación del script
                        Hebert Hernández
+================================================================*/
DO
$$
DECLARE
  -- RECORD para recorrer la tabla que se ha creado de tipos de concepto
  l_record RECORD;

  --//
  --// Guarda los datos de los registros a insertar y a actualizar en un contador,
  --// registros que se insertarán en una tabla para saber el log, en caso de no poder
  --// apreciar la consola.

  l_new_concepts INTEGER DEFAULT 0; -- Nuevos conceptos
  l_upd_concepts INTEGER DEFAULT 0; -- Conceptos actualizados


  -- Variable donde se guardarán los logs del proceso
  l_log_text VARCHAR(200);
  l_approve_contype BOOLEAN DEFAULT FALSE;
  _c text;
  
  l_count_tmp    INTEGER;
BEGIN
  raise notice 'Iniciando proceso de actualización de conceptos existentes. Hora: %', current_timestamp;

  -- Crea la tabla de donde se obtendrá la información en base a un concept type id
  PERFORM xx_fn_log('Borrando tabla XX_ECCMA_DATA_FROM_TMP');
  --DROP TABLE IF EXISTS XX_ECCMA_DATA_FROM_TMP;
  DROP TABLE IF EXISTS xx_concepts;

  --//
  --// Crea otra tabla temporal para aumentar el performance
  PERFORM xx_fn_log('Creando la Tabla xx_concepts');
  CREATE TABLE IF NOT EXISTS xx_concepts AS
    SELECT tmp.*
           ,con.id id_concept
      FROM tmp_dn tmp
      JOIN concepts con ON con.eccma_eotd = tmp.concept_id;
  
  
  SELECT COUNT(1)
    INTO l_count_tmp
  FROM xx_concepts;
  
  IF l_count_tmp = 0 THEN
     PERFORM xx_fn_log('Tabla xx_concepts vacia... Saliendo del proceso');
     RETURN;
  END IF;
  
  --//
  --// Pone la información de la tabla tmp1 en otra tabla temporal
  --// esto es porque la información irá aumentando
  --PERFORM xx_fn_log('Creando tabla XX_ECCMA_DATA_FROM_TMP');
  PERFORM xx_fn_log('Actualizando conceptos existentes...');
  -- a) Actualiza los conceptos ya existentes de forma masiva
  WITH upsert_data AS (
      SELECT * 
        FROM tmp_dn
      ),
      update_concept AS (
      UPDATE concepts
         SET is_deprecated =  CAST (upsert_data.concept_is_deprecated AS BOOLEAN),
             updated_at = current_timestamp
        FROM upsert_data
       WHERE concepts.eccma_eotd = upsert_data.concept_id
         AND concepts.is_deprecated <> CAST (upsert_data.concept_is_deprecated AS BOOLEAN)
      RETURNING  concept_id
    )
  SELECT COUNT(1) 
    INTO l_upd_concepts 
  FROM update_concept;

  raise notice '================================================================================';
  --// =================================================================================================================
  --// Datos que se van a actualizar

  l_log_text := 'Conceptos actualizados: ' || l_upd_concepts;
  PERFORM xx_fn_log(l_log_text);

  PERFORM xx_fn_log('Ciclo terminado correctamente, hora: ' || now());

  EXCEPTION
     WHEN OTHERS THEN
        GET STACKED DIAGNOSTICS _c = PG_EXCEPTION_CONTEXT;
        RAISE NOTICE 'context: >>%<<', _c;
        raise notice 'Ha ocurrido un error en la función: xxkon_fn_upate_eotd';
        raise notice 'Error: % %', sqlstate, sqlerrm;
END;
$$
LANGUAGE plpgsql;