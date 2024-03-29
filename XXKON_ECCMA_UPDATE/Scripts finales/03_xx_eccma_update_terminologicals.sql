/*===============================================================+
FILE:     03_xx_eccma_update_terminologicals
DESCRIPTION:   Procedimiento anónimo para actualizar los registros
               que fueron establecidos en el script número 2.

RETURNS:       Void

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

  l_new_terms INTEGER DEFAULT 0; -- Nuevos términos insertados
  l_upd_terms INTEGER DEFAULT 0; -- Términos actualizados

  l_new_defs INTEGER DEFAULT 0; -- Nuevas definiciones insertadas
  l_upd_defs INTEGER DEFAULT 0; -- Definiciones actualizadas

  l_new_abbr INTEGER DEFAULT 0; -- Nuevas abreviaciones insertadas
  l_upd_abbr INTEGER DEFAULT 0; -- Abreviaciones actualizadas



  l_term_is_deprecated BOOLEAN DEFAULT FALSE;
  l_abbr_is_deprecated BOOLEAN DEFAULT FALSE;
  l_def_is_deprecated BOOLEAN DEFAULT FALSE;
  l_con_is_deprecated BOOLEAN DEFAULT FALSE;

  -- Variable donde se guardarán los logs del proceso
  l_log_text VARCHAR(200);

  l_approve_contype BOOLEAN DEFAULT FALSE;

  _c text;
BEGIN
  PERFORM xx_fn_log('Iniciando proceso de actualización en la tabla terminologicals. Hora: ' || now());

  --//
  --// Realiza las actualizaciones de las tablas generadas temporalmente para los términos
  l_log_text := 'Realizando update de los términos (eccma_update_terms) hora:' || current_timestamp;
  PERFORM xx_fn_log(l_log_text);

  UPDATE terminologicals t
     SET is_deprecated = u.is_deprecated,
         updated_at = current_timestamp
    FROM xx_eccma_update_terms u
   WHERE t.id = u.id;

  --//
  --// Realiza las actualizaciones de las tablas generadas temporalmente para las definiciones
  l_log_text := 'Realizando update de las definiciones (eccma_update_defs) hora: ' || current_timestamp;
  PERFORM xx_fn_log(l_log_text);

  UPDATE terminologicals t
     SET is_deprecated = u.is_deprecated,
         updated_at = current_timestamp
    FROM xx_eccma_update_defs u
   WHERE t.id = u.id;

  --//
  --// Realiza las actualizaciones de las tablas generadas temporalmente para las abreviaciones
  l_log_text := 'Realizando update de las abreviaciones (xx_eccma_update_abbr) hora: ' || current_timestamp;
  PERFORM xx_fn_log(l_log_text);

  UPDATE terminologicals t
     SET is_deprecated = u.is_deprecated,
         updated_at = current_timestamp
    FROM xx_eccma_update_abbr u
   WHERE t.id = u.id;

  raise notice '================================================================================';
  --// =================================================================================================================
  --// Datos que se van a actualizar

  PERFORM xx_fn_log('Tercer ciclo terminado correctamente, hora: ' || now());

  EXCEPTION
     WHEN OTHERS THEN
        GET STACKED DIAGNOSTICS _c = PG_EXCEPTION_CONTEXT;
        RAISE NOTICE 'context: >>%<<', _c;
        raise notice 'Ha ocurrido un error en el script: 03_xx_eccma_update_terminologicals';
        raise notice 'Error: % %', sqlstate, sqlerrm;
END;
$$
LANGUAGE plpgsql;
