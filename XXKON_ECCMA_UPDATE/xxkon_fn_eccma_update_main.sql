/*===============================================================+
PROCEDURE:     XXKON_FN_UPDATE_ECCMA_EOTD
DESCRIPTION:   Procedimiento para actualizar eOTD general.
ARGUMENTS:     p_concept_eccma_eotd "Tipo de concepto de eccma"
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
CREATE OR REPLACE FUNCTION XXKON_FN_UPDATE_ECCMA_EOTD(p_concept_eccma_eotd VARCHAR(200))
  RETURNS VOID AS $$
DECLARE
  -- RECORD para recorrer la tabla que se ha creado de tipos de concepto
  l_record RECORD;

  --//
  --// Guarda los datos de los registros a insertar y a actualizar en un contador,
  --// registros que se insertarán en una tabla para saber el log, en caso de no poder
  --// apreciar la consola.
  l_new_concept_types INTEGER DEFAULT 0; -- Nuevos tipos de conceptos

  l_new_concepts INTEGER DEFAULT 0; -- Nuevos conceptos

  l_new_terms INTEGER DEFAULT 0; -- Nuevos términos insertados
  l_upd_terms INTEGER DEFAULT 0; -- Términos actualizados

  l_new_defs INTEGER DEFAULT 0; -- Nuevas definiciones insertadas
  l_upd_defs INTEGER DEFAULT 0; -- Definiciones actualizadas

  l_new_abbr INTEGER DEFAULT 0; -- Nuevas abreviaciones insertadas
  l_upd_abbr INTEGER DEFAULT 0; -- Abreviaciones actualizadas

  --//
  --// Variables usadas para guardar los id se las consultas que se van generando
  l_org_id            INTEGER DEFAULT 0;
  l_def_id            INTEGER DEFAULT 0;
  l_term_id           INTEGER DEFAULT 0;
  l_abbvr_id          INTEGER DEFAULT 0;
  l_language_id       INTEGER DEFAULT 0;
  l_concept_id        INTEGER DEFAULT 0;
  l_concept_type_id   INTEGER DEFAULT 0;
  l_concept_type_code INTEGER DEFAULT 0;

  -- Variable donde se guardarán los logs del proceso
  l_log_text VARCHAR(200);
BEGIN
  raise notice 'Truncando xx_eccma_update_terms';
  raise notice 'Truncando xx_eccma_update_definition';
  raise notice 'Truncando xx_eccma_update_log';

  TRUNCATE TABLE xx_eccma_update_terms;
  TRUNCATE TABLE xx_eccma_update_defs;
  TRUNCATE TABLE xx_eccma_update_log;

  raise notice 'Iniciando proceso de actualización para el tipo de concepto: %', p_concept_eccma_eotd;

  raise notice 'Borrando y creando la tabla XX_ECCMA_DATA_FROM_TMP en base al concept_type: %', p_concept_eccma_eotd;

  -- Crea la tabla de donde se obtendrá la información en base a un concept type id
  DROP TABLE IF EXISTS XX_ECCMA_DATA_FROM_TMP;
  PERFORM xx_fn_log(p_concept_eccma_eotd, 'Borrando tabla XX_ECCMA_DATA_FROM_TMP');

  --//
  --// Pone la información de la tabla tmp1 en otra tabla temporal
  --// esto es porque la información irá aumentando
  CREATE TABLE IF NOT EXISTS XX_ECCMA_DATA_FROM_TMP AS
    SELECT t1.*
      FROM tmp1 t1
      LEFT JOIN terminologicals t2
        ON t2.eccma_eotd = t1.term_id
     WHERE t1.concept_type_id = p_concept_eccma_eotd;

  PERFORM xx_fn_log(p_concept_eccma_eotd, 'Tabla XX_ECCMA_DATA_FROM_TMP creada');

  -- 1) Ciclo para conceptos de tipo others
  FOR l_record IN (SELECT tmp.*,
                     c.id concept_id_table
                   FROM XX_ECCMA_DATA_FROM_TMP tmp, CONCEPTS c
                   WHERE 1 = 1
                         AND c.eccma_eotd = tmp.concept_id)
  LOOP
    -- a) Al existir el concepto, se procede a actualizarlo en el campo deprecated
    --// FIXME: si el concept_is_deprecated no se acutaliza no debería de actualizarse nada más,
    --// en cambio, todos los registros que pasan por este bloque se actualizan
    UPDATE concepts
    SET is_deprecated = CAST(l_record.concept_is_deprecated AS BOOLEAN),
      updated_at = now()
    WHERE eccma_eotd = l_record.concept_id;

    -- b) Validar la organización del término
    -- Realiza primero la consulta para verificar que la organización exista.

    l_org_id := xx_fn_get_organization(l_record.term_organization_id);

    IF l_org_id = 0 THEN -- Significa que el término de la orgnización no existe, entonces la crea
      l_log_text := 'Insertando el término de la organización: ' || l_org_id;
      /* PERFORM xx_fn_log(p_concept_eccma_eotd, l_log_text); */

      INSERT INTO organizations VALUES (nextval('organizations_id_seq1'),-- Id
                                        l_record.term_organization_id,   -- eccma_eotd
                                        l_record.term_organization_name, -- name
                                        NULL,                            -- mail_address
                                        now(),                           -- created_at
                                        now()                            -- updated_at
      );
    END IF;

    -- c) Validación de términos, que eixstan o no...
    -- Obtiene el id del lenguaje para después utilizarlo en las próximas consultas
    l_language_id := xxkon_fn_get_language(l_record.language_id);
    l_term_id := xx_fn_get_terminological_id('term',
                                             l_record.term_id,
                                             l_language_id,
                                             l_record.concept_id_table,
                                             l_org_id);

    IF (l_record.term_id <> '' OR l_record.term_id IS NOT NULL OR l_record.term_id <> 'NULL') THEN
      IF l_term_id = 0 THEN -- Si no se obtiene valores, se crea el nuevo término.
        raise notice 'Insertando término en terminologicals';
        INSERT INTO terminologicals (id,
                                     terminology_class,
                                     eccma_eotd,
                                     content,
                                     is_deprecated,
                                     language_id,
                                     organization_id,
                                     tsv_content,
                                     orginator_reference,
                                     concept_id,
                                     created_at,
                                     updated_at
                                    )
                                    VALUES
                                    (nextval('terminologicals_id_seq1'),
                                     'term',
                                     l_record.term_id,
                                     l_record.term_content,
                                     CAST(l_record.term_is_deprecated AS BOOLEAN),
                                     language_id,
                                     l_org_id,
                                     to_tsvector('pg_catalog.simple', coalesce(unaccent(l_record.term_content),'')),
                                     l_record.term_originator_reference,
                                     l_record.concept_id_table,
                                     current_timestamp,
                                     current_timestamp
                                    );
        l_new_terms := l_new_terms + 1;
      ELSE -- Actualiza entonces los términos, llena primero la tabla para los registros que serán actualizados
        INSERT INTO xx_eccma_update_terms VALUES (l_term_id,
                                                  CAST(l_record.term_is_deprecated AS BOOLEAN),
                                                  l_record.term_content,
                                                  l_record.term_id
        );
        l_upd_terms := l_upd_terms + 1;
      END IF;
    END IF;

    -- d) Validar la organización de la definición (Existe o no la organización, si no, se crea, si existe, no se hace nada)
    IF (l_record.definition_id <> '' OR l_record.definition_id IS NOT NULL OR l_record.definition_id <> 'NULL' AND
        l_record.definition_organization_id <> '' OR  l_record.definition_organization_id IS NOT NULL OR  l_record.definition_organization_id <> 'NULL') THEN

      l_org_id := xx_fn_get_organization(l_record.definition_organization_id);

      IF l_org_id IS NULL THEN
        l_log_text := 'Insertando una nueva organización para la definición';
        /* PERFORM xx_fn_log(p_concept_eccma_eotd, l_log_text); */

        INSERT INTO organizations VALUES (nextval('organizations_id_seq1'),
                                          l_record.definition_organization_name,
                                          l_record.definition_organization_id
        );

      END IF; -- Sólo realiza éste if para la definición de la organización

      -- e) Validar definiciones (Agregar en caso de no existir, o actualizar content y deprecated)
      -- Realiza una consulta para la definición
      l_def_id := xx_fn_get_terminological_id('definition',
                                              l_record.definition_id,
                                              l_language_id,
                                              l_record.concept_id_table,
                                              l_org_id);

      IF l_def_id = 0 THEN
        l_log_text := 'Insertando nueva definición de la organización: ' || l_record.organization_id;
        /* PERFORM xx_fn_log(p_concept_eccma_eotd, l_log_text); */

        INSERT INTO terminologicals (id,
                                     terminology_class,
                                     eccma_eotd,
                                     content,
                                     is_deprecated,
                                     concept_id,
                                     language_id,
                                     organization_id,
                                     tsv_content,
                                     orginator_reference,
                                     created_at,
                                     updated_at
                                    )
                                    VALUES
                                    (nextval('terminologicals_id_seq1'),
                                     'definition',
                                     l_record.definition_id,
                                     l_record.definition_content,
                                     CAST(l_record.definition_is_deprecated AS BOOLEAN),
                                     l_record.concept_id,
                                     l_record.language_id,
                                     l_record.organization_id,
                                     l_record.definition_originator_reference,
                                     to_tsvector('pg_catalog.simple', coalesce(unaccent(l_record.definition_content),'')),
                                     current_timestamp,
                                     current_timestamp
                                    );

        l_new_defs := l_new_defs + 1;
      ELSE
        INSERT INTO xx_eccma_update_defs VALUES(l_def_id,
                                                CAST(l_record.definition_is_deprecated AS BOOLEAN),
                                                l_record.definition_content,
                                                l_record.definition_id
        );
        l_upd_defs := l_upd_defs + 1;
        l_log_text := 'Nueva definición para actualizar, id:' || l_def_id;
        /* PERFORM xx_fn_log(p_concept_eccma_eotd, l_log_text); */
      END IF;

    END IF;

    --F) VALIDAR ORGANIZACIONES DE ABREVIACIONES(EXISTE O NO. SI NO EXISTE SE CREA. SI EXISTE NO SE HACE NADA)
    IF (l_record.abbreviation_id <> '' OR l_record.abbreviation_id IS NOT NULL OR l_record.abbreviation_id <> 'NULL') THEN

      l_abbvr_id := xx_fn_get_terminological_id('abbreviation',
                                                l_record.abbreviation_id,
                                                l_language_id,
                                                l_record.concept_id_table,
                                                l_org_id);

      --// Si no está, entonces la agrega
      IF l_abbvr_id = 0 THEN
        l_log_text := 'Insertando una nueva abreviación de la organización: ' || l_record.organization_id;
        /* PERFORM xx_fn_log(p_concept_eccma_eotd, l_log_text); */

        INSERT INTO terminologicals (id,
                                     terminology_class,
                                     eccma_eotd,
                                     content,
                                     is_deprecated,
                                     language_id,
                                     organization_id,
                                     tsv_content,
                                     orginator_reference,
                                     created_at,
                                     updated_at
                                    ) VALUES
                                    (nextval('terminologicals_id_seq1'),
                                     'abbreviation',
                                     l_record.abbreviation_id,
                                     l_record.abbreviation_content,
                                     l_record.abbreviation_is_deprecated,
                                     l_record.language_id,
                                     l_org_id,
                                     to_tsvector('pg_catalog.simple', coalesce(unaccent(l_record.abbreviation_content),'')),
                                     l_record.abbreviation_originator_ref,
                                     current_timestamp,
                                     current_timestamp
                                    );
        l_new_abbr := l_new_abbr + 1;
      ELSE
        l_log_text := 'Nueva abreviación para actualizar: ' || l_abbvr_id;
        /* PERFORM xx_fn_log(p_concept_eccma_eotd, l_log_text); */

        INSERT INTO xx_eccma_update_abbr VALUES (l_abbvr_id, l_record.abbreviation_content, l_record.abbreviation_id);
        l_upd_abbr := l_upd_abbr + 1;
      END IF;

    END IF;
  END LOOP;

  l_log_text := 'Términos a insertar: ' || l_new_terms;
  PERFORM xx_fn_log(p_concept_eccma_eotd, l_log_text);

  l_log_text := 'Términos a actualizar: ' || l_upd_terms;
  PERFORM xx_fn_log(p_concept_eccma_eotd, l_log_text);

  l_log_text := 'Definiciones a insertar: ' || l_new_defs;
  PERFORM xx_fn_log(p_concept_eccma_eotd, l_log_text);

  l_log_text := 'Definiciones a actualizar: ' || l_upd_defs;
  PERFORM xx_fn_log(p_concept_eccma_eotd, l_log_text);

  l_log_text := 'Abreviaciones a insertar: ' || l_new_abbr;
  PERFORM xx_fn_log(p_concept_eccma_eotd, l_log_text);

  l_log_text := 'Abreviaciones a actualizar: ' || l_upd_abbr;
  PERFORM xx_fn_log(p_concept_eccma_eotd, l_log_text);

  --//
  --// Realiza las actualizaciones de las tablas generadas temporalmente para los términos
  l_log_text := 'Realizando update de los términos (eccma_update_terms) hora:' || now();
  PERFORM xx_fn_log(p_concept_eccma_eotd, l_log_text);

  UPDATE terminologicals t
  SET is_deprecated = u.is_deprecated,
    content = u.content,
    updated_at = now()
  FROM xx_eccma_update_terms u
  WHERE t.id = u.id;


  --//
  --// Realiza las actualizaciones de las tablas generadas temporalmente para las definiciones
  l_log_text := 'Realizando update de las definiciones (eccma_update_defs) hora: ' || now();
  PERFORM xx_fn_log(p_concept_eccma_eotd, l_log_text);

  UPDATE terminologicals t
    SET is_deprecated = u.is_deprecated,
        content = u.content,
        updated_at = current_timestamp
   FROM xx_eccma_update_defs u
  WHERE t.id = u.id;


  --//
  --// Realiza las actualizaciones de las tablas generadas temporalmente para las abreviaciones
  l_log_text := 'Realizando update de las abreviaciones (xx_eccma_update_abbr) hora: ' || now();
  PERFORM xx_fn_log(p_concept_eccma_eotd, l_log_text);

  UPDATE terminologicals t
     SET content = u.content,
         updated_at = current_timestamp
    FROM xx_eccma_update_abbr u
   WHERE t.id = u.id;

  raise notice '================================================================================';


  --// =================================================================================================================
  --// Inicia con la inserción de nuevos conceptos, términos, definiciones y abreviaciones, etc.

  --// Conceptos
  --// Crea una tabla temporal para obtener la diferencia de los conceptos que existen en el servidor remoto, pero no
  --// en la base de datos local.
  l_log_text := 'Nuevos elementos cuando cuando el concepto no existe';
  PERFORM xx_fn_log(p_concept_eccma_eotd, l_log_text);

  --// Se hace una tabla que sólo contenga los nuevos elementos a insertar para no requerir todo el volumen de la tabla temporal.
  DROP TABLE IF EXISTS xx_eccma_new_rows;
  CREATE TABLE IF NOT EXISTS xx_eccma_new_rows AS (SELECT tmp.*
                                                         FROM tmp1 tmp
                                                        WHERE 1 = 1
                                                          AND NOT EXISTS(SELECT *
                                                                           FROM concepts con
                                                                          WHERE con.eccma_eotd = tmp.concept_id)
                                                      );
  l_log_text := 'Tabla xx_eccma_new_rows, borrada y creada para el tipo de concepto: ' || p_concept_eccma_eotd || ' ' || now();
  PERFORM xx_fn_log(p_concept_eccma_eotd, l_log_text);

  --// FIXME: Falta consulta para obtener la cantidad de los nuevos tipos de conceptos

  --// Obtiene la cifra de nuevos términos
  l_new_terms := xx_fn_get_elements('term');
  PERFORM xx_fn_log(p_concept_eccma_eotd, 'Nuevos términos a insertar: ' || l_new_terms);

  --// Obtiene la cifra de nuevas definiciones
  l_new_defs := xx_fn_get_elements('definition');
  PERFORM xx_fn_log(p_concept_eccma_eotd, 'Nuevas definiciones a insertar: ' || l_new_defs);

  --// Obtiene la cifra de nuevas abreviaciones
  l_new_abbr := xx_fn_get_elements('abbreviation');
  PERFORM xx_fn_log(p_concept_eccma_eotd, 'Nuevas abreviaciones a insertar: ' || l_new_abbr);


  FOR l_record IN (SELECT * FROM xx_eccma_new_rows) LOOP

    IF l_record.concept_type_id <> '' OR l_record.concept_type_id IS NOT NULL OR l_record.concept_type_id <> 'NULL' THEN

      --// Inserta un nuevo tipo de concepto
      INSERT INTO concept_types SELECT nextval('concept_types_id_seq1'),
                                       l_record.concept_type_id,
                                       l_record.concept_type_name,
                                       NULL,
                                       lpad(cast((COUNT(1)) AS VARCHAR), 2, '0'),
                                       current_timestamp,
                                       current_timestamp
      WHERE NOT EXISTS (SELECT eccma_eotd FROM concept_types WHERE eccma_eotd = l_record.concept_type_id);
    END IF; --// Fin de la condicion del insert concept_type_id

    --// ===============================================================================================================

    --// Obtiene el concept_type_id actual
    SELECT id INTO l_concept_type_id FROM concept_types WHERE eccma_eotd = l_record.concept_type_id;

    l_log_text := 'Insertando un concepto: ' || l_record.concept_id;

    WITH ins_con AS (
      INSERT INTO concepts (id,
                            eccma_eotd,
                            is_deprecated,
                            created_at,
                            updated_at,
                            concept_type_id
      )
        SELECT nextval('concepts_id_seq1'),
          l_record.concept_id,
          CAST(l_record.definition_is_deprecated AS BOOLEAN),
          current_timestamp,
          current_timestamp,
          l_concept_type_id
        WHERE NOT EXISTS(SELECT eccma_eotd FROM concepts WHERE concepts.eccma_eotd = l_record.concept_id)
      RETURNING id
    )
    SELECT id INTO l_concept_id FROM ins_con;  --// Obtiene el concept_id que acaba de insertar

    --// ===============================================================================================================
    --// NUEVOS TÉRMINOS

    l_log_text := 'Insertando un nuevo término: ' || l_record.term_id;

    --// Id de la organización para insertar un nuevo término
    l_org_id := xx_fn_get_organization(l_record.definition_organization_id);

    --// Id del language
    SELECT id INTO l_language_id FROM languages WHERE eccma_eotd = l_record.language_id;

    --// Insertando un nuevo término si es que no existe éste eccma term
    INSERT INTO terminologicals (id,
                                 terminology_class,
                                 eccma_eotd,
                                 content,
                                 is_deprecated,
                                 language_id,
                                 organization_id,
                                 concept_id,
                                 tsv_content,
                                 orginator_reference,
                                 created_at,
                                 updated_at
                                )
                                SELECT
                                  nextval('terminologicals_id_seq1'),
                                  'term',
                                  l_record.term_id,
                                  l_record.term_content,
                                  CAST(l_record.term_is_deprecated AS BOOLEAN),
                                  l_language_id,
                                  l_org_id,
                                  l_concept_id,
                                  to_tsvector('pg_catalog.simple', coalesce(unaccent(l_record.term_content),'')),
                                  l_record.term_originator_reference,
                                  current_timestamp,
                                  current_timestamp
    WHERE NOT EXISTS (SELECT id FROM terminologicals
                       WHERE terminology_class = 'term'
                         AND terminologicals.eccma_eotd = l_record.term_id);

    --// ===============================================================================================================
    --// NUEVAS DEFINICIONES

    IF l_record.definition_id <> ''       OR l_record.definition_organization_id <> ''     OR
       l_record.definition_id <> 'NULL'   OR l_record.definition_organization_id <> 'NULL' OR
       l_record.definition_id IS NOT NULL OR l_record.definition_organization_id IS NOT NULL THEN

      l_log_text := 'Insertando una nueva definición: ' || l_record.definition_id;

      INSERT INTO terminologicals (id,
                                   terminology_class,
                                   eccma_eotd,
                                   content,
                                   is_deprecated,
                                   language_id,
                                   organization_id,
                                   concept_id,
                                   tsv_content,
                                   orginator_reference,
                                   created_at,
                                   updated_at
                                  )
                                  SELECT
                                    nextval('terminologicals_id_seq1'),
                                    'definition',
                                    l_record.definition_id,
                                    l_record.definition_content,
                                    CAST(l_record.definition_is_deprecated AS BOOLEAN),
                                    l_language_id,
                                    l_org_id,
                                    l_concept_id,
                                    to_tsvector('pg_catalog.simple', coalesce(unaccent(l_record.definition_content),'')),
                                    l_record.definition_originator_reference,
                                    current_timestamp,
                                    current_timestamp
      WHERE NOT EXISTS (SELECT id FROM terminologicals
                         WHERE terminology_class = 'definition'
                           AND terminologicals.eccma_eotd = l_record.definition_id);
    END IF;

    --// ===============================================================================================================
    --// NUEVAS ABREVIACIONES

    IF l_record.abbreviation_id <> '' OR l_record.abbreviation_id IS NOT NULL OR l_record.abbreviation_id <> 'NULL' THEN
      l_log_text := 'Insertando una nueva abreviación: ' || l_record.abbreviation_id;

      INSERT INTO terminologicals (id,
                                   terminology_class,
                                   term_id,
                                   eccma_eotd,
                                   content,
                                   is_deprecated,
                                   language_id,
                                   organization_id,
                                   tsv_content,
                                   orginator_reference,
                                   created_at,
                                   updated_at
                                  )
                                  SELECT
                                    nextval('terminologicals_id_seq1'),
                                    'abbreviation',
                                    l_record.term_id,
                                    l_record.abbreviation_ID,
                                    l_record.abbreviation_content,
                                    CAST(l_record.term_is_deprecated AS BOOLEAN),
                                    l_language_id,
                                    l_org_id,
                                    to_tsvector('pg_catalog.simple', coalesce(unaccent(l_record.abbreviation_content),'')),
                                    l_record.abbreviation_originator_ref,
                                    current_timestamp,
                                    current_timestamp
                                  WHERE NOT EXISTS (SELECT id FROM terminologicals
                                                     WHERE terminology_class = 'abbreviation'
                                                       AND terminologicals.eccma_eotd = l_record.abbreviation_id);
    END IF;
  END LOOP;

  PERFORM xx_fn_log(p_concept_eccma_eotd, 'Ciclo terminado correctamente, hora: ' || now());

EXCEPTION
WHEN OTHERS THEN
  raise notice 'Ha ocurrido un error en la función: xxkon_fn_upate_eotd';
  raise notice 'Error: % %', sqlstate, sqlerrm;
END;
$$
LANGUAGE plpgsql;
