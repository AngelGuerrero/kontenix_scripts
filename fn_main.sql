CREATE OR REPLACE FUNCTION XXKON_FN_UPDATE_ECCMA_EOTD(p_concept_eccma_eotd VARCHAR(200))
  RETURNS VOID AS $$
DECLARE
  -- RECORD para recorrer la tabla que se ha creado de tipos de concepto
  l_record RECORD;

  l_new_terms INTEGER DEFAULT 0; -- Nuevos términos insertados --
  l_upd_terms INTEGER DEFAULT 0; -- Términos actualizados --

  l_new_defs INTEGER DEFAULT 0; -- Nuevas definiciones insertadas --
  l_upd_defs INTEGER DEFAULT 0; -- Definiciones actualizadas --

  l_new_abbr INTEGER DEFAULT 0; -- Nuevas abreviaciones insertadas --
  l_upd_abbr INTEGER DEFAULT 0; -- Abreviaciones actualizadas --

  l_org_id      INTEGER;
  l_def_id      INTEGER;
  l_term_id     INTEGER;
  l_abbvr_id    INTEGER;
  l_language_id INTEGER;

  -- Variables para el log --
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
  INSERT INTO xx_eccma_update_log VALUES (now(), p_concept_eccma_eotd, 'Borrando tabla XX_ECCMA_DATA_FROM_TMP');

  CREATE TABLE IF NOT EXISTS XX_ECCMA_DATA_FROM_TMP AS
    SELECT t1.*
      FROM tmp1 t1
      LEFT JOIN terminologicals t2
        ON t2.eccma_eotd = t1.term_id
     WHERE t1.concept_type_id = p_concept_eccma_eotd;

  INSERT INTO xx_eccma_update_log VALUES (now(), p_concept_eccma_eotd, 'Tabla XX_ECCMA_DATA_FROM_TMP creada');

  -- 1) Ciclo para conceptos de tipo others
  FOR l_record IN (SELECT tmp.*,
                          c.id concept_id_table
                     FROM XX_ECCMA_DATA_FROM_TMP tmp, CONCEPTS c
                    WHERE 1 = 1
                      AND c.eccma_eotd = tmp.concept_id)
  LOOP
    -- a) Al existir el concepto, se procede a actualizarlo en el campo deprecated
    UPDATE concepts
    SET is_deprecated = CAST(l_record.concept_is_deprecated AS BOOLEAN),
        updated_at = now()
    WHERE eccma_eotd = l_record.concept_id;

    -- b) Validar la organización del término
    -- Realiza primero la consulta para verificar que la organización exista.

    l_org_id := xx_fn_get_organization(l_record.term_organization_id);

    IF l_org_id = 0 THEN -- Significa que el término de la orgnización no existe, entonces la crea
      l_log_text := 'Insertando el término de la organización: ' || l_org_id;
      /* raise notice '%', l_log_text; */
      INSERT INTO xx_eccma_update_log VALUES (now(), p_concept_eccma_eotd, l_log_text);

      INSERT INTO organizations VALUES (nextval('organizations_id_seq1'),-- Id
                                        l_record.term_organization_id,   -- eccma_eotd
                                        l_record.term_organization_name, -- name
                                        NULL,                            -- mail_address
                                        now(),               -- created_at
                                        now()                -- updated_at
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
        INSERT INTO terminologicals(id,
                                    eccma_eotd,
                                    language_id,
                                    concept_id,
                                    organization_id)
        VALUES
          (nextval('terminologicals_id_seq1'),
           language_id,
           l_record.concept_id,
           l_org_id
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
    IF (l_record.definition_id <> ''              OR l_record.definition_id IS NOT NULL OR l_record.definition_id <> 'NULL' AND
        l_record.definition_organization_id <> '' OR l_record.definition_organization_id IS NOT NULL OR l_record.definition_organization_id <> 'NULL') THEN

      l_org_id := xx_fn_get_organization(l_record.definition_organization_id);

      IF l_org_id IS NULL THEN
        l_log_text := 'Insertando una nueva organización para la definición';
        /* raise notice '%', l_log_text; */
        INSERT INTO xx_eccma_update_log VALUES (now(), p_concept_eccma_eotd, l_log_text);

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
        /* raise notice '%', l_log_text; */
        INSERT INTO xx_eccma_update_log VALUES (now(), p_concept_eccma_eotd, l_log_text);

        INSERT INTO terminologicals VALUES
                                    (nextval('terminologicals_id_seq1'),
                                     l_record.definition_id,
                                     l_record.language_id,
                                     l_record.concept_id,
                                     l_record.organization_id
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
        /* raise notice '%', l_log_text; */
        INSERT INTO xx_eccma_update_log VALUES (now(), p_concept_eccma_eotd, l_log_text);
      END IF;

    END IF;

    --F) VALIDAR ORGANIZACIONES DE ABREVIACIONES(EXISTE O NO. SI NO EXISTE SE CREA. SI EXISTE NO SE HACE NADA)
    IF (l_record.abbreviation_id <> '' OR l_record.abbreviation_id IS NOT NULL OR l_record.abbreviation_id <> 'NULL') THEN

      l_abbvr_id := xx_fn_get_terminological_id('abbreviation',
                                                l_record.abbreviation_id,
                                                l_language_id,
                                                l_record.concept_id_table,
                                                l_org_id
                                               );

      --// Si no está, entonces la agrega
      IF l_abbvr_id = 0 THEN
        l_log_text := 'Insertando una nueva abreviación de la organización: ' || l_record.organization_id;
        /* raise notice '%', l_log_text; */
        INSERT INTO xx_eccma_update_log VALUES (now(), p_concept_eccma_eotd, l_log_text);

        INSERT INTO terminologicals VALUES
                                    (nextval('terminologicals_id_seq1'),
                                     l_record.abbreviation_id,
                                     l_record.language_id,
                                     l_term_id
                                    );
        l_new_abbr := l_new_abbr + 1;
      ELSE
        l_log_text := 'Nueva abreviación para actualizar: ' || l_abbvr_id;
        /* raise notice '%', l_log_text; */
        INSERT INTO xx_eccma_update_log VALUES (now(), p_concept_eccma_eotd, l_log_text);

        INSERT INTO xx_eccma_update_abbr VALUES (l_abbvr_id, l_record.abbreviation_content, l_record.abbreviation_id);
        l_upd_abbr := l_upd_abbr + 1;
      END IF;

    END IF;


  END LOOP;

  l_log_text := 'Términos a insertar: ' || l_new_terms;
  raise notice '%', l_log_text;
  insert into xx_eccma_update_log VALUES (now(), p_concept_eccma_eotd, l_log_text);

  l_log_text := 'Términos a actualizar: ' || l_upd_terms;
  raise notice '%', l_log_text;
  insert into xx_eccma_update_log VALUES (now(), p_concept_eccma_eotd, l_log_text);


  l_log_text := 'Definiciones a insertar: ' || l_new_defs;
  raise notice '%', l_log_text;
  insert into xx_eccma_update_log VALUES (now(), p_concept_eccma_eotd, l_log_text);

  l_log_text := 'Definiciones a actualizar: ' || l_upd_defs;
  raise notice '%', l_log_text;
  insert into xx_eccma_update_log VALUES (now(), p_concept_eccma_eotd, l_log_text);


  l_log_text := 'Abreviaciones a insertar: ' || l_new_abbr;
  raise notice '%', l_log_text;
  insert into xx_eccma_update_log VALUES (now(), p_concept_eccma_eotd, l_log_text);

  l_log_text := 'Abreviaciones a actualizar: ' || l_upd_abbr;
  raise notice '%', l_log_text;
  insert into xx_eccma_update_log VALUES (now(), p_concept_eccma_eotd, l_log_text);

  raise notice '================================================================================';

  --//
  --// Realiza las actualizaciones de las tablas generadas temporalmente para los términos
  l_log_text := 'Realizando update de los términos (eccma_update_terms)';
  raise notice '%', l_log_text;
  INSERT INTO xx_eccma_update_log VALUES (now(), p_concept_eccma_eotd, l_log_text);

  UPDATE terminologicals t
     SET is_deprecated = u.is_deprecated,
         content = u.content,
         updated_at = now()
    FROM xx_eccma_update_terms u
   WHERE t.id = u.id;

   --//
   --// Realiza las actualizaciones de las tablas generadas temporalmente para las definiciones
   l_log_text := 'Realizando update de las definiciones (eccma_update_defs)';
   raise notice '%', l_log_text;
   INSERT INTO xx_eccma_update_log VALUES (now(), p_concept_eccma_eotd, l_log_text);

  UPDATE terminologicals t
     SET is_deprecated = u.is_deprecated,
         content = u.content,
         updated_at = current_timestamp
    FROM xx_eccma_update_defs u
   WHERE t.id = u.id;

  --//
  --// Realiza las actualizaciones de las tablas generadas temporalmente para las abreviaciones
  l_log_text := 'Realizando update de las abreviaciones (xx_eccma_update_abbr)';
  raise notice '%', l_log_text;
  INSERT INTO xx_eccma_update_log VALUES (now(), p_concept_eccma_eotd, l_log_text);

  UPDATE terminologicals t
     SET content = u.content,
         updated_at = current_timestamp
    FROM xx_eccma_update_abbr u
   WHERE t.id = u.id;

EXCEPTION
  WHEN OTHERS THEN
    raise notice 'Ha ocurrido un error en la función: xxkon_fn_upate_eotd';
    raise notice 'Error: % %', sqlstate, sqlerrm;
END;
$$
LANGUAGE plpgsql;

--------------------------------------------------------------------------------

DO
$$
DECLARE
  concepts_types VARCHAR[] := ARRAY['0161-1#CT-00#1'
                                    /* '0161-1#CT-01#1',
                                    '0161-1#CT-02#1',
                                    '0161-1#CT-03#1',
                                    '0161-1#CT-04#1',
                                    '0161-1#CT-05#1',
                                    '0161-1#CT-06#1',
                                    '0161-1#CT-07#1',
                                    '0161-1#CT-08#1' */
                                   ];
  c VARCHAR;
BEGIN
  FOREACH c IN ARRAY concepts_types LOOP
    PERFORM xxkon_fn_update_eccma_eotd(c);
  end loop;
END;
$$
