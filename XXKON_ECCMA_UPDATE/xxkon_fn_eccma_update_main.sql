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

  l_new_terms INTEGER DEFAULT 0; -- Nuevos términos insertados
  l_upd_terms INTEGER DEFAULT 0; -- Términos actualizados

  l_new_defs INTEGER DEFAULT 0; -- Nuevas definiciones insertadas
  l_upd_defs INTEGER DEFAULT 0; -- Definiciones actualizadas

  l_new_abbr INTEGER DEFAULT 0; -- Nuevas abreviaciones insertadas
  l_upd_abbr INTEGER DEFAULT 0; -- Abreviaciones actualizadas

  l_new_languages     INTEGER DEFAULT 0;
  l_new_conceptypes   INTEGER DEFAULT 0; -- Nuevos tipos de conceptos
  l_new_organizations INTEGER DEFAULT 0;


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

  l_abbr_is_deprecated BOOLEAN DEFAULT FALSE;
  l_def_is_deprecated BOOLEAN DEFAULT FALSE;
  l_con_is_deprecated BOOLEAN DEFAULT FALSE;

  -- Variable donde se guardarán los logs del proceso
  l_log_text VARCHAR(200);

  l_approve_contype BOOLEAN DEFAULT FALSE;

  _c text;

  l_count_tmp    INTEGER;
BEGIN
  raise notice 'Iniciando proceso de actualización. Hora: %', current_timestamp;
  raise notice 'Truncando tabla xx_eccma_update_terms';
  raise notice 'Truncando tabla xx_eccma_update_definition';
  raise notice 'Truncando tabla xx_eccma_update_abbr';

  TRUNCATE TABLE xx_eccma_update_terms;
  TRUNCATE TABLE xx_eccma_update_defs;
  TRUNCATE TABLE xx_eccma_update_abbr;

  -- Crea la tabla de donde se obtendrá la información en base a un concept type id
  DROP TABLE IF EXISTS XX_ECCMA_DATA_FROM_TMP;
  PERFORM xx_fn_log('Borrando tabla XX_ECCMA_DATA_FROM_TMP');

  --//
  --// Crea otra tabla temporal para aumentar el performance
  CREATE TABLE IF NOT EXISTS tmp2 AS
    SELECT tmp.*
           ,con.id id_concept
      FROM tmp1 tmp
      JOIN concepts con ON con.eccma_eotd = tmp.concept_id
  ;
  PERFORM xx_fn_log('Tabla tmp2 creada');

  SELECT COUNT(1)
    INTO l_count_tmp
	FROM tmp2;

  IF l_count_tmp = 0 THEN
     PERFORM xx_fn_log('Tabla tmp2 vacia... Saliendo del proceso');
     RETURN;
  END IF;

  --//
  --// Pone la información de la tabla tmp1 en otra tabla temporal
  --// esto es porque la información irá aumentando
  PERFORM xx_fn_log('Creando tabla XX_ECCMA_DATA_FROM_TMP');

  CREATE TABLE IF NOT EXISTS XX_ECCMA_DATA_FROM_TMP AS
     SELECT tmp.*
		    , org1.eccma_eotd eccma_eotd_org_term
			, org1.id id_org_term
			----------------------------------------------------
			, org2.eccma_eotd eccma_eotd_org_definition
			, org2.id id_org_definition
			----------------------------------------------------
			, org3.eccma_eotd eccma_eotd_org_abbreviation
			, org3.id id_org_abbreviation
			----------------------------------------------------
			, lan.id id_language
			, lan.eccma_eotd eccma_eotd_language
			----------------------------------------------------
			, ct.id id_concept_type
			, ct.eccma_eotd eccma_eotd_concept_type
			----------------------------------------------------
			, t1.id id_term
			, t1.eccma_eotd eccma_eotd_term
			, t1.is_deprecated eccma_eotd_is_dep_term
			, t1.content eccma_eotd_content_term
			----------------------------------------------------
			, t2.id id_definition
			, t2.eccma_eotd eccma_eotd_definition
			, t2.is_deprecated eccma_eotd_is_dep_definition
			, t2.content eccma_eotd_content_definition
			----------------------------------------------------
			, t3.id id_abbreviation
			, t3.eccma_eotd eccma_eotd_abbreviation
			, t3.is_deprecated eccma_eotd_is_dep_abbreviation
			, t3.content eccma_eotd_content_abbreviation
      FROM tmp2 tmp
      LEFT JOIN organizations org1 ON org1.eccma_eotd = tmp.term_organization_id
      LEFT JOIN organizations org2 ON org2.eccma_eotd = tmp.definition_organization_id
      LEFT JOIN organizations org3 ON org3.eccma_eotd = tmp.abbreviation_organization_id
      LEFT JOIN languages lan      ON lan.eccma_eotd = tmp.language_id
      LEFT JOIN concept_types ct   ON ct.eccma_eotd = tmp.concept_type_id
      LEFT JOIN terminologicals t1 on tmp.term_id = t1.eccma_eotd AND t1.terminology_class = 'term'
      LEFT JOIN terminologicals t2 ON tmp.definition_id = t2.eccma_eotd AND t2.terminology_class = 'definition'
      LEFT JOIN terminologicals t3 ON tmp.abbreviation_id = t3.eccma_eotd AND t3.terminology_class = 'abbreviation';

  PERFORM xx_fn_log('Actualizando conceptos existentes...');
  -- a) Actualiza los conceptos ya existentes de forma masiva
  WITH upsert_data AS (
      SELECT * from tmp1
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

  PERFORM xx_fn_log('Ciclo de validacion para terminos, definiciones y abreviaciones - conceptos existentes...');
  -- 1) Ciclo para conceptos que si existen
  FOR l_record IN (SELECT * FROM XX_ECCMA_DATA_FROM_TMP)
  LOOP

  --------------------------------------------------------------------------------------------------------
  --
  --------------------------------------------------------------------------------------------------------
    --HHH
	--FALTARIA VALIDAR SI EL LANGUAGE NO EXISTE
	IF LENGTH(l_record.language_id) >0 THEN
      IF l_record.id_language IS NULL THEN

	    BEGIN
		   SELECT id
		     INTO STRICT l_language_id
		     FROM languages
			WHERE eccma_eotd = l_record.language_id;
	    EXCEPTION
		   WHEN OTHERS THEN
		      l_language_id := 0;
		END;

		IF l_language_id = 0 THEN
		   INSERT INTO languages (id,
								  eccma_eotd,
								  country_code,
								  name,
								  description,
								  code,
								  created_at,
								  updated_at)
							VALUES(
								  nextval('languages_id_seq1'),
								  l_record.language_id,
								  l_record.country_code,
								  l_record.language_name,
								  l_record.language_name,
								  l_record.language_code,
								  current_timestamp,
								  current_timestamp
								  )
		   RETURNING id INTO l_language_id;
		   l_new_languages := l_new_languages + 1;
		END IF;
      ELSE
        l_language_id := l_record.id_language;
      END IF;
	END IF;

	-- b) Validar la organización del término
    --HHH 17 JUN 2018
    -- ESTE IF NO ESTABA, FALTABA AGREGARLO
    IF  length(l_record.term_organization_id) >0 THEN
        l_org_id := l_record.id_org_term;

        IF l_org_id IS NULL THEN
        --HHH IF COMENTADO PARA HOMOLOGAR CAMPO A VALIDAR SI ES NULO
        --IF length(l_record.eccma_eotd_org_term) = 0 THEN
        -- Significa que la organizacion del término no existe, entonces se crea

        l_log_text := 'Insertando la organización del término';
        --HHH
        --PERFORM xx_fn_log(l_log_text);

        INSERT INTO organizations VALUES (nextval('organizations_id_seq1'),-- Id
                                          l_record.term_organization_id,   -- eccma_eotd
                                          l_record.term_organization_name, -- name
                                          NULL,                            -- mail_address
                                          current_timestamp,                           -- created_at
                                          current_timestamp                            -- updated_at
                                          )
        RETURNING id INTO l_org_id;
        l_new_organizations := l_new_organizations + 1;

      --HHH COMENTADO, ESTA DEMAS EL ELSE
      --ELSE
      --  l_org_id := l_record.id_org_term;
      END IF;
      ------------------------------------------------------------------------------------------------------------

      --// TÉRMINOS
      IF length(l_record.term_id) > 0 THEN

        IF l_record.id_term IS NULL THEN
        -- Si no se obtiene valores, se crea el nuevo término.

          raise notice 'Insertando término en terminologicals';
          INSERT INTO terminologicals (id,
                                       terminology_class,
                                       eccma_eotd,
                                       content,
                                       orginator_reference,
                                       is_deprecated,
                                       language_id,
                                       organization_id,
                                       term_id,
                                       tsv_content,
                                       concept_id,
                                       created_at,
                                       updated_at)
								 VALUES(
									   nextval('terminologicals_id_seq1'),
									   'term',
									   l_record.term_id,
									   l_record.term_content,
									   l_record.term_originator_reference,
									   CAST(l_record.term_is_deprecated AS BOOLEAN),
									   l_language_id,
									   l_org_id,
									   NULL, --> term_id no se ocupa para el term
									   to_tsvector('pg_catalog.simple', coalesce(unaccent(l_record.term_content),'')),
									   l_record.id_concept,
									   current_timestamp,
									   current_timestamp
									   );
          l_new_terms := l_new_terms + 1;
        ELSE -- Actualiza entonces los términos, llena primero la tabla para los registros que serán actualizados
          IF l_record.eccma_eotd_is_dep_term <> l_record.term_is_deprecated THEN
             INSERT INTO xx_eccma_update_terms VALUES (l_record.id_term,
                                                       CAST(l_record.term_is_deprecated AS BOOLEAN),
                                                       l_record.term_content,
                                                       l_record.term_id
                                                      );
             l_upd_terms := l_upd_terms + 1;
          END IF;
        END IF;
      END IF;
    --HHH
    END IF;

----------------------------------------------------------------------------------------------------------------------------
    -- d) Validar la organización de la definición (Existe o no la organización, si no, se crea, si existe, no se hace nada)
    IF length(l_record.definition_organization_id) >0 THEN

      l_org_id := l_record.id_org_definition;

      IF l_org_id IS NULL THEN
         l_log_text := 'Insertando una nueva organización para la definición';
         -- PERFORM xx_fn_log(l_log_text);

         INSERT INTO organizations VALUES (nextval('organizations_id_seq1'),
                                           l_record.definition_organization_id,
                                           l_record.definition_organization_name,
                                           NULL,
                                           current_timestamp,
                                           current_timestamp
                                          )
         RETURNING id INTO l_org_id;
         l_new_organizations := l_new_organizations + 1;
      END IF; -- Sólo realiza éste if para la definición de la organización


      --// DEFINICIONES
      IF length(l_record.definition_id) >0 THEN

        CASE
          WHEN l_record.definition_is_deprecated = '1' THEN l_def_is_deprecated := TRUE;
          WHEN l_record.definition_is_deprecated = '0' THEN l_def_is_deprecated := FALSE;
        ELSE l_def_is_deprecated := FALSE;
        END CASE;

        IF l_record.id_definition IS NULL THEN -- Si no se obtiene valores, se crea una nueva definición.

          INSERT INTO terminologicals (id,
                                       terminology_class,
                                       eccma_eotd,
                                       content,
                                       orginator_reference,
                                       is_deprecated,
                                       language_id,
                                       organization_id,
                                       term_id,
                                       tsv_content,
                                       concept_id,
                                       created_at,
                                       updated_at)
								 VALUES(
									   nextval('terminologicals_id_seq1'),
									   'definition',
									   l_record.definition_id,
									   l_record.definition_content,
									   l_record.definition_originator_reference,
									   l_def_is_deprecated,
									   l_language_id,
									   l_org_id,
									   NULL, --> No se ocupa term_id para definition
									   to_tsvector('pg_catalog.simple', coalesce(unaccent(l_record.definition_content),'')),
									   l_record.id_concept,
									   current_timestamp,
									   current_timestamp
									   );

          l_new_defs := l_new_defs + 1;
        ELSE
          IF l_record.eccma_eotd_is_dep_definition <> l_def_is_deprecated THEN
             INSERT INTO xx_eccma_update_defs VALUES(l_record.id_definition,
                                                     l_def_is_deprecated,
                                                     l_record.definition_content,
                                                     l_record.definition_id
                                                    );
             --RETURNING id INTO l_def_id;
             l_upd_defs := l_upd_defs + 1;
          END IF;
        END IF;
      END IF;
    END IF; -- Cierre end if de organization_definition

    --F) VALIDAR ORGANIZACIONES DE ABREVIACIONES(EXISTE O NO. SI NO EXISTE SE CREA. SI EXISTE NO SE HACE NADA)

    IF length(l_record.abbreviation_id) >0 THEN
      l_org_id := l_record.id_org_abbreviation;

      IF length(l_record.abbreviation_organization_id) >0 THEN

        IF l_org_id IS NULL THEN
           l_log_text := 'Insertando una nueva organización para la abreviación';
           -- PERFORM xx_fn_log(l_log_text);

           INSERT INTO organizations(id,
                                     eccma_eotd,
                                     name,
                                     mail_address,
                                     created_at,
                                     updated_at)
							   VALUES(
								     nextval('organizations_id_seq1'),
									 l_record.abbreviation_organization_id,
									 l_record.abbreviation_organization_name,
									 NULL,
									 current_timestamp,
									 current_timestamp
									 )
           RETURNING id INTO l_org_id;
           l_new_organizations := l_new_organizations + 1;
        END IF;
      END IF;

      CASE
        WHEN l_record.abbreviation_is_deprecated = '1' THEN l_abbr_is_deprecated := TRUE;
        WHEN l_record.abbreviation_is_deprecated = '0' THEN l_abbr_is_deprecated := FALSE;
      ELSE l_abbr_is_deprecated := FALSE;
      END CASE;

      --// Si no está la abreviación entonces la agrega
      IF l_record.id_abbreviation IS NULL THEN
        l_log_text := 'Insertando una nueva abreviación';
        -- PERFORM xx_fn_log(l_log_text);

        BEGIN
          INSERT INTO terminologicals (id,
                                       terminology_class,
                                       eccma_eotd,
                                       term_id,
                                       content,
                                       is_deprecated,
                                       language_id,
                                       organization_id,
                                       tsv_content,
                                       orginator_reference,
                                       created_at,
                                       updated_at)
								 VALUES(
									    nextval('terminologicals_id_seq1'),
									    'abbreviation',
									    l_record.abbreviation_id,
									    l_record.id_term,
									    l_record.abbreviation_content,
									    l_abbr_is_deprecated,
									    l_language_id,
									    l_org_id,
									    to_tsvector('pg_catalog.simple', coalesce(unaccent(l_record.abbreviation_content),'')),
									    l_record.abbreviation_originator_ref,
									    current_timestamp,
									    current_timestamp
									    );
          l_new_abbr := l_new_abbr + 1;
          EXCEPTION
             WHEN OTHERS THEN
                GET STACKED DIAGNOSTICS _c = PG_EXCEPTION_CONTEXT;
                raise notice 'details: >>%<<', _c;
        END;
      ELSE
        IF l_record.eccma_eotd_is_dep_abbreviation <> l_abbr_is_deprecated THEN
           INSERT INTO xx_eccma_update_abbr VALUES (l_record.id_abbreviation,
                                                    l_abbr_is_deprecated,
                                                    l_record.abbreviation_content);
           --RETURNING id INTO l_abbvr_id;
           l_upd_abbr := l_upd_abbr + 1;
        END IF;
      END IF;
    END IF;
  END LOOP;

  --//
  --// Realiza las actualizaciones de las tablas generadas temporalmente para los términos
  l_log_text := 'Realizando update de los términos (eccma_update_terms) hora:' || current_timestamp;
  PERFORM xx_fn_log(l_log_text);

  UPDATE terminologicals t
     SET is_deprecated = u.is_deprecated,
         --content = u.content,
         --tsv_content = to_tsvector('pg_catalog.simple', coalesce(unaccent(u.content),'')),
         updated_at = current_timestamp
    FROM xx_eccma_update_terms u
   WHERE t.id = u.id;


  --//
  --// Realiza las actualizaciones de las tablas generadas temporalmente para las definiciones
  l_log_text := 'Realizando update de las definiciones (eccma_update_defs) hora: ' || current_timestamp;
  PERFORM xx_fn_log(l_log_text);

  UPDATE terminologicals t
     SET is_deprecated = u.is_deprecated,
         --content = u.content,
         --tsv_content = to_tsvector('pg_catalog.simple', coalesce(unaccent(u.content),'')),
         updated_at = current_timestamp
    FROM xx_eccma_update_defs u
   WHERE t.id = u.id;


  --//
  --// Realiza las actualizaciones de las tablas generadas temporalmente para las abreviaciones
  l_log_text := 'Realizando update de las abreviaciones (xx_eccma_update_abbr) hora: ' || current_timestamp;
  PERFORM xx_fn_log(l_log_text);

  UPDATE terminologicals t
     SET is_deprecated = u.is_deprecated,
         --content = u.content,
         --tsv_content = to_tsvector('pg_catalog.simple', coalesce(unaccent(u.content),'')),
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
  PERFORM xx_fn_log(l_log_text);

  --// Se hace una tabla que sólo contenga los nuevos elementos a insertar para no requerir todo el volumen de la tabla temporal.
  DROP TABLE IF EXISTS xx_eccma_new_rows;

  CREATE TABLE IF NOT EXISTS xx_eccma_new_rows AS (
    SELECT tmp.*
        , org1.eccma_eotd eccma_eotd_org_term
        , org1.id id_org_term
        , org2.eccma_eotd eccma_eotd_org_definition
        , org2.id id_org_definition
        , org3.eccma_eotd eccma_eotd_org_abbreviation
        , org3.id id_org_abbreviation
        , lan.id id_language
        , lan.eccma_eotd eccma_eotd_language
        , ct.id id_concept_type
        , ct.eccma_eotd eccma_eotd_concept_type
     FROM tmp1 tmp
     LEFT JOIN organizations org1 ON org1.eccma_eotd = tmp.term_organization_id
     LEFT JOIN organizations org2 ON org2.eccma_eotd = tmp.definition_organization_id
     LEFT JOIN organizations org3 ON org3.eccma_eotd = tmp.abbreviation_organization_id
     LEFT JOIN languages lan      ON lan.eccma_eotd = tmp.language_id
     LEFT JOIN concept_types ct   ON ct.eccma_eotd = tmp.concept_type_id
    WHERE NOT EXISTS(SELECT 1 FROM concepts con WHERE con.eccma_eotd = tmp.concept_id)
  );

  PERFORM xx_fn_log('Tabla xx_eccma_new_rows, borrada y creada para el tipo de concepto: ' || now());

  FOR l_record IN (SELECT *
                     FROM xx_eccma_new_rows
                    WHERE length(concept_type_id) > 0 )
  LOOP

    --CASE
    --  WHEN length(l_record.concept_type_id) >0 THEN l_approve_contype := TRUE;
    --END CASE;

    IF length(l_record.concept_type_id) >0 THEN

      BEGIN
        SELECT id
          INTO STRICT l_concept_type_id
          FROM concept_types
         WHERE concept_types.eccma_eotd = l_record.concept_type_id;

      EXCEPTION
        WHEN NO_DATA_FOUND THEN
          l_concept_type_id := 0;
        WHEN OTHERS THEN
          l_concept_type_id := 0;
      END;


      IF l_concept_type_id = 0 THEN
        INSERT INTO concept_types VALUES (nextval('concept_types_id_seq1'),
                                          l_record.concept_type_id,
                                          l_record.concept_type_name,
                                          NULL,
                                          (SELECT LPAD(CAST(MAX(CAST(code AS INTEGER)) +1 AS VARCHAR), 2, '0') FROM concept_types),
                                          current_timestamp,
                                          current_timestamp)
        RETURNING id INTO l_concept_type_id;

        l_new_conceptypes := l_new_conceptypes + 1;
      END IF;

    END IF; --// Fin de la condicion del insert concept_type_id

    --// ===============================================================================================================

    IF l_concept_type_id > 0 THEN
      --// Validación si el concepto ya está en la base de datos de Kontenix
      BEGIN
        SELECT id
          INTO STRICT l_concept_id
          FROM concepts
         WHERE eccma_eotd = l_record.concept_id;
      EXCEPTION
         WHEN OTHERS THEN
            l_concept_id := 0;
      END;

      CASE
        WHEN l_record.concept_is_deprecated = '1' THEN l_con_is_deprecated := TRUE;
        WHEN l_record.concept_is_deprecated = '0' THEN l_con_is_deprecated := FALSE;
      ELSE l_con_is_deprecated := FALSE;
      END CASE;

      IF l_concept_id = 0 THEN
         INSERT INTO concepts (id,
                               eccma_eotd,
                               is_deprecated,
                               created_at,
                               updated_at,
                               concept_type_id)
					     VALUES(
						       nextval('concepts_id_seq1'),
							   l_record.concept_id,
							   l_con_is_deprecated,
							   current_timestamp,
							   current_timestamp,
							   l_concept_type_id
						       )
        RETURNING id INTO l_concept_id;
        l_new_concepts := l_new_concepts + 1;
      END IF;

      --// ===============================================================================================================
      --// NUEVOS TÉRMINOS

      l_log_text := 'Insertando nuevo término: ' || l_record.term_id;

      --//
      --// Validación de language
      IF l_record.id_language IS NULL THEN
        INSERT INTO languages (id,
                               eccma_eotd,
                               country_code,
                               name,
                               description,
                               code,
                               created_at,
                               updated_at)
						VALUES(
							  nextval('languages_id_seq1'),
							  l_record.language_id,
							  l_record.country_code,
							  l_record.language_name,
							  l_record.language_name,
							  l_record.language_code,
							  current_timestamp,
							  current_timestamp
							  )
        RETURNING id INTO l_language_id;
        l_new_languages := l_new_languages + 1;
      ELSE
        l_language_id := l_record.id_language;
      END IF;

      --//
      --// Validar que el término no venga nulo, y que la organización exista
      IF length(l_record.term_id) >0 THEN

        IF length(l_record.eccma_eotd_org_term) = 0 THEN
          --// Valida el término de la organización
          INSERT INTO organizations(id,
                                    eccma_eotd,
                                    name,
                                    mail_address,
                                    created_at,
                                    updated_at)
							  VALUES(
								    nextval('organizations_id_seq1'),
								    l_record.term_organization_id,
								    l_record.term_organization_name,
								    NULL,
								    current_timestamp,
								    current_timestamp
								    )
          RETURNING id INTO l_org_id;
          l_new_organizations := l_new_organizations + 1;
        ELSE
          l_org_id := l_record.id_org_term;
        END IF;

        --// Insertando un nuevo término si es que no existe éste eccma term
        INSERT INTO terminologicals (id,
                                     terminology_class,
                                     eccma_eotd,
                                     content,
                                     orginator_reference,
                                     is_deprecated,
                                     language_id,
                                     organization_id,
                                     term_id,
                                     tsv_content,
                                     concept_id,
                                     created_at,
                                     updated_at)
							   VALUES(
								     nextval('terminologicals_id_seq1'),
								     'term',
								     l_record.term_id,
								     l_record.term_content,
								     l_record.term_originator_reference,
								     CAST(l_record.term_is_deprecated AS BOOLEAN),
								     l_record.id_language,
								     l_org_id,
								     NULL, --> term_id no se ocupa para el term
								     to_tsvector('pg_catalog.simple', coalesce(unaccent(l_record.term_content),'')),
								     l_concept_id,
								     current_timestamp,
								     current_timestamp
								    )
        RETURNING id INTO l_term_id;
        l_new_terms := l_new_terms + 1;
      END IF;

      --// ===============================================================================================================
      --// NUEVAS DEFINICIONES
      --//
      --// Validar que la definición organización no venga nulo
      IF length(l_record.definition_id) >0 THEN

        IF length(l_record.eccma_eotd_org_definition) = 0 THEN
          --// Valida la definición de la organización
          INSERT INTO organizations(id,
                                    eccma_eotd,
                                    name,
                                    mail_address,
                                    created_at,
                                    updated_at)
							 VALUES(
								    nextval('organizations_id_seq1'),
								    l_record.definition_organization_id,
								    l_record.definition_organization_name,
								    NULL,
								    current_timestamp,
								    current_timestamp
								    )
          RETURNING id INTO l_org_id;
          l_new_organizations := l_new_organizations + 1;
        ELSE
          l_org_id := l_record.id_org_definition;
        END IF;

        CASE
          WHEN l_record.definition_is_deprecated = '1' THEN l_def_is_deprecated := TRUE;
          WHEN l_record.definition_is_deprecated = '0' THEN l_def_is_deprecated := FALSE;
        ELSE l_def_is_deprecated := FALSE;
        END CASE;

        INSERT INTO terminologicals (id,
                                     terminology_class,
                                     eccma_eotd,
                                     content,
                                     orginator_reference,
                                     is_deprecated,
                                     language_id,
                                     organization_id,
                                     term_id,
                                     tsv_content,
                                     concept_id,
                                     created_at,
                                     updated_at)
							  VALUES(
									 nextval('terminologicals_id_seq1'),
									 'definition',
									 l_record.definition_id,
									 l_record.definition_content,
									 l_record.definition_originator_reference,
									 l_def_is_deprecated,
									 l_record.id_language,
									 l_org_id,
									 NULL, --> No se ocupa term_id para definition
									 to_tsvector('pg_catalog.simple', coalesce(unaccent(l_record.definition_content),'')),
									 l_concept_id,
									 current_timestamp,
									 current_timestamp
									 );
        l_new_defs := l_new_defs + 1;
      END IF;

      --// ===============================================================================================================
      --// NUEVAS ABREVIACIONES
      --//
      --// Validar que la abreviación organización esté creada

      IF length(l_record.abbreviation_id) >0 THEN

        IF length(l_record.eccma_eotd_org_abbreviation) = 0 THEN
          --// Valida la existencia de la organizacion de la abreviacion
          INSERT INTO organizations(id,
                                    eccma_eotd,
                                    name,
                                    mail_address,
                                    created_at,
                                    updated_at)
							 VALUES(
								    nextval('organizations_id_seq1'),
								    l_record.abbreviation_organization_id,
								    l_record.abbreviation_organization_name,
								    NULL,
								    current_timestamp,
								    current_timestamp
								    )
          RETURNING id INTO l_org_id;
          l_new_organizations := l_new_organizations + 1;
        ELSE
          l_org_id := l_record.id_org_abbreviation;
        END IF;

        BEGIN

          CASE
            WHEN l_record.abbreviation_is_deprecated = '1' THEN l_abbr_is_deprecated := TRUE;
            WHEN l_record.abbreviation_is_deprecated = '0' THEN l_abbr_is_deprecated := FALSE;
          ELSE l_abbr_is_deprecated := FALSE;
          END CASE;

          INSERT INTO terminologicals (id,
                                       terminology_class,
                                       eccma_eotd,
                                       content,
                                       orginator_reference,
                                       is_deprecated,
                                       language_id,
                                       organization_id,
                                       term_id,
                                       tsv_content,
                                       concept_id,
                                       created_at,
                                       updated_at)
                                 VALUES(
									   nextval('terminologicals_id_seq1'),
									   'abbreviation',
									   l_record.abbreviation_id,
									   l_record.abbreviation_content,
									   l_record.abbreviation_originator_ref,
									   l_abbr_is_deprecated,
									   l_record.id_language,
									   l_org_id,
									   l_record.id_term,
									   to_tsvector('pg_catalog.simple', coalesce(unaccent(l_record.abbreviation_content),'')),
									   NULL, --> No se ocupa el concept_id para la abreviación
									   current_timestamp,
									   current_timestamp
									   );
          EXCEPTION
          WHEN OTHERS THEN
            GET STACKED DIAGNOSTICS _c = PG_EXCEPTION_CONTEXT;
            raise notice 'context: >>%<<', _c;
        END;
        l_new_abbr := l_new_abbr + 1;
      END IF;
    END IF;
  END LOOP;

  raise notice '================================================================================';
  --// =================================================================================================================
  --// Datos que se van a actualizar

  l_log_text := 'Conceptos actualizados: ' || l_upd_concepts;
  PERFORM xx_fn_log(l_log_text);

  l_log_text := 'Términos actualizados: ' || l_upd_terms;
  PERFORM xx_fn_log(l_log_text);

  l_log_text := 'Definiciones actualizadas: ' || l_upd_defs;
  PERFORM xx_fn_log(l_log_text);

  l_log_text := 'Abreviaciones actualizadas: ' || l_upd_abbr;
  PERFORM xx_fn_log(l_log_text);

  --// Nuevos valores que se insertaron

  --// Obtiene la cifra de nuevos tipos de conceptos
  PERFORM xx_fn_log('Tipos de conceptos agregados: ' || l_new_conceptypes);

  --// Obtiene la cifra de nuevos lenguajes
  PERFORM xx_fn_log('Lenguajes agregados: ' || l_new_languages);

  --// Obtiene la cifra de nuevas organizaciones
  PERFORM xx_fn_log('Organzaciones agregadas: ' || l_new_organizations);

  --// Obtiene la cifra de nuevos conceptos
  PERFORM xx_fn_log('Conceptos agregados: ' || l_new_concepts);

  --// Obtiene la cifra de nuevos términos
  PERFORM xx_fn_log('Términos agregados: ' || l_new_terms);

  --// Obtiene la cifra de nuevas definiciones
  PERFORM xx_fn_log('Definiciones agregadas: ' || l_new_defs);

  --// Obtiene la cifra de nuevas abreviaciones
  PERFORM xx_fn_log('Abreviaciones agregadas: ' || l_new_abbr);

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
