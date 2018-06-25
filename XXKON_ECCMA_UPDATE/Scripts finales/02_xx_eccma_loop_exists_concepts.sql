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

  l_term_is_deprecated BOOLEAN DEFAULT FALSE;
  l_abbr_is_deprecated BOOLEAN DEFAULT FALSE;
  l_def_is_deprecated BOOLEAN DEFAULT FALSE;
  l_con_is_deprecated BOOLEAN DEFAULT FALSE;

  -- Variable donde se guardarán los logs del proceso
  l_log_text VARCHAR(200);

  l_approve_contype BOOLEAN DEFAULT FALSE;

  _c text;
  
BEGIN
  raise notice 'Iniciando proceso de actualización. Hora: %', current_timestamp;
  raise notice 'Truncando tabla xx_eccma_update_terms';
  raise notice 'Truncando tabla xx_eccma_update_definition';
  raise notice 'Truncando tabla xx_eccma_update_abbr';

  TRUNCATE TABLE xx_eccma_update_terms;
  TRUNCATE TABLE xx_eccma_update_defs;
  TRUNCATE TABLE xx_eccma_update_abbr;

  
  INSERT INTO languages(id,
						  eccma_eotd,
						  country_code,
						  name,
						  description,
						  code,
						  created_at,
						  updated_at) 
				SELECT nextval('languages_id_seq1'),
					   A.language_id,
					   A.country_code,
					   A.language_name,
					   A.language_name,
					   A.language_code,
					   current_timestamp,
					   current_timestamp
				FROM(SELECT distinct language_id,
					   language_code,
					   country_code,
					   language_name
				  FROM xx_concepts xc
				 where 1=1
				   AND LENGTH(language_id) > 0
				   AND NOT EXISTS (SELECT 1 FROM languages l where l.eccma_eotd = xc.language_id ) )A;    

   INSERT INTO organizations(id,
							 eccma_eotd,
							 name,
							 mail_address,
							 created_at,
							 updated_at) 
					 SELECT nextval('organizations_id_seq1'),
						    B.term_organization_id,
						    B.term_organization_name,
						    NULL,
						    current_timestamp,
						    current_timestamp
					  FROM (SELECT distinct term_organization_id,
							     term_organization_name
						    FROM xx_concepts xc
						   where 1=1
						     AND LENGTH(term_organization_id) >0
						     AND NOT EXISTS (SELECT 1 FROM organizations o where o.eccma_eotd = xc.term_organization_id )) B;

   INSERT INTO organizations(id,
							 eccma_eotd,
							 name,
							 mail_address,
							 created_at,
							 updated_at) 
					 SELECT nextval('organizations_id_seq1'),
						    C.definition_organization_id,
						    C.definition_organization_name,
						    NULL,
						    current_timestamp,
						    current_timestamp
					  FROM (SELECT distinct definition_organization_id,
							     definition_organization_name
						    FROM xx_concepts xc
						   where 1=1
						     AND LENGTH(definition_organization_id) >0
						     AND NOT EXISTS (SELECT 1 FROM organizations o where o.eccma_eotd = xc.definition_organization_id )) C;

   INSERT INTO organizations(id,
							 eccma_eotd,
							 name,
							 mail_address,
							 created_at,
							 updated_at) 
					 SELECT nextval('organizations_id_seq1'),
						    D.abbreviation_organization_id,
						    D.abbreviation_organization_name,
						    NULL,
						    current_timestamp,
						    current_timestamp
					  FROM (SELECT distinct abbreviation_organization_id,
							     abbreviation_organization_name
						    FROM xx_concepts xc
						   where 1=1
						     AND LENGTH(abbreviation_organization_id) >0
						     AND NOT EXISTS (SELECT 1 FROM organizations o where o.eccma_eotd = xc.abbreviation_organization_id )) D;


  -- Crea la tabla de donde se obtendrá la información en base a un concept type id
  PERFORM xx_fn_log('Borrando tabla XX_ECCMA_DATA_FROM_TMP');
  DROP TABLE IF EXISTS XX_ECCMA_DATA_FROM_TMP;

  
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
      FROM xx_concepts tmp
      LEFT JOIN organizations org1 ON org1.eccma_eotd = tmp.term_organization_id
      LEFT JOIN organizations org2 ON org2.eccma_eotd = tmp.definition_organization_id
      LEFT JOIN organizations org3 ON org3.eccma_eotd = tmp.abbreviation_organization_id
      JOIN languages lan      ON lan.eccma_eotd = tmp.language_id
      LEFT JOIN concept_types ct   ON ct.eccma_eotd = tmp.concept_type_id
      LEFT JOIN terminologicals t1 on tmp.term_id = t1.eccma_eotd AND t1.terminology_class = 'term'
      LEFT JOIN terminologicals t2 ON tmp.definition_id = t2.eccma_eotd AND t2.terminology_class = 'definition'
      LEFT JOIN terminologicals t3 ON tmp.abbreviation_id = t3.eccma_eotd AND t3.terminology_class = 'abbreviation';


  PERFORM xx_fn_log('Ciclo de validacion para terminos, definiciones y abreviaciones - conceptos existentes...');
  -- 1) Ciclo para conceptos que si existen
  FOR l_record IN (SELECT * FROM XX_ECCMA_DATA_FROM_TMP) 
  LOOP
  --------------------------------------------------------------------------------------------------------
  --
  --------------------------------------------------------------------------------------------------------
  
  -- b) Validar la organización del término
    --HHH 17 JUN 2018
    -- ESTE IF NO ESTABA, FALTABA AGREGARLO
    IF  length(l_record.term_organization_id) >0 THEN
        --l_org_id := l_record.id_org_term;
      ------------------------------------------------------------------------------------------------------------
      --// TÉRMINOS
      IF length(l_record.term_id) > 0 THEN

          CASE
            WHEN l_record.term_is_deprecated = '1' THEN l_term_is_deprecated := TRUE;
            WHEN l_record.term_is_deprecated = '0' THEN l_term_is_deprecated := FALSE;
          ELSE l_term_is_deprecated := FALSE;
          END CASE;
      
        IF l_record.id_term IS NULL THEN 
        -- Si no se obtiene valores, se crea el nuevo término.

          --raise notice 'Insertando término en terminologicals';
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
									 --CAST(l_record.term_is_deprecated AS BOOLEAN),
									 l_term_is_deprecated,
									 l_record.id_language,
									 l_record.id_org_term,
									 NULL, --> term_id no se ocupa para el term
									 to_tsvector('pg_catalog.simple', coalesce(unaccent(l_record.term_content),'')),
									 l_record.id_concept,
									 current_timestamp,
									 current_timestamp
									 );
          l_new_terms := l_new_terms + 1;
        ELSE -- Actualiza entonces los términos, llena primero la tabla para los registros que serán actualizados
          
          IF l_record.eccma_eotd_is_dep_term <> l_term_is_deprecated THEN
          --IF l_record.eccma_eotd_is_dep_term <> l_record.term_is_deprecated THEN
             INSERT INTO xx_eccma_update_terms VALUES (l_record.id_term,
                                                       l_term_is_deprecated,
                                                       l_record.term_content,
                                                       l_record.term_id
                                                      );
             l_upd_terms := l_upd_terms + 1;
          END IF;
        END IF;
      END IF;
    END IF;

----------------------------------------------------------------------------------------------------------------------------
    -- d) Validar la organización de la definición (Existe o no la organización, si no, se crea, si existe, no se hace nada)
    IF length(l_record.definition_organization_id) >0 THEN
      --l_org_id := l_record.id_org_definition;

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
									 l_record.id_language,
									 l_record.id_org_definition,
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
             l_upd_defs := l_upd_defs + 1;
          END IF;
        END IF;
      END IF;
    END IF; -- Cierre end if de organization_definition

    --F) VALIDAR ORGANIZACIONES DE ABREVIACIONES(EXISTE O NO. SI NO EXISTE SE CREA. SI EXISTE NO SE HACE NADA)

    IF length(l_record.abbreviation_id) >0 THEN
      --l_org_id := l_record.id_org_abbreviation;

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
									  l_record.id_language,
									  l_record.id_org_abbreviation,
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

/*
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
*/
  raise notice '================================================================================';
  --// =================================================================================================================
  --// Datos que se van a actualizar

  l_log_text := 'Términos actualizados: ' || l_upd_terms;
  PERFORM xx_fn_log(l_log_text);

  l_log_text := 'Definiciones actualizadas: ' || l_upd_defs;
  PERFORM xx_fn_log(l_log_text);

  l_log_text := 'Abreviaciones actualizadas: ' || l_upd_abbr;
  PERFORM xx_fn_log(l_log_text);

  --// Nuevos valores que se insertaron

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