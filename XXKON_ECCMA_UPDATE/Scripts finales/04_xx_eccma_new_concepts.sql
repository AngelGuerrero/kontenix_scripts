/*===============================================================+
PROCEDURE:     04_xx_eccma_new_concepts
DESCRIPTION:   Procedimiento anónimo para agregar todos los registros
			   que no están en la base de Kontenix

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
  l_new_defs INTEGER DEFAULT 0; -- Nuevas definiciones insertadas
  l_new_abbr INTEGER DEFAULT 0; -- Nuevas abreviaciones insertadas


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
  l_new_eccma      INTEGER DEFAULT 0;
  _c text;
BEGIN
  raise notice 'Iniciando proceso de nuevos conceptos. Hora: %', current_timestamp;

  raise notice '================================================================================';

  --// =================================================================================================================
  --// Inicia con la inserción de nuevos conceptos, términos, definiciones y abreviaciones, etc.
  --// Conceptos
  --// Crea una tabla temporal para obtener la diferencia de los conceptos que existen en el servidor remoto, pero no
  --// en la base de datos local.

  --// Se hace una tabla que sólo contenga los nuevos elementos a insertar para no requerir todo el volumen de la tabla temporal.
  DROP TABLE IF EXISTS xx_concept_news;
  DROP TABLE IF EXISTS xx_eccma_new_rows;
  PERFORM xx_fn_log('Tablas xx_concept_news y xx_eccma_new_rows eliminadas: ' || now());

  CREATE TABLE xx_concept_news AS (
    SELECT tmp.*
     FROM tmp_dn tmp
    WHERE NOT EXISTS(SELECT 1 FROM concepts con WHERE con.eccma_eotd = tmp.concept_id));  
  
  PERFORM xx_fn_log('Tabla xx_concept_news creada: ' || now());
  
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
				  FROM xx_concept_news xc
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
						    FROM xx_concept_news xc
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
						    FROM xx_concept_news xc
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
						    FROM xx_concept_news xc
						   where 1=1
						     AND LENGTH(abbreviation_organization_id) >0
						     AND NOT EXISTS (SELECT 1 FROM organizations o where o.eccma_eotd = xc.abbreviation_organization_id )) D; 


   INSERT INTO concept_types(id,
                             eccma_eotd,
							 name,
							 definition,
							 code,
							 created_at,
							 updated_at)
					 SELECT nextval('concept_types_id_seq1'),
						    E.concept_type_id,
						    E.concept_type_name,
						    NULL,
							(SELECT LPAD(CAST(MAX(CAST(code AS INTEGER)) +1 AS VARCHAR), 2, '0') FROM concept_types),
						    current_timestamp,
						    current_timestamp		 
                      FROM (SELECT DISTINCT concept_type_id,
											concept_type_name
							  FROM xx_concept_news xc
							 WHERE 1=1
							   AND LENGTH(concept_type_id) >0
							   AND NOT EXISTS (SELECT 1 FROM concept_types c WHERE c.eccma_eotd = xc.concept_type_id )) E;    

  PERFORM xx_fn_log('Nuevos languages, organizations y concept_types creados: ' || now());							   
  ----------------------------------------------------------------------------------------
  
  CREATE TABLE xx_eccma_new_rows AS (
    SELECT tmp2.*
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
     FROM xx_concept_news tmp2
     LEFT JOIN organizations org1 ON org1.eccma_eotd = tmp2.term_organization_id
     LEFT JOIN organizations org2 ON org2.eccma_eotd = tmp2.definition_organization_id
     LEFT JOIN organizations org3 ON org3.eccma_eotd = tmp2.abbreviation_organization_id
     JOIN languages lan      ON lan.eccma_eotd = tmp2.language_id
     JOIN concept_types ct   ON ct.eccma_eotd = tmp2.concept_type_id);

  PERFORM xx_fn_log('Tabla xx_eccma_new_rows creada para nuevos conceptos: ' || now());
  
  FOR l_record IN (SELECT *
                     FROM xx_eccma_new_rows
                    WHERE LENGTH(concept_type_id) > 0 ) 
  LOOP

    --// ===============================================================================================================

    IF LENGTH(l_record.concept_type_id) > 0 THEN
	--raise notice 'concept_type : %', l_record.concept_type_id;
	
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
							 l_record.id_concept_type
						   )
        RETURNING id INTO l_concept_id; 
        l_new_concepts := l_new_concepts + 1;
      END IF;

      --// ===============================================================================================================
      --// NUEVOS TÉRMINOS
      --// Validación de language
      --raise notice 'concept_id : %', l_concept_id;
	  
	  IF LENGTH(l_record.language_id) >0  THEN 
		  
		  --// Validar que el término no venga nulo, y que la organización exista
		  IF LENGTH(l_record.term_id) >0 AND LENGTH(l_record.term_organization_id) >0 THEN

			CASE
			  WHEN l_record.term_is_deprecated = '1' THEN l_term_is_deprecated := TRUE;
			  WHEN l_record.term_is_deprecated = '0' THEN l_term_is_deprecated := FALSE;
			  ELSE l_term_is_deprecated := FALSE;
			END CASE;
            
			--raise notice 'org_id : %', l_org_id;
			--// Insertando un nuevo término si es que no existe éste eccma term
			IF l_record.id_language > 0 AND  l_record.id_org_term >0 THEN
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
											 l_concept_id,
											 current_timestamp,
											 current_timestamp
											);
				l_new_terms := l_new_terms + 1;
			END IF;
		  END IF;

		  --// ===============================================================================================================
		  --// NUEVAS DEFINICIONES
		  --//
		  --// Validar que la definición organización no venga nulo
		  IF LENGTH(l_record.definition_id) >0 AND LENGTH(l_record.definition_organization_id) >0 THEN

			CASE
			  WHEN l_record.definition_is_deprecated = '1' THEN l_def_is_deprecated := TRUE;
			  WHEN l_record.definition_is_deprecated = '0' THEN l_def_is_deprecated := FALSE;
			ELSE l_def_is_deprecated := FALSE;
			END CASE;
			
            IF l_record.id_language > 0 AND  l_record.id_org_definition >0 THEN
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
										   l_concept_id,
										   current_timestamp,
										   current_timestamp
										   );
				l_new_defs := l_new_defs + 1;
			END IF;
		  END IF;

		  --// ===============================================================================================================
		  --// NUEVAS ABREVIACIONES
		  --//
		  --// Validar que la abreviación organización esté creada

		  IF LENGTH(l_record.abbreviation_id) >0 AND LENGTH(l_record.abbreviation_organization_id) >0 THEN
			
			
			BEGIN
			   SELECT id 
			     INTO STRICT l_term_id
				 FROM terminologicals
				WHERE 1=1
				  AND terminology_class = 'term'
				  AND eccma_eotd = l_record.term_id;
			EXCEPTION
			   WHEN OTHERS THEN
			   l_term_id := 0;
			END;
			
			BEGIN
			  CASE
				WHEN l_record.abbreviation_is_deprecated = '1' THEN l_abbr_is_deprecated := TRUE;
				WHEN l_record.abbreviation_is_deprecated = '0' THEN l_abbr_is_deprecated := FALSE;
			  ELSE l_abbr_is_deprecated := FALSE;
			  END CASE;
              
			  IF l_record.id_language > 0 AND  l_record.id_org_abbreviation >0 THEN
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
											 l_record.id_org_abbreviation,
											 l_term_id,
											 to_tsvector('pg_catalog.simple', coalesce(unaccent(l_record.abbreviation_content),'')),
											 NULL, --> No se ocupa el concept_id para la abreviación
											 current_timestamp,
											 current_timestamp
											 );
			     l_new_abbr := l_new_abbr + 1;								 
			  END IF;							 
		    EXCEPTION
		    WHEN OTHERS THEN
			   GET STACKED DIAGNOSTICS _c = PG_EXCEPTION_CONTEXT;
			   raise notice 'context: >>%<<', _c;
			END;		
		  END IF;-- ABBREVIATION_ID
	  END IF; -- END IF DE VALIDACION DEL LANGUAGE
    END IF; -- END IF DE LA VALIDACION DEL TIPO DE CONCEPTO
  END LOOP;

  raise notice '================================================================================';
  --// =================================================================================================================
  --// Datos que se van a actualizar
  --// Nuevos valores que se insertaron

  --// Obtiene la cifra de nuevos conceptos
  PERFORM xx_fn_log('Conceptos agregados: ' || l_new_concepts);

  --// Obtiene la cifra de nuevos términos
  PERFORM xx_fn_log('Términos agregados: ' || l_new_terms);

  --// Obtiene la cifra de nuevas definiciones
  PERFORM xx_fn_log('Definiciones agregadas: ' || l_new_defs);

  --// Obtiene la cifra de nuevas abreviaciones
  PERFORM xx_fn_log('Abreviaciones agregadas: ' || l_new_abbr);

  PERFORM xx_fn_log('Cuarto ciclo terminado correctamente, hora: ' || now());

  EXCEPTION
     WHEN OTHERS THEN
        GET STACKED DIAGNOSTICS _c = PG_EXCEPTION_CONTEXT;
        RAISE NOTICE 'context: >>%<<', _c;
        raise notice 'Ha ocurrido un error en el script: 04_xx_eccma_new_concepts';
        raise notice 'Error: % %', sqlstate, sqlerrm;
END;
$$
LANGUAGE plpgsql;