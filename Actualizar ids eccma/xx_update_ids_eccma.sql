--JOSE HEBERT HERNANDEZ
--13/JUL/2018

--// Ángel Guerrero
--// 19 de Julio de 2018
DO
$$
DECLARE

  l_record RECORD;

  --// Variables para los contadores
  l_new_org           INTEGER DEFAULT 0;

  l_upd_concept       INTEGER DEFAULT 0;
  l_upd_term          INTEGER DEFAULT 0;
  l_upd_def           INTEGER DEFAULT 0;
  l_upd_abbr          INTEGER DEFAULT 0;

  l_not_found         INTEGER DEFAULT 0;

  --// Variable para manejar los errores
  _c TEXT;

BEGIN

  raise notice 'Inicio proceso...';

  TRUNCATE xxdata_not_found;

  --// Verifica los términos que no están en la base de datos de Kontenix
  INSERT INTO xxdata_not_found(terminology_class, eccma_eotd, content, created_at, updated_at)
    SELECT 'term', eccma_term_id, term_content, current_timestamp, current_timestamp
      FROM (SELECT xeccma.term_content
                 , xeccma.eccma_term_id
               FROM xx_eccma_new_ids xeccma
               WHERE 1 = 1
                 AND xeccma.eccma_term_id IS NOT NULL --// Que no venga vacío el campo del id eccma término
                 AND NOT EXISTS(SELECT term.content
                                FROM terminologicals term
                                WHERE terminology_class = 'term'
                                  AND term.content = xeccma.term_content
                                  AND term.term_id IS NULL
                               )) A;

  --// Verifica las definiciones que no están en la base de datos de Kontenix
  INSERT INTO xxdata_not_found(terminology_class, eccma_eotd, content, created_at, updated_at)
    SELECT 'definition', eccma_definition_id, definition_content, current_timestamp, current_timestamp
      FROM (SELECT xeccma.definition_content
                 , xeccma.eccma_definition_id
               FROM xx_eccma_new_ids xeccma
               WHERE 1 = 1
                 AND xeccma.eccma_definition_id IS NOT NULL --// Que no venga vacío el campo del id eccma definición
                 AND NOT EXISTS(SELECT term.content
                                FROM terminologicals term
                                WHERE terminology_class = 'definition'
                                  AND term.content = xeccma.definition_content
                                  AND term.term_id IS NULL
                                )) A;

  --// Verifica las abreviaciones que no están en la base de datos de Kontenix
  INSERT INTO xxdata_not_found(terminology_class, eccma_eotd, content, created_at, updated_at)
    SELECT 'abbreviation', eccma_abbreviation_id, abbreviation_content, current_timestamp, current_timestamp
      FROM (SELECT xeccma.abbreviation_content
                 , xeccma.eccma_abbreviation_id
               FROM xx_eccma_new_ids xeccma
               WHERE 1 = 1
                 AND xeccma.eccma_abbreviation_id IS NOT NULL --// Que no venga vacío el campo id eccma abbreviation
                 AND NOT EXISTS(SELECT term.content
                                FROM terminologicals term
                                WHERE terminology_class = 'abbreviation'
                                  AND term.content = xeccma.abbreviation_content
                                  AND term.term_id IS NOT NULL
                                )) A;

  --1
  --INSERTAS LAS ORGANIZACIONES
  WITH insert_org AS (
    INSERT INTO organizations (id,
                               eccma_eotd,
                               name,
                               mail_address,
                               created_at,
                               updated_at)
      SELECT
        nextval('organizations_id_seq1'),
        eccma_organization_id,
        organization_name,
        organization_mail_address,
        current_timestamp,
        current_timestamp
      FROM (
             SELECT DISTINCT
               x.eccma_organization_id,
               x.organization_name,
               x.organization_mail_address
             FROM xx_eccma_new_ids x
             WHERE 1 = 1
               AND x.eccma_organization_id IS NOT NULL
               AND x.organization_name IS NOT NULL
               AND NOT EXISTS(SELECT eccma_eotd
                              FROM organizations org
                              WHERE 1 = 1
                                AND x.eccma_organization_id = org.eccma_eotd
                                AND x.organization_name = org.name)
           ) A
    RETURNING id
  ) SELECT COUNT(1) INTO l_new_org FROM insert_org;


  --2
  --ACTUALIZAR ID ECCMA PARA TERMINOS
  WITH new_eccma AS (
      SELECT DISTINCT t.id,
             x.eccma_concept_id      new_concept_id_eccma,
             x.eccma_term_id         new_term_id_eccma,
             x.eccma_organization_id new_organization_id_eccma,
             --------------------------------------------------
             t.content contenido_actual,
             --------------------------------------------------
             t.eccma_eotd      actual_term_id_eccma,
             t.concept_id      concept_id_terminology,
             t.language_id     language_id_terminology,
             t.organization_id actual_id_organization,
             --------------------------------------------------
             l.eccma_eotd language_eotd,
             --------------------------------------------------
             o.id new_id_organization
        FROM xx_eccma_new_ids x
           , terminologicals t
           , languages l
           , organizations o
       WHERE 1 = 1
         AND x.term_content = t.content
         AND l.id = t.language_id
         AND o.eccma_eotd = x.eccma_organization_id
         AND t.terminology_class = 'term'
  ),
      update_terminologicals AS (
      UPDATE terminologicals
         SET eccma_eotd = new_eccma.new_term_id_eccma ,
             organization_id = new_eccma.new_id_organization,
             is_deprecated = FALSE,
             updated_at = current_timestamp
        FROM new_eccma
       WHERE terminologicals.id = new_eccma.id
         AND terminologicals.terminology_class = 'term'
         AND terminologicals.term_id IS NULL --// Los términos no poseen un term_id
      RETURNING  new_eccma.id
    )
  SELECT COUNT(1)
  INTO l_upd_term
  FROM update_terminologicals;


  --3
  --ACTUALIZAR ID ECCMA PARA DEFINICIONES
  WITH new_eccma2 AS (
      SELECT DISTINCT t.id,
             x.eccma_concept_id      new_concept_id_eccma,
             x.eccma_definition_id   new_definition_id_eccma,
             x.eccma_organization_id new_organization_id_eccma,
             x.definition_content    definition_content,
             --------------------------------------------------
             t.eccma_eotd      actual_definition_id_eccma,
             t.concept_id      concept_id_terminology,  --// integer
             t.language_id     language_id_terminology, --// integer
             t.organization_id actual_id_organization,  --// integer
             --------------------------------------------------
             l.id         id_language,
             l.eccma_eotd language_eotd,
             --------------------------------------------------
             o.id new_id_organization --// integer
        FROM xx_eccma_new_ids x
           , terminologicals t
           , languages l
           , organizations o
       WHERE 1 = 1
         AND t.terminology_class = 'definition'
         AND x.definition_content = t.content
         AND l.id = t.language_id
         AND o.eccma_eotd = x.eccma_organization_id
  ),
      update_terminologicals AS (
      UPDATE terminologicals
         SET eccma_eotd = new_eccma2.new_definition_id_eccma ,
             organization_id = new_eccma2.new_id_organization,
             is_deprecated = FALSE,
             updated_at = current_timestamp
        FROM new_eccma2
       WHERE terminologicals.id = new_eccma2.id
         AND terminologicals.terminology_class = 'definition'
         AND terminologicals.term_id IS NULL --// Las definiciones no poseen un term_id
         AND terminologicals.language_id = new_eccma2.id_language
      RETURNING new_eccma2.id
    )
  SELECT COUNT(1)
  INTO l_upd_def
  FROM update_terminologicals;


  --4
  --ACTUALIZAR ID ECCMA PARA ABREVIACIONES
  WITH new_eccma3 AS (
    SELECT DISTINCT t.id,
           x.eccma_concept_id      new_concept_id_eccma,
           x.eccma_abbreviation_id new_abbr_id_eccma,
           x.eccma_organization_id new_organization_id_eccma,
           x.abbreviation_content,
           --------------------------------------------------
           t.content,
           t.eccma_eotd      actual_abbr_id_eccma,
           t.concept_id      concept_id_terminology,  --// integer
           t.language_id     language_id_terminology, --// integer
           t.organization_id actual_id_organization,  --// integer
           --------------------------------------------------
           l.id         id_language,   --// integer
           l.eccma_eotd language_eotd,
           --------------------------------------------------
           o.id new_id_organization
      FROM xx_eccma_new_ids x
         , terminologicals t
         , languages l
         , organizations o
     WHERE 1 = 1
       AND t.terminology_class = 'abbreviation'
       AND x.abbreviation_content = t.content
       AND l.id = t.language_id
       AND o.eccma_eotd = x.eccma_organization_id
       AND t.term_id IS NOT NULL
  ),
      update_terminologicals AS (
      UPDATE terminologicals
      SET eccma_eotd = new_eccma3.new_abbr_id_eccma ,
          organization_id = new_eccma3.new_id_organization,
          is_deprecated = FALSE,
          updated_at = current_timestamp
      FROM new_eccma3
      WHERE terminologicals.id = new_eccma3.id
        AND terminologicals.terminology_class = 'abbreviation'
        AND terminologicals.term_id IS NOT NULL --// Las abreviaciones están asociadas a un término
        AND terminologicals.language_id = new_eccma3.id_language
      RETURNING new_eccma3.id
    )
  SELECT COUNT(1)
  INTO l_upd_abbr
  FROM update_terminologicals;

  --5
  --ACTUALIZAR ID ECCMA PARA CONCEPTOS
  --// Actualiza únicamente para los que no tienen un ECCMA ID
  WITH new_eccma4 AS (
      SELECT DISTINCT c.eccma_eotd actual_concept_id_eccma,
                      x.eccma_concept_id new_concept_id_eccma,
                      t.eccma_eotd actual_term_id_eccma,
                      x.eccma_term_id new_term_id_eccma,
                      t.concept_id concept_id_terminology
      FROM xx_eccma_new_ids x
        ,terminologicals t
        ,concepts c
      WHERE 1=1
        AND x.eccma_term_id = t.eccma_eotd
        AND c.id = t.concept_id
        AND (t.is_deprecated IS NULL OR NOT t.is_deprecated )
        AND c.eccma_eotd IS NULL --// Actualiza únicamente los registros que no tienen id ECCMA
  ),
      update_concepts AS (
      UPDATE concepts
      SET eccma_eotd = new_eccma4.new_concept_id_eccma,
        is_deprecated = FALSE,
        updated_at = current_timestamp
      FROM new_eccma4
      WHERE concepts.id = new_eccma4.concept_id_terminology
            AND concepts.eccma_eotd IS NULL
      RETURNING  new_eccma4.concept_id_terminology
    )
  SELECT COUNT(1)
  INTO l_upd_concept
  FROM update_concepts;

  BEGIN
    SELECT COUNT(1)
      INTO STRICT l_not_found
      FROM xxdata_not_found;
  EXCEPTION
    WHEN OTHERS THEN
      l_not_found := 0;
  END;

  IF (l_not_found > 0) THEN
    raise notice 'Los siguientes datos no coinciden con los que existen en la base de datos: ';
    FOR l_record IN (SELECT * FROM xxdata_not_found) LOOP
      raise notice 'Tipo: %, contenido: %, eccma_eotd: %', l_record.terminology_class, l_record.content, l_record.eccma_eotd;
    end loop;
  END IF;

  raise notice '================================================================================';
  raise notice 'Total de organizaciones agregadas: %', l_new_org;
  raise notice 'Total de Terminos actualizados: %', l_upd_term;
  raise notice 'Total de definiciones actualizadas: %', l_upd_def;
  raise notice 'Total de abreviaciones actualizadas: %', l_upd_abbr;
  raise notice 'Total de Conceptos actualizados: %', l_upd_concept;
  raise notice 'Total datos no encontrados: %', l_not_found;
  raise notice '================================================================================';
  raise notice 'Fin proceso';

  EXCEPTION
  WHEN OTHERS THEN
    GET STACKED DIAGNOSTICS _c = PG_EXCEPTION_CONTEXT;
    RAISE NOTICE 'context: >>%<<', _c;
    raise notice 'Ha ocurrido un error...';
    raise notice 'Error: % %', sqlstate, sqlerrm;
END;
$$
LANGUAGE plpgsql;
