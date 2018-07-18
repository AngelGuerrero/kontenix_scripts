--JOSE HEBERT HERNANDEZ
--13/JUL/2018
DO
$$
DECLARE
  l_record            RECORD;
  l_org_id            INTEGER DEFAULT 0;
  l_concept_id        INTEGER DEFAULT 0;
  l_con_eccma_aux     VARCHAR(20) DEFAULT '';


  --// Variables para los contadores
  l_new_org           INTEGER DEFAULT 0;

  l_upd_concept       INTEGER DEFAULT 0;
  l_upd_term          INTEGER DEFAULT 0;
  l_upd_def           INTEGER DEFAULT 0;
  l_upd_abbr          INTEGER DEFAULT 0;


  --// Variable para manejar los errores
  _c TEXT;

BEGIN

  raise notice 'Inicio proceso...';
  --1
  --INSERTAS LAS ORGANIZACIONES

  INSERT INTO organizations(id,
                            eccma_eotd,
                            name,
                            mail_address,
                            created_at,
                            updated_at)
    SELECT nextval('organizations_id_seq1'),
      eccma_organization_id,
      organization_name,
      organization_mail_address,
      current_timestamp,
      current_timestamp
    FROM (
           SELECT DISTINCT x.eccma_organization_id
             , x.organization_name
             , x.organization_mail_address
           FROM xx_eccma_new_ids x
           WHERE NOT EXISTS (SELECT eccma_eotd
                             FROM organizations org
                             WHERE x.eccma_organization_id = org.eccma_eotd)
         ) A;


  --2
  --ACTUALIZAR ID ECCMA PARA TERMINOS
  WITH new_eccma AS (
      SELECT DISTINCT t.id,
        x.eccma_concept_id new_concept_id_eccma,
        t.eccma_eotd actual_term_id_eccma,
        x.eccma_term_id new_term_id_eccma,
        t.concept_id concept_id_terminology,
        t.language_id language_id_terminology,
        l.eccma_eotd language_eotd,
        t.organization_id actual_id_organization,
        x.eccma_organization_id new_organization_id_eccma,
        o.id new_id_organization
      FROM xx_eccma_new_ids x
        ,terminologicals t
        ,languages l
        ,organizations o
      WHERE 1=1
            AND x.term_content = t.content
            --and x.eccma_language_id = l.eccma_eotd
            AND l.id = t.language_id
            AND o.eccma_eotd = x.eccma_organization_id
            AND (t.is_deprecated IS NULL OR NOT t.is_deprecated )
            AND t.eccma_eotd IS NULL
  ),
      update_terminologicals AS (
      UPDATE terminologicals
      SET eccma_eotd = new_eccma.new_term_id_eccma ,
          organization_id = new_eccma.new_id_organization,
          is_deprecated = FALSE,
          updated_at = current_timestamp
      FROM new_eccma
      WHERE terminologicals.id = new_eccma.id
      RETURNING  new_eccma.id
    )
  SELECT COUNT(1)
  INTO l_upd_term
  FROM update_terminologicals;

  --3
  --ACTUALIZAR ID ECCMA PARA DEFINICIONES
  WITH new_eccma2 AS (
      SELECT DISTINCT t.id,
        x.eccma_concept_id new_concept_id_eccma,
        t.eccma_eotd actual_definition_id_eccma,
        x.eccma_definition_id new_definition_id_eccma,
        t.concept_id concept_id_terminology,
        t.language_id language_id_terminology,
        l.eccma_eotd language_eotd,
        t.organization_id actual_id_organization,
        x.eccma_organization_id new_organization_id_eccma,
        o.id new_id_organization
      FROM xx_eccma_new_ids x
        ,terminologicals t
        ,languages l
        ,organizations o
      WHERE 1=1
            AND x.definition_content = t.content
            --and x.eccma_language_id = l.eccma_eotd
            AND l.id = t.language_id
            AND o.eccma_eotd = x.eccma_organization_id
            AND (t.is_deprecated IS NULL OR NOT t.is_deprecated )
            AND t.eccma_eotd IS NULL
  ),
      update_terminologicals AS (
      UPDATE terminologicals
      SET eccma_eotd = new_eccma2.new_definition_id_eccma ,
        organization_id = new_eccma2.new_id_organization,
        is_deprecated = FALSE,
        updated_at = current_timestamp
      FROM new_eccma2
      WHERE terminologicals.id = new_eccma2.id
      RETURNING  new_eccma2.id
    )
  SELECT COUNT(1)
  INTO l_upd_def
  FROM update_terminologicals;


  --4
  --ACTUALIZAR ID ECCMA PARA ABREVIACIONES
  WITH new_eccma3 AS (
      SELECT DISTINCT t.id,
        x.eccma_concept_id new_concept_id_eccma,
        t.eccma_eotd actual_abbr_id_eccma,
        x.eccma_abbreviation_id new_abbr_id_eccma,
        t.concept_id concept_id_terminology,
        t.language_id language_id_terminology,
        l.eccma_eotd language_eotd,
        t.organization_id actual_id_organization,
        x.eccma_organization_id new_organization_id_eccma,
        o.id new_id_organization
      FROM xx_eccma_new_ids x
        ,terminologicals t
        ,languages l
        ,organizations o
      WHERE 1=1
            AND x.abbreviation_content = t.content
            AND l.id = t.language_id
            AND o.eccma_eotd = x.eccma_organization_id
            AND (t.is_deprecated IS NULL OR NOT t.is_deprecated )
            AND t.eccma_eotd IS NULL
  ),
      update_terminologicals AS (
      UPDATE terminologicals
      SET eccma_eotd = new_eccma3.new_abbr_id_eccma ,
        organization_id = new_eccma3.new_id_organization,
        is_deprecated = FALSE,
        updated_at = current_timestamp
      FROM new_eccma3
      WHERE terminologicals.id = new_eccma3.id
      RETURNING  new_eccma3.id
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

  raise notice 'Fin proceso';
  raise notice 'Total de Terminos actualizados: %', l_upd_term;
  raise notice 'Total de definiciones actualizadas: %', l_upd_def;
  raise notice 'Total de abreviaciones actualizadas: %', l_upd_abbr;
  raise notice 'Total de Conceptos actualizados: %', l_upd_concept;
  raise notice '================================================================================';

  EXCEPTION
  WHEN OTHERS THEN
    GET STACKED DIAGNOSTICS _c = PG_EXCEPTION_CONTEXT;
    RAISE NOTICE 'context: >>%<<', _c;
    raise notice 'Ha ocurrido un error...';
    raise notice 'Error: % %', sqlstate, sqlerrm;
END;
$$
LANGUAGE plpgsql;