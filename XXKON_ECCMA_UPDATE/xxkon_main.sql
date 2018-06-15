--// Procedimiento anónimo que llama a la función xxkon_fn_upate_eotd

DO
$$
DECLARE
BEGIN
  raise notice 'Truncando tabla de log.';
  TRUNCATE TABLE xx_eccma_update_log;
  PERFORM XX_FN_UPDATE_ECCMA_EOTD;
END;
$$


DO
$$
DECLARE
  l_record RECORD;
  l_text VARCHAR(500);
BEGIN
  FOR l_record IN (SELECT tmp.*
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
                       , con.id id_concept
                     FROM tmp1 tmp
                       JOIN concepts con ON con.eccma_eotd = tmp.concept_id
                       LEFT JOIN organizations org1 ON org1.eccma_eotd = tmp.term_organization_id
                       LEFT JOIN organizations org2 ON org2.eccma_eotd = tmp.definition_organization_id
                       LEFT JOIN organizations org3 ON org3.eccma_eotd = tmp.abbreviation_organization_id
                       LEFT JOIN languages lan      ON lan.eccma_eotd = tmp.language_id
                       LEFT JOIN concept_types ct   ON ct.eccma_eotd = tmp.concept_type_id LIMIT 10) LOOP

    l_text := l_record.term_id;

    raise notice '%', l_text;

  end loop;
END;
$$ LANGUAGE plpgsql;


















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
  , con.id id_concept
  , t1.id id_term
  , t1.eccma_eotd eccma_eotd_term
  , t2.id id_definition
  , t2.eccma_eotd eccma_eotd_definition
  , t3.id id_abbreviation
  , t3.eccma_eotd eccma_eotd_abbreviation
FROM tmp1 tmp
  JOIN concepts con ON con.eccma_eotd = tmp.concept_id
  LEFT JOIN organizations org1 ON org1.eccma_eotd = tmp.term_organization_id
  LEFT JOIN organizations org2 ON org2.eccma_eotd = tmp.definition_organization_id
  LEFT JOIN organizations org3 ON org3.eccma_eotd = tmp.abbreviation_organization_id
  LEFT JOIN languages lan      ON lan.eccma_eotd = tmp.language_id
  LEFT JOIN concept_types ct   ON ct.eccma_eotd = tmp.concept_type_id
  LEFT JOIN terminologicals t1 on tmp.term_id = t1.eccma_eotd AND t1.terminology_class = 'term'
  LEFT JOIN terminologicals t2 ON tmp.definition_id = t2.eccma_eotd AND t2.terminology_class = 'definition'
  LEFT JOIN terminologicals t3 ON tmp.abbreviation_id = t3.eccma_eotd AND t3.terminology_class = 'abbreviation'
;


SELECT DISTINCT terminology_class
  FROM terminologicals;

select *
  from terminologicals
 WHERE
   terminology_class = 'abbreviation'
 AND concept_id IS NOT NULL;
