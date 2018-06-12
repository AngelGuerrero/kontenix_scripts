select *
  -- from dictionaries
  -- from organizations
  from concepts
  -- from languages
  -- from terminologicals
  where 1 = 1
 -- where terminology_class = 'definition'
   and updated_at > (current_date- 1)
   -- and created_at > (current_date - 1)
      ;

select distinct terminology_class
  from terminologicals;

select COUNT(1)
  -- from dictionaries
  -- from terminologicals
  from concepts
  where updated_at >= (current_date - 1)
 --limit 1000
     ;

select count(1)
  from tmp1;

SELECT COUNT(1)
  FROM (SELECT DISTINCT *
          FROM tmp1
       ) AS A
     ;

SELECT *
  FROM tmp1 LEFT JOIN terminologicals
    ON eccma_eotd = tmp1.term_id
 WHERE 1 = 1
   AND tmp1.concept_type_id = '0161-1#CT-00#1'
   AND updated_at > (current_date - 1)
 limit 1000
     ;

SELECT *
  FROM tmp1 Tempo
  LEFT OUTER JOIN concepts Concept
    ON Concept.eccma_eotd = Tempo.concept_type_id
 WHERE 1 = 1
   AND Tempo.concept_type_id = '0161-1#CT-01#1'
 limit 100
     ;

SELECT DISTINCT concept_type_name
  FROM tmp1;

SELECT *
  FROM tmp1 T
 WHERE 1 = 1
   --AND concept_type_name = 'Other'
   AND NOT EXISTS(
                  SELECT 1
                    FROM concepts C
                   WHERE C.eccma_eotd = T.concept_id
                 );

SELECT *
FROM tmp1 T1
WHERE 1 = 1
      AND concept_type_name = 'Class'
      AND NOT EXISTS(
    SELECT 1
    FROM terminologicals T2
    WHERE T2.eccma_eotd = T1.term_id
);

SELECT COUNT(1)
  FROM tmp1;

SELECT *
FROM tmp1 T
WHERE 1 = 1
      AND concept_type_name = 'Class'
limit 100;

SELECT *
  FROM concepts
 WHERE 1 = 1
   AND eccma_eotd = '0161-1#01-1174900#1';

SELECT current_time;

SELECT COUNT(1)
  FROM concepts;

SELECT COUNT(1)
  FROM  tmp1;

COPY (SELECT tmp1.*
      FROM tmp1
        LEFT JOIN terminologicals
          ON eccma_eotd = tmp1.term_id
      WHERE tmp1.concept_type_id = '0161-1#CT-00#1'
)
TO '/home/angel/Escritorio/tmp_others.csv' With CSV;

COPY (SELECT tmp1.*
      FROM tmp1
        LEFT JOIN terminologicals
          ON eccma_eotd = tmp1.term_id
      WHERE tmp1.concept_type_id = '0161-1#CT-01#1'
)
TO '/home/angel/Escritorio/tmp_class.csv' With CSV;


drop table temporal;

create table tmp1
(
  term_id varchar(100) not null
    constraint tmp1_pkey
    primary key,
  concept_id varchar(100),
  language_id varchar(100),
  language_code varchar(100),
  country_code varchar(100),
  language_name varchar(100),
  term_content text,
  term_originator_reference text,
  term_document_id text,
  term_url text,
  term_description text,
  term_organization_id text,
  term_organization_name text,
  term_is_deprecated boolean,
  concept_type_id text,
  concept_type_name text,
  concept_is_deprecated text,
  definition_id text,
  definition_content text,
  definition_under_development text,
  definition_originator_reference text,
  definition_document_id text,
  definition_url text,
  definition_description text,
  definition_organization_id text,
  definition_organization_name text,
  definition_is_deprecated text,
  definition_is_default text,
  label_id text,
  label_content text,
  label_originator_reference text,
  label_document_id text,
  label_url text,
  label_description text,
  label_organization_id text,
  label_organization_name text,
  label_is_deprecated text,
  abbreviation_id text,
  abbreviation_content text,
  abbreviation_originator_ref text,
  abbreviation_document_id text,
  abbreviation_url text,
  abbreviation_description text,
  abbreviation_organization_id text,
  abbreviation_organization_name text,
  abbreviation_is_deprecated text,
  plural_id text,
  plural_singular_term_item_id text,
  plural_content text,
  plural_originator_reference text,
  plural_document_id text,
  plural_url text,
  plural_description text,
  plural_organization_id text,
  plural_organization_name text,
  plural_is_deprecated text,
  nain text
);

COPY tmp1 from '/home/angel/Documentos/Kontenix/historial/concept_dn_dic.csv' delimiter ',' csv header;

SELECT count(1)
  FROM tmp1;


select count(1)
  from concepts join terminologicals t on concepts.id = t.concept_id
 where 1 = 1
 --and concepts.updated_at > (current_date- 1)
   -- and concepts.id = 321491
   and concepts.concept_type_id = 2
   -- and concepts.is_deprecated is true
   and t.terminology_class = 'definition'
   --and t.is_deprecated is false
;

select count(1)
  from concepts
 where concept_type_id = 2;

select *
  from concept_types;


SELECT tmp1.term_id
     , tmp1.term_is_deprecated
  FROM tmp1  LEFT JOIN terminologicals
    on terminologicals.eccma_eotd = tmp1.term_id
 WHERE tmp1.concept_type_id = '0161-1#CT-00#1'
   AND tmp1.term_is_deprecated <> terminologicals.is_deprecated
     ;

SELECT tempo.definition_id
     , temp.definition_is_deprecated
  FROM tmp1 tempo LEFT JOIN terminologicals ter ON ter.eccma_eotd = temp.term_id
 WHERE tempo.concept_type_id = '0161-1#CT-02#1'
   AND ter.definition_is_deprecated <> (CASE WHEN ter.is_deprecated = 't' THEN '1' ELSE '0' END)
   AND tempo.definition_id IS NOT NULL
   AND tempo.definition_id <> ''
      ;

select *
  from terminologicals
 where 1 = 1
   and definition_id = '0161-1#DF-419589#1'
     ;

select count(1)
  from tmp1;


select *
  from terminologicals
 where 1 = 1
   and terminology_class = 'term'
   and eccma_eotd IS NOT NULL
   --and concept_id IS NOT NULL
   --and orginator_reference IS NOT NULL
   --and orginator_reference <> ''
 limit 100
;

select count(1)
  from organizations;

select count(1)
  from concepts;

select count(1)
  from dictionaries;

select count(1)
  from languages;

-------------------------------------------------------------------

TRUNCATE xx_eccma_others;

SELECT COUNT(1)
  FROM xx_eccma_others;

SELECT COUNT(1) --*
  FROM terminologicals
 WHERE 1 = 1
   AND updated_at > (current_date - 1)
     ;

SELECT *
  FROM concepts
 WHERE id = 7585;

SELECT *
  FROM terminologicals
 WHERE concept_id = 7585
   AND terminology_class = 'term'
   AND content LIKE '%CABALLO%';



select abbreviation_id
  from tmp1
 WHERE 1 = 1
   AND abbreviation_id IS NOT NULL
 limit 100;



DO
$$
DECLARE
  l_record RECORD;
BEGIN
  FOR l_record IN (SELECT * FROM tmp1 limit 100)
  LOOP
    raise notice '%', l_record.abbreviation_id;
  END LOOP;
END;
$$
language plpgsql;

DO
$$
DECLARE
  l_term_id_seq INTEGER;
BEGIN
  SELECT currval(last_value)
    INTO l_term_id_seq
    FROM terminologicals_id_seq;

  raise notice '%', l_term_id_seq;
END;
$$
LANGUAGE plpgsql;

SELECT currval(pg_get_serial_sequence('terminologicals', 'id'));

SELECT NEXTVAL('terminologicals_id_seq1');

SELECT COUNT(1) --*
  FROM terminologicals
 LIMIT 10;

SELECT *
  FROM xx_eccma_update_log;

select now();

DO
$$
BEGIN
  FOR i IN 1..100 LOOP
    raise notice '%', now();
  END LOOP;
END;
$$ LANGUAGE plpgsql;

SELECT *
  FROM xx_eccma_update_log;

SELECT COUNT(1)
  FROM
    xx_eccma_update_defs
    -- xx_eccma_update_terms
    -- xx_eccma_update_abbr
     ;

SELECT COUNT(1)
FROM xx_eccma_update_abbr
;

SELECT *
  FROM terminologicals
 WHERE 1 = 1
   AND updated_at > (current_date - 1)
   AND terminology_class = 'term';


SELECT DISTINCT terminology_class
 FROM terminologicals;




SELECT COUNT(1)
  FROM (SELECT DISTINCT id
          FROM xx_eccma_update_defs) A
;

SELECT id, COUNT(1)
  FROM xx_eccma_update_defs
 GROUP BY id
 HAVING COUNT(1) > 1;

SELECT *
  FROM xx_eccma_update_defs;

COPY (SELECT Tempo.*
  FROM terminologicals T,
       (SELECT id, COUNT(1)
          FROM xx_eccma_update_defs
         GROUP BY id
        HAVING COUNT(1) > 1) U,
       tmp1 Tempo
 WHERE T.terminology_class = 'definition'
  AND T.id = U.id
  AND T.eccma_eotd = Tempo.definition_id
 order by 2) TO '/home/angel/Escritorio/duplicated_definitions.csv' DELIMITER ',' CSV HEADER;

-- Consulta para saber los conceptos que no están en la base de datos
COPY (SELECT tmp.*
      FROM tmp1 tmp
      WHERE 1 = 1
            AND NOT EXISTS(
          SELECT *
          FROM concepts con
          WHERE con.eccma_eotd = tmp.concept_id
      )
) TO '/home/angel/Gitlab/scripts_kontenix/resultados/conceptos_no_existentes_en_kontenix.csv' DELIMITER ',' CSV HEADER;

SELECT tmp.*
 FROM tmp1 tmp
 WHERE 1 = 1
       AND NOT EXISTS(
     SELECT *
     FROM concepts con
     WHERE con.eccma_eotd = tmp.concept_id);

SELECT t.*
  FROM terminologicals t
 WHERE 1 = 1
   AND terminology_class = 'definition'
   AND eccma_eotd = '0161-1#DF-2929118#1'
 LIMIT 100
;


SELECT eccma_eotd, COUNT(1)
  FROM terminologicals
 WHERE 1 = 1
   AND terminology_class = 'term'
   AND eccma_eotd IS NOT NULL
 GROUP BY eccma_eotd
HAVING COUNT(1) > 1
     ;

SELECT SUM(2)
  FROM (SELECT eccma_eotd, COUNT(1)
        FROM terminologicals
        WHERE 1 = 1
              AND terminology_class = 'term'
              AND eccma_eotd IS NOT NULL
        GROUP BY eccma_eotd
        HAVING COUNT(1) > 1) A;

SELECT COUNT(1)
  FROM terminologicals
 WHERE terminology_class = 'term';
-- 2939415

SELECT  T.id
      , T.eccma_eotd
      , T.is_deprecated
      , T.content
      , T.concept_id
      , T.language_id
      , T.created_at
      , T.organization_id
      , C.eccma_eotd Concept_id_eccma
      , CT.name concept_type_name
    FROM terminologicals T, concepts C, concept_types CT
    WHERE 1 = 1
      AND terminology_class = 'term'
      -- AND T.eccma_eotd = '0161-1#TM-2504729#1'
      -- AND T.eccma_eotd = '0161-1#TM-2707007#1'
      AND C.id = T.concept_id
      AND C.concept_type_id = CT.id
      --AND NOT EXISTS(SELECT concept_id FROM tmp1)
    ;


SELECT T.id
     , T.eccma_eotd
     , T.is_deprecated
     , T.content
     , T.concept_id
     , T.language_id
     , T.created_at
     , T.organization_id
     , C.eccma_eotd Concept_id_eccma
     , CT.name concept_type_name
  FROM terminologicals T, concepts C, concept_types CT
 WHERE 1 = 1
   -- AND T.eccma_eotd = '0161-1#TM-2504729#1'
   AND T.eccma_eotd = '0161-1#TM-2707007#1'
   AND C.id = T.concept_id
   AND C.concept_type_id = CT.id
      ;

-- 0161-1#01-1101876#1  Concepto no existe en la base de datos de ECCMA, pero sí en la de KONTENIX
-- 0161-1#01-043047#1

SELECT *
  FROM tmp1
 WHERE term_id = '0161-1#TM-2504729#1';

SELECT *
  FROM terminologicals
 WHERE terminology_class = 'abbreviation'
   AND eccma_eotd = '0161-1#AB-011963#1';



-- Cantidad de términos que hay en la base de datos Kontenix
SELECT count(1)
  FROM terminologicals
 WHERE terminology_class = 'abbreviation';


SELECT COUNT(1)
  FROM (SELECT DISTINCT eccma_eotd from organizations) O
 ORDER BY 1;


SELECT con.eccma_eotd, COUNT(1) Repetidos
  FROM (SELECT eccma_eotd FROM concepts) con
 WHERE 1 = 1
  GROUP BY eccma_eotd
 HAVING COUNT(1) > 1;

SELECT *
  FROM concepts
 WHERE eccma_eotd = '0161-1#00-001115#1';

SELECT *
  FROM concepts
 WHERE concept_type_id NOT IN (1, 2, 3, 4, 5, 6, 7, 8, 9);

SELECT C.*
  FROM terminologicals T, concepts C
 WHERE 1 = 1
   AND terminology_class = 'term'
   AND T.eccma_eotd = C.eccma_eotd
 LIMIT 100
     ;

SELECT *
  FROM concepts
 WHERE eccma_eotd IS NULL;


SELECT COUNT(1)
  FROM concepts;

select *
  from xx_eccma_update_defs
 WHERE 1 = 1
   AND ID = 4133725;
  -- AND ID = 3941010;

SELECT *
  FROM tmp1
 WHERE definition_id = '0161-1#DF-2041699#1';

SELECT *
FROM tmp1
WHERE concept_id = '0161-1#00-014198#1'
  AND definition_id = '0161-1#DF-2041579#1';


SELECT *
  FROM terminologicals
 limit 10;

select *
  from abbreviation_dictionaries;

SELECT COUNT(1)
  FROM terminologicals
 WHERE 1 = 1
   AND updated_at > (current_date - 1)
   --AND terminology_class = 'term'
   AND terminology_class = 'definition'
    --AND id = 3034667
      ;

WITH updated_rows AS (
  UPDATE terminologicals
     SET is_deprecated = FALSE
   WHERE id = 3034667
  RETURNING updated_at
)
SELECT updated_at
  FROM updated_rows;


UPDATE terminologicals
   SET is_deprecated = FALSE
 WHERE id = 3034667;

SELECT *
  FROM xx_eccma_update_defs;



SELECT *
  FROM terminologicals
 WHERE 1 = 1
   AND created_at > (current_date - 1);



