--// Funciones necesarias para realizar el procedimiento de actualización del eOTD

CREATE TABLE IF NOT EXISTS xx_eccma_update_log  (date_log  TIMESTAMP,concept_type varchar(100) ,message_log varchar(500));
CREATE TABLE IF NOT EXISTS xx_eccma_update_terms(id integer, is_deprecated boolean, content text, eccma_eotd varchar(20));
CREATE TABLE IF NOT EXISTS xx_eccma_update_defs(id integer,is_deprecated boolean,content text, eccma_eotd varchar(20));
CREATE TABLE IF NOT EXISTS xx_eccma_update_abbr(id integer,content text, eccma_eotd varchar(20));

DROP TABLE IF EXISTS xx_eccma_update_log;
DROP TABLE IF EXISTS xx_eccma_update_terms;
DROP TABLE IF EXISTS xx_eccma_update_defs;
DROP TABLE IF EXISTS xx_eccma_update_abbr;

-----------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION XX_FN_GET_ORGANIZATION(p_organization_id VARCHAR(100))
  RETURNS INTEGER AS $$
DECLARE
   l_id INTEGER;
BEGIN
   SELECT id
     INTO l_id
     FROM organizations
    WHERE 1=1
    AND eccma_eotd = p_organization_id;
   RETURN l_id;
EXCEPTION
  WHEN OTHERS THEN
     RETURN 0;
  END;
$$ LANGUAGE plpgsql;

--------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION XXKON_FN_GET_LANGUAGE(p_language VARCHAR(100))
  RETURNS INTEGER AS $$
DECLARE
   l_id INTEGER;
BEGIN
   SELECT id
     INTO l_id
       FROM languages
      WHERE eccma_eotd = p_language;
   RETURN l_id;
EXCEPTION
  WHEN OTHERS THEN
     RETURN 0;
END;
$$ LANGUAGE plpgsql;
------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION xx_fn_get_terminological_id(p_concept_type        VARCHAR(50),
                                                       p_eccma_id        	   VARCHAR(100),
                                                       p_language_id         INTEGER,
                                                       p_concept_id          INTEGER,
                                                       p_organization_id     INTEGER)
  RETURNS INTEGER AS $$
DECLARE
   l_id INTEGER;
BEGIN
  SELECT id
    INTO l_id
    FROM terminologicals
  WHERE terminology_class = p_concept_type
     AND eccma_eotd   = p_eccma_id
     AND language_id  = p_language_id
     AND concept_id   =  p_concept_id
     AND organization_id = p_organization_id;

   RETURN l_id;
EXCEPTION
  WHEN OTHERS THEN
     RETURN 0;
  END;

  $$ LANGUAGE plpgsql;
------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION xx_fn_log(p_concept_eccma_eotd VARCHAR(50), p_text VARCHAR(200))
  RETURNS VOID AS $$
DECLARE
BEGIN
  raise notice '% %', p_concept_eccma_eotd, p_text;

  INSERT INTO xx_eccma_update_log VALUES (now(), p_concept_eccma_eotd, p_text);
EXCEPTION
  WHEN OTHERS THEN
    raise notice 'Ha ocurrido un error al tratar de registrar el log, contexto: %, %', p_concept_eccma_eotd, p_text;
END;
$$ LANGUAGE plpgsql;
-------------------------------------------------------------------------------------------------------

CREATE OR REPLACE FUNCTION xx_fn_get_elements(p_terminology_class VARCHAR(50))
  RETURNS INTEGER AS $$
DECLARE
  l_count INTEGER DEFAULT 0;
BEGIN

  SELECT COUNT(1)
    INTO l_count
    FROM xx_eccma_new_rows xxnews
   WHERE NOT EXISTS(SELECT *
                      FROM terminologicals t
                     WHERE 1 = 1
                       AND t.terminology_class = p_terminology_class

                       AND CASE p_terminology_class
                             WHEN 'term' THEN
                                t.eccma_eotd = xxnews.term_id
                             WHEN 'definition' THEN
                                t.eccma_eotd = xxnews.definition_id
                             WHEN 'abbreviation' THEN
                                t.eccma_eotd = xxnews.abbreviation_id
                           END
                   );

  RETURN l_count;
EXCEPTION
  WHEN OTHERS THEN
    raise notice 'Ha ocurrido un error en la función: xx_fn_get_elements';
    raise notice 'Error: % %', SQLSTATE, sqlerrm;
    RETURN l_count;
END;
$$ LANGUAGE plpgsql;

------------------------------------------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION xx_fn_get_terminological_id(p_concept_type       VARCHAR(50),
                                                       p_eccma_id        	VARCHAR(100),
                                                       p_language_id         INTEGER,
                                                       p_concept_id          INTEGER,
                                                       p_organization_id     INTEGER)
  RETURNS INTEGER AS $$
DECLARE
  l_id INTEGER;
BEGIN
  SELECT id
  INTO l_id
  FROM terminologicals
  WHERE terminology_class = p_concept_type
        AND eccma_eotd   = p_eccma_id
        AND language_id  = p_language_id
        AND concept_id   =  p_concept_id
        AND organization_id = p_organization_id;

  RETURN l_id;
EXCEPTION
  WHEN OTHERS THEN
    RETURN 0;
END;

$$ LANGUAGE plpgsql;
