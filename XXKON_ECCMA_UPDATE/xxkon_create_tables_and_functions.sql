--// Funciones necesarias para realizar el procedimiento de actualización del eOTD

CREATE TABLE IF NOT EXISTS xx_eccma_update_log (id INTEGER, date_log  TIMESTAMP, message_log varchar(500));

CREATE TABLE IF NOT EXISTS xx_eccma_update_terms(id integer, is_deprecated boolean, content text, eccma_eotd varchar(20));
CREATE TABLE IF NOT EXISTS xx_eccma_update_defs(id integer,is_deprecated boolean,content text, eccma_eotd varchar(20));
CREATE TABLE IF NOT EXISTS xx_eccma_update_abbr(id integer,is_deprecated boolean, content text, eccma_eotd varchar(20));

DROP TABLE IF EXISTS xx_eccma_update_log;
DROP TABLE IF EXISTS xx_eccma_update_terms;
DROP TABLE IF EXISTS xx_eccma_update_defs;
DROP TABLE IF EXISTS xx_eccma_update_abbr;


CREATE SEQUENCE log_seq START 1;

CREATE OR REPLACE FUNCTION xx_fn_log(p_text VARCHAR(200))
  RETURNS VOID AS $$
DECLARE
BEGIN
  raise notice '%', p_text;

  INSERT INTO xx_eccma_update_log VALUES (nextval('log_seq'), now(), p_text);
EXCEPTION
  WHEN OTHERS THEN
    raise notice 'Ha ocurrido un error al tratar de registrar el log, contexto: %, %', p_concept_eccma_eotd, p_text;
END;
$$ LANGUAGE plpgsql;
-------------------------------------------------------------------------------------------------------
DROP FUNCTION xx_fn_log(p_concept_eccma_eotd VARCHAR(50), p_text VARCHAR(200));


SELECT xx_fn_log('test');

SELECT * FROM xx_eccma_update_log;
