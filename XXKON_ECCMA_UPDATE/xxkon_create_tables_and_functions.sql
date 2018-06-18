--// Funciones necesarias para realizar el procedimiento de actualización del eOTD

--// Tabla para registrar los mensajes del proceso
CREATE TABLE IF NOT EXISTS xx_eccma_update_log (id INTEGER, date_log  TIMESTAMP, message_log varchar(500));
CREATE SEQUENCE log_seq START 1;


--// Tabla para registrar los términos que se deben de actualizar
CREATE TABLE IF NOT EXISTS xx_eccma_update_terms(id integer, is_deprecated boolean, content text, eccma_eotd varchar(20));

--// Tabla para registrar las definiciones que se deben actualizar
CREATE TABLE IF NOT EXISTS xx_eccma_update_defs(id integer,is_deprecated boolean,content text, eccma_eotd varchar(20));

--// Tabla para registar las abreviaciones que se deban actualizar
CREATE TABLE IF NOT EXISTS xx_eccma_update_abbr(id integer,is_deprecated boolean, content text, eccma_eotd varchar(20));

--// Función que se llama desde el proceso principal para registrar los mensajes de log.
CREATE OR REPLACE FUNCTION xx_fn_log(p_text VARCHAR(200))
  RETURNS VOID AS $$
DECLARE
BEGIN
  raise notice '%', p_text;

  INSERT INTO xx_eccma_update_log(id, date_log, message_log) VALUES (nextval('log_seq'), now(), p_text);
EXCEPTION
  WHEN OTHERS THEN
    raise notice 'Ha ocurrido un error al tratar de registrar el log: %', p_text;
END;
$$ LANGUAGE plpgsql;
-------------------------------------------------------------------------------------------------------


/*
DROP TABLE IF EXISTS xx_eccma_update_log;
DROP TABLE IF EXISTS xx_eccma_update_terms;
DROP TABLE IF EXISTS xx_eccma_update_defs;
DROP TABLE IF EXISTS xx_eccma_update_abbr;
*/