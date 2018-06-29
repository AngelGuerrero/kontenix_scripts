--=============================================================================================================
-- FUNCIONES Y CREACIONES DE OBJETOS QUE SE DEBEN DE REALIZAR UNA ÚNICA VEZ

--// Tablas y funciones necesarias para realizar el procedimiento de actualización del eOTD

CREATE TABLE IF NOT EXISTS xx_eccma_update_log (id INTEGER, date_log  TIMESTAMP, message_log varchar(500));
CREATE TABLE IF NOT EXISTS xx_eccma_update_terms(id integer, is_deprecated boolean, content text, eccma_eotd varchar(20));
CREATE TABLE IF NOT EXISTS xx_eccma_update_defs(id integer,is_deprecated boolean,content text, eccma_eotd varchar(20));
CREATE TABLE IF NOT EXISTS xx_eccma_update_abbr(id integer,is_deprecated boolean, content text, eccma_eotd varchar(20));

CREATE INDEX xx_eccma_update_terms_idx1 ON xx_eccma_update_terms (id);
CREATE INDEX xx_eccma_update_defs_idx1 ON xx_eccma_update_defs (id);
CREATE INDEX xx_eccma_update_abbr_idx1 ON xx_eccma_update_abbr (id);

CREATE SEQUENCE log_seq START 1;

CREATE OR REPLACE FUNCTION xx_fn_log(p_text VARCHAR(200))
  RETURNS VOID AS $$
DECLARE
BEGIN
  raise notice '%', p_text;

  INSERT INTO xx_eccma_update_log(id, date_log, message_log) VALUES (nextval('log_seq'), current_timestamp, p_text);
EXCEPTION
  WHEN OTHERS THEN
    raise notice 'Ha ocurrido un error al tratar de registrar el log: %', p_text;
END;
$$ LANGUAGE plpgsql;

--nueva tabla tmp_dn donde se almacena el dump de ecmma
CREATE TABLE IF NOT EXISTS tmp_dn
(
    term_id character varying(100) COLLATE pg_catalog."default",
    concept_id character varying(100) COLLATE pg_catalog."default",
    language_id character varying(100) COLLATE pg_catalog."default",
    language_code character varying(100) COLLATE pg_catalog."default",
    country_code character varying(100) COLLATE pg_catalog."default",
    language_name character varying(100) COLLATE pg_catalog."default",
    term_content text COLLATE pg_catalog."default",
    term_originator_reference text COLLATE pg_catalog."default",
    term_document_id text COLLATE pg_catalog."default",
    term_url text COLLATE pg_catalog."default",
    term_description text COLLATE pg_catalog."default",
    term_organization_id text COLLATE pg_catalog."default",
    term_organization_name text COLLATE pg_catalog."default",
    term_is_deprecated text COLLATE pg_catalog."default",
    concept_type_id text COLLATE pg_catalog."default",
    concept_type_name text COLLATE pg_catalog."default",
    concept_is_deprecated text COLLATE pg_catalog."default",
    definition_id text COLLATE pg_catalog."default",
    definition_content text COLLATE pg_catalog."default",
    definition_under_development text COLLATE pg_catalog."default",
    definition_originator_reference text COLLATE pg_catalog."default",
    definition_document_id text COLLATE pg_catalog."default",
    definition_url text COLLATE pg_catalog."default",
    definition_description text COLLATE pg_catalog."default",
    definition_organization_id text COLLATE pg_catalog."default",
    definition_organization_name text COLLATE pg_catalog."default",
    definition_is_deprecated text COLLATE pg_catalog."default",
    definition_is_default text COLLATE pg_catalog."default",
    label_id text COLLATE pg_catalog."default",
    label_content text COLLATE pg_catalog."default",
    label_originator_reference text COLLATE pg_catalog."default",
    label_document_id text COLLATE pg_catalog."default",
    label_url text COLLATE pg_catalog."default",
    label_description text COLLATE pg_catalog."default",
    label_organization_id text COLLATE pg_catalog."default",
    label_organization_name text COLLATE pg_catalog."default",
    label_is_deprecated text COLLATE pg_catalog."default",
    abbreviation_id text COLLATE pg_catalog."default",
    abbreviation_content text COLLATE pg_catalog."default",
    abbreviation_originator_ref text COLLATE pg_catalog."default",
    abbreviation_document_id text COLLATE pg_catalog."default",
    abbreviation_url text COLLATE pg_catalog."default",
    abbreviation_description text COLLATE pg_catalog."default",
    abbreviation_organization_id text COLLATE pg_catalog."default",
    abbreviation_organization_name text COLLATE pg_catalog."default",
    abbreviation_is_deprecated text COLLATE pg_catalog."default",
    plural_id text COLLATE pg_catalog."default",
    plural_singular_term_item_id text COLLATE pg_catalog."default",
    plural_content text COLLATE pg_catalog."default",
    plural_originator_reference text COLLATE pg_catalog."default",
    plural_document_id text COLLATE pg_catalog."default",
    plural_url text COLLATE pg_catalog."default",
    plural_description text COLLATE pg_catalog."default",
    plural_organization_id text COLLATE pg_catalog."default",
    plural_organization_name text COLLATE pg_catalog."default",
    plural_is_deprecated text COLLATE pg_catalog."default",
    nain text COLLATE pg_catalog."default",
    CONSTRAINT tmpdn_pkey PRIMARY KEY (term_id)
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;

CREATE INDEX xx_tmp_dn_idx1 ON tmp_dn (concept_id);
CREATE INDEX xx_tmp_dn_idx2 ON tmp_dn (definition_id);
CREATE INDEX xx_tmp_dn_idx3 ON tmp_dn (abbreviation_id);



--=======================================================================
-- ACTIVIDADES QUE SE DEBEN DE SEGUIR SI YA SE HAN REALIZADO LOS PASOS ANTERIORES ---

-- Despues de esto se debe de carga el archivo csv  en la tabla tmp_dn
-- es importante que tenga un slash al principio, esto por tema de permisos
\COPY tmp_dn FROM '/home/ubuntu/concept_dn_19JUN_FULL.csv_10' DELIMITER ',' CSV;

--DESHABILITAR TRIGGER EN TABLA TERMINOLOGICALS
alter table terminologicals disable trigger terminologicals_before_insert_update_row_tr;

VACUUM(ANALYZE) terminologicals;
VACUUM(ANALYZE) concepts;
VACUUM(ANALYZE) concept_types;
