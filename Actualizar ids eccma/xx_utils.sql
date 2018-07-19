CREATE TABLE IF NOT EXISTS xx_eccma_new_ids
(
  eccma_concept_id character varying(50) COLLATE pg_catalog."default",
  eccma_term_id character varying(50) COLLATE pg_catalog."default",
  term_content text COLLATE pg_catalog."default",
  eccma_definition_id character varying(50) COLLATE pg_catalog."default",
  definition_content text COLLATE pg_catalog."default",
  eccma_abbreviation_id character varying(50) COLLATE pg_catalog."default",
  abbreviation_content text COLLATE pg_catalog."default",
  eccma_language_id character varying(50) COLLATE pg_catalog."default",
  language_name character varying(50) COLLATE pg_catalog."default",
  eccma_organization_id character varying(50) COLLATE pg_catalog."default",
  organization_name text COLLATE pg_catalog."default",
  organization_mail_address text COLLATE pg_catalog."default"
)
WITH (
OIDS = FALSE
)
TABLESPACE pg_default;

CREATE TABLE IF NOT EXISTS xxdata_not_found(terminology_class VARCHAR(20), eccma_eotd VARCHAR(20), content TEXT, created_at TIMESTAMP, updated_at TIMESTAMP);
