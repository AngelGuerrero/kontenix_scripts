CREATE TABLE IF NOT EXISTS xx_eccma_update_log  (date_log  TIMESTAMP,concept_type varchar(100) ,message_log varchar(500));
CREATE TABLE IF NOT EXISTS xx_eccma_update_terms(id integer,is_deprecated boolean,content text);
CREATE TABLE IF NOT EXISTS xx_eccma_update_defs(id integer,is_deprecated boolean,content text);
CREATE TABLE IF NOT EXISTS xx_eccma_update_abbr(id integer,content text);
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
	    -- Procedure to create table xx_eccma_others
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