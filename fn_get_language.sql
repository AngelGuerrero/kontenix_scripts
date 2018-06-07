    -- Procedure to create table xx_eccma_others --
CREATE OR REPLACE FUNCTION XXKON_FN_GET_LANGUAGE(p_language VARCHAR(100)) 
RETURNS INTEGER AS $$

DECLARE
   l_lang_id INTEGER;
BEGIN
   SELECT id
     INTO l_lang_id
     FROM languages
    WHERE eccma_eotd = p_language;
	
   RETURN l_lang_id;	
EXCEPTION
  WHEN OTHERS THEN
     RETURN 0;
END;
$$ LANGUAGE plpgsql;