     -- si no se obtiene un id para el término de la organizacion entonces lo inserta, si no, lo actualiza
		 IF l_term_id = 0 THEN
       raise notice 'Término para crear: %', l_term_id;
       l_new_term := l_new_term + 1;

       raise notice 'term  %', l_cursor.term_id;
       raise notice 'concept  %', l_cursor.concept_id_table;
       raise notice 'language  %', l_language_id;
       raise notice 'organization  %', l_org_id;
       l_up_term := l_up_term +1;
    -- INSERT INTO xx_eccma_update_terms VALUES (l_term_id,
		-- 											l_cursor.term_id,
		-- 											CAST (l_cursor.term_is_deprecated AS BOOLEAN),
		-- 											l_cursor.term_content);
		 ELSE
       l_up_term := l_up_term + 1;
     /*
     UPDATE terminologicals t
        SET is_deprecated =  u.is_deprecated,
              content     =  u.content,
              updated_at  =  current_timestamp
     FROM  xx_eccma_update_terms u
      WHERE t.id = u.id;
     */
		 END IF;



		   -- CREATE TABLE IF NOT EXISTS XX_ECCMA_CLASS AS
  -- SELECT tmp1.*
  --   FROM tmp1 LEFT JOIN terminologicals
  --     ON eccma_eotd = tmp1.term_id
  --  WHERE tmp1.concept_id =  '0161-1#CT-01#1';

  -- CREATE TABLE IF NOT EXISTS XX_ECCMA_PROPERTY AS
  -- SELECT tmp1.*
  --   FROM tmp1 LEFT JOIN terminologicals
  --     ON eccma_eotd = tmp1.term_id
  --  WHERE tmp1.concept_id =  '0161-1#CT-02#1';

  -- CREATE TABLE IF NOT EXISTS XX_ECCMA_FEATURE AS
  -- SELECT tmp1.*
  --   FROM tmp1 LEFT JOIN terminologicals
  --     ON eccma_eotd = tmp1.term_id
  --  WHERE tmp1.concept_id =  '0161-1#CT-03#1';

  -- CREATE TABLE IF NOT EXISTS XX_ECCMA_REPRESENTATION AS
  -- SELECT tmp1.*
  --   FROM tmp1 LEFT JOIN terminologicals
  --     ON eccma_eotd = tmp1.term_id
  --  WHERE tmp1.concept_id =  '0161-1#CT-04#1';

  -- CREATE TABLE IF NOT EXISTS XX_ECCMA_UM AS
  -- SELECT tmp1.*
  --   FROM tmp1 LEFT JOIN terminologicals
  --     ON eccma_eotd = tmp1.term_id
  --  WHERE tmp1.concept_id =  '0161-1#CT-05#1';

  -- CREATE TABLE IF NOT EXISTS XX_ECCMA_QM AS
  -- SELECT tmp1.*
  --   FROM tmp1 LEFT JOIN terminologicals
  --     ON eccma_eotd = tmp1.term_id
  --  WHERE tmp1.concept_id =  '0161-1#CT-06#1';

  -- CREATE TABLE IF NOT EXISTS XX_ECCMA_PROPERTY_VALUE AS
  -- SELECT tmp1.*
  --   FROM tmp1 LEFT JOIN terminologicals
  --     ON eccma_eotd = tmp1.term_id
  --  WHERE tmp1.concept_id =  '0161-1#CT-07#1';

  -- CREATE TABLE IF NOT EXISTS XX_ECCMA_CURRENCY AS
  -- SELECT tmp1.*
  --   FROM tmp1 LEFT JOIN terminologicals
  --     ON eccma_eotd = tmp1.term_id
  --  WHERE tmp1.concept_id =  '0161-1#CT-08#1';