
 CREATE TABLE IF NOT EXISTS xx_eccma_update_terms AS SELECT t2.id,
	          t2.is_deprecated,
			  t2.content
	     FROM tmp1 t1
	     LEFT JOIN terminologicals t2
		   ON t2.eccma_eotd = t1.term_id	
		WHERE t1.concept_type_id = '0161-1#CT-00#1' 
		  and t2.concept_id = 1578675;
