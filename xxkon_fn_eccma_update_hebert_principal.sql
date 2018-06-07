CREATE OR REPLACE FUNCTION XXKON_FN_ECCMA_UPDATE(p_concept_type VARCHAR(100)) 
    RETURNS void AS $$
	
	DECLARE
	   rec_exist record;	   
	   
	   l_count           INTEGER;
	   
	   l_new_term        INTEGER := 0;  
	   l_new_definition  INTEGER := 0;
	   l_new_abbr        INTEGER := 0;
	   
	   l_up_term         INTEGER := 0;
	   l_up_definition   INTEGER := 0;
	   l_up_abbr         INTEGER := 0;
	   ------------------------------------------
	   --VARIABLES PARA ORGANIZACION DEL TERMINO
	   l_name_org TEXT;
	   l_addr_org TEXT;
	   l_org_id  INTEGER;
	   
	   -----------------------------------------
	   l_language_id  INTEGER;
	   l_concept_id   INTEGER;
	   l_id           INTEGER;
	   
    BEGIN
	   INSERT INTO xx_eccma_update_log  VALUES(current_timestamp,'others','Iniciando proceso de actualización Eccma...'); 	   
	   
	   TRUNCATE TABLE xx_eccma_update_terms;
	   TRUNCATE TABLE xx_eccma_update_definition;
	   TRUNCATE TABLE xx_eccma_update_log;
	   
	   DROP TABLE IF EXISTS xx_eccma_others;
	   	   
       CREATE TABLE xx_eccma_others AS
       SELECT t1.*
	     FROM tmp1 t1
	     LEFT JOIN terminologicals t2
		   ON t2.eccma_eotd = t1.term_id	
		WHERE t1.concept_type_id = '0161-1#CT-00#1';
		   
	   --1) CICLO PARA CONCEPTOS TIPO OTHERS
	   --   PRIMERO  PROCESAMOS LOS CONCEPTOS TIPO OTHERS QUE EXISTEN 
	   --   YA EN LA BASE DE DATOS DE KONTENIX A NIVEL DE CONCEPTO
	   INSERT INTO xx_eccma_update_log  VALUES(current_timestamp,'others','Inicio de ciclo principal');
	   
	   FOR rec_exist IN
		    SELECT  o.*,
			        c.id concept_id_table
			  FROM  xx_eccma_others o,
			        CONCEPTS c
			WHERE 1=1
			  AND c.eccma_eotd = o.concept_id 
	   LOOP
		   --A) AL EXISTIR EL CONCEPTO, SE PROCEDE A ACTUALIZARLO EN EL CAMPO DEPRECATED
		    UPDATE concepts
			   SET is_deprecated =  CAST (rec_exist.concept_is_deprecated AS BOOLEAN),
			       updated_at    = current_timestamp
			WHERE eccma_eotd =  rec_exist.concept_id;
			 
		   --B) VALIDAR LA ORGANIZACION ASOCIADA AL TERMINO
		   l_org_id := xx_fn_get_organization(rec_exist.term_organization_id);
		   
		   IF l_org_id < 1 THEN
		      raise notice 'Insertando en tabla organizations : %', rec_exist.term_organization_id; 
			  SELECT organization_id_seq.NEXTVALUE INTO l_org_id;			  
		      INSERT INTO organizations VALUES(l_org_id,
											   rec_exist.term_organization_id,
											   rec_exist.term_organization_name,
											   NULL,
											   current_timestamp,
											   current_timestamp
											   );
		    
		   END IF;
		   
           --C) VALIDAR TERMINOS DEL CONCEPTO (SI EXISTEN SE ACTUALIZAN, DE LO CONTRARIO SE INSERTAN)
           l_language_id := xxkon_fn_get_language(rec_exist.language_id);		  
           l_id := xx_fn_get_terminological_id('term',
											   rec_exist.term_id,
											   l_language_id,
											   rec_exist.concept_id_table,
											   l_org_id);
		   IF l_id > 0 THEN
		      l_up_term := l_up_term +1;					
			  INSERT INTO xx_eccma_update_terms VALUES (l_id,
													    CAST (rec_exist.term_is_deprecated AS BOOLEAN), 
													    rec_exist.term_content);
		   ELSE
		      --FALTA CODIGO PARA EL INSERT
		      l_new_term := l_new_term +1;
		   END IF;
		   
           --D) VALIDAR ORGANIZACION ASOCIADA A LA DEFINICION (SI NO EXISTE SE CREA. SI EXISTE NO SE HACE NADA) 
		   l_org_id := xx_fn_get_organization(rec_exist.definition_organization_id);
		   
		   IF l_org_id < 1 THEN
		      raise notice 'Insertando en tabla organizations (nueva organizacion asociada a la Definicion): %', rec_exist.definition_organization_id; 
			  SELECT organization_id_seq.NEXTVALUE INTO l_org_id;			  
		      INSERT INTO organizations VALUES(l_org_id,
											   rec_exist.definition_organization_id,
											   rec_exist.definition_organization_name,
											   NULL,
											   current_timestamp,
											   current_timestamp
											   );
		    
		   END IF;		   
		   
           --E) VALIDAR DEFINICIONES (AGREGAR EN CASO DE NO EXISTIR, O ACTUALIZAR CONTENT Y DEPRECATED)
           l_id := xx_fn_get_terminological_id('definition',
											   rec_exist.definition_id,
											   l_language_id,
											   rec_exist.concept_id_table,
											   l_org_id);           
		   IF l_id > 0 THEN
		      l_up_definition := l_up_definition +1;					
			  INSERT INTO xx_eccma_update_definition VALUES (l_id,
													    CAST (rec_exist.definition_is_deprecated AS BOOLEAN), 
													    rec_exist.definition_content);
		   ELSE
		      --FALTA CODIGO PARA EL INSERT
			  l_new_definition := l_new_definition + 1;
		   END IF;
		   
		   --F) VALIDAR ORGANIZACIONES DE ABREVIACIONES(EXISTE O NO. SI NO EXISTE SE CREA. SI EXISTE NO SE HACE NADA)
		   
           --G) VALIDAR ABREVIACIONES (AGREGAR EN CASO DE NO EXISTIR, O ACTUALIZAR CONTENT)		   
		  
	   END LOOP;


	  INSERT INTO xx_eccma_update_log  VALUES(current_timestamp,'others','fin de ciclo principal');  
	  raise notice 'Total de term a crear  %', l_new_term;	
	  raise notice 'Total de term a actualizar  %', l_up_term;	
	  INSERT INTO xx_eccma_update_log  VALUES(current_timestamp,'others','Total de term a crear : '||l_new_term); 
	  INSERT INTO xx_eccma_update_log  VALUES(current_timestamp,'others','Total de term a actualizar : '||l_up_term); 
	  
	  raise notice 'Total de definition a crear  %', l_new_definition;	
	  raise notice 'Total de definition a actualizar  %', l_up_definition;	      
	  INSERT INTO xx_eccma_update_log  VALUES(current_timestamp,'others','Total de definition a crear : '||l_new_definition); 
	  INSERT INTO xx_eccma_update_log  VALUES(current_timestamp,'others','Total de definition a actualizar : '||l_up_definition); 
	  
	  raise notice 'Inicio update terminologicals terms ... %', current_time;	
	  INSERT INTO xx_eccma_update_log  VALUES(current_timestamp,'others','Inicio bulk update terminologicals (terms)');  
	   UPDATE terminologicals t
	      SET is_deprecated     =  u.is_deprecated,
		    	    content     =  u.content,
			        updated_at  =  current_timestamp
		FROM  xx_eccma_update_terms u
	    WHERE t.id = u.id;	
	  INSERT INTO xx_eccma_update_log  VALUES(current_timestamp,'others','Fin bulk update terminologicals (terms)');  
	  
	  ------------------------------------------------------------------------------------------------------------------
	  --
	  
	  raise notice 'Inicio update terminologicals definition ... %', current_time;	
	  INSERT INTO xx_eccma_update_log  VALUES(current_timestamp,'others','Inicio bulk update terminologicals (definition)');  
	   UPDATE terminologicals t
	      SET is_deprecated     =  u.is_deprecated,
		    	    content     =  u.content,
			        updated_at  =  current_timestamp
		FROM  xx_eccma_update_definition u
	    WHERE t.id = u.id;	
	  INSERT INTO xx_eccma_update_log  VALUES(current_timestamp,'others','Fin bulk update terminologicals (definition)');  
	
	  INSERT INTO xx_eccma_update_log  VALUES(current_timestamp,'others','Fin de proceso de actualización Eccma'); 
	EXCEPTION
	  WHEN OTHERS THEN
	     BEGIN
	        raise notice 'Ocurrio un error global: %', sqlerrm;
	     END;		
    END;	
    $$ LANGUAGE plpgsql;

DO
$$
BEGIN
	PERFORM xxkon_fn_eccma_update('');
END;
$$