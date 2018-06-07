
---------------------------------------------------------------------------
-- Procedure to create table xx_eccma_others
CREATE OR REPLACE FUNCTION XXKON_FN_ECCMA_UPDATE_CONCEPTS(p_concept_type VARCHAR(100))
  RETURNS void AS $$

DECLARE
  rec_exist record;

  titles TEXT DEFAULT '...';

  l_count      INTEGER;
  l_new_term   INTEGER := 0;
  l_up_term    INTEGER := 0;
  ------------------------------------------
  --VARIABLES PARA ORGANIZACION DEL TERMINO
  l_name_org TEXT;
  l_addr_org TEXT;
  l_org_id  INTEGER;

  -----------------------------------------
  l_language_id  INTEGER;
  l_concept_id   INTEGER;
  l_id           INTEGER; -- id de la tabla de término

BEGIN

  raise notice 'Iniciando proceso de actualización Eccma  %', titles;
  DROP TABLE IF EXISTS xx_eccma_others;
  TRUNCATE TABLE xx_eccma_update_terms;

  CREATE TABLE XX_ECCMA_OTHERS AS
    SELECT t1.*
    FROM tmp1 t1
      LEFT JOIN terminologicals t2
        ON t2.eccma_eotd = t1.term_id
    WHERE t1.concept_type_id = '0161-1#CT-00#1';


  --1) CICLO 1 PARA CONCEPTOS TIPO OTHERS
  --PRIMERO  PROCESAMOS LOS CONCEPTOS TIPO OTHERS QUE EXISTEN YA EN LA BASE DE DATOS DE KONTENIX A NIVEL DE CONCEPTO
  FOR rec_exist IN
  SELECT  o.*,
    c.id concept_id_table
  FROM  xx_eccma_others o, CONCEPTS c
  WHERE 1=1
        AND c.eccma_eotd = o.concept_id
  LOOP

    --A) AL EXISTIR EL CONCEPTO, SE PROCEDE A ACTUALIZARLO EN EL CAMPO DEPRECATED
    UPDATE concepts
    SET is_deprecated =  CAST (rec_exist.concept_is_deprecated AS BOOLEAN),
      updated_at    = current_timestamp
    WHERE eccma_eotd =  rec_exist.concept_id;

    --B) VALIDAR LA ORGANIZACION DEL TERMINO
    BEGIN
      SELECT id
      INTO l_org_id
      FROM organizations
      WHERE 1=1
            AND eccma_eotd = rec_exist.term_organization_id;
      EXCEPTION
      WHEN NO_DATA_FOUND THEN
        l_org_id := 0;
      WHEN OTHERS THEN
        l_org_id := 0;
    END;

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

    --C) VALIDAR TERMINOS ( QUE EXISTA O NO)
    l_language_id := xxkon_fn_get_language(rec_exist.language_id);

    BEGIN
      SELECT id
      INTO l_id
      FROM terminologicals
      WHERE terminology_class = 'term'
            AND eccma_eotd = rec_exist.term_id
            AND language_id = l_language_id
            AND concept_id  =  rec_exist.concept_id_table
            AND organization_id = l_org_id;

      EXCEPTION
      WHEN OTHERS THEN
        l_id := 0;
    END;


    IF l_id > 0 THEN
      l_up_term := l_up_term +1;


      INSERT INTO xx_eccma_update_terms VALUES (l_id,
                                                CAST (rec_exist.term_is_deprecated AS BOOLEAN),
                                                rec_exist.term_content);




    ELSE
      l_new_term := l_new_term +1;
    --raise notice 'valor id term  %', l_term_id;
    --raise notice 'term  %', rec_exist.term_id;
    --raise notice 'concept  %', rec_exist.concept_id_table;
    --raise notice 'language  %', l_language_id;
    --raise notice 'organization  %', l_org_id;
    END IF;

    --D) VALIDAR ORGANIZACION DE LA DEFINICION (EXISTE O NO. SI NO EXISTE SE CREA. SI EXISTE NO SE HACE NADA)

    --E) VALIDAR DEFINICIONES (AGREGAR EN CASO DE NO EXISTIR, O ACTUALIZAR CONTENT Y DEPRECATED)

    --F) VALIDAR ORGANIZACIONES DE ABREVIACIONES(EXISTE O NO. SI NO EXISTE SE CREA. SI EXISTE NO SE HACE NADA)

    --G) VALIDAR ABREVIACIONES (AGREGAR EN CASO DE NO EXISTIR, O ACTUALIZAR CONTENT)

  END LOOP;

  raise notice 'Total de term a crear  %', l_new_term;
  raise notice 'Total de term a actualizar  %', l_up_term;
  raise notice 'Fin de proceso de actualización Eccma ... ';

  UPDATE terminologicals t
     SET is_deprecated     =  u.is_deprecated,
           content     =  u.content,
           updated_at  =  current_timestamp
  FROM  xx_eccma_update_terms u
   WHERE t.id = u.id;

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
  PERFORM XXKON_FN_ECCMA_UPDATE_CONCEPTS('');
end;
$$;