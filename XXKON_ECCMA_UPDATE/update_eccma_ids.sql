DO
$$
DECLARE
  l_record RECORD;
  l_org_id INTEGER DEFAULT 0;
  l_concept_id INTEGER DEFAULT 0;
  l_con_eccma_aux VARCHAR(20) DEFAULT '';

  --// Variables para los contadores
  l_new_org  INTEGER DEFAULT 0;
  l_upd_con  INTEGER DEFAULT 0;
  l_upd_term INTEGER DEFAULT 0;
  l_upd_def  INTEGER DEFAULT 0;
  l_upd_abb  INTEGER DEFAULT 0;

  --// Variable para manejar los errores
  _c TEXT;
BEGIN

  --// Agrega las nuevas organizaciones si es que no existieran en Kontenix
  FOR l_record IN SELECT xxeccma.eccma_organization_id
                       , xxeccma.organization_name
                       , xxeccma.organization_mail_address
                    FROM xx_eccma_ids xxeccma
                   WHERE NOT EXISTS (SELECT eccma_eotd
                                       FROM organizations org
                                      WHERE xxeccma.eccma_organization_id = org.eccma_eotd)
  LOOP
    INSERT INTO organizations(id,
                              eccma_eotd,
                              name,
                              mail_address,
                              created_at,
                              UPDATEd_at)
    VALUES (
      nextval('organizations_id_seq1'),
      l_record.eccma_organization_id,
      l_record.organization_name,
      l_record.organization_mail_address,
      current_date,
      current_date
    );

    l_new_org := l_new_org + 1;
  END LOOP;

  --// Hace un join entre lo que se va a actualizar
  --// y las organizaciones, para obtener el id de la organización
  FOR l_record IN (SELECT xxeccma.*      --// Todo lo que se actualizará
                        , org.id id_org  --// Id de la organización que lo registró
                     FROM xx_eccma_ids xxeccma, organizations org
                    WHERE xxeccma.eccma_organization_id = org.eccma_eotd) LOOP

    --// Actualización de los términos
    IF (l_record.eccma_term_id <> NULL) THEN
      UPDATE terminologicals t
         SET eccma_eotd = l_record.eccma_term_id,
             organization_id = l_record.id_org,
             updated_at = current_date
       WHERE t.terminology_class = 'term'
         AND t.content = l_record.term_content
      RETURNING t.concept_id INTO l_concept_id;
    --// FIX: otra condición si es que contiene el language_id

      --// Actualización del concepto asociado a éste término
      --// Verifica primero si el ECCMA_EOTD del concepto ya existe
      BEGIN
        SELECT c.eccma_eotd
          INTO STRICT l_con_eccma_aux
          FROM concepts c
         WHERE c.id = l_concept_id;
      EXCEPTION
        WHEN OTHERS THEN
          l_con_eccma_aux := '';
      END;

      --// Si es diferente de vacío, siginifica que aún no está actualizado el campo ECCMA_EOTD
      IF (l_con_eccma_aux <> '') THEN
        UPDATE concepts c
          SET eccma_eotd = l_record.eccma_concept_id,
              updated_at = current_date
          WHERE c.id = l_concept_id;

        l_upd_con := l_upd_con + 1;
      END IF;

      l_upd_term := l_upd_term + 1;
    END IF;

    --// Actualización de las definiciones
    IF (l_record.eccma_definition_id <> NULL) THEN
      UPDATE terminologicals t
      SET eccma_eotd = l_record.eccma_definition_id,
          organization_id = l_record.id_org,
          updated_at = current_date
      WHERE t.terminology_class = 'definition'
        AND t.content = l_record.definition_content
      RETURNING t.concept_id INTO l_concept_id;
    --// FIX: otra condición si es que contiene el language_id

      --// Actualización del concepto asociado a esta definición
      --// Verifica primero si el ECCMA_EOTD del concepto ya existe
      BEGIN
        SELECT c.eccma_eotd
        INTO STRICT l_con_eccma_aux
        FROM concepts c
        WHERE c.id = l_concept_id;
        EXCEPTION
        WHEN OTHERS THEN
          l_con_eccma_aux := '';
      END;

      --// Si es diferente de vacío, siginifica que aún no está actualizado el campo ECCMA_EOTD
      IF (l_con_eccma_aux <> '') THEN
        UPDATE concepts c
        SET eccma_eotd = l_record.eccma_concept_id,
          updated_at = current_date
        WHERE c.id = l_concept_id;

        l_upd_con := l_upd_con + 1;
      END IF;

      l_upd_def := l_upd_def + 1;
    END IF;

    --// Actualización de las abreviaciones
    IF (l_record.eccma_abbreviation_id <> NULL) THEN
      UPDATE terminologicals t
         SET eccma_eotd = l_record.eccma_abbreviation_id,
             organization_id = l_record.id_org,
             updated_at = current_date
      WHERE t.terminology_class = 'abbreviation'
        AND t.content = l_record.abbreviation_content
      RETURNING t.concept_id INTO l_concept_id;
    --// FIX: otra condición si es que contiene el language_id

      --// Actualización del concepto asociado a esta abreviación
      --// Verifica primero si el ECCMA_EOTD del concepto ya existe
      BEGIN
        SELECT c.eccma_eotd
          INTO STRICT l_con_eccma_aux
          FROM concepts c
         WHERE c.id = l_concept_id;
        EXCEPTION
        WHEN OTHERS THEN
          l_con_eccma_aux := '';
      END;

      --// Si es diferente de vacío, siginifica que aún no está actualizado el campo ECCMA_EOTD
      IF (l_con_eccma_aux <> '') THEN
        UPDATE concepts c
        SET eccma_eotd = l_record.eccma_concept_id,
          updated_at = current_date
        WHERE c.id = l_concept_id;

        l_upd_con := l_upd_con + 1;
      END IF;

      l_upd_abb := l_upd_abb + 1;
    END IF;
  END LOOP;

EXCEPTION
  WHEN OTHERS THEN
    get stacked diagnostics _c = pg_exception_context;
    raise notice 'context: >>%<<', _c;
    raise notice 'Ha ocurrido un error en la función: xxkon_fn_upate_eotd';
    raise notice 'Error: % %', sqlstate, sqlerrm;
END;
$$ LANGUAGE plpgsql;
