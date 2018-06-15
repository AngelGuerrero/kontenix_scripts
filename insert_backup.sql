
INSERT INTO xx_terminologicals (id,
                             terminology_class,
                             eccma_eotd,
                             content,
                             orginator_reference,
                             is_deprecated,
                             language_id,
                             organization_id,
                             term_id,
                             tsv_content,
                             concept_id,
                             created_at,
                             updated_at
                            )
                            VALUES
                            (nextval('terminologicals_id_seq1'),
                            'term',
                            l_record.term_id,
                            l_record.term_content,
                            l_record.term_originator_reference,
                            CAST(l_record.term_is_deprecated AS BOOLEAN),
                            l_record.id_language,
                            l_org_id,
                            NULL, --> term_id no se ocupa para el term
                            to_tsvector('pg_catalog.simple', coalesce(unaccent(l_record.term_content),'')),
                            l_record.id_concept,
                            current_timestamp,
                            current_timestamp
                            );



INSERT INTO xx_terminologicals (id,
                             terminology_class,
                             eccma_eotd,
                             content,
                             orginator_reference,
                             is_deprecated,
                             language_id,
                             organization_id,
                             term_id,
                             tsv_content,
                             concept_id,
                             created_at,
                             updated_at
                            )
                            VALUES
                              (nextval('terminologicals_id_seq1'),
                                'definition',
                                l_record.definition_id,
                                l_record.definition_content,
                                l_record.definition_originator_reference,
                                CAST(l_record.definition_is_deprecated AS BOOLEAN),
                                l_record.id_language,
                                l_org_id,
                                NULL, --> No se ocupa term_id para definition
                                to_tsvector('pg_catalog.simple', coalesce(unaccent(l_record.definition_content),'')),
                                l_record.id_concept,
                                current_timestamp,
                               current_timestamp
                              );



INSERT INTO xx_terminologicals (id,
                             terminology_class,
                             eccma_eotd,
                             content,
                             orginator_reference,
                             is_deprecated,
                             language_id,
                             organization_id,
                             term_id,
                             tsv_content,
                             concept_id,
                             created_at,
                             updated_at
                             )
                             VALUES
                             (nextval('terminologicals_id_seq1'),
                              'abbreviation',
                              l_record.abbreviation_id,
                              l_record.abbreviation_content,
                              l_record.abbreviation_originator_ref,
                              CAST(l_record.abbreviation_is_deprecated AS BOOLEAN),
                              l_record.id_language,
                              l_org_id,
                              l_record.id_term,
                              to_tsvector('pg_catalog.simple', coalesce(unaccent(l_record.abbreviation_content),'')),
                              NULL, --> No se ocupa el concept_id para la abreviación
                              current_timestamp,
                             current_timestamp
                            );
