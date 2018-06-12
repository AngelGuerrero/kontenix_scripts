

















DO
	begin
	WITH
	new_concept AS ( -- Nuevos conceptos
		INSERT INTO concepts SELECT nextval('concepts_id_seq1'),
																l_record.concept_id,           -- eccma
																l_record.concept_is_deprecated,
																current_timestamp,
																current_timestamp,
																l_record.concept_type_id
		WHERE NOT EXISTS (SELECT eccma_eotd FROM concepts WHERE concepts.eccma_eotd = l_record.concept_id)
		RETURNING id INTO con_id
	),

	new_terms AS ( -- Nuevos términos
		INSERT INTO terminologicals (id,
																 eccma_eotd,
																 content,
																 is_deprecated,
																 concept_id,
																 language_id,
																 organization_id,
																 created_at,
																 updated_at
																)
																SELECT nextval('terminologicals_id_seq1'),
																			 l_record.term_id,
																			 l_record.term_content,
																			 l_record.concept_is_deprecated,
																			 con_id,
																			 l_record.language_id,
																			 l_org_id,
																			 current_timestamp,
																			 current_timestamp
		WHERE NOT EXISTS (SELECT id
												FROM terminologicals
											 WHERE terminology_class = 'term'
												 AND terminologicals.eccma_eotd = l_record.term_id)
		RETURNING id INTO term_id
	),

	new_defs AS ( --Nuevas definiciones
		INSERT INTO terminologicals (id)
																SELECT nextval('terminologicals_id_seq1'),
																			 l_record.definition_id,
																			 l_record.language_id,
																			 con_id,
																			 l_org_id
																			 CAST(l_record.definition_is_deprecated AS BOOLEAN),
																			 l_record.definition_content,

	)

	end;
