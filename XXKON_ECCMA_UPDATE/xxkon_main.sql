--// Procedimiento anónimo que llama a la función xxkon_fn_upate_eotd

DO
$$
DECLARE
  concepts_types VARCHAR[] := ARRAY['0161-1#CT-00#1'
                                    '0161-1#CT-01#1',
                                    '0161-1#CT-02#1',
                                    '0161-1#CT-03#1',
                                    '0161-1#CT-04#1',
                                    '0161-1#CT-05#1',
                                    '0161-1#CT-06#1',
                                    '0161-1#CT-07#1',
                                    '0161-1#CT-08#1'
                                   ];
  c VARCHAR;
BEGIN

  raise notice 'Truncando tabla de log.';

  TRUNCATE TABLE xx_eccma_update_log;

  --// Ciclo principal donde se ejecutan los tipos de conceptos
  FOREACH c IN ARRAY concepts_types LOOP
    PERFORM xxkon_fn_update_eccma_eotd(c);
  end loop;
END;
$$
