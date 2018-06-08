CREATE OR REPLACE FUNCTION XXKON_FN_TEST()
  RETURNS INTEGER AS $$
DECLARE
  affected_rows INTEGER DEFAULT 0;
BEGIN
  UPDATE terminologicals
  SET is_deprecated = FALSE
  WHERE id = 3034667
  RETURNING id;
END;
$$
LANGUAGE plpgsql;


SELECT xxkon_fn_test();

WITH updated_rows AS (
  UPDATE terminologicals
  SET is_deprecated = FALSE
  WHERE id = 3034667
  RETURNING *
)
SELECT *
FROM updated_rows;