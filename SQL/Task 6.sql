CREATE OR REPLACE FUNCTION fn_avg_price_by_genre(p_genre_id INTEGER)
RETURNS NUMERIC(10, 2)
LANGUAGE plpgsql
AS $$
DECLARE
    v_avg_price NUMERIC(10, 2);
BEGIN
    SELECT AVG(price)
    INTO v_avg_price
    FROM Books
    WHERE genre_id = p_genre_id;

    RETURN v_avg_price;
END;
$$;

SELECT fn_avg_price_by_genre(1);