CREATE OR REPLACE PROCEDURE sp_bulk_update_book_prices_by_genre(
    p_genre_id INTEGER,
    p_percentage_change NUMERIC(5, 2)
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_updated_count INTEGER;
BEGIN
    UPDATE Books
    SET price = price * (1 + p_percentage_change / 100)
    WHERE genre_id = p_genre_id;
    
    GET DIAGNOSTICS v_updated_count = ROW_COUNT;

    RAISE NOTICE 'Number of books updated: %', v_updated_count;
END;
$$;

CALL sp_bulk_update_book_prices_by_genre(3, 5.00);