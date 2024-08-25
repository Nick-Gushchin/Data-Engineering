CREATE OR REPLACE PROCEDURE sp_archive_old_sales(p_cutoff_date DATE)
LANGUAGE plpgsql
AS $$
DECLARE
    sales_cursor CURSOR FOR
    SELECT sale_id, book_id, customer_id, quantity, sale_date
    FROM Sales
    WHERE sale_date < p_cutoff_date;

    v_sale_id INTEGER;
    v_book_id INTEGER;
    v_customer_id INTEGER;
    v_quantity INTEGER;
    v_sale_date DATE;
BEGIN
    OPEN sales_cursor;

    LOOP
        FETCH sales_cursor INTO v_sale_id, v_book_id, v_customer_id, v_quantity, v_sale_date;
        
        EXIT WHEN NOT FOUND;
        
        INSERT INTO SalesArchive (sale_id, book_id, customer_id, quantity, sale_date)
        VALUES (v_sale_id, v_book_id, v_customer_id, v_quantity, v_sale_date);
        
        DELETE FROM Sales
        WHERE sale_id = v_sale_id;
    END LOOP;

    CLOSE sales_cursor;

END;
$$;

CALL sp_archive_old_sales('2023-01-01');