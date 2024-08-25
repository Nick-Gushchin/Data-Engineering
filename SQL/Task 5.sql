CREATE OR REPLACE PROCEDURE sp_update_customer_join_date()
LANGUAGE plpgsql
AS $$
BEGIN
    UPDATE Customers c
    SET join_date = (
        SELECT MIN(s.sale_date)
        FROM Sales s
        WHERE s.customer_id = c.customer_id
    )
    WHERE EXISTS (
        SELECT 1
        FROM Sales s
        WHERE s.customer_id = c.customer_id
          AND s.sale_date < c.join_date
    );
END;
$$;

CALL sp_update_customer_join_date();