CREATE OR REPLACE FUNCTION fn_adjust_book_price()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
DECLARE
    v_total_quantity INTEGER;
    v_current_price NUMERIC(10, 2);
BEGIN
    SELECT SUM(quantity)
    INTO v_total_quantity
    FROM Sales
    WHERE book_id = NEW.book_id;
    
    IF v_total_quantity >= 5 THEN
        SELECT price
        INTO v_current_price
        FROM Books
        WHERE book_id = NEW.book_id;
        
        UPDATE Books
        SET price = v_current_price * 1.10
        WHERE book_id = NEW.book_id;
    END IF;

    RETURN NEW;
END;
$$;

CREATE TRIGGER tr_adjust_book_price
AFTER INSERT ON Sales
FOR EACH ROW
EXECUTE FUNCTION fn_adjust_book_price();