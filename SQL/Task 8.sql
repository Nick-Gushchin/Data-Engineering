CREATE OR REPLACE FUNCTION fn_log_sensitive_data_changes()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    IF OLD.first_name IS DISTINCT FROM NEW.first_name THEN
        INSERT INTO CustomersLog (column_name, old_value, new_value, changed_by)
        VALUES ('first_name', OLD.first_name, NEW.first_name, current_user);
    END IF;

    IF OLD.last_name IS DISTINCT FROM NEW.last_name THEN
        INSERT INTO CustomersLog (column_name, old_value, new_value, changed_by)
        VALUES ('last_name', OLD.last_name, NEW.last_name, current_user);
    END IF;

    IF OLD.email IS DISTINCT FROM NEW.email THEN
        INSERT INTO CustomersLog (column_name, old_value, new_value, changed_by)
        VALUES ('email', OLD.email, NEW.email, current_user);
    END IF;

    RETURN NEW;
END;
$$;

CREATE TRIGGER tr_log_sensitive_data_changes
AFTER UPDATE ON Customers
FOR EACH ROW
EXECUTE FUNCTION fn_log_sensitive_data_changes();