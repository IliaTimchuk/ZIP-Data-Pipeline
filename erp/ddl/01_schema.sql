CREATE SCHEMA erp;

CREATE FUNCTION erp.set_updated_at() RETURNS trigger 
LANGUAGE plpgsql
AS $$
BEGIN
    NEW.updated_at = now();
    RETURN NEW;
END;
$$;