CREATE TABLE erp.region (
    region_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    iso_code TEXT NOT NULL UNIQUE,
    region_name TEXT NOT NULL,
    region_type TEXT NOT NULL,
    sales_territory TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TRIGGER region_set_updated_at 
    BEFORE UPDATE on erp.region
    FOR EACH ROW EXECUTE FUNCTION erp.set_updated_at();


CREATE TABLE erp.address (
    address_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    region_id BIGINT NOT NULL REFERENCES erp.region (region_id),
    city TEXT NOT NULL,
    district TEXT,
    street TEXT NOT NULL,
    house_number TEXT NOT NULL,
    block_number TEXT,
    premise TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX address_region_id_index ON erp.address (region_id);


CREATE TABLE erp.customer (
    customer_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    customer_code TEXT NOT NULL UNIQUE,
    legal_name TEXT NOT NULL,
    contact_phone TEXT NOT NULL,
    contact_email TEXT NOT NULL,
    industry TEXT NOT NULL,
    billing_address_id BIGINT NOT NULL REFERENCES erp.address (address_id),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX customer_billing_address_id_index ON erp.customer (billing_address_id);

CREATE TRIGGER customer_set_updated_at
    BEFORE UPDATE on erp.customer
    FOR EACH ROW EXECUTE FUNCTION erp.set_updated_at();


CREATE TABLE erp.customer_warehouse (
    customer_warehouse_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    customer_id BIGINT NOT NULL REFERENCES erp.customer (customer_id),
    customer_warehouse_code TEXT NOT NULL,
    customer_warehouse_name TEXT NOT NULL,
    address_id BIGINT NOT NULL REFERENCES erp.address (address_id),
    opened_date DATE NOT NULL,
    closed_date DATE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (customer_id, customer_warehouse_code),
    UNIQUE (customer_warehouse_id, customer_id),
    CHECK (closed_date >= opened_date)
);

CREATE INDEX customer_warehouse_address_id_index ON erp.customer_warehouse (address_id);

CREATE TRIGGER customer_warehouse_set_updated_at
    BEFORE UPDATE on erp.customer_warehouse
    FOR EACH ROW EXECUTE FUNCTION erp.set_updated_at();


CREATE TABLE erp.manufacturer_warehouse (
    manufacturer_warehouse_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    manufacturer_warehouse_code TEXT NOT NULL UNIQUE,
    manufacturer_warehouse_name TEXT NOT NULL,
    address_id BIGINT NOT NULL REFERENCES erp.address (address_id),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX manufacturer_warehouse_address_id_index ON erp.manufacturer_warehouse (address_id);

CREATE TRIGGER manufacturer_warehouse_set_updated_at
    BEFORE UPDATE on erp.manufacturer_warehouse
    FOR EACH ROW EXECUTE FUNCTION erp.set_updated_at();


CREATE TABLE erp.brand (
    brand_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    brand_name TEXT NOT NULL UNIQUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TRIGGER brand_set_updated_at
    BEFORE UPDATE on erp.brand
    FOR EACH ROW EXECUTE FUNCTION erp.set_updated_at();


CREATE TABLE erp.dosage_form (
    dosage_form_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    dosage_form_name TEXT NOT NULL UNIQUE,
    unit_of_measure TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TRIGGER dosage_form_set_updated_at
    BEFORE UPDATE on erp.dosage_form
    FOR EACH ROW EXECUTE FUNCTION erp.set_updated_at();


CREATE TABLE erp.product (
    product_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    gtin TEXT NOT NULL UNIQUE,
    brand_id BIGINT NOT NULL REFERENCES erp.brand (brand_id),
    dosage_form_id BIGINT NOT NULL REFERENCES erp.dosage_form (dosage_form_id),
    product_name TEXT NOT NULL,
    active_ingredient_name TEXT NOT NULL,
    strength TEXT NOT NULL,
    pack_quantity INTEGER NOT NULL CHECK (pack_quantity > 0),
    shelf_life_months INTEGER NOT NULL CHECK (shelf_life_months > 0),
    launch_date DATE NOT NULL,
    discontinued_date DATE,
    is_prescription BOOLEAN NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK (discontinued_date >= launch_date)
);

CREATE INDEX product_brand_id_index ON erp.product (brand_id);
CREATE INDEX product_dosage_form_id_index ON erp.product (dosage_form_id);

CREATE TRIGGER product_set_updated_at
    BEFORE UPDATE on erp.product
    FOR EACH ROW EXECUTE FUNCTION erp.set_updated_at();


CREATE TABLE erp.batch (
    batch_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    batch_number TEXT NOT NULL UNIQUE,
    product_id BIGINT NOT NULL REFERENCES erp.product (product_id),
    manufacture_date DATE NOT NULL,
    expiry_date DATE NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK (expiry_date > manufacture_date)
);

CREATE INDEX batch_product_id_index ON erp.batch (product_id);


CREATE TABLE erp.price_list (
    price_list_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    product_id BIGINT NOT NULL REFERENCES erp.product (product_id),
    valid_from DATE NOT NULL,
    list_price NUMERIC(12, 2) NOT NULL CHECK (list_price > 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (product_id, valid_from),
    UNIQUE (price_list_id, product_id)
);