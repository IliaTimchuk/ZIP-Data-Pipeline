CREATE TABLE erp.customer_order (
    customer_order_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    order_number TEXT NOT NULL UNIQUE,
    customer_warehouse_id BIGINT NOT NULL,
    customer_id BIGINT NOT NULL REFERENCES erp.customer (customer_id),
    order_timestamp TIMESTAMPTZ NOT NULL,
    requested_delivery_date DATE NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('new', 'confirmed', 'partially_shipped', 'shipped', 'cancelled')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    FOREIGN KEY (customer_warehouse_id, customer_id)
        REFERENCES erp.customer_warehouse (customer_warehouse_id, customer_id)
);

CREATE INDEX customer_order_customer_warehouse_id_index ON erp.customer_order (customer_warehouse_id);
CREATE INDEX customer_order_customer_id_index ON erp.customer_order (customer_id);

CREATE TRIGGER customer_order_set_updated_at
    BEFORE UPDATE ON erp.customer_order
    FOR EACH ROW EXECUTE FUNCTION erp.set_updated_at();


CREATE TABLE erp.order_item (
    order_item_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    customer_order_id BIGINT NOT NULL REFERENCES erp.customer_order (customer_order_id),
    product_id BIGINT NOT NULL REFERENCES erp.product (product_id),
    price_list_id BIGINT NOT NULL,
    ordered_quantity INTEGER NOT NULL CHECK (ordered_quantity > 0),
    confirmed_quantity INTEGER CHECK (confirmed_quantity >= 0),
    status TEXT NOT NULL CHECK (status IN ('pending', 'confirmed', 'shipped', 'cancelled')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (customer_order_id, product_id),
    FOREIGN KEY (price_list_id, product_id)
        REFERENCES erp.price_list (price_list_id, product_id),
    CHECK (confirmed_quantity <= ordered_quantity)
);

CREATE INDEX order_item_product_id_index ON erp.order_item (product_id);
CREATE INDEX order_item_price_list_id_index ON erp.order_item (price_list_id);

CREATE TRIGGER order_item_set_updated_at
    BEFORE UPDATE ON erp.order_item
    FOR EACH ROW EXECUTE FUNCTION erp.set_updated_at();


CREATE TABLE erp.shipment (
    shipment_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    shipment_number TEXT NOT NULL UNIQUE,
    customer_order_id BIGINT NOT NULL REFERENCES erp.customer_order (customer_order_id),
    manufacturer_warehouse_id BIGINT NOT NULL REFERENCES erp.manufacturer_warehouse (manufacturer_warehouse_id),
    ship_timestamp TIMESTAMPTZ NOT NULL,
    delivery_date DATE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX shipment_customer_order_id_index ON erp.shipment (customer_order_id);
CREATE INDEX shipment_manufacturer_warehouse_id_index ON erp.shipment (manufacturer_warehouse_id);

CREATE TRIGGER shipment_set_updated_at
    BEFORE UPDATE ON erp.shipment
    FOR EACH ROW EXECUTE FUNCTION erp.set_updated_at();


CREATE TABLE erp.shipment_item (
    shipment_item_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    shipment_id BIGINT NOT NULL REFERENCES erp.shipment (shipment_id),
    order_item_id BIGINT NOT NULL REFERENCES erp.order_item (order_item_id),
    batch_id BIGINT NOT NULL REFERENCES erp.batch (batch_id),
    shipped_quantity INTEGER NOT NULL CHECK (shipped_quantity > 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (shipment_id, order_item_id, batch_id)
);

CREATE INDEX shipment_item_order_item_id_index ON erp.shipment_item (order_item_id);
CREATE INDEX shipment_item_batch_id_index ON erp.shipment_item (batch_id);


CREATE TABLE erp.customer_return (
    customer_return_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    return_number TEXT NOT NULL UNIQUE,
    customer_warehouse_id BIGINT NOT NULL REFERENCES erp.customer_warehouse (customer_warehouse_id),
    manufacturer_warehouse_id BIGINT NOT NULL REFERENCES erp.manufacturer_warehouse (manufacturer_warehouse_id),
    request_date DATE NOT NULL,
    received_date DATE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    CHECK (received_date >= request_date)
);

CREATE INDEX customer_return_customer_warehouse_id_index ON erp.customer_return (customer_warehouse_id);
CREATE INDEX customer_return_manufacturer_warehouse_id_index ON erp.customer_return (manufacturer_warehouse_id);

CREATE TRIGGER customer_return_set_updated_at
    BEFORE UPDATE ON erp.customer_return
    FOR EACH ROW EXECUTE FUNCTION erp.set_updated_at();


CREATE TABLE erp.customer_return_item (
    customer_return_item_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    customer_return_id BIGINT NOT NULL REFERENCES erp.customer_return (customer_return_id),
    shipment_item_id BIGINT NOT NULL REFERENCES erp.shipment_item (shipment_item_id),
    returned_quantity INTEGER NOT NULL CHECK (returned_quantity > 0),
    reason TEXT NOT NULL CHECK (reason IN ('expired', 'short_expiry', 'damaged')),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (customer_return_id, shipment_item_id)
);

CREATE INDEX customer_return_item_shipment_item_id_index ON erp.customer_return_item (shipment_item_id);
