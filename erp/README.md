## **ERP system**

<p>Our ERP is a PostgreSQL database in 3NF. It is a simplified sales module of the manufacturer's ERP and covers only what sell-in needs.</p>

<p align="center">
  <img src="../docs/images/ERP_entity_relationship.svg" alt="ERP_entity_relationship">
</p>

| Table | Description |
|---|---|
| `product` | Drugs we produce, each in one strength and pack size. Each product has its own Global Trade Item Number (GTIN) - a unique, universal product identifier. |
| `brand` | Trade names under which we sell our drugs. |
| `dosage_form` | Physical forms in which products are made, with the units their pack sizes are measured in. |
| `batch` | Batches in which products are produced. |
| `price_list` | Prices we charge partners for products, starting from given dates. When a price changes, a new row is added and the old one is kept. |
| `customer` | Partner companies that buy our products directly from us. |
| `customer_warehouse` | Partners' warehouses that receive our shipments. |
| `manufacturer_warehouse` | Our own warehouses where products are stored. |
| `address` | All addresses in the database. |
| `region` | Regions of the country with their ISO 3166-2 codes and the sales territories they belong to. Sales territories are our own groups of regions that we use in sales reports. |
| `customer_order` | Customers' requests to buy our products, delivered to their warehouses. `status` is one of `new`, `confirmed`, `partially_shipped`, `shipped` and `cancelled`. |
| `order_item` | Products in customer orders, with how many packs customers asked for, how many we confirmed, and the prices that apply. `status` is one of `pending`, `confirmed`, `shipped` and `cancelled`. |
| `shipment` | Shipments of products from our warehouses for orders. An order may need several shipments. |
| `shipment_item` | Products of one batch sent in a shipment for one order item. An order item filled from two batches has two shipment items. |
| `customer_return` | Customers' requests to send products back from their warehouses to ours. |
| `customer_return_item` | Packs returned from shipment items. `reason` is one of `expired`, `short_expiry` and `damaged`. |

### **How the database is created**

<p>The <code>erp-db</code> service mounts <code>erp/ddl</code> as <code>/docker-entrypoint-initdb.d</code>. On the first start, Postgres runs the scripts in file name order: <code>01_schema.sql</code> creates the <code>erp</code> schema and the <code>updated_at</code> trigger function, <code>02_master_tables.sql</code> creates the master data tables and <code>03_transaction_tables.sql</code> creates the order, shipment and return tables.</p>

<p>The scripts run only when the <code>erp-db-data</code> volume is empty. On later starts, Postgres skips them, so changes to the DDL do not reach an existing database. To apply them, remove the container and its volume, then start the service again. This deletes all data in the ERP database:</p>

```bash
docker compose rm -sf erp-db
docker volume rm zip-data-pipeline_erp-db-data
docker compose up -d erp-db
```

<p>Besides primary, unique and foreign keys, the DDL enforces only rules that can be checked within one row, such as positive quantities and allowed status values.</p>

Read more about business_case: [business_case.md](../docs/business_case.md)