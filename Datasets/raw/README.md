# Raw Batch Inputs

The batch notebook expects products, customers, and orders CSVs in this directory layout when data is staged locally:

- `products/products.csv`
- `customers/customers.csv`
- `orders/orders.csv`
- `incremental/orders_updates/orders_updates.csv`
- `schema_evolution/orders_v2/orders_v2.csv`

The original CSV files are not included in this repository. Do not populate these folders with invented rows; use the authorized trainer-provided files or the configured Unity Catalog volume.
