# Gold Tables and Business Queries

The batch notebook publishes two Gold Delta tables in the configured output schema:

| Gold table | Grain | Business question |
|---|---|---|
| `gold_category_sales` | Category and sales channel | Which category/channel combinations contribute the most net sales and units? |
| `gold_customer_spend` | Customer | Which customers have the highest valid-order count and net spend? |

Cancelled orders are excluded from these aggregates. Net revenue subtracts `discount_amount`, treating a missing legacy discount as zero. The notebook uses the `nexus.retail_nexus` schema by default; change the catalog/schema in [business-queries.sql](business-queries.sql) if the Databricks widgets target a different schema.

The repository does not include a separate dashboard application. Run the SQL queries in Databricks SQL or a compatible Spark SQL session and build charts there if a visual dashboard is required. Refresh the tables and inspect current output before presenting results.
