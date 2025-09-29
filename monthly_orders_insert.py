# dags/monthly_orders_insert.py

from __future__ import annotations

from datetime import datetime
from dateutil.relativedelta import relativedelta

from airflow import DAG
from airflow.decorators import task
from airflow.providers.snowflake.operators.snowflake import SQLExecuteQueryOperator
from airflow.operators.empty import EmptyOperator


with DAG(
    dag_id="monthly_orders_insert",
    description="Monthly insert: process stable month (exec month - 2) into Transformed.public.ORDERS",
    schedule="0 6 1 * *",
    start_date=datetime(2024, 3, 1),
    catchup=True,
    max_active_runs=1,
    render_template_as_native_obj=True,
    tags=["monthly", "orders", "stable-month", "snowflake"],
) as dag:

    RUN_INSERT_ORDERS = SQLExecuteQueryOperator(
        task_id="RUN_INSERT_ORDERS",
        conn_id="snowflake_default",  # ✅ VALID for airflow.providers.snowflake
        sql="""
        INSERT INTO Transformed.public.ORDERS
WITH latest_files AS (
    SELECT 
        TO_CHAR(TRY_TO_DATE(OrderDate, 'DD-MM-YYYY'), 'YYYYMM') AS business_month,
        MAX(tbl_dt) AS max_tbl_dt
    FROM STG.raw_data.ORDERS 
     WHERE DATE_TRUNC('MONTH', TO_DATE(tbl_dt::STRING, 'YYYYMMDD'))
      = DATE_TRUNC('MONTH', ADD_MONTHS(CURRENT_DATE, -1))
          group by 1 
)
SELECT
    o.RowID,
    o.OrderID,

    -- clean dates
    TRY_TO_DATE(o.OrderDate, 'DD-MM-YYYY') AS OrderDate,
    TRY_TO_DATE(o.ShipDate, 'DD-MM-YYYY')  AS ShipDate,

    o.ShipMode,
    o.CustomerID,
    o.Segment,
    o.Country,
    o.ProductID,
    o.Sales    AS Sales,
    o.Quantity AS Quantity,
    o.Discount AS Discount,
    o.Profit   AS Profit,
    o.postalcode as postalcode,

    -- clean strings
    REGEXP_REPLACE(o.CustomerName, '[^a-zA-Z0-9\\s\\.,-]', '') AS CustomerName,
    REGEXP_REPLACE(o.City, '[^a-zA-Z0-9\\s\\.,-]', '')         AS City,
    REGEXP_REPLACE(o.State, '[^a-zA-Z0-9\\s\\.,-]', '')        AS State,
    REGEXP_REPLACE(o.Region, '[^a-zA-Z0-9\\s\\.,-]', '')       AS Region,
    REGEXP_REPLACE(o.Category, '[^a-zA-Z0-9\\s\\.,-]', '')     AS Category,
    COALESCE(m.SUBCATEGORY, 'Unmapped')                        AS SubCategory,  -- << from mapping
    REGEXP_REPLACE(o.ProductName, '[^a-zA-Z0-9\\s\\.,-]', '')  AS ProductName,

    -- metadata
    TRY_TO_DATE(OrderDate, 'DD-MM-YYYY') AS actual_date,
    o.TBL_DT,
    o.ingested_at,
    o.FILE_NAME,
    o.offset_id

FROM STG.raw_data.ORDERS o
JOIN latest_files lf 
  ON TO_CHAR(TRY_TO_DATE(o.OrderDate, 'DD-MM-YYYY'), 'YYYYMM') = lf.business_month
 AND o.tbl_dt = lf.max_tbl_dt

-- bring in subcategory from mapping
LEFT JOIN transformed.public.PRODUCT_SUBCATEGORY_MAP m
       ON REGEXP_REPLACE(o.ProductName, '[^a-zA-Z0-9\\s\\.,-]', '') = m.PRODUCTNAME

WHERE o.Profit > 0
  AND TRY_TO_DATE(o.OrderDate, 'DD-MM-YYYY') IS NOT NULL;
        """
    )

    RUN_INSERT_CUSTOMERS = SQLExecuteQueryOperator(
        task_id="RUN_INSERT_CUSTOMERS",
        conn_id="snowflake_default",  # ✅ VALID for airflow.providers.snowflake
        sql="""
        MERGE INTO transformed.public.customers c
USING (
    SELECT DISTINCT customerid, customername, segment
    FROM transformed.public.orders
) s
ON c.customerid = s.customerid
WHEN NOT MATCHED THEN
  INSERT (customerid, customername, segment)
  VALUES (s.customerid, s.customername, s.segment);
        """
    )
    RUN_INSERT_PRODUCTS = SQLExecuteQueryOperator(
        task_id="RUN_INSERT_PRODUCTS",
        conn_id="snowflake_default",  # ✅ VALID for airflow.providers.snowflake
        sql="""
        MERGE INTO transformed.public.products p
USING (
    SELECT DISTINCT productid, category, subcategory, productname
    FROM transformed.public.orders
) s
ON p.productid = s.productid
WHEN NOT MATCHED THEN
  INSERT (productid, category, subcategory, productname)
  VALUES (s.productid, s.category, s.subcategory, s.productname);
        """
    )
    RUN_INSERT_GEOGRAPHY = SQLExecuteQueryOperator(
        task_id="RUN_INSERT_GEOGRAPHY",
        conn_id="snowflake_default",  # ✅ VALID for airflow.providers.snowflake
        sql="""
        MERGE INTO transformed.public.geography g
USING (
    SELECT DISTINCT country, city, state, postalcode, region
    FROM transformed.public.orders
) s
ON g.country    = s.country
   AND g.city   = s.city
   AND g.state  = s.state
   AND g.postalcode = s.postalcode
   AND g.region = s.region
WHEN NOT MATCHED THEN
  INSERT (country, city, state, postalcode, region)
  VALUES (s.country, s.city, s.state, s.postalcode, s.region);
        """
    )
    RUN_INSERT_FACT_ORDER = SQLExecuteQueryOperator(
        task_id="RUN_INSERT_FACT_ORDER",
        conn_id="snowflake_default",  # ✅ VALID for airflow.providers.snowflake
        sql="""
        MERGE INTO transformed.public.fact_orders f
USING (
    SELECT
        o.orderid,
        o.orderdate,
        o.shipdate,
        o.shipmode,
        o.sales,
        o.quantity,
        o.discount,
        o.profit,
        c.customerid,
        p.productid,
        g.geography_id AS geoid
    FROM transformed.public.orders o
    JOIN  transformed.public.customers c
      ON o.customerid = c.customerid
    JOIN  transformed.public.products p
      ON o.productid = p.productid
    JOIN  transformed.public.geography g
      ON o.country    = g.country
     AND o.city       = g.city
     AND o.state      = g.state
     AND o.postalcode = g.postalcode
     AND o.region     = g.region
) s
ON f.orderid = s.orderid
WHEN NOT MATCHED THEN
  INSERT (
    orderid, orderdate, shipdate, shipmode, sales,
    quantity, discount, profit, customerid, productid, geoid
  )
  VALUES (
    s.orderid, s.orderdate, s.shipdate, s.shipmode, s.sales,
    s.quantity, s.discount, s.profit, s.customerid, s.productid, s.geoid
  );
        """
    )


    # DAG dependencies
    start_task = EmptyOperator(task_id='start', dag=dag)
    end_task = EmptyOperator(task_id='end', dag=dag)


    start_task >> RUN_INSERT_ORDERS >> RUN_INSERT_CUSTOMERS >> RUN_INSERT_PRODUCTS >> RUN_INSERT_GEOGRAPHY >> RUN_INSERT_FACT_ORDER >>  end_task
