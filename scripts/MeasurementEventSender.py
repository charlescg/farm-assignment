# Databricks notebook source
# MAGIC %md
# MAGIC # Zerobus Ingest REST
# MAGIC
# MAGIC Zerobus Ingest is the foundational layer of our data platform, designed to handle high-frequency data streams with ultra-low latency. It serves as the bridge between external data producers and your Lakehouse.
# MAGIC
# MAGIC ### Documentation
# MAGIC - [AWS Docs](https://docs.databricks.com/aws/en/ingestion/zerobus-overview)
# MAGIC - [Azure Docs](https://learn.microsoft.com/en-us/azure/databricks/ingestion/zerobus-overview)
# MAGIC - [Databricks Workspace URL](https://docs.databricks.com/aws/en/ingestion/zerobus-ingest#get-your-workspace-url)
# MAGIC - [Zerobus Ingest Regions](https://docs.databricks.com/aws/en/ingestion/zerobus-limits#workspace)

# COMMAND ----------

dbutils.widgets.text("measure_date", "2023-01-01")

# COMMAND ----------

measure_date = dbutils.widgets.get("measure_date")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1: Configuration

# COMMAND ----------

# Databricks Workspace Information
DATABRICKS_WORKSPACE_ID = "7405605694903875"
DATABRICKS_WORKSPACE_URL = "https://adb-7405605694903875.15.azuredatabricks.net"
DATABRICKS_REGION = "westeurope"

# Zerobus Ingest URL
ZEROBUS_INGEST_URL = f"https://{DATABRICKS_WORKSPACE_ID}.zerobus.{DATABRICKS_REGION}.azuredatabricks.net"

# Service Princple Authentication
CLIENT_ID = "c02b5336-b6ba-4277-be4b-1e708f532286"
CLIENT_SECRET = "dose9d01664aa2b86da5c1d617459f0b85a9"

# Table Information
CATALOG =  "dev_farm"
SCHEMA = "brz_sensor"
TABLE = "cow_measurements"

# COMMAND ----------

# Grant your service principal required permissions to the table.
spark.sql(f"GRANT USE CATALOG ON CATALOG {CATALOG} TO " + "`" + CLIENT_ID + "`;").collect()
spark.sql(f"GRANT USE SCHEMA ON SCHEMA {CATALOG}.{SCHEMA} TO " + "`" + CLIENT_ID + "`;").collect()
spark.sql(f"GRANT MODIFY, SELECT ON TABLE {CATALOG}.{SCHEMA}.{TABLE} TO " + "`" + CLIENT_ID + "`;").collect()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3: Fetch an OAuth token

# COMMAND ----------

import json
import requests
from requests.auth import HTTPBasicAuth

authorization_details=json.dumps([
{
  "type": "unity_catalog_privileges",
  "privileges": ["USE CATALOG"],
  "object_type": "CATALOG",
  "object_full_path": CATALOG
},
{
  "type": "unity_catalog_privileges",
  "privileges": ["USE SCHEMA"],
  "object_type": "SCHEMA",
  "object_full_path": f"{CATALOG}.{SCHEMA}"
},
{
  "type": "unity_catalog_privileges",
  "privileges": ["SELECT", "MODIFY"],
  "object_type": "TABLE",
  "object_full_path": f"{CATALOG}.{SCHEMA}.{TABLE}"
}])

access_token = requests.post(
  f"{DATABRICKS_WORKSPACE_URL}/oidc/v1/token",
  auth=HTTPBasicAuth(CLIENT_ID, CLIENT_SECRET),
  data={
    "grant_type": "client_credentials",
    "scope": "all-apis",
    "resource": f"api://databricks/workspaces/{DATABRICKS_WORKSPACE_ID}/zerobusDirectWriteApi",
    "authorization_details": authorization_details
  }
).json()["access_token"]

# COMMAND ----------

# MAGIC %md
# MAGIC ### Dataset record ingestion

# COMMAND ----------

from pyspark.sql.functions import col, when, lit, to_timestamp, from_unixtime, to_date
import pandas as pd
from typing import Iterator, Dict, Any, Optional


def iterate_sensor_records_pandas(measure_date: str) -> Iterator[Dict[str, Any]]:
  measurements_df = (
    spark.read
          .parquet("/Volumes/dev_farm/brz_sensor/resources/measurements.parquet")
          .withColumn("measure_date",to_date(to_timestamp(from_unixtime(col("timestamp"))), "yyyy-MM-dd"))
          .where(f"measure_date = '{measure_date}'")

  )

  sensors_df = spark.read.parquet("/Volumes/dev_farm/brz_sensor/resources/sensors.parquet")

  cow_metrics_df =(
                    measurements_df
                      .join(sensors_df, measurements_df.sensor_id == sensors_df.id)
                      .withColumn("sensor_type", 
                                  when(col("unit") == lit("kg"), lit("weight"))
                                      .otherwise(when(col("unit") == lit("L"), lit("milk_production"))
                                                  .otherwise(lit("unknown"))
                                      )
                                  )
                      .select(
                          "sensor_id",
                          "cow_id",
                          "timestamp",
                          "sensor_type",
                          "value",
                          "unit"

                      )
                )

  for index, row in cow_metrics_df.toPandas().iterrows():  # para velocidad, ver alternativa itertuples()
      yield {
              "index": index,
              "time_serie_event": row["timestamp"],
              "payload": {
                "sensor_id": row["sensor_id"],
                "cow_id": row["cow_id"],     
                "sensor_type": row["sensor_type"],
                "value": row["value"],
                "unit": row["unit"]
              }
            }



# COMMAND ----------

# MAGIC %md
# MAGIC ### Single record REST ingestion

# COMMAND ----------

def send_message_to_target(message: dict, target_table: str):

    data = json.dumps(
                        { 
                        "id": message["index"],
                        "time_serie_event": message["time_serie_event"],
                        "payload": json.dumps(message["payload"])
                        }
                    )

    print(f"sending message: {data} to {target_table}")
  
    requests.post(
        f"{ZEROBUS_INGEST_URL}/ingest-record?table_name={target_table}",
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {access_token}",
            "unity-catalog-endpoint": DATABRICKS_WORKSPACE_URL,
            "x-databricks-zerobus-table-name": target_table
        },
        data=data
    )
  

# COMMAND ----------

for record in iterate_sensor_records_pandas(measure_date):
    send_message_to_target(record, f"{CATALOG}.{SCHEMA}.{TABLE}")
