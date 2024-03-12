from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from mortgage_analytics.config.ConfigStore import *
from mortgage_analytics.udfs.UDFs import *

def customers_nv(spark: SparkSession) -> DataFrame:
    return spark.read\
        .schema(
          StructType([
            StructField("Cust ID", StringType(), True), StructField("First_Name", StringType(), True), StructField("Last_Name", StringType(), True), StructField("Gender", StringType(), True), StructField("Age", StringType(), True), StructField("Income", StringType(), True), StructField("Phone Number", StringType(), True), StructField("Email Address", StringType(), True), StructField("Zip Code", StringType(), True), StructField("Credit Rating", StringType(), True)
        ])
        )\
        .option("header", True)\
        .option("sep", ",")\
        .csv("dbfs:/FileStore/bobwelshmer/mortgage/MortgageCustomerNV.csv")
