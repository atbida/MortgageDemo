from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from mortgage_analytics.config.ConfigStore import *
from mortgage_analytics.udfs.UDFs import *

def csv_mortgage_west(spark: SparkSession) -> DataFrame:
    return spark.read\
        .schema(
          StructType([
            StructField("Account", StringType(), True), StructField("City", StringType(), True), StructField("State", StringType(), True), StructField("Property Type", StringType(), True), StructField("Servicer", StringType(), True), StructField("Servicer Type", StringType(), True), StructField("Loan Type", StringType(), True), StructField("UnPaid Balance", DoubleType(), True), StructField("0", IntegerType(), True), StructField("30", IntegerType(), True), StructField("60", IntegerType(), True), StructField("90", IntegerType(), True), StructField("REO", IntegerType(), True), StructField("Current Prop Value", DoubleType(), True), StructField("LTV", StringType(), True), StructField("Avg OEP", StringType(), True), StructField("Unemployment", StringType(), True), StructField("Avg Values", StringType(), True), StructField("Inventory", StringType(), True), StructField("Days on Market", StringType(), True)
        ])
        )\
        .option("header", True)\
        .option("sep", ",")\
        .csv("dbfs:/FileStore/bobwelshmer/mortgage/MortgageWest.csv")
